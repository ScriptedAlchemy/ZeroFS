//! A host-only listener transfers an exclusive, already labeled device to an
//! isolated engine. The requesting process never resolves a host device path.
use super::{Claim, PhysicalRole, checked, require};
use crate::{Identity, RawStore, Role, p3::P3Layout, region::Region};
use rustix::net::{
    self, AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags,
    SendAncillaryBuffer, SendAncillaryMessage, SendFlags, SocketAddrUnix, SocketFlags, SocketType,
};
use std::fs::{self, File};
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

const MAGIC: &[u8; 4] = b"ZCL1";
const REQUEST_LEN: usize = 37;
const GRANT_LEN: usize = REQUEST_LEN + 32;
const REFUSED: &[u8; 4] = b"ERR1";
const ABSENT: &[u8; 4] = b"ABS1";

#[derive(Clone, Copy)]
struct Request {
    role: PhysicalRole,
    pool: [u8; 16],
    member: [u8; 16],
}

impl Request {
    fn encode(self) -> [u8; REQUEST_LEN] {
        let mut data = [0; REQUEST_LEN];
        data[..4].copy_from_slice(MAGIC);
        data[4] = match self.role {
            PhysicalRole::Nvme => 1,
            PhysicalRole::Hdd6 => 2,
            PhysicalRole::Hdd18 => 3,
        };
        data[5..21].copy_from_slice(&self.pool);
        data[21..].copy_from_slice(&self.member);
        data
    }

    fn parse(data: &[u8]) -> io::Result<Self> {
        require(
            data.len() == REQUEST_LEN && data.starts_with(MAGIC),
            "invalid claim request",
        )?;
        let role = match data[4] {
            1 => PhysicalRole::Nvme,
            2 => PhysicalRole::Hdd6,
            3 => PhysicalRole::Hdd18,
            _ => return Err(io::Error::other("invalid claim designation")),
        };
        let pool = data[5..21].try_into().unwrap();
        let member = data[21..].try_into().unwrap();
        require(
            pool != [0; 16] && member != [0; 16] && pool != member,
            "invalid claim UUIDs",
        )?;
        Ok(Self { role, pool, member })
    }

    fn verify_labels(self, claim: &Claim) -> io::Result<()> {
        if self.role == PhysicalRole::Nvme {
            P3Layout::nas_fit(claim.size)?.verify_labels(&claim.device, self.pool, self.member)?;
        } else {
            RawStore::binding(
                &Region::whole(claim.device.try_clone()?, claim.size)?,
                Identity {
                    pool_uuid: self.pool,
                    member_uuid: self.member,
                    role: Role::Data,
                },
            )?;
        }
        Ok(())
    }
}

fn peer(fd: &OwnedFd) -> io::Result<libc::ucred> {
    // A peer in the parent PID namespace has pid 0 from the child. Read the
    // kernel's signed pid_t, including that value, without assuming NonZero.
    let mut cred = MaybeUninit::<libc::ucred>::uninit();
    let mut len = size_of::<libc::ucred>() as libc::socklen_t;
    checked(unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            cred.as_mut_ptr().cast(),
            &mut len,
        )
    })?;
    require(
        len as usize == size_of::<libc::ucred>(),
        "missing peer credentials",
    )?;
    Ok(unsafe { cred.assume_init() })
}

fn configure_timeout(fd: &OwnedFd, seconds: u64) -> io::Result<()> {
    for direction in [net::sockopt::Timeout::Recv, net::sockopt::Timeout::Send] {
        net::sockopt::set_socket_timeout(fd, direction, Some(Duration::from_secs(seconds)))?;
    }
    Ok(())
}

fn socket() -> io::Result<OwnedFd> {
    Ok(net::socket_with(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::CLOEXEC,
        None,
    )?)
}

fn receive(socket: &OwnedFd, data: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
    let mut control = RecvAncillaryBuffer::new(&mut space);
    let msg = net::recvmsg(
        socket,
        &mut [IoSliceMut::new(data)],
        &mut control,
        RecvFlags::CMSG_CLOEXEC,
    )?;
    require(
        msg.bytes > 0
            && !msg
                .flags
                .intersects(ReturnFlags::TRUNC | ReturnFlags::CTRUNC),
        "incomplete claim packet",
    )?;
    let mut fd = None;
    for item in control.drain() {
        let RecvAncillaryMessage::ScmRights(rights) = item else {
            return Err(io::Error::other("unexpected claim ancillary data"));
        };
        for right in rights {
            require(fd.replace(right).is_none(), "too many claim descriptors")?;
        }
    }
    Ok((msg.bytes, fd))
}

fn send(socket: &OwnedFd, data: &[u8], file: Option<&File>) -> io::Result<()> {
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
    let mut control = SendAncillaryBuffer::new(&mut space);
    let rights;
    if let Some(file) = file {
        rights = [file.as_fd()];
        require(
            control.push(SendAncillaryMessage::ScmRights(&rights)),
            "cannot attach claim descriptor",
        )?;
    }
    let n = net::sendmsg(
        socket,
        &[IoSlice::new(data)],
        &mut control,
        SendFlags::NOSIGNAL,
    )?;
    require(n == data.len(), "incomplete claim send")
}

fn authorize(connection: &OwnedFd, uid: u32, gid: u32) -> io::Result<()> {
    let cred = peer(connection)?;
    require(
        cred.uid == uid && cred.gid == gid && cred.pid > 0,
        "claim client credentials rejected",
    )?;
    // Bind the namespace lookup to this connection's original process, even
    // if a PID exits and its number is reused during the lookup.
    let mut raw = -1i32;
    let mut len = size_of::<i32>() as libc::socklen_t;
    checked(unsafe {
        libc::getsockopt(
            connection.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERPIDFD,
            (&mut raw as *mut i32).cast(),
            &mut len,
        )
    })?;
    require(
        raw >= 0 && len as usize == size_of::<i32>(),
        "missing peer pidfd",
    )?;
    let process = unsafe { OwnedFd::from_raw_fd(raw) };
    let dir = File::open(format!("/proc/{}", cred.pid))?;
    let namespaces = std::path::PathBuf::from(format!("/proc/self/fd/{}/ns", dir.as_raw_fd()));
    for (kind, shared) in [("pid", false), ("mnt", false), ("user", true)] {
        let remote = fs::metadata(namespaces.join(kind))?;
        let local = fs::metadata(Path::new("/proc/self/ns").join(kind))?;
        require(
            ((remote.dev(), remote.ino()) == (local.dev(), local.ino())) == shared,
            "claim client must have distinct PID and mount namespaces and share the broker's user namespace",
        )?;
    }
    let mut poll = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let state = unsafe { libc::poll(&mut poll, 1, 0) };
    checked(state)?;
    require(state == 0, "claim peer exited during authentication")
}

fn grant(connection: &OwnedFd, uid: u32, gid: u32) -> io::Result<()> {
    authorize(connection, uid, gid)?;
    configure_timeout(connection, 2)?;
    let mut bytes = [0; REQUEST_LEN];
    let (n, supplied_fd) = receive(connection, &mut bytes)?;
    require(
        supplied_fd.is_none(),
        "claim request must not supply descriptors",
    )?;
    let request = Request::parse(&bytes[..n])?;
    let claim = match Claim::physical(request.role) {
        Err(error) if super::is_device_absent(&error) => {
            let mut response = request.encode();
            response[..ABSENT.len()].copy_from_slice(ABSENT);
            return send(connection, &response, None);
        }
        result => result?,
    };
    request.verify_labels(&claim)?;
    let mut response = bytes.to_vec();
    for value in super::physical::held_geometry(&claim.device, request.role.expected_bytes())?
        .into_iter()
        .chain([fs::metadata("/proc/self/ns/mnt")?.ino()])
    {
        response.extend_from_slice(&value.to_le_bytes());
    }
    send(connection, &response, Some(&claim.device))
}

/// Serve one bounded request at a time in the trusted host namespace. Use a
/// dedicated, root-owned socket directory with no group/other write access.
pub fn serve(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
    require(
        unsafe { libc::geteuid() } == 0
            && ![0, 65534, u32::MAX].contains(&uid)
            && ![0, 65534, u32::MAX].contains(&gid),
        "broker requires root and dedicated nonroot client IDs",
    )?;
    Claim::verify_host_inventory_access()?;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing broker directory"))?;
    require(
        path.is_absolute() && fs::canonicalize(parent)? == parent,
        "broker directory must be absolute and canonical",
    )?;
    let directory = File::open(parent)?;
    for component in parent.ancestors() {
        let meta = fs::symlink_metadata(component)?;
        require(
            meta.is_dir() && meta.uid() == 0 && meta.mode() & 0o022 == 0,
            "untrusted broker directory",
        )?;
    }
    // Serialize listener replacement across restarts. Only reclaim our
    // expected socket inode after the previous process released the lock.
    checked(unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) })?;
    let addr = SocketAddrUnix::new(path)?;
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            require(
                meta.file_type().is_socket()
                    && meta.uid() == 0
                    && meta.gid() == gid
                    && meta.mode() & 0o777 == 0o660,
                "unexpected existing broker endpoint",
            )?;
            match net::connect(socket()?, &addr) {
                Err(rustix::io::Errno::CONNREFUSED) => fs::remove_file(path)?,
                _ => return Err(io::Error::other("existing broker endpoint is busy")),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = socket()?;
    net::bind(&listener, &addr)?;
    std::os::unix::fs::chown(path, Some(0), Some(gid))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
    net::listen(&listener, 8)?;
    loop {
        let connection = match net::accept_with(&listener, SocketFlags::CLOEXEC) {
            Err(rustix::io::Errno::INTR) => continue,
            other => other?,
        };
        if let Err(e) = grant(&connection, uid, gid) {
            let mut reason = REFUSED.to_vec();
            reason.extend(e.to_string().bytes().take(240));
            let _ = send(&connection, &reason, None);
        }
    }
}

impl Claim {
    pub fn from_broker(
        socket_path: &Path,
        role: PhysicalRole,
        pool: [u8; 16],
        member: [u8; 16],
    ) -> io::Result<Self> {
        require(
            socket_path.is_absolute(),
            "claim socket path must be absolute",
        )?;
        // Authenticate uid 0 using host IDs, including in a nested PID namespace.
        for map in ["/proc/self/uid_map", "/proc/self/gid_map"] {
            let mapping = fs::read_to_string(map)?;
            require(
                mapping
                    .split_ascii_whitespace()
                    .eq(["0", "0", "4294967295"]),
                "claim client requires host UID/GID mapping",
            )?;
        }
        let connection = socket()?;
        configure_timeout(&connection, 30)?;
        net::connect(&connection, &SocketAddrUnix::new(socket_path)?)?;
        let cred = peer(&connection)?;
        require(
            unsafe { libc::geteuid() } != 0 && cred.uid == 0 && cred.pid == 0,
            "claim client requires a root broker outside its PID namespace",
        )?;
        let request = Request { role, pool, member };
        let expected = request.encode();
        send(&connection, &expected, None)?;
        let mut response = [0; 256];
        let (n, fd) = receive(&connection, &mut response)?;
        let response = &response[..n];
        if let Some(binding) = response.strip_prefix(ABSENT) {
            require(
                role != PhysicalRole::Nvme && fd.is_none() && binding == &expected[MAGIC.len()..],
                "invalid absent-device response",
            )?;
            return Err(super::device_absent());
        }
        if let Some(reason) = response.strip_prefix(REFUSED) {
            require(fd.is_none(), "descriptor in claim refusal")?;
            return Err(io::Error::other(format!(
                "physical claim refused: {}",
                String::from_utf8_lossy(reason)
            )));
        }
        require(
            response.len() == GRANT_LEN && response.starts_with(&expected),
            "claim response binding mismatch",
        )?;
        let device =
            File::from(fd.ok_or_else(|| io::Error::other("missing exclusive claim descriptor"))?);
        let mut fields = response[REQUEST_LEN..]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|b| u64::from_le_bytes(*b));
        let actual = super::physical::held_geometry(&device, role.expected_bytes())?;
        require(
            actual.into_iter().all(|v| fields.next() == Some(v)),
            "transferred descriptor identity mismatch",
        )?;
        require(
            fields.next() != Some(fs::metadata("/proc/self/ns/mnt")?.ino()),
            "claim client requires a separate mount namespace",
        )?;
        Ok(Self {
            device,
            size: actual[2],
            _backing_lock: None,
        })
    }
}
