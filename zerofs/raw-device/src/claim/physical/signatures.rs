//! Probe an already-held descriptor with util-linux, without consulting its cache.
use super::{ErrorKind, File, io, require};
use std::ffi::{CStr, c_char, c_int, c_void};
use std::os::fd::AsRawFd;
use std::ptr::NonNull;

type ProbePtr = *mut c_void;
type ProbeOption = unsafe extern "C" fn(ProbePtr, c_int) -> c_int;
type KnownType = unsafe extern "C" fn(*const c_char) -> c_int;

const BLKID_SUBLKS_TYPE: c_int = 1 << 5;
const BLKID_SUBLKS_BADCSUM: c_int = 1 << 10;
const BLKID_PARTS_FORCE_GPT: c_int = 1 << 1;

pub(super) fn require_no_foreign_signatures(file: &File, bytes: u64) -> io::Result<()> {
    Blkid::load()?.probe(file, bytes)
}

struct Library(NonNull<c_void>);

impl Library {
    fn open() -> io::Result<Self> {
        NonNull::new(unsafe {
            libc::dlopen(c"libblkid.so.1".as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL)
        })
        .map(Self)
        .ok_or_else(|| {
            io::Error::new(
                ErrorKind::Unsupported,
                "physical claim requires libblkid.so.1",
            )
        })
    }

    fn symbol(&self, name: &CStr) -> io::Result<NonNull<c_void>> {
        NonNull::new(unsafe { libc::dlsym(self.0.as_ptr(), name.as_ptr()) }).ok_or_else(|| {
            io::Error::new(
                ErrorKind::Unsupported,
                format!(
                    "physical claim requires libblkid symbol {}",
                    name.to_string_lossy()
                ),
            )
        })
    }
}

impl Drop for Library {
    fn drop(&mut self) {
        unsafe { libc::dlclose(self.0.as_ptr()) };
    }
}

struct Blkid {
    _library: Library,
    new_probe: unsafe extern "C" fn() -> ProbePtr,
    free_probe: unsafe extern "C" fn(ProbePtr),
    set_device: unsafe extern "C" fn(ProbePtr, c_int, i64, i64) -> c_int,
    enable_superblocks: ProbeOption,
    superblock_flags: ProbeOption,
    enable_partitions: ProbeOption,
    partition_flags: ProbeOption,
    safeprobe: unsafe extern "C" fn(ProbePtr) -> c_int,
}

impl Blkid {
    fn load() -> io::Result<Self> {
        let library = Library::open()?;
        // The exported libblkid API fixes these C ABI signatures.
        macro_rules! load {
            ($name:literal, $function:ty) => {{
                let address = library.symbol($name)?;
                unsafe { std::mem::transmute::<*mut c_void, $function>(address.as_ptr()) }
            }};
        }
        let known_fstype = load!(c"blkid_known_fstype", KnownType);
        let known_pttype = load!(c"blkid_known_pttype", KnownType);
        if ![c"apfs", c"zfs_member", c"swap", c"linux_raid_member"]
            .into_iter()
            .all(|name| unsafe { known_fstype(name.as_ptr()) == 1 })
            || ![c"gpt", c"dos"]
                .into_iter()
                .all(|name| unsafe { known_pttype(name.as_ptr()) == 1 })
        {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                "libblkid lacks required storage signature recognizers",
            ));
        }
        Ok(Self {
            new_probe: load!(c"blkid_new_probe", unsafe extern "C" fn() -> ProbePtr),
            free_probe: load!(c"blkid_free_probe", unsafe extern "C" fn(ProbePtr)),
            set_device: load!(
                c"blkid_probe_set_device",
                unsafe extern "C" fn(ProbePtr, c_int, i64, i64) -> c_int
            ),
            enable_superblocks: load!(c"blkid_probe_enable_superblocks", ProbeOption),
            superblock_flags: load!(c"blkid_probe_set_superblocks_flags", ProbeOption),
            enable_partitions: load!(c"blkid_probe_enable_partitions", ProbeOption),
            partition_flags: load!(c"blkid_probe_set_partitions_flags", ProbeOption),
            safeprobe: load!(
                c"blkid_do_safeprobe",
                unsafe extern "C" fn(ProbePtr) -> c_int
            ),
            _library: library,
        })
    }

    fn probe(&self, file: &File, bytes: u64) -> io::Result<()> {
        let bytes = i64::try_from(bytes).map_err(io::Error::other)?;
        let probe = Probe {
            blkid: self,
            ptr: NonNull::new(unsafe { (self.new_probe)() })
                .ok_or_else(|| io::Error::other("cannot allocate libblkid probe"))?,
        };
        let ptr = probe.ptr.as_ptr();
        require(
            // libblkid borrows this descriptor for the probe's lifetime.
            unsafe { (self.set_device)(ptr, file.as_raw_fd(), 0, bytes) } == 0
                && unsafe { (self.enable_superblocks)(ptr, 1) } == 0
                && unsafe {
                    (self.superblock_flags)(ptr, BLKID_SUBLKS_TYPE | BLKID_SUBLKS_BADCSUM)
                } == 0
                && unsafe { (self.enable_partitions)(ptr, 1) } == 0
                && unsafe { (self.partition_flags)(ptr, BLKID_PARTS_FORCE_GPT) } == 0,
            "cannot configure held-device libblkid probe",
        )?;
        match unsafe { (self.safeprobe)(ptr) } {
            1 => Ok(()),
            0 => Err(io::Error::new(
                ErrorKind::InvalidData,
                "physical disk has a recognized foreign storage signature",
            )),
            -2 => Err(io::Error::new(
                ErrorKind::InvalidData,
                "physical disk has ambiguous storage signatures",
            )),
            status => Err(io::Error::other(format!(
                "held-device libblkid probe failed ({status})"
            ))),
        }
    }
}

struct Probe<'a> {
    blkid: &'a Blkid,
    ptr: NonNull<c_void>,
}

impl Drop for Probe<'_> {
    fn drop(&mut self) {
        unsafe { (self.blkid.free_probe)(self.ptr.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{BLOCK, LOG_START, Label, SlotState, sha256};
    use crate::{Identity, Role};
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::FileExt;

    #[test]
    #[ignore = "requires runtime libblkid with APFS, ZFS, swap, RAID and partition recognizers"]
    fn detects_old_partition_despite_valid_zerofs_label() {
        let bytes = 16 * 1024 * 1024;
        let fd = unsafe {
            libc::memfd_create(
                c"zerofs-physical-claim-signatures".as_ptr(),
                libc::MFD_CLOEXEC,
            )
        };
        assert!(fd >= 0, "{}", io::Error::last_os_error());
        let image = unsafe { File::from_raw_fd(fd) };
        image.set_len(bytes).unwrap();
        require_no_foreign_signatures(&image, bytes).unwrap();
        let label = Label {
            generation: 1,
            device_size: bytes,
            identity: Identity {
                pool_uuid: [1; 16],
                member_uuid: [2; 16],
                role: Role::Data,
            },
            nonce: [3; 16],
            durable_tail: LOG_START,
            last_seq: 0,
            bank: 0,
            bank_generation: 1,
        };
        let mut first = label.encode(0);
        image.write_all_at(&first[..], 0).unwrap();
        image
            .write_all_at(&label.encode(1)[..], BLOCK as u64)
            .unwrap();
        require_no_foreign_signatures(&image, bytes).unwrap();
        let mut table = [0; 66];
        table[4] = 0x83;
        table[8..12].copy_from_slice(&2048u32.to_le_bytes());
        table[12..16].copy_from_slice(&8192u32.to_le_bytes());
        table[64..].copy_from_slice(&[0x55, 0xaa]);
        first[446..512].copy_from_slice(&table);
        let checksum = sha256(&first[..BLOCK - 32]);
        first[BLOCK - 32..].copy_from_slice(&checksum);
        assert!(matches!(Label::decode(&first, 0), SlotState::Valid(_)));
        image.write_all_at(&first[..], 0).unwrap();
        let error = require_no_foreign_signatures(&image, bytes).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("recognized foreign storage signature"),
            "{error}"
        );
        assert_eq!(image.metadata().unwrap().len(), bytes);
    }
}
