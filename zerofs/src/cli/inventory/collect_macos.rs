use super::record::{
    Field, Filesystem, RawAcl, RawBytes, Reason, Timestamp, TrustedXattrNamespace,
};
use super::walk::MountIdentity;
use rustix::fd::{AsRawFd, BorrowedFd};
use rustix::fs::{Stat, StatFs};
use rustix::io::Errno;
use std::ffi::{CStr, OsStr, c_char, c_int, c_void};

const ACL_TYPE_EXTENDED: c_int = 0x100;

pub(super) fn trusted_xattr_namespace() -> TrustedXattrNamespace {
    TrustedXattrNamespace {
        visible: None,
        probe: "macOS has no trusted.* xattr namespace.",
        user_namespace_policy: "not_applicable",
    }
}

unsafe extern "C" {
    fn acl_get_fd_np(fd: c_int, acl_type: c_int) -> *mut c_void;
    fn acl_to_text(acl: *mut c_void, length: *mut libc::ssize_t) -> *mut c_char;
    fn acl_free(object: *mut c_void) -> c_int;
    fn acl_get_entry(acl: *mut c_void, entry_id: c_int, entry: *mut *mut c_void) -> c_int;
}

struct AclAllocation(*mut c_void);
impl Drop for AclAllocation {
    fn drop(&mut self) {
        // SAFETY: Each non-null allocation from the ACL API is owned exactly once.
        unsafe {
            acl_free(self.0);
        }
    }
}

pub(super) fn acl(fd: BorrowedFd<'_>) -> Field<Vec<RawAcl>> {
    // SAFETY: fd is live; ACL_TYPE_EXTENDED is the macOS ABI constant.
    let ptr = unsafe { acl_get_fd_np(fd.as_raw_fd(), ACL_TYPE_EXTENDED) };
    let raw = if ptr.is_null() {
        match last_errno() {
            // The descriptor is valid. ENOENT reports an absent extended ACL.
            Errno::NOENT => Field::Observed(None),
            error => Field::error(error),
        }
    } else {
        let acl = AclAllocation(ptr);
        let mut entry = std::ptr::null_mut();
        // SAFETY: acl is live and entry points to writable storage. ACL_FIRST_ENTRY is zero.
        let result = unsafe { acl_get_entry(acl.0, 0, &mut entry) };
        if result == -1 && last_errno() == Errno::INVAL {
            // macOS reports EINVAL when there is no first entry.
            Field::Observed(None)
        } else if result == -1 {
            Field::error(last_errno())
        } else {
            let mut length = 0;
            // SAFETY: acl is live and length points to writable storage.
            let text = unsafe { acl_to_text(acl.0, &mut length) };
            if text.is_null() {
                Field::error(last_errno())
            } else {
                let text = AclAllocation(text.cast());
                // SAFETY: acl_to_text returned length bytes in an owned allocation.
                let bytes =
                    unsafe { std::slice::from_raw_parts(text.0.cast::<u8>(), length as usize) };
                Field::Observed(Some(RawBytes::new(bytes)))
            }
        }
    };
    Field::Observed(vec![RawAcl {
        format: "macos-extended-text",
        raw: raw.format_probe(),
        decoded: Field::unavailable(Reason::NotDecoded),
        posix_translation: None,
    }])
}

pub(super) fn birth(_parent: BorrowedFd<'_>, _name: &OsStr, stat: &Stat) -> Field<Timestamp> {
    Field::Observed(Timestamp {
        seconds: stat.st_birthtime,
        nanoseconds: stat.st_birthtime_nsec as u32,
    })
}

pub(super) fn flags(stat: &Stat) -> Field<u32> {
    Field::Observed(stat.st_flags)
}

pub(super) fn filesystem(stat: &StatFs) -> Filesystem {
    // SAFETY: f_fstypename is a NUL-terminated kernel-provided fixed buffer.
    let name = unsafe { CStr::from_ptr(stat.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let network = matches!(
        name.as_str(),
        "smbfs" | "nfs" | "webdav" | "afpfs" | "cifs" | "sshfs"
    );
    Filesystem {
        type_name: Some(name),
        type_magic: stat.f_type as u64,
        network,
    }
}

pub(super) fn mount_identity(fd: BorrowedFd<'_>) -> Field<MountIdentity> {
    match rustix::fs::fstatfs(fd) {
        Ok(stat) => Field::Observed(MountIdentity {
            // SAFETY: fsid_t is the C struct { int32_t val[2]; } with a private field in libc.
            fsid: unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(stat.f_fsid) },
            // SAFETY: f_mntonname is a NUL-terminated kernel-provided fixed buffer.
            mount_point: unsafe { CStr::from_ptr(stat.f_mntonname.as_ptr()) }
                .to_bytes()
                .to_vec(),
        }),
        Err(error) => Field::Unavailable {
            reason: Reason::MountIdUnavailable,
            errno: Some(error.raw_os_error()),
        },
    }
}

fn last_errno() -> Errno {
    Errno::from_raw_os_error(
        std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO),
    )
}
