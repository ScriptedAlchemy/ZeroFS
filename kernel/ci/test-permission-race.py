#!/usr/bin/env python3
"""Execute the production permission callback with a deterministic RPC/rmdir race.

The kernel/RPC boundary is mocked; this is not a mounted filesystem test.
"""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "vfs/namespace.rs"
START = 'pub(super) unsafe extern "C" fn zerofs_permission('
END = 'pub(super) unsafe extern "C" fn zerofs_setattr('

SHIM = r'''
#![allow(dead_code, non_camel_case_types)]
use std::ffi;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
type Result<T> = std::result::Result<T, i32>;
const ENOENT: i32 = -2;
const ECHILD: i32 = -10;
const RELAXED_CACHE_REVALIDATE_NS: u64 = 1_000_000_000;
static LINKS: AtomicU64 = AtomicU64::new(2);
static REMOVE_DURING_RPC: AtomicI32 = AtomicI32::new(0);
static RPC_ERROR: AtomicI32 = AtomicI32::new(0);
static RPC_CALLS: AtomicI32 = AtomicI32::new(0);
static PERMISSION_ERROR: AtomicI32 = AtomicI32::new(0);
static PERMISSION_CALLS: AtomicI32 = AtomicI32::new(0);
struct InodeState { metadata_fresh_ns: AtomicU64 }
static INODE_STATE: InodeState = InodeState { metadata_fresh_ns: AtomicU64::new(0) };
struct Mount { client: () }
static MOUNT: Mount = Mount { client: () };
impl Mount {
    fn is_relaxed(&self) -> bool { false }
    fn begin_cache_observation(&self) {}
}
struct InodeRef(*mut bindings::inode);
impl InodeRef {
    unsafe fn from_raw(raw: *mut bindings::inode) -> Result<Self> { Ok(Self(raw)) }
    fn state(&self) -> Result<&'static InodeState> { Ok(&INODE_STATE) }
    fn mount(&self) -> Result<&'static Mount> { Ok(&MOUNT) }
    fn link_count(&self) -> u64 { LINKS.load(Ordering::SeqCst) }
    fn file_type(&self) -> u32 { 0o040000 }
    fn as_ptr(&self) -> *mut bindings::inode { self.0 }
}
struct RebindCredentials;
impl RebindCredentials { fn current() -> Result<Self> { Ok(Self) } }
fn monotonic_now_ns() -> u64 { 1 }
fn expected_qid_type(_: u32) -> Result<u8> { Ok(0x80) }
fn getattr_attributes(_: &(), _: &InodeRef, _: &RebindCredentials, _: u8) -> Result<()> {
    RPC_CALLS.fetch_add(1, Ordering::SeqCst);
    // The initial nlink check has passed. Complete the competing rmdir now,
    // then deliver its ENOENT response to this in-flight permission refresh.
    if REMOVE_DURING_RPC.load(Ordering::SeqCst) != 0 {
        LINKS.store(0, Ordering::SeqCst);
    }
    to_result(RPC_ERROR.load(Ordering::SeqCst))
}
fn validate_stat_for_inode(_: &InodeRef, _: &()) -> Result<()> { Ok(()) }
fn cache_inode_attributes_at(_: &InodeRef, _: &(), _: ()) -> bool { true }
fn to_result(value: i32) -> Result<()> { if value < 0 { Err(value) } else { Ok(()) } }
fn from_result(body: impl FnOnce() -> Result<i32>) -> i32 { body().unwrap_or_else(|e| e) }
mod bindings {
    use super::*;
    pub struct mnt_idmap;
    pub struct inode;
    pub const MAY_NOT_BLOCK: u32 = 0x80;
    pub unsafe fn generic_permission(_: *mut mnt_idmap, _: *mut inode, _: i32) -> i32 {
        PERMISSION_CALLS.fetch_add(1, Ordering::SeqCst);
        PERMISSION_ERROR.load(Ordering::SeqCst)
    }
}
mod production {
    use super::*;
    // PRODUCTION_CALLBACK
}
fn main() {
    let args: Vec<i32> = std::env::args().skip(1).map(|s| s.parse().unwrap()).collect();
    LINKS.store(args[0] as u64, Ordering::SeqCst);
    REMOVE_DURING_RPC.store(args[1], Ordering::SeqCst);
    RPC_ERROR.store(args[2], Ordering::SeqCst);
    PERMISSION_ERROR.store(args[3], Ordering::SeqCst);
    let result = unsafe { production::zerofs_permission(std::ptr::null_mut(), std::ptr::null_mut(), args[4]) };
    println!("{} {} {}", result, RPC_CALLS.load(Ordering::SeqCst), PERMISSION_CALLS.load(Ordering::SeqCst));
}
'''


def callback_source():
    source = SOURCE.read_text()
    return START + source.split(START, 1)[1].split(END, 1)[0]


class PermissionRemovalRaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="zerofs-permission-race-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.root = Path(cls.directory.name)
        cls.callback = callback_source()
        cls.executable = cls.compile(cls.callback, "permission")

    @classmethod
    def compile(cls, callback, name):
        source = cls.root / f"{name}.rs"
        source.write_text(SHIM.replace("// PRODUCTION_CALLBACK", callback))
        executable = cls.root / name
        subprocess.run(
            [os.environ.get("RUSTC", "rustc"), "--edition=2021", str(source), "-o", str(executable)],
            check=True, capture_output=True, text=True,
        )
        return executable

    def run_case(self, links=2, remove=0, rpc_error=0, permission_error=0, mask=1, executable=None):
        result = subprocess.run(
            [str(executable or self.executable), *map(str, (links, remove, rpc_error, permission_error, mask))],
            check=True, capture_output=True, text=True,
        )
        return tuple(map(int, result.stdout.split()))

    def test_removal_during_refresh_still_checks_local_permissions(self):
        self.assertEqual(self.run_case(remove=1, rpc_error=-2), (0, 1, 1))

    def test_removed_inode_does_not_bypass_permission_denial(self):
        self.assertEqual(self.run_case(remove=1, rpc_error=-2, permission_error=-13), (-13, 1, 1))

    def test_linked_inode_enoent_still_fails(self):
        self.assertEqual(self.run_case(rpc_error=-2), (-2, 1, 0))

    def test_other_rpc_errors_still_fail_after_removal(self):
        for error in (-5, -13):
            with self.subTest(error=error):
                self.assertEqual(self.run_case(remove=1, rpc_error=error), (error, 1, 0))

    def test_existing_unlinked_inode_uses_local_permissions(self):
        self.assertEqual(self.run_case(links=0), (0, 0, 1))

    def test_linked_success_and_rcu_fallback_are_unchanged(self):
        self.assertEqual(self.run_case(), (0, 1, 1))
        self.assertEqual(self.run_case(mask=0x81), (-10, 0, 0))

    def test_negative_control_reproduces_pre_fix_enoent(self):
        # Restore the original propagation at the identified boundary, while
        # retaining the rest of the actual callback and the same race harness.
        start = self.callback.index("            let attributes =\n")
        end = self.callback.index("            validate_stat_for_inode", start)
        original = self.callback[:start] + (
            "            let attributes =\n"
            "                getattr_attributes(&mount.client, &inode, &credentials, expected_type)?;\n"
        ) + self.callback[end:]
        baseline = self.compile(original, "pre_fix")
        self.assertEqual(self.run_case(remove=1, rpc_error=-2, executable=baseline), (-2, 1, 0))


if __name__ == "__main__":
    unittest.main()
