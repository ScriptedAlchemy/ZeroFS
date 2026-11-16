#[path = "../../../zerofs/src/storage_readiness/identity.rs"]
mod identity;
use identity::{DeviceObservation, FilesystemIdentity, MountedFilesystem};
#[path = "../../../zerofs/src/storage_readiness/linux.rs"]
mod linux;

#[path = "../tests/real_mounts.rs"]
mod integration;
