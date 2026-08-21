pub(crate) mod admin;
pub mod controller;
pub mod identity;
pub mod node;
pub mod proto;
pub mod server;
pub(crate) mod targets;

/// CSI driver name, as registered with the CO and referenced by
/// StorageClass.provisioner.
const DRIVER_NAME: &str = "csi.zerofs.net";

/// Reported as vendor_version in GetPluginInfo.
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default root directory for volumes on the gateway filesystem. One
/// subdirectory per volume, e.g. /volumes/pvc-<uuid>.
const DEFAULT_VOLUMES_ROOT: &str = "/volumes";
