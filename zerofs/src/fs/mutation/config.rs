//! Normalized shared write-acknowledgement contract.
//!
//! Every write protocol (NBD, NFS, 9P, WebUI) consumes one resolved
//! [`FilesystemWriteAckSettings`] instead of re-deriving acknowledgement
//! semantics from raw configuration fields. `[filesystem]` is the
//! authoritative form; the legacy `[servers.nbd]` volatile fields are
//! deprecated migration inputs that must agree exactly when both are present.

use anyhow::{Result, bail};

/// Point at which an ordinary client write is acknowledged.
///
/// `volatile_memory` is intentionally unsafe across process or power loss:
/// explicit flush barriers (fsync/COMMIT/FLUSH/FUA) remain the durability
/// boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemWriteAckMode {
    #[default]
    Materialized,
    VolatileMemory,
}

/// Which configuration form produced the normalized setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemWriteAckSource {
    DefaultMaterialized,
    Filesystem,
    LegacyNbd,
    MatchingFilesystemAndLegacyNbd,
}

/// Where an acknowledged write becomes durable from the client's point of
/// view. Resolved once here; protocol adapters must consume this field and
/// never hard-code SSD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientDurabilityTarget {
    LocalSsd,
    RemoteBackend,
}

/// The resolved acknowledgement contract shared by every protocol adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FilesystemWriteAckSettings {
    pub(crate) mode: FilesystemWriteAckMode,
    pub(crate) volatile_memory_bytes: u64,
    pub(crate) volatile_max_operations: usize,
    pub(crate) source: FilesystemWriteAckSource,
    pub(crate) client_durability_target: ClientDurabilityTarget,
}

/// Default in-flight volatile operation cap.
pub(crate) const DEFAULT_VOLATILE_MAX_OPERATIONS: usize = 65536;
/// Upper bound on the operation cap: beyond this the per-operation
/// bookkeeping stops being a meaningful admission bound.
pub(crate) const MAX_VOLATILE_MAX_OPERATIONS: usize = 1 << 20;

/// Largest single NBD WRITE (`nbd::server::MAX_REQUEST_LENGTH`).
pub(crate) const NBD_MAX_WRITE_BYTES: u64 = 128 * 1024 * 1024;
/// Largest single 9P message (`ninep_proto::P9_MAX_MSIZE`).
pub(crate) const NINEP_MAX_WRITE_BYTES: u64 = ninep_proto::P9_MAX_MSIZE as u64;
/// The WebUI tunnels 9P frames, so it shares the 9P bound.
pub(crate) const WEBUI_MAX_WRITE_BYTES: u64 = NINEP_MAX_WRITE_BYTES;
/// NFS wtmax advertised in FSINFO (`nfs` handler).
pub(crate) const NFS_MAX_WRITE_BYTES: u64 = 1024 * 1024;

/// Everything the resolver needs, gathered by `Settings` so this module
/// stays free of serde plumbing.
pub(crate) struct FilesystemWriteAckRequest {
    /// `[filesystem] write_ack_mode`; `None` when the key is omitted.
    pub(crate) configured_mode: Option<FilesystemWriteAckMode>,
    /// `[filesystem] volatile_memory_gb` (0.0 when omitted).
    pub(crate) volatile_memory_gb: f64,
    /// `[filesystem] volatile_max_operations`; `None` selects the default.
    pub(crate) volatile_max_operations: Option<usize>,
    /// Deprecated `[servers.nbd] write_ack_mode = "volatile_memory"`.
    pub(crate) legacy_nbd_volatile: bool,
    /// Deprecated `[servers.nbd] volatile_memory_gb`.
    pub(crate) legacy_nbd_volatile_memory_gb: f64,
    pub(crate) writeback_enabled: bool,
    pub(crate) replication_enabled: bool,
    pub(crate) ignore_fsync: bool,
    /// False for read-only and checkpoint servers.
    pub(crate) read_write_server: bool,
    /// Largest maximum write among the enabled protocols, as
    /// `(protocol name, bytes)`; `None` when no write protocol is enabled.
    pub(crate) largest_enabled_protocol_write: Option<(&'static str, u64)>,
}

impl FilesystemWriteAckRequest {
    pub(crate) fn resolve(&self) -> Result<FilesystemWriteAckSettings> {
        let (mode, source, budget_gb) = self.mode_source_and_budget()?;

        if self.configured_mode != Some(FilesystemWriteAckMode::VolatileMemory)
            && self.volatile_memory_gb != 0.0
        {
            bail!(
                "[filesystem] volatile_memory_gb is only valid when write_ack_mode = \"volatile_memory\""
            );
        }
        if mode == FilesystemWriteAckMode::Materialized && self.volatile_max_operations.is_some() {
            bail!(
                "[filesystem] volatile_max_operations is only valid when write_ack_mode = \"volatile_memory\""
            );
        }
        let volatile_max_operations = self
            .volatile_max_operations
            .unwrap_or(DEFAULT_VOLATILE_MAX_OPERATIONS);
        if !(1..=MAX_VOLATILE_MAX_OPERATIONS).contains(&volatile_max_operations) {
            bail!(
                "[filesystem] volatile_max_operations must be between 1 and {MAX_VOLATILE_MAX_OPERATIONS}"
            );
        }

        if mode == FilesystemWriteAckMode::Materialized {
            return Ok(FilesystemWriteAckSettings {
                mode,
                volatile_memory_bytes: 0,
                volatile_max_operations,
                source,
                client_durability_target: if self.writeback_enabled {
                    ClientDurabilityTarget::LocalSsd
                } else {
                    ClientDurabilityTarget::RemoteBackend
                },
            });
        }

        self.validate_volatile_environment()?;
        let volatile_memory_bytes = volatile_budget_bytes(budget_gb)?;
        if let Some((protocol, max_write)) = self.largest_enabled_protocol_write
            && volatile_memory_bytes < max_write
        {
            bail!(
                "volatile_memory_gb must hold at least one maximum {protocol} write ({max_write} bytes)"
            );
        }

        Ok(FilesystemWriteAckSettings {
            mode,
            volatile_memory_bytes,
            volatile_max_operations,
            source,
            // Volatile acknowledgement only exists in front of the local
            // writeback tier; `validate_volatile_environment` guarantees it.
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        })
    }

    /// Merge the shared `[filesystem]` form with the deprecated
    /// `[servers.nbd]` form: legacy-only selects volatile, both forms must
    /// agree exactly, and a legacy materialized default never vetoes an
    /// explicit shared setting.
    fn mode_source_and_budget(
        &self,
    ) -> Result<(FilesystemWriteAckMode, FilesystemWriteAckSource, f64)> {
        match (self.configured_mode, self.legacy_nbd_volatile) {
            (None, false) => Ok((
                FilesystemWriteAckMode::Materialized,
                FilesystemWriteAckSource::DefaultMaterialized,
                0.0,
            )),
            (None, true) => Ok((
                FilesystemWriteAckMode::VolatileMemory,
                FilesystemWriteAckSource::LegacyNbd,
                self.legacy_nbd_volatile_memory_gb,
            )),
            (Some(mode), false) => Ok((
                mode,
                FilesystemWriteAckSource::Filesystem,
                self.volatile_memory_gb,
            )),
            (Some(FilesystemWriteAckMode::VolatileMemory), true) => {
                if self.volatile_memory_gb != self.legacy_nbd_volatile_memory_gb {
                    bail!(
                        "[filesystem] volatile_memory_gb ({}) and the deprecated [servers.nbd] volatile_memory_gb ({}) must agree exactly",
                        self.volatile_memory_gb,
                        self.legacy_nbd_volatile_memory_gb
                    );
                }
                Ok((
                    FilesystemWriteAckMode::VolatileMemory,
                    FilesystemWriteAckSource::MatchingFilesystemAndLegacyNbd,
                    self.volatile_memory_gb,
                ))
            }
            (Some(FilesystemWriteAckMode::Materialized), true) => bail!(
                "[filesystem] write_ack_mode = \"materialized\" conflicts with the deprecated [servers.nbd] write_ack_mode = \"volatile_memory\"; both forms must agree exactly"
            ),
        }
    }

    fn validate_volatile_environment(&self) -> Result<()> {
        if !self.read_write_server {
            bail!(
                "volatile_memory write acknowledgement requires a read-write server; it is incompatible with read-only / checkpoint modes"
            );
        }
        if self.replication_enabled {
            bail!("volatile_memory write acknowledgement is not supported with [replication]");
        }
        if self.ignore_fsync {
            bail!(
                "volatile_memory write acknowledgement requires functional flush barriers; [filesystem] ignore_fsync must be false"
            );
        }
        if !self.writeback_enabled {
            bail!(
                "volatile_memory write acknowledgement requires the enabled [writeback] tier: without it there is no local durability target"
            );
        }
        Ok(())
    }
}

fn volatile_budget_bytes(budget_gb: f64) -> Result<u64> {
    if !budget_gb.is_finite() || budget_gb <= 0.0 {
        bail!("volatile_memory_gb must be a finite positive number");
    }
    let bytes = budget_gb * 1_000_000_000.0;
    if bytes > u64::MAX as f64 {
        bail!("volatile_memory_gb exceeds this platform's address space");
    }
    Ok(bytes.round() as u64)
}
