use crate::fs::mutation::config::{
    FilesystemWriteAckMode, FilesystemWriteAckRequest, FilesystemWriteAckSettings,
    NBD_MAX_WRITE_BYTES, NFS_MAX_WRITE_BYTES, NINEP_MAX_WRITE_BYTES, WEBUI_MAX_WRITE_BYTES,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct};
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

/// Compression algorithm configuration for extent data.
/// Supports lz4 and zstd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionConfig {
    /// LZ4 compression: fast with moderate compression ratio
    Lz4,
    /// Zstd compression with configurable level (1-22)
    /// Level 1 is fastest, level 22 is maximum compression
    Zstd(i32),
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig::Zstd(3)
    }
}

impl Serialize for CompressionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionConfig::Lz4 => serializer.serialize_str("lz4"),
            CompressionConfig::Zstd(level) => serializer.serialize_str(&format!("zstd-{}", level)),
        }
    }
}

impl<'de> Deserialize<'de> for CompressionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CompressionConfigVisitor;

        impl de::Visitor<'_> for CompressionConfigVisitor {
            type Value = CompressionConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("'zstd-{level}' where level is 1-22, or 'lz4'")
            }

            fn visit_str<E>(self, value: &str) -> Result<CompressionConfig, E>
            where
                E: de::Error,
            {
                if value == "lz4" {
                    return Ok(CompressionConfig::Lz4);
                }

                if let Some(level_str) = value.strip_prefix("zstd-") {
                    let level: i32 = level_str.parse().map_err(|_| {
                        de::Error::invalid_value(
                            de::Unexpected::Str(value),
                            &"'zstd-{level}' where level is a number 1-22",
                        )
                    })?;

                    if !(1..=22).contains(&level) {
                        return Err(de::Error::invalid_value(
                            de::Unexpected::Signed(level as i64),
                            &"zstd level must be between 1 and 22",
                        ));
                    }

                    return Ok(CompressionConfig::Zstd(level));
                }

                Err(de::Error::invalid_value(
                    de::Unexpected::Str(value),
                    &"'zstd-{level}' where level is 1-22, or 'lz4'",
                ))
            }
        }

        deserializer.deserialize_str(CompressionConfigVisitor)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WalConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    /// Object storage class/tier for WAL writes.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string"
    )]
    pub storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
}

impl WalConfig {
    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub cache: CacheConfig,
    pub storage: StorageConfig,
    pub servers: ServerConfig,
    /// Process-level resource limits used when container namespaces hide the
    /// parent cgroup envelope.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub runtime: Option<RuntimeConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub filesystem: Option<FilesystemConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub lsm: Option<LsmConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub gc: Option<GcConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcsConfig>,
    /// Strict SSH transport settings used only when `[storage].url` is SFTP.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sftp: Option<SftpConfig>,
    /// Optional local RAM/SSD dirty-data tier in front of the remote store.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub writeback: Option<crate::writeback::config::WritebackConfig>,
    /// Location of a pre-2.0 volume's separate WAL store.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub wal: Option<WalConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default = "default_telemetry")]
    pub telemetry: Option<TelemetryConfig>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub prometheus: Option<PrometheusConfig>,
    /// HA replication. Absent means single-node (non-replicated behavior).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub replication: Option<ReplicationConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Administrator-declared ZeroFS-dedicated memory envelope in decimal GB.
    /// Use this when a container namespace hides the dedicated service limit.
    /// A shared parent cgroup is only a ceiling and cannot supply this budget.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub memory_limit_gb: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct SftpConfig {
    /// Private key used for non-interactive public-key authentication.
    #[serde(
        default = "default_sftp_identity_file",
        deserialize_with = "deserialize_expandable_path"
    )]
    pub identity_file: PathBuf,
    /// OpenSSH known-hosts file used for strict server identity verification.
    #[serde(
        default = "default_sftp_known_hosts",
        deserialize_with = "deserialize_expandable_path"
    )]
    pub known_hosts: PathBuf,
    /// Shared connection budget for the whole account, leaving provider headroom.
    #[serde(default = "default_sftp_max_connections")]
    pub max_connections: usize,
    /// Maximum read concurrency within the shared connection budget.
    #[serde(default = "default_sftp_direction_concurrency")]
    pub read_concurrency: usize,
    /// Maximum write concurrency within the shared connection budget.
    #[serde(default = "default_sftp_direction_concurrency")]
    pub write_concurrency: usize,
    /// Packed segment size. Smaller segments let SFTP publish independent files
    /// concurrently instead of contending on disjoint writes to one large file.
    #[serde(default = "default_sftp_segment_size_mib")]
    pub segment_size_mib: usize,
    /// Aligned SSD/RAM cache part used for cold segment reads. A larger part
    /// amortizes SFTP open/stat/header latency while the adaptive prefetcher
    /// still bounds random-read amplification.
    #[serde(default = "default_sftp_read_cache_part_size_kib")]
    pub read_cache_part_size_kib: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SftpDataProfile {
    pub segment_size_bytes: usize,
    pub max_inflight_seals: usize,
    pub read_cache_part_size_bytes: usize,
    pub read_fetch_window_min_bytes: usize,
    pub read_fetch_window_max_bytes: usize,
}

/// Data-plane tuning for the configured storage backend, resolved once at
/// startup and carried as one value instead of per-knob arguments.
///
/// `Default` is what every backend gets unless it publishes a profile of its
/// own: `None` means "keep the crate default" for that knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StoreProfile {
    /// Open-segment seal threshold in bytes.
    pub seal_threshold: Option<usize>,
    /// Segment seals allowed in flight at once.
    pub max_inflight_seals: Option<usize>,
    /// Clean-cache budget for decoded plaintext extents.
    pub decoded_extent_cache_bytes: Option<usize>,
    /// Cold segment-read prefetch geometry.
    pub prefetch: crate::object_store_prefetch::PrefetchProfile,
}

impl From<SftpDataProfile> for StoreProfile {
    fn from(profile: SftpDataProfile) -> Self {
        Self {
            seal_threshold: Some(profile.segment_size_bytes),
            max_inflight_seals: Some(profile.max_inflight_seals),
            decoded_extent_cache_bytes: None,
            prefetch: crate::object_store_prefetch::PrefetchProfile::tuned(
                profile.read_cache_part_size_bytes,
                profile.read_fetch_window_min_bytes,
                profile.read_fetch_window_max_bytes,
            ),
        }
    }
}

impl Default for SftpConfig {
    fn default() -> Self {
        Self {
            identity_file: default_sftp_identity_file(),
            known_hosts: default_sftp_known_hosts(),
            max_connections: default_sftp_max_connections(),
            read_concurrency: default_sftp_direction_concurrency(),
            write_concurrency: default_sftp_direction_concurrency(),
            segment_size_mib: default_sftp_segment_size_mib(),
            read_cache_part_size_kib: default_sftp_read_cache_part_size_kib(),
        }
    }
}

impl SftpConfig {
    pub const MAX_ACCOUNT_CONNECTIONS: usize = 8;
    pub const MAX_DIRECTION_CONCURRENCY: usize = 7;

    fn validate(&self) -> Result<()> {
        if self.identity_file.to_string_lossy().trim().is_empty() {
            anyhow::bail!("[sftp] identity_file must name a private SSH key");
        }
        if self.known_hosts.to_string_lossy().trim().is_empty() {
            anyhow::bail!(
                "[sftp] known_hosts must name a file used for strict host-key verification"
            );
        }
        if !(1..=Self::MAX_ACCOUNT_CONNECTIONS).contains(&self.max_connections) {
            anyhow::bail!(
                "[sftp] max_connections must be between 1 and {}",
                Self::MAX_ACCOUNT_CONNECTIONS
            );
        }
        for (name, value) in [
            ("read_concurrency", self.read_concurrency),
            ("write_concurrency", self.write_concurrency),
        ] {
            if !(1..=Self::MAX_DIRECTION_CONCURRENCY).contains(&value) {
                anyhow::bail!(
                    "[sftp] {name} must be between 1 and {}",
                    Self::MAX_DIRECTION_CONCURRENCY
                );
            }
            if value > self.max_connections {
                anyhow::bail!(
                    "[sftp] {name} ({value}) must not exceed max_connections ({})",
                    self.max_connections
                );
            }
        }
        if !(8..=256).contains(&self.segment_size_mib) {
            anyhow::bail!("[sftp] segment_size_mib must be between 8 and 256");
        }
        if !(256..=8192).contains(&self.read_cache_part_size_kib)
            || !self.read_cache_part_size_kib.is_power_of_two()
        {
            anyhow::bail!(
                "[sftp] read_cache_part_size_kib must be a power of two between 256 and 8192"
            );
        }
        Ok(())
    }

    pub fn data_profile(&self) -> SftpDataProfile {
        SftpDataProfile {
            segment_size_bytes: self.segment_size_mib * 1024 * 1024,
            max_inflight_seals: self.write_concurrency,
            read_cache_part_size_bytes: self.read_cache_part_size_kib * 1024,
            read_fetch_window_min_bytes: self.read_cache_part_size_kib * 1024,
            read_fetch_window_max_bytes: self.segment_size_mib * 1024 * 1024,
        }
    }
}

fn default_sftp_known_hosts() -> PathBuf {
    PathBuf::from(shellexpand::tilde("~/.ssh/known_hosts").into_owned())
}

fn default_sftp_identity_file() -> PathBuf {
    PathBuf::from(shellexpand::tilde("~/.ssh/id_ed25519").into_owned())
}

const fn default_sftp_max_connections() -> usize {
    SftpConfig::MAX_ACCOUNT_CONNECTIONS
}

const fn default_sftp_direction_concurrency() -> usize {
    SftpConfig::MAX_DIRECTION_CONCURRENCY
}

const fn default_sftp_segment_size_mib() -> usize {
    32
}

const fn default_sftp_read_cache_part_size_kib() -> usize {
    1024
}

/// Parsed endpoint information for transport setup.
#[derive(Clone, PartialEq, Eq)]
pub struct SftpEndpoint {
    pub host: String,
    pub port: u16,
    pub username: String,
}

impl fmt::Debug for SftpEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SftpEndpoint")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

/// Node role within an HA pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// Bootstraps as the active leader, opening the data db as writer.
    Leader,
    /// Watches the leader's heartbeats; takes over (opens the data db as writer)
    /// when they stop.
    Standby,
}

impl Serialize for ReplicationRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            ReplicationRole::Leader => "leader",
            ReplicationRole::Standby => "standby",
        })
    }
}

impl<'de> Deserialize<'de> for ReplicationRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "leader" => Ok(ReplicationRole::Leader),
            "standby" => Ok(ReplicationRole::Standby),
            other => Err(de::Error::invalid_value(
                de::Unexpected::Str(other),
                &"\"leader\" or \"standby\"",
            )),
        }
    }
}

/// Configuration for one node in a two-node HA pair.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    /// This node's stable identity within the pair.
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub node_id: String,
    /// Role: "leader" or "standby".
    pub role: ReplicationRole,
    /// Peer replication endpoint. Required with `replication_listen`.
    #[serde(default, deserialize_with = "deserialize_expandable_string_vec")]
    pub peers: Vec<String>,
    /// Local endpoint for replication, heartbeats, and role election.
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string",
        default
    )]
    pub replication_listen: Option<String>,
    /// One-startup authorization to replace durable HA ownership.
    /// All former participants must be stopped before this is enabled.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force_recovery: bool,
}

impl ReplicationConfig {
    pub fn validate(&self) -> Result<()> {
        if self.node_id.trim().is_empty() {
            anyhow::bail!("[replication] node_id must not be empty");
        }
        if self.node_id.trim() != self.node_id {
            anyhow::bail!("[replication] node_id must not contain leading or trailing whitespace");
        }

        // A standby must be reachable to receive the leader's ships and to watch
        // its heartbeats; with no listen it can do neither (it would fail at startup
        // with "standby has no replication_listen; cannot watch heartbeats").
        if self.role == ReplicationRole::Standby && self.replication_listen.is_none() {
            anyhow::bail!(
                "[replication] role = \"standby\" requires replication_listen (the address it \
                 receives the leader's ships on)"
            );
        }

        // The receiver binds replication_listen verbatim, so it must be a socket
        // address (host:port); catch a typo here rather than at bind time.
        if let Some(listen) = &self.replication_listen {
            listen.parse::<SocketAddr>().with_context(|| {
                format!(
                    "[replication] replication_listen {listen:?} is not a valid socket address \
                     (expected host:port)"
                )
            })?;
        }

        for peer in &self.peers {
            if peer.trim().is_empty() {
                anyhow::bail!("[replication] peers must not contain an empty entry");
            }
            // A node replicating to its own listen address is always a mistake.
            if let Some(listen) = &self.replication_listen {
                let bare = peer
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                if bare == listen {
                    anyhow::bail!(
                        "[replication] peer {peer:?} is this node's own replication_listen; a \
                         node must not replicate to itself"
                    );
                }
            }
        }

        if self.peers.len() > 1 {
            anyhow::bail!(
                "[replication] peers must contain at most one address; ZeroFS HA supports \
                 exactly two participants"
            );
        }

        if self.force_recovery {
            if self.role != ReplicationRole::Leader {
                anyhow::bail!("[replication] force_recovery = true requires role = \"leader\"");
            }
            if !self.peers.is_empty() {
                anyhow::bail!(
                    "[replication] force_recovery = true requires peers = []; verify every \
                     former peer is down before removing it from the configuration"
                );
            }
            tracing::warn!(
                "[replication] force_recovery is enabled for node {:?}; this may fence a live \
                 partitioned writer. Remove force_recovery after this recovery startup succeeds",
                self.node_id
            );
        }

        // Automatic role swaps require both a local endpoint and a peer.
        let (has_peers, has_listen) = (!self.peers.is_empty(), self.replication_listen.is_some());
        if !self.force_recovery && has_peers != has_listen {
            anyhow::bail!(
                "[replication] automatic HA requires peers and replication_listen together; \
                 configure both for role swaps, or neither for a standalone node"
            );
        }

        Ok(())
    }
}

/// What slatedb block-cache content to warm for the metadata segment at startup.
///
/// Every filesystem op is a point lookup gated by per-SST bloom filters and the
/// SST index, so warming those removes the cold-cache latency cliff a fresh node
/// or restart otherwise pays on its first reads. The bulk extent segment is never
/// warmed.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WarmMetadata {
    /// Don't warm anything on startup.
    Off,
    /// Warm the metadata SST filters and indexes only (small, bounded). Default.
    #[default]
    FiltersIndex,
    /// Also warm the metadata data blocks (larger; for metadata-heavy workloads
    /// whose working set fits the cache).
    Full,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub dir: PathBuf,
    pub disk_size_gb: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_size_gb: Option<f64>,
    #[serde(default)]
    pub warm_metadata: WarmMetadata,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub url: String,
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub encryption_password: String,
    /// Object storage class/tier for data writes, passed through verbatim as the
    /// per-backend tiering header.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_string"
    )]
    pub storage_class: Option<String>,
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageConfig")
            .field("url", &redacted_storage_url(&self.url))
            .field("encryption_password", &"[REDACTED]")
            .field("storage_class", &self.storage_class)
            .finish()
    }
}

impl Serialize for StorageConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct(
            "StorageConfig",
            2 + usize::from(self.storage_class.is_some()),
        )?;
        state.serialize_field("url", &redacted_storage_url(&self.url))?;
        state.serialize_field("encryption_password", &self.encryption_password)?;
        if let Some(storage_class) = &self.storage_class {
            state.serialize_field("storage_class", storage_class)?;
        }
        state.end()
    }
}

fn redacted_storage_url(raw: &str) -> String {
    if !has_sftp_scheme(raw) {
        return raw.to_owned();
    }

    let Ok(mut url) = url::Url::parse(raw) else {
        return "sftp://[REDACTED_INVALID_URL]".to_owned();
    };
    if url.password().is_none() {
        return raw.to_owned();
    }
    if url.set_password(None).is_err() {
        return "sftp://[REDACTED_INVALID_URL]".to_owned();
    }
    url.into()
}

fn url_scheme(raw: &str) -> Option<&str> {
    raw.split_once(':').map(|(scheme, _)| scheme)
}

fn has_sftp_scheme(raw: &str) -> bool {
    url_scheme(raw).is_some_and(|scheme| scheme.eq_ignore_ascii_case("sftp"))
}

/// What the configured storage backend can do. One table so a backend that
/// lacks a feature is declared once instead of re-derived from scheme names at
/// every validation site.
struct BackendCapabilities {
    /// Name used in operator-facing "not supported" diagnostics.
    display_name: &'static str,
    supports_storage_class: bool,
    supports_replication: bool,
}

impl BackendCapabilities {
    /// Backends we hold no restrictions for: object stores, and any scheme we
    /// do not recognize (the historical default).
    fn full(display_name: &'static str) -> Self {
        Self {
            display_name,
            supports_storage_class: true,
            supports_replication: true,
        }
    }
}

/// Resolve backend capabilities from the storage URL scheme. This is the only
/// place a scheme name decides what a backend supports.
fn backend_capabilities(url: &str) -> BackendCapabilities {
    match url_scheme(url) {
        Some(scheme) if scheme.eq_ignore_ascii_case("sftp") => BackendCapabilities {
            display_name: "SFTP",
            supports_storage_class: false,
            supports_replication: false,
        },
        _ => BackendCapabilities::full("object storage"),
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConfig {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_size_gb: Option<f64>,
    /// Compression algorithm for extent data: "zstd-{level}" (default: "zstd-3", level 1-22) or "lz4"
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Treat `fsync` as a no-op: a client fsync/COMMIT returns without forcing a
    /// flush to object storage. Intended for HA, where semi-sync replication
    /// already holds the write on the standby, making the per-fsync flush
    /// redundant. Trades object-store durability for latency: un-flushed writes are
    /// lost if both nodes (or a standalone node) die before a background flush.
    #[serde(default)]
    pub ignore_fsync: bool,
    /// Point at which an ordinary write on any protocol is acknowledged.
    /// Omission selects `materialized`. `volatile_memory` is intentionally
    /// unsafe across process or power loss: explicit flush barriers remain
    /// the durability boundary. Supersedes the deprecated
    /// `[servers.nbd] write_ack_mode`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) write_ack_mode: Option<FilesystemWriteAckMode>,
    /// Global RAM ceiling for volatile writes, shared by every protocol.
    /// Required (finite, positive) when `write_ack_mode = "volatile_memory"`.
    #[serde(default)]
    pub(crate) volatile_memory_gb: f64,
    /// In-flight volatile operation cap; omission selects the default.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) volatile_max_operations: Option<usize>,
}

impl FilesystemConfig {
    pub fn max_bytes(&self) -> u64 {
        self.max_size_gb
            .filter(|&gb| gb.is_finite() && gb > 0.0)
            .map(|gb| (gb * 1_000_000_000.0) as u64)
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct LsmConfig {
    /// Maximum number of SST files in level 0 before triggering compaction
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub l0_max_ssts: Option<usize>,
    /// Maximum number of concurrent compactions
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub max_concurrent_compactions: Option<usize>,
    /// Interval in seconds between periodic flushes
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flush_interval_secs: Option<u64>,
    /// When true, every committed write is durably flushed to object storage
    /// before returning success. Trades per-op latency for zero unflushed data
    /// in case of a crash. Expensive: the WAL is off, so each write forces a
    /// full seal + memtable flush.
    ///
    /// This does NOT change POSIX semantics: with `sync_writes = false` (the
    /// default), explicit fsync from clients is still honored and
    /// waits for durable persistence. The flag only changes what happens to
    /// writes between fsync calls, making them durable on return rather than
    /// buffered until the next flush.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sync_writes: Option<bool>,
    /// Deprecated, ignored: the WAL is permanently off (sealing correctness
    /// requires it). Accepted so pre-2.0 configs still parse; never re-emitted.
    #[serde(default, skip_serializing)]
    pub wal_enabled: Option<bool>,
    /// Deprecated, ignored: unflushed-data budgeting went away with the WAL.
    /// Accepted so pre-2.0 configs still parse; never re-emitted.
    #[serde(default, skip_serializing)]
    pub max_unflushed_gb: Option<f64>,
}

impl LsmConfig {
    /// Default l0_max_ssts: 256. With the WAL off every `db.flush()` (periodic
    /// and each fsync) freezes a fresh L0 SST, so L0 must hold a deep backlog
    /// or flushes stall on compaction. The SSTs are small, bloom-filtered
    /// metadata, so the point-lookup cost is negligible. Applies to
    /// `l0_max_ssts_per_key` too.
    pub const DEFAULT_L0_MAX_SSTS: usize = 256;
    /// Default max_concurrent_compactions
    pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 2;
    /// Default flush_interval_sec
    pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;

    /// Minimum l0_max_ssts to maintain reasonable performance
    pub const MIN_L0_MAX_SSTS: usize = 4;
    /// Minimum max_concurrent_compactions: 1
    pub const MIN_MAX_CONCURRENT_COMPACTIONS: usize = 1;
    /// Minimum flush_interval_secs: 5 seconds
    pub const MIN_FLUSH_INTERVAL_SECS: u64 = 5;

    pub fn l0_max_ssts(&self) -> usize {
        self.l0_max_ssts
            .unwrap_or(Self::DEFAULT_L0_MAX_SSTS)
            .max(Self::MIN_L0_MAX_SSTS)
    }

    pub fn max_concurrent_compactions(&self) -> usize {
        self.max_concurrent_compactions
            .unwrap_or(Self::DEFAULT_MAX_CONCURRENT_COMPACTIONS)
            .max(Self::MIN_MAX_CONCURRENT_COMPACTIONS)
    }

    pub fn flush_interval_secs(&self) -> u64 {
        self.flush_interval_secs
            .unwrap_or(Self::DEFAULT_FLUSH_INTERVAL_SECS)
            .max(Self::MIN_FLUSH_INTERVAL_SECS)
    }

    pub fn sync_writes(&self) -> bool {
        self.sync_writes.unwrap_or(false)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    /// Seconds between segment-GC passes while the filesystem is active. The
    /// ceiling of the adaptive cadence: the loop never sleeps longer. Values
    /// below the ~30 s flush cadence make a busy pass's barrier seal real
    /// sub-1-MiB segments — each itself future GC work.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interval_secs: Option<u64>,
    /// Seconds between passes while a saturated backlog meets an idle store
    /// (no reads, no writes since the previous pass). A fast pass performs a
    /// full reclamation round plus roughly two small bookkeeping PUTs of
    /// fixed overhead (the previous pass's own commits flushing); total
    /// fast-mode work is bounded by the backlog. Setting it equal to
    /// interval_secs disables the idle acceleration tier only; the
    /// busy-backlog drain tier is independent.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub idle_interval_secs: Option<u64>,
    /// Whether reads steer compaction (nominations, seam heat, chain repacks).
    /// The counter-driven policy and the tail scrub are unaffected.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub read_directed: Option<bool>,
    /// Tail-scrub floor: a write-cold segment more than this percent dead —
    /// but not dead enough for normal compaction candidacy — is repacked with
    /// leftover pass budget. The space-amplification dial: worst-case overhead
    /// on write-cold data is 1/(1 - floor/100) of live bytes, bought at up to
    /// (100 - floor)/floor bytes rewritten per byte reclaimed. 0 disables the
    /// scrub; 50 empties the band, same effect.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tail_scrub_min_dead_percent: Option<u64>,
    /// Compaction batches a pass runs before it yields to foreground load:
    /// below the floor it drains regardless of client activity, above it a
    /// client op ends the pass. Idle stores always drain to the internal
    /// per-pass cap. 1 restores the historical single-batch busy pass.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub min_batches_per_pass: Option<usize>,
    /// Pass interval while the store is active but its dead backlog is large
    /// (dead space >= busy_backlog_dead_percent). Clamped to
    /// [MIN_INTERVAL_SECS, interval_secs], so a loaded store keeps draining
    /// without waiting the full base interval. >= interval_secs disables the tier.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub busy_backlog_interval_secs: Option<u64>,
    /// Store dead-space percent at or above which busy_backlog_interval_secs
    /// applies. Below it a busy store uses interval_secs.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub busy_backlog_dead_percent: Option<u64>,
    /// Per-round compaction budget in MiB: the live-byte selection cap, the
    /// heat reserve (half), and the stored-byte gather cap. Raising it packs
    /// hot seams whose cheapest pair exceeds half the default round (the
    /// "over-reserve" chains) and lifts per-batch dead-space throughput, at
    /// ~this much peak gather RAM per batch (more when the leading seam
    /// chain alone exceeds it, gathered whole). Default 256.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub compact_round_max_mib: Option<u64>,
}

impl GcConfig {
    /// Default interval_secs: 60 seconds, the historical fixed cadence.
    pub const DEFAULT_INTERVAL_SECS: u64 = 60;
    /// Default idle_interval_secs: 5 seconds (~12x drain while idle).
    pub const DEFAULT_IDLE_INTERVAL_SECS: u64 = 5;
    /// Default read_directed: true.
    pub const DEFAULT_READ_DIRECTED: bool = true;
    /// Default tail_scrub_min_dead_percent: 5 (1.053x worst-case space
    /// amplification at up to 19x rewrite per reclaimed byte; the request-cost
    /// break-even on S3 is near 1.5%, so 5 is the write-amplification choice).
    pub const DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT: u64 = 5;
    /// Default min_batches_per_pass: 4 (~4x the single-batch reclaim rate under
    /// load; 1 restores single-batch passes).
    pub const DEFAULT_MIN_BATCHES_PER_PASS: usize = 4;
    /// Default busy_backlog_interval_secs: 15 (a loaded, dirty store drains
    /// ~4x more often than the base interval).
    pub const DEFAULT_BUSY_BACKLOG_INTERVAL_SECS: u64 = 15;
    /// Default busy_backlog_dead_percent: 20.
    pub const DEFAULT_BUSY_BACKLOG_DEAD_PERCENT: u64 = 20;
    /// Default compact_round_max_mib: 256 (the historical fixed round budget).
    pub const DEFAULT_COMPACT_ROUND_MAX_MIB: u64 = 256;
    /// Min compact_round_max_mib: below the pack target a round can't hold one
    /// output segment.
    pub const MIN_COMPACT_ROUND_MAX_MIB: u64 = 64;
    /// Max compact_round_max_mib: a hard ceiling on per-batch gather RAM.
    pub const MAX_COMPACT_ROUND_MAX_MIB: u64 = 4096;

    /// Minimum interval_secs: each pass runs a flush barrier, so the LSM
    /// flush floor applies.
    pub const MIN_INTERVAL_SECS: u64 = LsmConfig::MIN_FLUSH_INTERVAL_SECS;
    /// Minimum idle_interval_secs: below ~1 s the pass's barrier round-trips
    /// stop amortizing.
    pub const MIN_IDLE_INTERVAL_SECS: u64 = 1;

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
            .unwrap_or(Self::DEFAULT_INTERVAL_SECS)
            .max(Self::MIN_INTERVAL_SECS)
    }

    pub fn idle_interval_secs(&self) -> u64 {
        self.idle_interval_secs
            .unwrap_or(Self::DEFAULT_IDLE_INTERVAL_SECS)
            .max(Self::MIN_IDLE_INTERVAL_SECS)
            .min(self.interval_secs())
    }

    pub fn read_directed(&self) -> bool {
        self.read_directed.unwrap_or(Self::DEFAULT_READ_DIRECTED)
    }

    /// `None` = scrub disabled (configured 0).
    pub fn tail_scrub_min_dead_percent(&self) -> Option<u64> {
        match self.tail_scrub_min_dead_percent {
            Some(0) => None,
            v => Some(
                v.unwrap_or(Self::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT)
                    .clamp(1, 50),
            ),
        }
    }

    pub fn min_batches_per_pass(&self) -> usize {
        self.min_batches_per_pass
            .unwrap_or(Self::DEFAULT_MIN_BATCHES_PER_PASS)
            .max(1)
    }

    /// Clamped to [MIN_INTERVAL_SECS, interval_secs]; equal to interval_secs
    /// disables the busy-backlog tier.
    pub fn busy_backlog_interval_secs(&self) -> u64 {
        self.busy_backlog_interval_secs
            .unwrap_or(Self::DEFAULT_BUSY_BACKLOG_INTERVAL_SECS)
            .max(Self::MIN_INTERVAL_SECS)
            .min(self.interval_secs())
    }

    pub fn busy_backlog_dead_percent(&self) -> u64 {
        self.busy_backlog_dead_percent
            .unwrap_or(Self::DEFAULT_BUSY_BACKLOG_DEAD_PERCENT)
            .min(100)
    }

    /// Per-round compaction budget in bytes (MiB config, clamped).
    pub fn compact_round_bytes(&self) -> u64 {
        self.compact_round_max_mib
            .unwrap_or(Self::DEFAULT_COMPACT_ROUND_MAX_MIB)
            .clamp(
                Self::MIN_COMPACT_ROUND_MAX_MIB,
                Self::MAX_COMPACT_ROUND_MAX_MIB,
            )
            << 20
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NfsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ninep: Option<NinePConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webui: Option<WebUIConfig>,
}

impl ServerConfig {
    fn has_listener_endpoint(&self) -> bool {
        let has_endpoint = self
            .nfs
            .as_ref()
            .and_then(|config| config.addresses.as_ref())
            .is_some_and(|addresses| !addresses.is_empty())
            || self.ninep.as_ref().is_some_and(NinePConfig::has_endpoint)
            || self.nbd.as_ref().is_some_and(NbdConfig::has_endpoint)
            || self.rpc.as_ref().is_some_and(RpcConfig::has_endpoint);

        #[cfg(feature = "webui")]
        let has_endpoint = has_endpoint
            || self
                .webui
                .as_ref()
                .is_some_and(|config| !config.addresses.is_empty());

        has_endpoint
    }

    fn validate(&self) -> Result<()> {
        if self
            .nfs
            .as_ref()
            .is_some_and(|config| config.addresses.as_ref().is_none_or(HashSet::is_empty))
        {
            anyhow::bail!("[servers.nfs] must configure at least one address endpoint");
        }
        for (name, valid) in [
            (
                "ninep",
                self.ninep.as_ref().is_none_or(NinePConfig::has_endpoint),
            ),
            ("nbd", self.nbd.as_ref().is_none_or(NbdConfig::has_endpoint)),
            ("rpc", self.rpc.as_ref().is_none_or(RpcConfig::has_endpoint)),
        ] {
            if !valid {
                anyhow::bail!(
                    "[servers.{name}] must configure at least one address or unix_socket endpoint"
                );
            }
        }
        if self
            .webui
            .as_ref()
            .is_some_and(|config| config.addresses.is_empty())
        {
            anyhow::bail!("[servers.webui] must configure at least one address endpoint");
        }
        if let Some(nbd) = &self.nbd {
            nbd.validate()?;
        }
        Ok(())
    }

    /// Require at least one endpoint that the current build can serve.
    pub fn require_listener_endpoint(&self) -> Result<()> {
        if !self.has_listener_endpoint() {
            anyhow::bail!(
                "[servers] must configure at least one listener endpoint (address or unix_socket)"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WebUIConfig {
    #[serde(
        default = "default_webui_addresses",
        deserialize_with = "deserialize_expandable_socket_addrs"
    )]
    pub addresses: HashSet<SocketAddr>,
    pub uid: u32,
    pub gid: u32,
}

fn default_webui_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    ));
    set
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NfsConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_identity: Option<NfsSharedIdentity>,
}

/// Optional all-client identity for an NFS export.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NfsSharedIdentity {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NinePConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
    /// Optional all-client identity for a shared writable namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_identity: Option<NfsSharedIdentity>,
}

impl NinePConfig {
    fn has_endpoint(&self) -> bool {
        self.addresses
            .as_ref()
            .is_some_and(|addresses| !addresses.is_empty())
            || self
                .unix_socket
                .as_ref()
                .is_some_and(|path| !path.as_os_str().is_empty())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
    /// Point at which an ordinary NBD WRITE is acknowledged.
    ///
    /// `volatile_memory` is intentionally unsafe across process or power loss:
    /// FLUSH/FUA remain the durability boundary.
    #[serde(default)]
    pub write_ack_mode: NbdWriteAckMode,
    /// Global RAM ceiling for volatile NBD writes, shared by every export.
    #[serde(default)]
    pub volatile_memory_gb: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NbdWriteAckMode {
    #[default]
    Materialized,
    VolatileMemory,
}

const MIN_NBD_VOLATILE_MEMORY_BYTES: u64 = 128 * 1024 * 1024;

impl NbdConfig {
    fn has_endpoint(&self) -> bool {
        self.addresses
            .as_ref()
            .is_some_and(|addresses| !addresses.is_empty())
            || self
                .unix_socket
                .as_ref()
                .is_some_and(|path| !path.as_os_str().is_empty())
    }

    fn validate(&self) -> Result<()> {
        match self.write_ack_mode {
            NbdWriteAckMode::Materialized if self.volatile_memory_gb != 0.0 => anyhow::bail!(
                "[servers.nbd] volatile_memory_gb is only valid when write_ack_mode = \"volatile_memory\""
            ),
            NbdWriteAckMode::VolatileMemory
                if !self.volatile_memory_gb.is_finite()
                    || self.volatile_memory_gb * 1_000_000_000.0
                        < MIN_NBD_VOLATILE_MEMORY_BYTES as f64 =>
            {
                anyhow::bail!(
                    "[servers.nbd] volatile_memory_gb must be finite and hold at least one maximum NBD WRITE ({} bytes) when write_ack_mode = \"volatile_memory\"",
                    MIN_NBD_VOLATILE_MEMORY_BYTES
                )
            }
            _ => Ok(()),
        }
    }

    pub fn volatile_memory_bytes(&self) -> Result<u64> {
        self.validate()?;
        if self.write_ack_mode == NbdWriteAckMode::Materialized {
            return Ok(0);
        }
        let bytes = self.volatile_memory_gb * 1_000_000_000.0;
        if bytes > u64::MAX as f64 {
            anyhow::bail!("[servers.nbd] volatile_memory_gb exceeds this platform's address space");
        }
        Ok(bytes.round() as u64)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_socket_addrs",
        default
    )]
    pub addresses: Option<HashSet<SocketAddr>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_expandable_path",
        default
    )]
    pub unix_socket: Option<PathBuf>,
}

impl RpcConfig {
    fn has_endpoint(&self) -> bool {
        self.addresses
            .as_ref()
            .is_some_and(|addresses| !addresses.is_empty())
            || self
                .unix_socket
                .as_ref()
                .is_some_and(|path| !path.as_os_str().is_empty())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

fn default_telemetry() -> Option<TelemetryConfig> {
    Some(TelemetryConfig { enabled: true })
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(
        default = "default_prometheus_addresses",
        deserialize_with = "deserialize_expandable_socket_addrs"
    )]
    pub addresses: HashSet<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub benchmark_authority: Option<BenchmarkAuthorityConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkAdapter {
    Nfs,
    Ninep,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkAuthorityConfig {
    pub adapter: BenchmarkAdapter,
    #[serde(deserialize_with = "deserialize_expandable_string")]
    pub export_id: String,
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub tls_certificate: PathBuf,
    #[serde(deserialize_with = "deserialize_expandable_path")]
    pub tls_private_key: PathBuf,
}

impl BenchmarkAuthorityConfig {
    pub(crate) fn validate_label(value: &str, role: &str) -> Result<()> {
        if value.is_empty()
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
            })
        {
            anyhow::bail!("[prometheus.benchmark_authority] {role} must match [A-Za-z0-9._:/-]+");
        }
        Ok(())
    }

    fn validate(&self, prometheus: &PrometheusConfig, servers: &ServerConfig) -> Result<()> {
        Self::validate_label(&self.export_id, "export_id")?;
        if prometheus.addresses.len() != 1 {
            anyhow::bail!(
                "[prometheus.benchmark_authority] requires exactly one Prometheus address"
            );
        }
        for (role, path) in [
            ("tls_certificate", &self.tls_certificate),
            ("tls_private_key", &self.tls_private_key),
        ] {
            if !path.is_absolute() {
                anyhow::bail!("[prometheus.benchmark_authority] {role} must be an absolute path");
            }
        }
        if self.tls_certificate == self.tls_private_key {
            anyhow::bail!(
                "[prometheus.benchmark_authority] tls_certificate and tls_private_key must be different files"
            );
        }

        if servers.nbd.is_some() || servers.webui.is_some() {
            anyhow::bail!(
                "[prometheus.benchmark_authority] requires one isolated NFS or 9P export; disable NBD and WebUI exports"
            );
        }

        match self.adapter {
            BenchmarkAdapter::Nfs => self.validate_nfs(servers),
            BenchmarkAdapter::Ninep => self.validate_ninep(servers),
        }
    }

    fn validate_nfs(&self, servers: &ServerConfig) -> Result<()> {
        if servers.ninep.is_some() {
            anyhow::bail!(
                "[prometheus.benchmark_authority] requires an isolated NFS export; disable 9P"
            );
        }
        let addresses = servers
            .nfs
            .as_ref()
            .and_then(|config| config.addresses.as_ref())
            .filter(|addresses| addresses.len() == 1)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "[prometheus.benchmark_authority] requires exactly one NFS endpoint"
                )
            })?;
        let address = addresses.iter().next().expect("checked one NFS address");
        let IpAddr::V4(ip) = address.ip() else {
            anyhow::bail!(
                "[prometheus.benchmark_authority] NFS authority requires one IPv4 endpoint"
            );
        };
        if ip.is_unspecified() {
            anyhow::bail!(
                "[prometheus.benchmark_authority] NFS authority cannot derive a source from a wildcard endpoint"
            );
        }
        let expected = format!("{ip}:/");
        if self.export_id != expected {
            anyhow::bail!(
                "[prometheus.benchmark_authority] export_id must exactly match the NFS source {expected}"
            );
        }
        Ok(())
    }

    fn validate_ninep(&self, servers: &ServerConfig) -> Result<()> {
        if servers.nfs.is_some() {
            anyhow::bail!(
                "[prometheus.benchmark_authority] requires an isolated 9P export; disable NFS"
            );
        }
        let ninep = servers.ninep.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "[prometheus.benchmark_authority] selected 9P but [servers.ninep] is missing"
            )
        })?;
        let endpoint_count = ninep
            .addresses
            .as_ref()
            .map_or(0, HashSet::len)
            .saturating_add(usize::from(ninep.unix_socket.is_some()));
        if endpoint_count != 1 {
            anyhow::bail!("[prometheus.benchmark_authority] requires exactly one 9P endpoint");
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AwsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AwsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AwsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct AzureConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for AzureConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(AzureConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct GcsConfig(pub std::collections::HashMap<String, String>);

impl<'de> Deserialize<'de> for GcsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(GcsConfig(deserialize_expandable_hashmap(deserializer)?))
    }
}

fn default_nfs_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        2049,
    ));
    set
}

fn default_9p_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        5564,
    ));
    set
}

fn default_nbd_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        10809,
    ));
    set
}

fn default_prometheus_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        9091,
    ));
    set
}

fn default_rpc_addresses() -> HashSet<SocketAddr> {
    let mut set = HashSet::new();
    set.insert(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        7000,
    ));
    set
}

fn deserialize_expandable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(expanded.into_owned()),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_string_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let items = Vec::<String>::deserialize(deserializer)?;
    items
        .into_iter()
        .map(|s| match shellexpand::env(&s) {
            Ok(expanded) => Ok(expanded.into_owned()),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

/// Expand `${VAR}` in each entry, then parse to a `SocketAddr`. Bind addresses
/// deserialize straight to `SocketAddr`, so without this a templated value like
/// `"${POD_IP}:2049"` would fail to parse before it could be expanded.
fn expand_socket_addrs<E>(raw: Vec<String>) -> Result<HashSet<SocketAddr>, E>
where
    E: de::Error,
{
    let mut set = HashSet::with_capacity(raw.len());
    for s in raw {
        let expanded = shellexpand::env(&s).map_err(|e| {
            de::Error::custom(format!("Failed to expand environment variable: {}", e))
        })?;
        let addr = expanded.parse::<SocketAddr>().map_err(|e| {
            de::Error::custom(format!(
                "invalid socket address {expanded:?} (expected host:port): {e}"
            ))
        })?;
        set.insert(addr);
    }
    Ok(set)
}

fn deserialize_expandable_socket_addrs<'de, D>(
    deserializer: D,
) -> Result<HashSet<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    expand_socket_addrs(Vec::<String>::deserialize(deserializer)?)
}

fn deserialize_optional_expandable_socket_addrs<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<SocketAddr>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer)?
        .map(expand_socket_addrs)
        .transpose()
}

pub(crate) fn deserialize_expandable_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    }
}

fn deserialize_optional_expandable_path<'de, D>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| match shellexpand::env(&s) {
        Ok(expanded) => Ok(PathBuf::from(expanded.into_owned())),
        Err(e) => Err(serde::de::Error::custom(format!(
            "Failed to expand environment variable: {}",
            e
        ))),
    })
    .transpose()
}

fn deserialize_expandable_hashmap<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    map.into_iter()
        .map(|(k, v)| match shellexpand::env(&v) {
            Ok(expanded) => Ok((k, expanded.into_owned())),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to expand environment variable: {}",
                e
            ))),
        })
        .collect()
}

impl Settings {
    pub fn max_bytes(&self) -> u64 {
        self.filesystem
            .as_ref()
            .map(|fs| fs.max_bytes())
            .unwrap_or(u64::MAX)
    }

    pub fn compression(&self) -> CompressionConfig {
        self.filesystem
            .as_ref()
            .map(|fs| fs.compression)
            .unwrap_or_default()
    }

    pub fn runtime_memory_limit_bytes(&self) -> Result<Option<u64>> {
        let Some(value) = self
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.memory_limit_gb)
        else {
            return Ok(None);
        };
        if !value.is_finite() || value <= 0.0 {
            anyhow::bail!("[runtime] memory_limit_gb must be a finite positive number");
        }
        let bytes = value * 1_000_000_000.0;
        if bytes > u64::MAX as f64 {
            anyhow::bail!("[runtime] memory_limit_gb is too large");
        }
        Ok(Some(bytes.round() as u64))
    }

    pub fn from_file(config_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = config_path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let mut settings: Settings = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        if settings.sftp_endpoint()?.is_some() && settings.sftp.is_none() {
            settings.sftp = Some(SftpConfig::default());
        }

        settings.validate()?;

        Ok(settings)
    }

    /// Cross-section validation applied after deserialization.
    pub fn validate(&self) -> Result<()> {
        self.servers.validate()?;
        self.runtime_memory_limit_bytes()?;
        if let Some(prometheus) = &self.prometheus
            && let Some(authority) = &prometheus.benchmark_authority
        {
            authority
                .validate(prometheus, &self.servers)
                .context("Invalid [prometheus.benchmark_authority] configuration")?;
        }

        // Parse the endpoint first so a malformed SFTP URL still reports the URL
        // error ahead of any capability diagnostic.
        if self.sftp_endpoint()?.is_some() {
            self.sftp.clone().unwrap_or_default().validate()?;
        }

        let backend = backend_capabilities(&self.storage.url);
        if !backend.supports_replication && self.replication.is_some() {
            anyhow::bail!(
                "[replication] is not supported with an {} storage backend; use single-node mode",
                backend.display_name
            );
        }
        if !backend.supports_storage_class && self.storage.storage_class.is_some() {
            anyhow::bail!(
                "[storage] storage_class is not supported with an {} backend",
                backend.display_name
            );
        }

        if let Some(replication) = &self.replication {
            replication
                .validate()
                .context("Invalid [replication] configuration")?;
        }

        self.writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)?;

        if self
            .servers
            .nbd
            .as_ref()
            .is_some_and(|nbd| nbd.write_ack_mode == NbdWriteAckMode::VolatileMemory)
        {
            if self.replication.is_some() {
                anyhow::bail!(
                    "[servers.nbd] volatile_memory acknowledgement is not supported with [replication]"
                );
            }
            if self
                .filesystem
                .as_ref()
                .is_some_and(|filesystem| filesystem.ignore_fsync)
            {
                anyhow::bail!(
                    "[servers.nbd] volatile_memory acknowledgement requires functional FLUSH/FUA; [filesystem] ignore_fsync must be false"
                );
            }
        }

        if self
            .writeback
            .as_ref()
            .is_some_and(|writeback| writeback.enabled)
            && self
                .filesystem
                .as_ref()
                .is_some_and(|filesystem| filesystem.ignore_fsync)
        {
            anyhow::bail!(
                "[filesystem] ignore_fsync is incompatible with [writeback]: fsync must retain the local SSD durability boundary"
            );
        }

        if let Some(fs) = &self.filesystem
            && fs.ignore_fsync
            && self.lsm.as_ref().map(|l| l.sync_writes()).unwrap_or(false)
        {
            anyhow::bail!(
                "[filesystem] ignore_fsync and [lsm] sync_writes are contradictory: \
                 sync_writes flushes after every write, ignore_fsync skips the fsync flush"
            );
        }

        // Deprecated [lsm] keys still parse (an upgrade must not brick startup
        // on an old config) but no longer do anything; nudge the operator to
        // drop them.
        if let Some(lsm) = &self.lsm {
            if lsm.wal_enabled.is_some() {
                tracing::warn!(
                    "[lsm] wal_enabled is deprecated and ignored (the WAL is permanently off); \
                     remove it from the config"
                );
            }
            if lsm.max_unflushed_gb.is_some() {
                tracing::warn!(
                    "[lsm] max_unflushed_gb is deprecated and ignored (it tuned the removed WAL); \
                     remove it from the config"
                );
            }
        }
        Ok(())
    }

    pub fn writeback_settings(
        &self,
        access_mode: crate::writeback::config::WritebackAccessMode,
    ) -> Result<Option<crate::writeback::config::WritebackSettings>> {
        let Some(writeback) = &self.writeback else {
            return Ok(None);
        };
        let sftp_write_concurrency = if self.sftp_endpoint()?.is_some() {
            Some(
                self.sftp
                    .as_ref()
                    .cloned()
                    .unwrap_or_default()
                    .write_concurrency,
            )
        } else {
            None
        };
        writeback.normalize(
            &self.cache.dir,
            sftp_write_concurrency,
            access_mode,
            self.replication.is_some(),
        )
    }

    /// Resolve the shared write-acknowledgement contract once, merging the
    /// authoritative `[filesystem]` form with the deprecated `[servers.nbd]`
    /// volatile fields. Every protocol adapter consumes the result.
    pub(crate) fn filesystem_write_ack_settings(
        &self,
        access_mode: crate::writeback::config::WritebackAccessMode,
    ) -> Result<FilesystemWriteAckSettings> {
        let filesystem = self.filesystem.as_ref();
        let nbd = self.servers.nbd.as_ref();
        FilesystemWriteAckRequest {
            configured_mode: filesystem.and_then(|fs| fs.write_ack_mode),
            volatile_memory_gb: filesystem.map_or(0.0, |fs| fs.volatile_memory_gb),
            volatile_max_operations: filesystem.and_then(|fs| fs.volatile_max_operations),
            legacy_nbd_volatile: nbd
                .is_some_and(|nbd| nbd.write_ack_mode == NbdWriteAckMode::VolatileMemory),
            legacy_nbd_volatile_memory_gb: nbd.map_or(0.0, |nbd| nbd.volatile_memory_gb),
            writeback_enabled: self
                .writeback
                .as_ref()
                .is_some_and(|writeback| writeback.enabled),
            replication_enabled: self.replication.is_some(),
            ignore_fsync: filesystem.is_some_and(|fs| fs.ignore_fsync),
            read_write_server: access_mode
                == crate::writeback::config::WritebackAccessMode::ReadWrite,
            largest_enabled_protocol_write: self.largest_enabled_protocol_write(),
        }
        .resolve()
    }

    /// Largest maximum write among the enabled write protocols, as
    /// `(protocol name, bytes)`. The volatile RAM budget must hold at least
    /// one such write or admission could deadlock on a single request.
    fn largest_enabled_protocol_write(&self) -> Option<(&'static str, u64)> {
        let servers = &self.servers;
        [
            ("NBD", servers.nbd.is_some(), NBD_MAX_WRITE_BYTES),
            ("9P", servers.ninep.is_some(), NINEP_MAX_WRITE_BYTES),
            ("WebUI", servers.webui.is_some(), WEBUI_MAX_WRITE_BYTES),
            ("NFS", servers.nfs.is_some(), NFS_MAX_WRITE_BYTES),
        ]
        .into_iter()
        .filter(|(_, enabled, _)| *enabled)
        .map(|(name, _, bytes)| (name, bytes))
        .max_by_key(|(_, bytes)| *bytes)
    }

    /// Data-plane tuning for the configured backend. SFTP publishes its own
    /// profile; every other backend runs on the defaults.
    pub fn store_profile(&self) -> Result<StoreProfile> {
        Ok(match self.sftp_endpoint()? {
            // `from_file` fills `[sftp]` whenever the URL is SFTP; fall back to
            // the same defaults `validate` uses so a Settings built in code
            // cannot turn a missing section into a panic.
            Some(_) => StoreProfile::from(self.sftp.clone().unwrap_or_default().data_profile()),
            None => StoreProfile::default(),
        })
    }

    /// Return normalized SFTP endpoint data without retaining URL credentials.
    pub fn sftp_endpoint(&self) -> Result<Option<SftpEndpoint>> {
        if !has_sftp_scheme(&self.storage.url) {
            return Ok(None);
        }

        let url =
            url::Url::parse(&self.storage.url).context("[storage] url is not a valid SFTP URL")?;
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .context("[storage] SFTP URL must include a host")?;
        if url.username().is_empty() {
            anyhow::bail!("[storage] SFTP URL must include a username");
        }
        if url.password().is_some() {
            anyhow::bail!(
                "[storage] SFTP URL must not contain a password; configure SSH key authentication"
            );
        }

        Ok(Some(SftpEndpoint {
            host: host.to_owned(),
            port: url.port().unwrap_or(22),
            username: url.username().to_owned(),
        }))
    }

    pub fn cloud_provider_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = Vec::new();
        if let Some(aws) = &self.aws {
            for (k, v) in &aws.0 {
                env_vars.push((format!("aws_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(azure) = &self.azure {
            for (k, v) in &azure.0 {
                env_vars.push((format!("azure_{}", k.to_lowercase()), v.clone()));
            }
        }
        if let Some(gcp) = &self.gcp {
            for (k, v) in &gcp.0 {
                env_vars.push((format!("google_{}", k.to_lowercase()), v.clone()));
            }
        }
        env_vars
    }

    pub fn generate_default() -> Self {
        let mut aws_config = std::collections::HashMap::new();
        aws_config.insert(
            "access_key_id".to_string(),
            "${AWS_ACCESS_KEY_ID}".to_string(),
        );
        aws_config.insert(
            "secret_access_key".to_string(),
            "${AWS_SECRET_ACCESS_KEY}".to_string(),
        );

        Settings {
            cache: CacheConfig {
                dir: PathBuf::from("${HOME}/.cache/zerofs"),
                disk_size_gb: 10.0,
                memory_size_gb: Some(1.0),
                warm_metadata: WarmMetadata::default(),
            },
            storage: StorageConfig {
                url: "s3://your-bucket/zerofs-data".to_string(),
                encryption_password: "${ZEROFS_PASSWORD}".to_string(),
                storage_class: None,
            },
            servers: ServerConfig {
                nfs: Some(NfsConfig {
                    addresses: Some(default_nfs_addresses()),
                    shared_identity: None,
                }),
                ninep: Some(NinePConfig {
                    addresses: Some(default_9p_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.9p.sock")),
                    shared_identity: None,
                }),
                nbd: Some(NbdConfig {
                    addresses: Some(default_nbd_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.nbd.sock")),
                    write_ack_mode: NbdWriteAckMode::default(),
                    volatile_memory_gb: 0.0,
                }),
                rpc: Some(RpcConfig {
                    addresses: Some(default_rpc_addresses()),
                    unix_socket: Some(PathBuf::from("/tmp/zerofs.rpc.sock")),
                }),
                webui: Some(WebUIConfig {
                    addresses: default_webui_addresses(),
                    uid: 1000,
                    gid: 1000,
                }),
            },
            runtime: None,
            filesystem: None,
            lsm: None,
            gc: None,
            aws: Some(AwsConfig(aws_config)),
            azure: None,
            gcp: None,
            sftp: None,
            writeback: None,
            wal: None,
            telemetry: None,
            prometheus: None,
            replication: None,
        }
    }

    pub fn render_default_config() -> Result<String> {
        let default = Self::generate_default();
        let mut toml_string = toml::to_string_pretty(&default)?;

        // Inject a commented storage_class hint into the [storage] section. It
        // can't be appended like the others below because [storage] is not the
        // last table in the serialized output.
        toml_string = toml_string.replace(
            "encryption_password = \"${ZEROFS_PASSWORD}\"\n",
            "encryption_password = \"${ZEROFS_PASSWORD}\"\n\
             # storage_class = \"...\"   # Optional object storage class/tier for all writes (provider-specific value).\n"
        );

        // Document warm_metadata in place (the [cache] table is not last, so the
        // hint can't be appended like the sections below).
        toml_string = toml_string.replace(
            "warm_metadata = \"filters_index\"\n",
            "# Keep the metadata block cache warm so reads don't pay cold object-store latency\n\
             # on startup and right after each compaction. Values:\n\
             #   \"filters_index\" (default)  warm metadata SST filters + indexes (small, bounded)\n\
             #   \"full\"                      also warm the metadata data blocks (metadata-heavy working sets)\n\
             #   \"off\"                       disable warming\n\
             warm_metadata = \"filters_index\"\n",
        );

        toml_string.push_str("\n# Optional AWS S3 settings (uncomment to use):\n");
        toml_string.push_str(
            "# endpoint = \"https://s3.us-east-1.amazonaws.com\"  # For S3-compatible services\n",
        );
        toml_string.push_str("# default_region = \"us-east-1\"\n");
        toml_string.push_str("# allow_http = \"true\"  # For non-HTTPS endpoints\n");
        toml_string.push_str("# conditional_put = \"redis://localhost:6379\"  # For S3-compatible stores without conditional put support\n");

        toml_string.push_str("\n# Optional filesystem configuration\n");
        toml_string
            .push_str("# Limit the maximum size of the filesystem to prevent unlimited growth\n");
        toml_string
            .push_str("# If not specified, defaults to 16 EiB, the maximum filesystem size\n");
        toml_string.push_str("#\n");
        toml_string.push_str("# Compression algorithm for extent data:\n");
        toml_string.push_str(
            "#   - \"zstd-{level}\" (default: \"zstd-3\"): Configurable compression (level 1-22)\n",
        );
        toml_string.push_str("#     Level 1 is fastest, level 22 is maximum compression\n");
        toml_string.push_str("#   - \"lz4\": Faster compression, lower ratio (prefer for write-throughput-bound workloads)\n");
        toml_string.push_str("#\n");
        toml_string
            .push_str("# Note: Compression can be changed at any time. Existing data remains\n");
        toml_string
            .push_str("# readable regardless of compression setting (auto-detected on read).\n");
        toml_string.push_str("\n# [filesystem]\n");
        toml_string.push_str("# max_size_gb = 100.0     # Limit filesystem to 100 GB\n");
        toml_string.push_str("# compression = \"zstd-3\"  # or \"lz4\", \"zstd-19\", etc.\n");
        toml_string.push_str("# ignore_fsync = false    # HA: make fsync a no-op, relying on the standby for durability (see [replication])\n");

        toml_string.push_str("\n# Optional LSM tree tuning parameters\n");
        toml_string
            .push_str("# Advanced performance tuning for the underlying LSM tree storage engine\n");
        toml_string.push_str("# Only modify these if you understand LSM tree behavior\n");
        toml_string.push_str("\n# [lsm]\n");
        toml_string.push_str("# l0_max_ssts = 256                # Max SST files in L0 before compaction (default: 256, min: 4)\n");
        toml_string.push_str("# max_concurrent_compactions = 2   # Max concurrent compaction operations (default: 2, min: 1)\n");
        toml_string.push_str("# flush_interval_secs = 30         # Interval between periodic flushes in seconds (default: 30, min: 5)\n");
        toml_string.push_str("# sync_writes = false              # Flush every write to object storage before returning success (default: false).\n");
        toml_string.push_str("                                   # Does NOT affect POSIX fsync semantics: explicit fsync from clients\n");
        toml_string.push_str("                                   # is always honored. This flag only governs writes between fsync calls. When on,\n");
        toml_string.push_str("                                   # they become durable on return instead of buffered until the next periodic flush.\n");
        toml_string.push_str("                                   # Expensive: the WAL is off, so each write forces a full seal + memtable flush.\n");

        toml_string
            .push_str("\n# Optional segment garbage-collection tuning. Governs the segment\n");
        toml_string.push_str("# reclamation loop.\n");
        toml_string.push_str("\n# [gc]\n");
        toml_string.push_str("# interval_secs = 60               # Pass interval while the store is active (default: 60, min: 5).\n");
        toml_string.push_str("#                                  # Below the ~30 s flush cadence, busy passes seal sub-1-MiB segments.\n");
        toml_string.push_str("# idle_interval_secs = 5           # Pass interval while a saturated backlog meets an idle store\n");
        toml_string.push_str("#                                  # (default: 5, min: 1, capped at interval_secs; equal = adaptation off).\n");
        toml_string.push_str("#                                  # A fast pass runs a full reclamation round plus ~2 small PUTs of\n");
        toml_string.push_str("#                                  # overhead; total fast-mode work is bounded by the backlog.\n");
        toml_string.push_str("# read_directed = true             # Reads steer compaction: nominations, seam heat, chain repacks\n");
        toml_string.push_str("#                                  # (default: true). Counter-driven reclamation is unaffected.\n");
        toml_string.push_str("# tail_scrub_min_dead_percent = 5  # Repack write-cold segments more than this % dead that normal\n");
        toml_string.push_str("#                                  # candidacy would strand forever (default: 5, range 1-50; 0 disables\n");
        toml_string.push_str("#                                  # the scrub).\n");
        toml_string.push_str("#                                  # Space overhead on write-cold data is capped at 1/(1 - floor/100)\n");
        toml_string.push_str("#                                  # of live bytes, paid with up to (100-floor)/floor bytes rewritten\n");
        toml_string.push_str("#                                  # per byte reclaimed, using leftover pass budget only.\n");
        toml_string.push_str("# min_batches_per_pass = 4         # Compaction batches a busy pass runs before yielding to load\n");
        toml_string.push_str("#                                  # (default: 4, min: 1); each drains one round budget. 1 restores\n");
        toml_string.push_str("#                                  # single-batch busy passes. Idle stores always drain the per-pass cap.\n");
        toml_string.push_str("# busy_backlog_interval_secs = 15  # Pass interval while busy AND dead space >= busy_backlog_dead_percent\n");
        toml_string.push_str("#                                  # (default: 15, min: 5, capped at interval_secs; equal disables).\n");
        toml_string.push_str("# busy_backlog_dead_percent = 20   # Dead-space percent at/above which busy_backlog_interval_secs\n");
        toml_string.push_str("#                                  # applies while the store is active (default: 20).\n");
        toml_string.push_str("# compact_round_max_mib = 256      # Per-round compaction budget in MiB (default: 256, range 64-4096):\n");
        toml_string.push_str("#                                  # live-byte selection cap, heat reserve (half), and stored-byte gather\n");
        toml_string.push_str("#                                  # RAM cap. Raise to pack over-reserve hot seams and lift dead-space\n");
        toml_string.push_str("#                                  # throughput per batch, at up to this many MiB peak gather RAM.\n");

        toml_string.push_str(
            "\n# Optional HA replication: a leader + standby pair over one object store, with\n",
        );
        toml_string
            .push_str("# automatic failover. Each node has its OWN config (node_id, role,\n");
        toml_string.push_str(
            "# replication_listen, and peers differ per node). See the High Availability docs.\n",
        );
        toml_string.push_str("\n# [replication]\n");
        toml_string.push_str("# node_id = \"node-a\"\n");
        toml_string.push_str("# role = \"leader\"                       # or \"standby\"\n");
        toml_string.push_str("# replication_listen = \"10.0.0.1:9000\"  # this node receives ships + heartbeats here\n");
        toml_string.push_str(
            "# peers = [\"10.0.0.2:9000\"]             # the other node's replication_listen\n",
        );
        toml_string.push_str(
            "# force_recovery = false                   # break-glass override; see HA recovery docs\n",
        );

        toml_string.push_str("\n# Optional Prometheus metrics endpoint\n");
        toml_string.push_str("# Exposes filesystem, LSM, and cache metrics in Prometheus format\n");
        toml_string.push_str("\n# [prometheus]\n");
        toml_string.push_str("# addresses = [\"127.0.0.1:9091\"]\n");
        toml_string.push_str("#\n");
        toml_string.push_str(
            "# Benchmark authority mode is TLS-only and supports one isolated NFS or 9P export.\n",
        );
        toml_string.push_str("# export_id must exactly match `findmnt -nro SOURCE -M <mountpoint>` on the benchmark host.\n");
        toml_string.push_str("# [prometheus.benchmark_authority]\n");
        toml_string.push_str("# adapter = \"nfs\"\n");
        toml_string.push_str("# export_id = \"10.10.10.30:/\"\n");
        toml_string.push_str("# tls_certificate = \"/etc/zerofs/metrics.crt\"\n");
        toml_string.push_str(
            "# tls_private_key = \"/etc/zerofs/metrics.key\"  # Must deny group/world access.\n",
        );

        toml_string.push_str("\n# Optional Azure settings can be added to [azure] section\n");

        // Add commented-out Azure section
        toml_string.push_str("\n# [azure]\n");
        toml_string.push_str("# storage_account_name = \"${AZURE_STORAGE_ACCOUNT_NAME}\"\n");
        toml_string.push_str("# storage_account_key = \"${AZURE_STORAGE_ACCOUNT_KEY}\"\n");

        toml_string.push_str("\n# Optional GCS (Google Cloud Storage) settings\n");
        toml_string.push_str("# Use gs:// URLs with the [gcp] section\n");

        // Add commented-out GCS section
        toml_string.push_str("\n# [gcp]\n");
        toml_string.push_str(
            "# service_account = \"${GCS_SERVICE_ACCOUNT}\"  # Path to service account JSON file\n",
        );
        toml_string
            .push_str("# Or use application_credentials = \"${GOOGLE_APPLICATION_CREDENTIALS}\"\n");

        toml_string.push_str("\n# Optional strict SFTP settings\n");
        toml_string.push_str(
            "# Hetzner Storage Box example: set [storage].url to\n\
             # sftp://u123456@u123456.your-storagebox.de:23/zerofs/v1\n",
        );
        toml_string
            .push_str("# Passwords in SFTP URLs are rejected; use SSH key authentication.\n");
        toml_string.push_str("# [sftp]\n");
        toml_string.push_str("# identity_file = \"${HOME}/.ssh/id_ed25519\"\n");
        toml_string.push_str("# known_hosts = \"${HOME}/.ssh/known_hosts\"\n");
        toml_string.push_str("# max_connections = 8\n");
        toml_string.push_str("# read_concurrency = 7\n");
        toml_string.push_str("# write_concurrency = 7\n");
        toml_string.push_str("# segment_size_mib = 32\n");
        toml_string.push_str("# read_cache_part_size_kib = 1024\n");

        toml_string.push_str("\n# Optional persistent dirty-data tier (disabled by default)\n");
        toml_string.push_str("# [writeback]\n");
        toml_string.push_str("# enabled = true\n");
        toml_string.push_str("# dir = \"/var/cache/zerofs-writeback\"\n");
        toml_string.push_str(
            "# ack_mode = \"memory\"           # memory (default) | ssd | remote; client flushes always reach the SSD journal\n",
        );
        toml_string.push_str(
            "# memory_size_gb = 16.0          # additional dirty-write RAM; does not consume [cache] memory\n",
        );
        toml_string.push_str("# disk_size_gb = 512.0\n");
        toml_string.push_str("# min_free_gb = 256.0\n");
        toml_string.push_str("# high_watermark_percent = 95\n");
        toml_string.push_str("# resume_percent = 85\n");
        toml_string.push_str("# local_concurrency = 4\n");
        toml_string.push_str(
            "# upload_concurrency = 4         # generic default; SFTP auto-defaults to 7 and pipelines 64 requests per session\n",
        );
        toml_string.push_str("# shutdown_flush = \"local\"       # local | remote\n");

        toml_string.push_str("\n# Anonymous telemetry (enabled by default)\n");
        toml_string.push_str(
            "# Shares anonymous usage data (version, OS, backend type, filesystem size) to help improve ZeroFS\n",
        );
        toml_string.push_str(
            "# No file contents, paths, or personally identifiable information is collected\n",
        );
        toml_string.push_str("\n# [telemetry]\n");
        toml_string.push_str("# enabled = false  # Set to false to disable\n");

        let commented = format!(
            "# ZeroFS Configuration File\n\
             # Generated by ZeroFS v{}\n\
             #\n\
             # ============================================================================\n\
             # ENVIRONMENT VARIABLE SUBSTITUTION\n\
             # ============================================================================\n\
             # This config file supports environment variable substitution.\n\
             # \n\
             # Supported syntax:\n\
             #   - ${{VAR}} or $VAR  : Environment variable substitution\n\
             # \n\
             # Examples:\n\
             #   encryption_password = \"${{ZEROFS_PASSWORD}}\"\n\
             #   dir = \"${{HOME}}/.cache/zerofs\"\n\
             #   access_key_id = \"${{AWS_ACCESS_KEY_ID}}\"\n\
             #   peers = [\"${{PEER_A_ADDR}}\", \"${{PEER_B_ADDR}}\"]\n\
             # \n\
             # In array values (e.g. [replication] peers) each entry is expanded on\n\
             # its own; a single variable is not split into multiple entries.\n\
             #\n\
             # All referenced environment variables must be set, or the config will fail to load.\n\
             #\n\
             # ============================================================================\n\
             # SERVER CONFIGURATION\n\
             # ============================================================================\n\
             # - To disable a server, remove or comment out its entire section\n\
             # - Unix sockets are optional for 9P and NBD servers\n\
             # - NFS only supports TCP connections\n\
             # - Each protocol supports multiple bind addresses\n\
             # \n\
             # Examples:\n\
             #   addresses = [\"127.0.0.1:2049\"]                  # IPv4 localhost only\n\
             #   addresses = [\"0.0.0.0:2049\"]                    # All IPv4 interfaces\n\
             #   addresses = [\"[::]:2049\"]                       # All IPv6 interfaces\n\
             #   addresses = [\"127.0.0.1:2049\", \"[::1]:2049\"]  # Both IPv4 and IPv6 localhost\n\
             #   addresses = [\"${{POD_IP}}:2049\"]                # env vars are expanded before the address is parsed,\n\
             #                                                  # so the host, the port, or the whole value may come from one\n\
             #\n\
             # ============================================================================\n\
             # CLOUD STORAGE\n\
             # ============================================================================\n\
             # - For S3: Configure [aws] section with your credentials\n\
             # - For Azure: Configure [azure] section with your credentials\n\
             # - For GCS: Configure [gcp] section or set GOOGLE_APPLICATION_CREDENTIALS env var\n\
             # - For local storage: Use file:// URLs (no cloud config needed)\n\
             # ============================================================================\n\
             \n{}",
            env!("CARGO_PKG_VERSION"),
            toml_string
        );

        Ok(commented)
    }

    pub fn write_default_config(path: impl AsRef<std::path::Path>) -> Result<()> {
        fs::write(path, Self::render_default_config()?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::NamedTempFile;

    #[test]
    fn test_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_PASSWORD", "secret123");
            env::set_var("ZEROFS_TEST_BUCKET", "my-bucket");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://${ZEROFS_TEST_BUCKET}/data"
encryption_password = "${ZEROFS_TEST_PASSWORD}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.storage.url, "s3://my-bucket/data");
        assert_eq!(settings.storage.encryption_password, "secret123");
    }

    #[test]
    fn test_home_env_var() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME", home_dir.to_str().unwrap());
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME}/test-cache"
disk_size_gb = 1.0

[storage]
url = "file://${ZEROFS_TEST_HOME}/data"
encryption_password = "test"

[servers]

[servers.ninep]
unix_socket = "${ZEROFS_TEST_HOME}/zerofs.sock"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        assert_eq!(settings.cache.dir, home_dir.join("test-cache"));
        assert_eq!(
            settings.storage.url,
            format!("file://{}/data", home_dir.display())
        );
        if let Some(ninep) = settings.servers.ninep {
            assert_eq!(ninep.unix_socket.unwrap(), home_dir.join("zerofs.sock"));
        } else {
            panic!("Expected 9P config");
        }
    }

    #[test]
    fn test_undefined_env_var_error() {
        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "${ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST}"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.contains("ZEROFS_TEST_UNDEFINED_VAR_THAT_SHOULD_NOT_EXIST"),
            "Error was: {}",
            error
        );
    }

    #[test]
    fn test_mixed_expansion() {
        let home_dir = env::home_dir().expect("HOME not set");
        unsafe {
            env::set_var("ZEROFS_TEST_HOME_MIX", home_dir.to_str().unwrap());
            env::set_var("ZEROFS_TEST_DIR_MIX", "mydir");
        }

        let config_content = r#"
[cache]
dir = "${ZEROFS_TEST_HOME_MIX}/${ZEROFS_TEST_DIR_MIX}/cache"
disk_size_gb = 1.0

[storage]
url = "file:///data"
encryption_password = "test"

[servers]
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(settings.cache.dir, home_dir.join("mydir/cache"));
    }

    #[test]
    fn test_aws_azure_config_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_AWS_KEY", "aws123");
            env::set_var("ZEROFS_TEST_AWS_SECRET", "aws_secret");
            env::set_var("ZEROFS_TEST_AZURE_KEY", "azure456");
        }

        let config_content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "${ZEROFS_TEST_AWS_KEY}"
secret_access_key = "${ZEROFS_TEST_AWS_SECRET}"

[azure]
storage_account_key = "${ZEROFS_TEST_AZURE_KEY}"
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_content).unwrap();

        let settings = Settings::from_file(temp_file.path().to_str().unwrap()).unwrap();

        let aws = settings.aws.unwrap();
        assert_eq!(aws.0.get("access_key_id").unwrap(), "aws123");
        assert_eq!(aws.0.get("secret_access_key").unwrap(), "aws_secret");

        let azure = settings.azure.unwrap();
        assert_eq!(azure.0.get("storage_account_key").unwrap(), "azure456");
    }

    #[test]
    fn test_aws_bool_values() {
        let config_with_bool = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = true
"#;

        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), config_with_bool).unwrap();

        // This should fail because we can't deserialize a bool into a String
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());

        // Now test with string "true"
        let config_with_string = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

[aws]
access_key_id = "key"
allow_http = "true"
"#;

        std::fs::write(temp_file.path(), config_with_string).unwrap();
        let result = Settings::from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_ok());
        let settings = result.unwrap();
        assert_eq!(settings.aws.unwrap().0.get("allow_http").unwrap(), "true");
    }

    fn base_config_with_replication(replication: &str) -> String {
        format!(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers]

{replication}
"#
        )
    }

    fn write_and_load(content: &str) -> Result<Settings> {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), content).unwrap();
        Settings::from_file(temp_file.path().to_str().unwrap())
    }

    #[test]
    fn rejects_server_configuration_without_a_listener_endpoint() {
        let error = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers]

[servers.nbd]
"#,
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("[servers.nbd]"),
            "unexpected error: {message}"
        );
        assert!(message.contains("endpoint"), "unexpected error: {message}");
    }

    #[test]
    fn nbd_volatile_memory_ack_requires_an_explicit_positive_budget() {
        let error = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
"#,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("volatile_memory_gb"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn nbd_volatile_memory_ack_normalizes_its_byte_budget() {
        let settings = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
"#,
        )
        .unwrap();

        let nbd = settings.servers.nbd.as_ref().unwrap();
        assert_eq!(nbd.write_ack_mode, NbdWriteAckMode::VolatileMemory);
        assert_eq!(nbd.volatile_memory_bytes().unwrap(), 2_000_000_000);
    }

    #[test]
    fn nbd_volatile_memory_budget_must_fit_one_maximum_write() {
        let error = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 0.125
"#,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("maximum NBD WRITE"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn nbd_volatile_memory_ack_allows_nfs_ninep_and_webui() {
        let settings = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 1.0

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[servers.ninep]
addresses = ["127.0.0.1:5564"]

[servers.webui]
addresses = ["127.0.0.1:8080"]
uid = 1000
gid = 1000
"#,
        )
        .expect("volatile acknowledgement must be able to run with NFS, 9P, and WebUI");
        assert!(settings.servers.nfs.is_some());
        assert!(settings.servers.ninep.is_some());
        assert!(settings.servers.webui.is_some());
    }

    #[test]
    fn serving_requires_at_least_one_configured_listener_endpoint() {
        let settings = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers]
"#,
        )
        .unwrap();

        let error = settings.servers.require_listener_endpoint().unwrap_err();
        assert!(
            error.to_string().contains("listener endpoint"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn unix_socket_is_a_real_listener_endpoint() {
        let settings = write_and_load(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers]

[servers.rpc]
unix_socket = "/tmp/zerofs-test.sock"
"#,
        )
        .unwrap();

        settings.servers.require_listener_endpoint().unwrap();
    }

    fn sftp_config(url: &str, extra: &str) -> String {
        format!(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = {url:?}
encryption_password = "test-password"

[servers]

{extra}
"#
        )
    }

    fn writeback_sftp_config(clean_memory_gb: f64, writeback: &str) -> String {
        format!(
            r#"
[cache]
dir = "/var/cache/zerofs/clean"
disk_size_gb = 512.0
memory_size_gb = {clean_memory_gb}

[storage]
url = "sftp://alice@example.com/data"
encryption_password = "test-password"

[servers]

{writeback}
"#
        )
    }

    #[test]
    fn writeback_disabled_by_default_has_no_runtime_budget() {
        let settings = write_and_load(&writeback_sftp_config(16.0, "")).unwrap();

        assert!(
            settings
                .writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn runtime_memory_limit_is_an_explicit_decimal_gb_fallback() {
        let config = writeback_sftp_config(1.0, "").replace(
            "[storage]",
            "[runtime]\nmemory_limit_gb = 96.0\n\n[storage]",
        );

        let settings = write_and_load(&config).unwrap();

        assert_eq!(
            settings.runtime_memory_limit_bytes().unwrap(),
            Some(96_000_000_000)
        );
    }

    #[test]
    fn proxmox_production_template_is_loadable_by_shipping_settings() {
        let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../proxmox/templates/zerofs-prod.toml.example");
        let template = std::fs::read_to_string(&template_path)
            .unwrap()
            .replace("${ZEROFS_STORAGE_PASSWORD}", "test-password");

        let settings = write_and_load(&template).unwrap();

        assert_eq!(
            settings.runtime_memory_limit_bytes().unwrap(),
            Some(96_000_000_000)
        );
        let nfs_identity = settings.servers.nfs.unwrap().shared_identity.unwrap();
        let ninep_identity = settings.servers.ninep.unwrap().shared_identity.unwrap();
        let webui = settings.servers.webui.unwrap();
        assert_eq!((nfs_identity.uid, nfs_identity.gid), (501, 20));
        assert_eq!((ninep_identity.uid, ninep_identity.gid), (501, 20));
        assert_eq!((webui.uid, webui.gid), (501, 20));
    }

    #[test]
    fn runtime_memory_limit_rejects_non_finite_and_non_positive_values() {
        for value in ["0.0", "-1.0", "nan", "inf"] {
            let config = writeback_sftp_config(1.0, "").replace(
                "[storage]",
                &format!("[runtime]\nmemory_limit_gb = {value}\n\n[storage]"),
            );

            let error = write_and_load(&config).unwrap_err();

            assert!(
                format!("{error:#}").contains("memory_limit_gb must be a finite positive number"),
                "unexpected error for {value}: {error:#}"
            );
        }
    }

    #[test]
    fn writeback_enabled_without_ack_mode_defaults_to_memory() {
        let settings = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0"#,
        ))
        .unwrap();

        let writeback = settings
            .writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)
            .unwrap()
            .unwrap();
        // Memory acknowledgement is the point of the tier: bursts land at RAM
        // speed while client flush barriers still force SSD durability. The
        // default upload concurrency fills the SFTP write-stream budget.
        // Per-session WRITE pipelining is 64; the leftover default of 4
        // upload lanes is not preserved.
        assert_eq!(
            writeback.ack_mode,
            crate::writeback::config::AckMode::Memory
        );
        assert_eq!(writeback.disk_bytes, 512_000_000_000);
        assert_eq!(writeback.local_concurrency, 4);
        assert_eq!(writeback.upload_concurrency, 7);
    }

    #[test]
    fn writeback_default_upload_concurrency_clamps_to_sftp_write_streams() {
        let settings = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[sftp]
write_concurrency = 2

[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0"#,
        ))
        .unwrap();

        let writeback = settings
            .writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)
            .unwrap()
            .unwrap();
        assert_eq!(
            writeback.upload_concurrency, 2,
            "a defaulted upload concurrency must follow a lowered SFTP write budget \
             instead of failing validation"
        );
    }

    #[test]
    fn writeback_local_concurrency_is_positive_and_bounded() {
        for value in [0, 257] {
            let error = write_and_load(&writeback_sftp_config(
                16.0,
                &format!(
                    r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
local_concurrency = {value}"#
                ),
            ))
            .unwrap_err();
            assert!(format!("{error:#}").contains("local_concurrency"));
        }
    }

    #[test]
    fn writeback_dirty_memory_budget_is_additional_to_clean_read_cache() {
        let settings = write_and_load(&writeback_sftp_config(
            11.0,
            r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
ack_mode = "memory"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
high_watermark_percent = 95
resume_percent = 85
upload_concurrency = 7
shutdown_flush = "local""#,
        ))
        .unwrap();

        let writeback = settings
            .writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)
            .unwrap()
            .unwrap();
        assert_eq!(settings.cache.memory_size_gb, Some(11.0));
        assert_eq!(writeback.memory_bytes, 16_000_000_000);
        assert_eq!(writeback.disk_bytes, 512_000_000_000);
        assert_eq!(writeback.min_free_bytes, 256_000_000_000);
    }

    #[test]
    fn writeback_memory_mode_requires_positive_independent_budgets() {
        for (field, memory, disk, reserve) in [
            ("memory_size_gb", 0.0, 512.0, 256.0),
            ("disk_size_gb", 16.0, 0.0, 256.0),
            ("min_free_gb", 16.0, 512.0, 0.0),
        ] {
            let body = format!(
                r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
ack_mode = "memory"
memory_size_gb = {memory}
disk_size_gb = {disk}
min_free_gb = {reserve}"#
            );
            let error = write_and_load(&writeback_sftp_config(16.0, &body)).unwrap_err();
            assert!(
                format!("{error:#}").contains(field),
                "field={field}: {error:#}"
            );
        }
    }

    #[test]
    fn writeback_remote_mode_still_requires_the_independent_ssd_journal() {
        let error = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
ack_mode = "remote"
memory_size_gb = 16.0
disk_size_gb = 0.0
min_free_gb = 0.0"#,
        ))
        .unwrap_err();

        assert!(format!("{error:#}").contains("disk_size_gb"));
    }

    #[test]
    fn writeback_watermark_hysteresis_must_be_ordered() {
        for (resume, high) in [(0, 95), (95, 95), (96, 95), (85, 101)] {
            let body = format!(
                r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
resume_percent = {resume}
high_watermark_percent = {high}"#
            );
            let error = write_and_load(&writeback_sftp_config(16.0, &body)).unwrap_err();
            assert!(
                format!("{error:#}").contains("resume_percent"),
                "resume={resume} high={high}: {error:#}"
            );
        }
    }

    #[test]
    fn writeback_upload_concurrency_cannot_exceed_sftp_write_concurrency() {
        let config = writeback_sftp_config(
            16.0,
            r#"[sftp]
write_concurrency = 4

[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
upload_concurrency = 5"#,
        );

        let error = write_and_load(&config).unwrap_err();
        assert!(format!("{error:#}").contains("upload_concurrency"));
    }

    #[test]
    fn writeback_directory_cannot_be_nested_in_clean_cache() {
        let error = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[writeback]
enabled = true
dir = "/var/cache/zerofs/clean/writeback"
disk_size_gb = 512.0
min_free_gb = 256.0"#,
        ))
        .unwrap_err();

        assert!(format!("{error:#}").contains("clean cache"));
    }

    #[test]
    fn writeback_rejects_read_only_and_checkpoint_servers() {
        let settings = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0"#,
        ))
        .unwrap();

        for mode in [
            crate::writeback::config::WritebackAccessMode::ReadOnly,
            crate::writeback::config::WritebackAccessMode::Checkpoint,
        ] {
            let error = settings.writeback_settings(mode).unwrap_err();
            assert!(format!("{error:#}").contains("read-write"));
        }
    }

    #[test]
    fn writeback_rejects_ignore_fsync() {
        let error = write_and_load(&writeback_sftp_config(
            16.0,
            r#"[filesystem]
ignore_fsync = true

[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0"#,
        ))
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("ignore_fsync"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn writeback_default_config_documents_separate_clean_and_dirty_memory() {
        let rendered = Settings::render_default_config().unwrap();

        assert!(rendered.contains("# [writeback]"));
        assert!(rendered.contains("# memory_size_gb = 16.0"));
        assert!(rendered.contains("additional dirty-write RAM"));
        assert!(rendered.contains("# ack_mode = \"memory\""));
    }

    #[test]
    fn sftp_defaults_are_strict_and_bounded() {
        let settings = write_and_load(&sftp_config("sftp://alice@example.com/data", "")).unwrap();
        let sftp = settings.sftp.as_ref().expect("effective SFTP defaults");

        assert!(sftp.identity_file.ends_with(".ssh/id_ed25519"));
        assert!(sftp.known_hosts.ends_with(".ssh/known_hosts"));
        assert_eq!(sftp.max_connections, 8);
        assert_eq!(sftp.read_concurrency, 7);
        assert_eq!(sftp.write_concurrency, 7);
        assert_eq!(sftp.segment_size_mib, 32);
        assert_eq!(sftp.read_cache_part_size_kib, 1024);

        let endpoint = settings.sftp_endpoint().unwrap().expect("SFTP endpoint");
        assert_eq!(endpoint.host, "example.com");
        assert_eq!(endpoint.username, "alice");
        assert_eq!(endpoint.port, 22);

        let rendered = Settings::render_default_config().unwrap();
        assert!(
            rendered.contains("# identity_file = \"${HOME}/.ssh/id_ed25519\""),
            "generated config must document native key auth"
        );
        assert!(
            !rendered.contains("ssh_program"),
            "native russh transport must not advertise an OpenSSH wrapper"
        );
    }

    #[test]
    fn sftp_segment_size_rejects_values_outside_the_packed_object_bounds() {
        for size in [0, 7, 257] {
            let extra =
                format!("[sftp]\nknown_hosts = \"/tmp/known_hosts\"\nsegment_size_mib = {size}");
            let error =
                write_and_load(&sftp_config("sftp://alice@example.com/data", &extra)).unwrap_err();
            assert!(
                error.to_string().contains("segment_size_mib"),
                "unexpected error for {size} MiB: {error}"
            );
        }

        for size in [8, 32, 256] {
            let extra =
                format!("[sftp]\nknown_hosts = \"/tmp/known_hosts\"\nsegment_size_mib = {size}");
            let settings =
                write_and_load(&sftp_config("sftp://alice@example.com/data", &extra)).unwrap();
            assert_eq!(settings.sftp.unwrap().segment_size_mib, size);
        }
    }

    #[test]
    fn sftp_data_profile_drives_segment_writes_and_cold_read_hydration() {
        let settings = write_and_load(&sftp_config(
            "sftp://alice@example.com/data",
            "[sftp]\nknown_hosts = \"/tmp/known_hosts\"\nwrite_concurrency = 3\nsegment_size_mib = 64\nread_cache_part_size_kib = 2048",
        ))
        .unwrap();
        let profile = settings.sftp.unwrap().data_profile();

        assert_eq!(profile.segment_size_bytes, 64 * 1024 * 1024);
        assert_eq!(profile.max_inflight_seals, 3);
        assert_eq!(profile.read_cache_part_size_bytes, 2 * 1024 * 1024);
        assert_eq!(profile.read_fetch_window_min_bytes, 2 * 1024 * 1024);
        assert_eq!(profile.read_fetch_window_max_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn sftp_hetzner_endpoint_keeps_explicit_port_23() {
        let settings = write_and_load(&sftp_config(
            "sftp://u123456@u123456.your-storagebox.de:23/zerofs/v1",
            "",
        ))
        .unwrap();

        assert_eq!(settings.sftp_endpoint().unwrap().unwrap().port, 23);
        let rendered = Settings::render_default_config().unwrap();
        assert!(
            rendered.contains("sftp://u123456@u123456.your-storagebox.de:23/zerofs/v1"),
            "generated Hetzner example must use its explicit SFTP port"
        );
    }

    #[test]
    fn sftp_known_hosts_path_expands_environment_variables() {
        unsafe {
            env::set_var(
                "ZEROFS_TEST_KNOWN_HOSTS",
                "/etc/zerofs/storagebox_known_hosts",
            );
        }
        let settings = write_and_load(&sftp_config(
            "sftp://alice@example.com/data",
            r#"[sftp]
known_hosts = "${ZEROFS_TEST_KNOWN_HOSTS}""#,
        ))
        .unwrap();

        assert_eq!(
            settings.sftp.unwrap().known_hosts,
            PathBuf::from("/etc/zerofs/storagebox_known_hosts")
        );
    }

    #[test]
    fn sftp_requires_host() {
        let err = format!(
            "{:#}",
            write_and_load(&sftp_config("sftp:///data", "")).unwrap_err()
        );
        assert_eq!(err, "[storage] SFTP URL must include a host");
    }

    #[test]
    fn sftp_requires_username() {
        let err = format!(
            "{:#}",
            write_and_load(&sftp_config("sftp://example.com/data", "")).unwrap_err()
        );
        assert_eq!(err, "[storage] SFTP URL must include a username");
    }

    #[test]
    fn sftp_rejects_password_in_url_without_echoing_it() {
        let secret = "login-secret-123";
        let err = format!(
            "{:#}",
            write_and_load(&sftp_config(
                &format!("sftp://alice:{secret}@example.com/data"),
                ""
            ))
            .unwrap_err()
        );

        assert!(err.contains("password"), "got: {err}");
        assert!(!err.contains(secret), "password leaked in error: {err}");
    }

    #[test]
    fn storage_config_debug_and_serialization_redact_sftp_url_password() {
        let secret = "storage-url-secret";
        let storage = StorageConfig {
            url: format!("sftp://alice:{secret}@example.com/data"),
            encryption_password: "volume-encryption-secret".to_owned(),
            storage_class: None,
        };

        let debug = format!("{storage:?}");
        let toml = toml::to_string(&storage).unwrap();
        let json = serde_json::to_string(&storage).unwrap();

        for rendered in [&debug, &toml, &json] {
            assert!(
                !rendered.contains(secret),
                "SFTP password leaked: {rendered}"
            );
        }
        assert!(
            toml.contains(r#"url = "sftp://alice@example.com/data""#),
            "sanitized URL was not preserved: {toml}"
        );
        assert!(
            json.contains(r#""url":"sftp://alice@example.com/data""#),
            "sanitized URL was not preserved: {json}"
        );
        assert!(
            !debug.contains("volume-encryption-secret"),
            "encryption password leaked through Debug: {debug}"
        );
        assert!(
            toml.contains("volume-encryption-secret"),
            "config serialization must retain the encryption password"
        );
    }

    #[test]
    fn settings_debug_and_serialization_redact_sftp_url_password() {
        let secret = "settings-url-secret";
        let settings: Settings = toml::from_str(&sftp_config(
            &format!("sftp://alice:{secret}@example.com/data"),
            "",
        ))
        .unwrap();

        for rendered in [
            format!("{settings:?}"),
            toml::to_string(&settings).unwrap(),
            serde_json::to_string(&settings).unwrap(),
        ] {
            assert!(
                !rendered.contains(secret),
                "SFTP password leaked: {rendered}"
            );
        }
    }

    #[test]
    fn sftp_endpoint_debug_is_host_only() {
        let settings =
            write_and_load(&sftp_config("sftp://alice@example.com:23/data", "")).unwrap();
        let endpoint = settings.sftp_endpoint().unwrap().unwrap();

        let debug = format!("{endpoint:?}");

        assert!(debug.contains("example.com"), "got: {debug}");
        assert!(debug.contains("23"), "got: {debug}");
        assert!(!debug.contains("alice"), "username leaked: {debug}");
    }

    #[test]
    fn sftp_connection_and_direction_limits_are_strict() {
        let invalid = [
            ("max_connections = 0", "max_connections"),
            ("max_connections = 9", "max_connections"),
            ("read_concurrency = 0", "read_concurrency"),
            ("read_concurrency = 8", "read_concurrency"),
            ("write_concurrency = 0", "write_concurrency"),
            ("write_concurrency = 8", "write_concurrency"),
            (
                "max_connections = 4\nread_concurrency = 5",
                "read_concurrency",
            ),
            (
                "max_connections = 4\nread_concurrency = 4\nwrite_concurrency = 5",
                "write_concurrency",
            ),
        ];

        for (limits, expected) in invalid {
            let extra = format!("[sftp]\nknown_hosts = \"/tmp/known_hosts\"\n{limits}");
            let err = format!(
                "{:#}",
                write_and_load(&sftp_config("sftp://alice@example.com/data", &extra)).unwrap_err()
            );
            assert!(err.contains(expected), "limits {limits:?}: got {err}");
        }
    }

    #[test]
    fn sftp_rejects_empty_known_hosts_path_and_insecure_mode() {
        for (extra, expected) in [
            ("[sftp]\nidentity_file = \"\"", "identity_file"),
            ("[sftp]\nknown_hosts = \"\"", "known_hosts"),
            (
                "[sftp]\ninsecure_skip_host_key_check = true",
                "unknown field",
            ),
        ] {
            let err = format!(
                "{:#}",
                write_and_load(&sftp_config("sftp://alice@example.com/data", extra)).unwrap_err()
            );
            assert!(err.contains(expected), "got: {err}");
        }
    }

    #[test]
    fn sftp_rejects_replication_and_storage_class() {
        for extra in [
            r#"[replication]
node_id = "n1"
role = "leader""#,
            r#"[sftp]
known_hosts = "/tmp/known_hosts""#,
        ] {
            let mut content = sftp_config("sftp://alice@example.com/data", extra);
            if extra.starts_with("[sftp]") {
                content = content.replace(
                    "encryption_password = \"test-password\"",
                    "encryption_password = \"test-password\"\nstorage_class = \"STANDARD\"",
                );
            }
            let err = format!("{:#}", write_and_load(&content).unwrap_err());
            assert!(
                err.contains("replication") || err.contains("storage_class"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn non_sftp_storage_does_not_apply_sftp_limits() {
        let settings = write_and_load(&sftp_config(
            "s3://bucket/data",
            "[sftp]\nknown_hosts = \"\"\nmax_connections = 0",
        ))
        .unwrap();

        assert!(settings.sftp.is_some());
        assert!(settings.sftp_endpoint().unwrap().is_none());
    }

    #[test]
    fn test_no_replication_is_single_node() {
        let content = base_config_with_replication("");
        let settings = write_and_load(&content).unwrap();
        assert!(settings.replication.is_none());
    }

    #[test]
    fn test_replication_defaults_validate() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader""#,
        );
        let settings = write_and_load(&content).unwrap();
        let repl = settings.replication.unwrap();
        assert_eq!(repl.node_id, "n1");
        assert_eq!(repl.role, ReplicationRole::Leader);
    }

    #[test]
    fn test_replication_standby_role() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n2"
role = "standby"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600"]"#,
        );
        let settings = write_and_load(&content).unwrap();
        assert_eq!(settings.replication.unwrap().role, ReplicationRole::Standby);
    }

    #[test]
    fn test_replication_standby_without_listen_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n2"
role = "standby""#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("replication_listen"), "got: {err}");
    }

    #[test]
    fn test_replication_invalid_listen_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "standby"
replication_listen = "not-an-address""#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("socket address"), "got: {err}");
    }

    #[test]
    fn test_replication_self_peer_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5599"]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("must not replicate to itself"), "got: {err}");
    }

    #[test]
    fn test_replication_balanced_pair_validates() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600"]"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.peers, vec!["127.0.0.1:5600".to_string()]);
    }

    #[test]
    fn test_replication_bad_role_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "witness""#,
        );
        assert!(write_and_load(&content).is_err());
    }

    #[test]
    fn test_replication_empty_node_id_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = ""
role = "leader""#,
        );
        assert!(write_and_load(&content).is_err());
    }

    #[test]
    fn replication_node_id_rejects_surrounding_whitespace() {
        for node_id in [" n1", "n1 "] {
            let content = base_config_with_replication(&format!(
                r#"[replication]
node_id = {node_id:?}
role = "leader""#
            ));
            let err = format!("{:#}", write_and_load(&content).unwrap_err());
            assert!(
                err.contains("leading or trailing whitespace"),
                "node_id {node_id:?}: got {err}"
            );
        }
    }

    #[test]
    fn test_ignore_fsync_parses() {
        let content = base_config_with_replication(
            r#"[filesystem]
ignore_fsync = true"#,
        );
        let settings = write_and_load(&content).unwrap();
        assert!(settings.filesystem.unwrap().ignore_fsync);
    }

    #[test]
    fn gc_section_parses_defaults_and_clamps() {
        // Absent section: every accessor returns the historical behavior.
        let gc: GcConfig = toml::from_str("").unwrap();
        assert_eq!(gc.interval_secs(), GcConfig::DEFAULT_INTERVAL_SECS);
        assert_eq!(
            gc.idle_interval_secs(),
            GcConfig::DEFAULT_IDLE_INTERVAL_SECS
        );
        assert!(gc.read_directed());
        assert_eq!(
            gc.tail_scrub_min_dead_percent(),
            Some(GcConfig::DEFAULT_TAIL_SCRUB_MIN_DEAD_PERCENT)
        );
        assert_eq!(
            gc.min_batches_per_pass(),
            GcConfig::DEFAULT_MIN_BATCHES_PER_PASS
        );
        assert_eq!(
            gc.busy_backlog_interval_secs(),
            GcConfig::DEFAULT_BUSY_BACKLOG_INTERVAL_SECS
        );
        assert_eq!(
            gc.busy_backlog_dead_percent(),
            GcConfig::DEFAULT_BUSY_BACKLOG_DEAD_PERCENT
        );
        assert_eq!(
            gc.compact_round_bytes(),
            GcConfig::DEFAULT_COMPACT_ROUND_MAX_MIB << 20
        );

        // Out-of-range values clamp silently
        let gc: GcConfig = toml::from_str(
            "interval_secs = 2\nidle_interval_secs = 30\n\
             tail_scrub_min_dead_percent = 90\nread_directed = false\n\
             min_batches_per_pass = 0\nbusy_backlog_interval_secs = 999\n\
             compact_round_max_mib = 999999\n",
        )
        .unwrap();
        assert_eq!(gc.interval_secs(), GcConfig::MIN_INTERVAL_SECS);
        assert_eq!(gc.idle_interval_secs(), GcConfig::MIN_INTERVAL_SECS);
        assert_eq!(gc.tail_scrub_min_dead_percent(), Some(50));
        assert!(!gc.read_directed());
        // min_batches floors at 1; busy-backlog caps at interval_secs; round
        // budget caps at the RAM ceiling.
        assert_eq!(gc.min_batches_per_pass(), 1);
        assert_eq!(gc.busy_backlog_interval_secs(), gc.interval_secs());
        assert_eq!(
            gc.compact_round_bytes(),
            GcConfig::MAX_COMPACT_ROUND_MAX_MIB << 20
        );

        // Lower clamps: round budget floors at 64 MiB, busy-backlog interval at
        // the flush floor (MIN_INTERVAL_SECS).
        let gc: GcConfig =
            toml::from_str("compact_round_max_mib = 1\nbusy_backlog_interval_secs = 1\n").unwrap();
        assert_eq!(
            gc.compact_round_bytes(),
            GcConfig::MIN_COMPACT_ROUND_MAX_MIB << 20
        );
        assert_eq!(gc.busy_backlog_interval_secs(), GcConfig::MIN_INTERVAL_SECS);

        // 0 is off, not a clamp to the most aggressive floor.
        let gc: GcConfig = toml::from_str("tail_scrub_min_dead_percent = 0").unwrap();
        assert_eq!(gc.tail_scrub_min_dead_percent(), None);

        // A [gc] table parses as part of Settings.
        let content = base_config_with_replication(
            r#"[gc]
interval_secs = 120
tail_scrub_min_dead_percent = 10"#,
        );
        let settings = write_and_load(&content).unwrap();
        let gc = settings.gc.unwrap();
        assert_eq!(gc.interval_secs(), 120);
        assert_eq!(gc.tail_scrub_min_dead_percent(), Some(10));
    }

    // Pre-2.0 configs set the removed [lsm] wal_enabled / max_unflushed_gb
    // keys; they must still parse (ignored, with a warning) so an upgrade
    // doesn't brick startup, and must be dropped on re-serialization.
    #[test]
    fn test_deprecated_lsm_keys_parse_and_round_trip() {
        let content = base_config_with_replication(
            r#"[lsm]
wal_enabled = true
max_unflushed_gb = 2.0
flush_interval_secs = 30"#,
        );
        let settings = write_and_load(&content).unwrap();
        let lsm = settings.lsm.as_ref().unwrap();
        assert_eq!(lsm.wal_enabled, Some(true));
        assert_eq!(lsm.max_unflushed_gb, Some(2.0));
        assert_eq!(lsm.flush_interval_secs, Some(30));

        // Round-trip: the deprecated keys are never re-emitted, and the
        // result still parses.
        let serialized = toml::to_string(&settings).unwrap();
        assert!(!serialized.contains("wal_enabled"), "got: {serialized}");
        assert!(
            !serialized.contains("max_unflushed_gb"),
            "got: {serialized}"
        );
        let reparsed: Settings = toml::from_str(&serialized).unwrap();
        let lsm = reparsed.lsm.unwrap();
        assert!(lsm.wal_enabled.is_none());
        assert!(lsm.max_unflushed_gb.is_none());
        assert_eq!(lsm.flush_interval_secs, Some(30));
    }

    // deny_unknown_fields still catches typos: only the two deprecated keys
    // get a pass.
    #[test]
    fn test_unknown_lsm_key_still_rejected() {
        let content = base_config_with_replication(
            r#"[lsm]
wal_enable = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("unknown field"), "got: {err}");
    }

    // The generated config must not resurrect the deprecated keys, nor
    // advertise the [wal] section (accepted only for upgraded 1.x volumes;
    // new volumes never write a WAL).
    #[test]
    fn test_generated_config_omits_deprecated_lsm_keys() {
        let rendered = Settings::render_default_config().unwrap();
        assert!(!rendered.contains("wal_enabled"));
        assert!(!rendered.contains("max_unflushed_gb"));
        assert!(!rendered.contains("[wal]"));
    }

    // A pre-2.0 config with a custom [wal] location must keep parsing: the
    // upgraded volume needs it to open (WAL replay / checkpoint references).
    #[test]
    fn test_wal_section_still_parses() {
        let content = base_config_with_replication(
            r#"[wal]
url = "file:///mnt/nvme/zerofs-wal""#,
        );
        let settings = write_and_load(&content).unwrap();
        assert_eq!(
            settings.wal.as_ref().map(|w| w.url.as_str()),
            Some("file:///mnt/nvme/zerofs-wal")
        );
    }

    #[test]
    fn test_ignore_fsync_with_sync_writes_rejected() {
        let content = base_config_with_replication(
            r#"[filesystem]
ignore_fsync = true

[lsm]
sync_writes = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("contradictory"), "got: {err}");
    }

    #[test]
    fn compression_config_serializes_to_canonical_strings() {
        assert_eq!(
            serde_json::to_string(&CompressionConfig::Lz4).unwrap(),
            "\"lz4\""
        );
        assert_eq!(
            serde_json::to_string(&CompressionConfig::Zstd(7)).unwrap(),
            "\"zstd-7\""
        );
    }

    #[test]
    fn compression_config_parses_valid_strings() {
        let parse = |s: &str| serde_json::from_str::<CompressionConfig>(s).unwrap();
        assert_eq!(parse("\"lz4\""), CompressionConfig::Lz4);
        assert_eq!(parse("\"zstd-1\""), CompressionConfig::Zstd(1));
        assert_eq!(parse("\"zstd-22\""), CompressionConfig::Zstd(22));
    }

    #[test]
    fn compression_config_round_trips() {
        for cfg in [
            CompressionConfig::Lz4,
            CompressionConfig::Zstd(1),
            CompressionConfig::Zstd(3),
            CompressionConfig::Zstd(22),
        ] {
            let s = serde_json::to_string(&cfg).unwrap();
            assert_eq!(serde_json::from_str::<CompressionConfig>(&s).unwrap(), cfg);
        }
    }

    #[test]
    fn compression_config_rejects_bad_strings() {
        for bad in [
            "\"zstd-0\"",   // below the 1..=22 range
            "\"zstd-23\"",  // above the range
            "\"zstd-abc\"", // non-numeric level
            "\"zstd-\"",    // missing level
            "\"gzip\"",     // unknown algorithm
            "\"\"",         // empty
        ] {
            assert!(
                serde_json::from_str::<CompressionConfig>(bad).is_err(),
                "expected {bad} to be rejected"
            );
        }
    }

    #[test]
    fn compression_config_default_is_zstd_3() {
        assert_eq!(CompressionConfig::default(), CompressionConfig::Zstd(3));
    }

    #[test]
    fn replication_role_round_trips() {
        for role in [ReplicationRole::Leader, ReplicationRole::Standby] {
            let s = serde_json::to_string(&role).unwrap();
            assert_eq!(serde_json::from_str::<ReplicationRole>(&s).unwrap(), role);
        }
        assert_eq!(
            serde_json::to_string(&ReplicationRole::Standby).unwrap(),
            "\"standby\""
        );
    }

    #[test]
    fn replication_leader_with_peers_but_no_listen_is_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
peers = ["127.0.0.1:5600"]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(
            err.contains("requires peers and replication_listen together"),
            "got: {err}"
        );
    }

    #[test]
    fn replication_leader_with_listen_but_no_peers_is_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599""#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(
            err.contains("requires peers and replication_listen together"),
            "got: {err}"
        );
    }

    #[test]
    fn replication_force_recovery_requires_leader() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "standby"
replication_listen = "127.0.0.1:5599"
force_recovery = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("requires role = \"leader\""), "got: {err}");
    }

    #[test]
    fn replication_force_recovery_requires_no_peers() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
peers = ["127.0.0.1:5600"]
force_recovery = true"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("requires peers = []"), "got: {err}");
    }

    #[test]
    fn replication_force_recovery_is_explicit_and_valid_for_solo_leader() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
force_recovery = true"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert!(repl.force_recovery);
    }

    #[test]
    fn replication_empty_peer_entry_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600", ""]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("empty entry"), "got: {err}");
    }

    #[test]
    fn replication_more_than_one_peer_is_rejected() {
        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "127.0.0.1:5599"
peers = ["127.0.0.1:5600", "127.0.0.1:5601"]"#,
        );
        let err = format!("{:#}", write_and_load(&content).unwrap_err());
        assert!(err.contains("at most one address"), "got: {err}");
    }

    #[test]
    fn test_replication_node_id_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_NODE_ID", "my-node");
        }

        let content = base_config_with_replication(
            r#"[replication]
node_id = "${ZEROFS_TEST_NODE_ID}"
role = "leader""#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.node_id, "my-node");
    }

    #[test]
    fn test_replication_listen_and_peers_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_LISTEN", "10.0.0.1:9000");
            env::set_var("ZEROFS_TEST_PEER", "10.0.0.2:9000");
        }

        let content = base_config_with_replication(
            r#"[replication]
node_id = "n1"
role = "leader"
replication_listen = "${ZEROFS_TEST_LISTEN}"
peers = ["${ZEROFS_TEST_PEER}"]"#,
        );
        let repl = write_and_load(&content).unwrap().replication.unwrap();
        assert_eq!(repl.replication_listen.as_deref(), Some("10.0.0.1:9000"));
        assert_eq!(repl.peers, vec!["10.0.0.2:9000".to_string()]);
    }

    #[test]
    fn test_storage_class_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_STORAGE_CLASS", "INTELLIGENT_TIERING");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"
storage_class = "${ZEROFS_TEST_STORAGE_CLASS}"

[servers]
"#;
        let settings = write_and_load(content).unwrap();
        assert_eq!(
            settings.storage.storage_class.as_deref(),
            Some("INTELLIGENT_TIERING")
        );
    }

    #[test]
    fn test_server_addresses_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_NFS_ADDR", "0.0.0.0:2049");
            env::set_var("ZEROFS_TEST_PROM_ADDR", "0.0.0.0:9091");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_NFS_ADDR}"]

[prometheus]
addresses = ["${ZEROFS_TEST_PROM_ADDR}"]
"#;
        let settings = write_and_load(content).unwrap();
        let nfs = settings.servers.nfs.unwrap().addresses.unwrap();
        assert!(nfs.contains(&"0.0.0.0:2049".parse().unwrap()));
        let prom = settings.prometheus.unwrap().addresses;
        assert!(prom.contains(&"0.0.0.0:9091".parse().unwrap()));
    }

    #[test]
    fn benchmark_authority_valid_single_nfs_source_loads() {
        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["10.10.10.30:2049"]

[prometheus]
addresses = ["10.10.10.30:9567"]

[prometheus.benchmark_authority]
adapter = "nfs"
export_id = "10.10.10.30:/"
tls_certificate = "/etc/zerofs/metrics.crt"
tls_private_key = "/etc/zerofs/metrics.key"
"#;

        let authority = write_and_load(content)
            .unwrap()
            .prometheus
            .unwrap()
            .benchmark_authority
            .unwrap();
        assert_eq!(authority.adapter, BenchmarkAdapter::Nfs);
        assert_eq!(authority.export_id, "10.10.10.30:/");
        assert_eq!(
            authority.tls_certificate,
            PathBuf::from("/etc/zerofs/metrics.crt")
        );
        assert_eq!(
            authority.tls_private_key,
            PathBuf::from("/etc/zerofs/metrics.key")
        );
    }

    #[test]
    fn benchmark_authority_rejects_invalid_label_bytes() {
        for invalid in [
            "",
            "host:/?query=1",
            "user@host:/",
            "host:/#fragment",
            "host:/ with-space",
            "host:/\nother",
        ] {
            let error = BenchmarkAuthorityConfig::validate_label(invalid, "export_id")
                .expect_err("invalid authority label must fail");
            assert!(
                error.to_string().contains("[A-Za-z0-9._:/-]+"),
                "unexpected error for {invalid:?}: {error:#}"
            );
        }
    }

    #[test]
    fn benchmark_authority_rejects_non_isolated_or_mismatched_nfs_exports() {
        let valid = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["10.10.10.30:2049"]

[prometheus]
addresses = ["10.10.10.30:9567"]

[prometheus.benchmark_authority]
adapter = "nfs"
export_id = "10.10.10.30:/"
tls_certificate = "/etc/zerofs/metrics.crt"
tls_private_key = "/etc/zerofs/metrics.key"
"#;

        for (name, content, expected) in [
            (
                "mismatched source",
                valid.replace(
                    "export_id = \"10.10.10.30:/\"",
                    "export_id = \"10.10.10.31:/\"",
                ),
                "must exactly match the NFS source",
            ),
            (
                "multiple NFS endpoints",
                valid.replace(
                    "addresses = [\"10.10.10.30:2049\"]",
                    "addresses = [\"10.10.10.30:2049\", \"10.10.10.31:2049\"]",
                ),
                "exactly one NFS endpoint",
            ),
            (
                "additional 9P export",
                valid.replace(
                    "[prometheus]",
                    "[servers.ninep]\naddresses = [\"10.10.10.30:5564\"]\n\n[prometheus]",
                ),
                "isolated NFS export",
            ),
            (
                "multiple metrics listeners",
                valid.replace(
                    "addresses = [\"10.10.10.30:9567\"]",
                    "addresses = [\"10.10.10.30:9567\", \"127.0.0.1:9567\"]",
                ),
                "exactly one Prometheus address",
            ),
            (
                "relative certificate",
                valid.replace(
                    "tls_certificate = \"/etc/zerofs/metrics.crt\"",
                    "tls_certificate = \"metrics.crt\"",
                ),
                "tls_certificate must be an absolute path",
            ),
        ] {
            let error = write_and_load(&content).expect_err(name);
            let message = format!("{error:#}");
            assert!(
                message.contains(expected),
                "unexpected {name} error: {message}"
            );
        }
    }

    #[test]
    fn test_nfs_shared_identity_is_opt_in_and_preserves_configured_ids() {
        let configured = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[servers.nfs.shared_identity]
uid = 501
gid = 20
"#;
        let shared_identity = write_and_load(configured)
            .unwrap()
            .servers
            .nfs
            .unwrap()
            .shared_identity
            .unwrap();
        assert_eq!(shared_identity.uid, 501);
        assert_eq!(shared_identity.gid, 20);

        let default = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["127.0.0.1:2049"]
"#;
        assert!(
            write_and_load(default)
                .unwrap()
                .servers
                .nfs
                .unwrap()
                .shared_identity
                .is_none()
        );
    }

    #[test]
    fn test_nfs_shared_identity_requires_both_ids() {
        let incomplete = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[servers.nfs.shared_identity]
uid = 501
"#;
        let error = format!("{:#}", write_and_load(incomplete).unwrap_err());
        assert!(error.contains("missing field `gid`"), "got: {error}");
    }

    // Expansion runs on the whole string before it is parsed, so a variable can
    // supply just the host (or just the port) with the rest written literally.
    #[test]
    fn test_address_partial_env_var_expansion() {
        unsafe {
            env::set_var("ZEROFS_TEST_HOST", "10.0.0.7");
            env::set_var("ZEROFS_TEST_PORT", "2049");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_HOST}:2049", "127.0.0.1:${ZEROFS_TEST_PORT}"]
"#;
        let nfs = write_and_load(content)
            .unwrap()
            .servers
            .nfs
            .unwrap()
            .addresses
            .unwrap();
        assert!(nfs.contains(&"10.0.0.7:2049".parse().unwrap()));
        assert!(nfs.contains(&"127.0.0.1:2049".parse().unwrap()));
    }

    #[test]
    fn test_address_invalid_after_expansion_rejected() {
        unsafe {
            env::set_var("ZEROFS_TEST_BAD_ADDR", "not-an-address");
        }

        let content = r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "s3://bucket/data"
encryption_password = "test"

[servers.nfs]
addresses = ["${ZEROFS_TEST_BAD_ADDR}"]
"#;
        let err = format!("{:#}", write_and_load(content).unwrap_err());
        assert!(err.contains("socket address"), "got: {err}");
    }

    // The generated config serializes its default addresses as SocketAddr
    // strings; they must re-parse through the expandable-address deserializer.
    #[test]
    fn test_generated_config_round_trips() {
        unsafe {
            env::set_var("ZEROFS_PASSWORD", "pw");
            env::set_var("AWS_ACCESS_KEY_ID", "key");
            env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
        }
        let rendered = Settings::render_default_config().unwrap();
        let settings = write_and_load(&rendered).unwrap();
        assert!(
            settings
                .servers
                .nfs
                .unwrap()
                .addresses
                .unwrap()
                .contains(&"127.0.0.1:2049".parse().unwrap())
        );
    }

    use crate::fs::mutation::config::{
        ClientDurabilityTarget, DEFAULT_VOLATILE_MAX_OPERATIONS, FilesystemWriteAckMode,
        FilesystemWriteAckSource, MAX_VOLATILE_MAX_OPERATIONS,
    };
    use crate::writeback::config::WritebackAccessMode;

    /// Parse without running `Settings::validate`, so the resolver's own
    /// diagnostics are observable even where an unrelated cross-section rule
    /// (the legacy NBD exclusivity rule, writeback/ignore_fsync) fires first
    /// during `from_file`.
    fn parse_settings(sections: &str) -> Settings {
        toml::from_str(&format!(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers]

{sections}
"#
        ))
        .unwrap()
    }

    const ENABLED_WRITEBACK: &str = r#"
[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
"#;

    fn resolve_write_ack(
        sections: &str,
    ) -> Result<crate::fs::mutation::config::FilesystemWriteAckSettings> {
        parse_settings(sections).filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
    }

    #[test]
    fn filesystem_write_ack_defaults_to_materialized_when_omitted() {
        let ack = resolve_write_ack(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]"#,
        )
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::Materialized);
        assert_eq!(ack.source, FilesystemWriteAckSource::DefaultMaterialized);
        assert_eq!(ack.volatile_memory_bytes, 0);
        assert_eq!(ack.volatile_max_operations, DEFAULT_VOLATILE_MAX_OPERATIONS);
        assert_eq!(
            ack.client_durability_target,
            ClientDurabilityTarget::RemoteBackend
        );
    }

    #[test]
    fn filesystem_write_ack_explicit_materialized_reports_filesystem_source() {
        let ack = resolve_write_ack(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "materialized""#,
        )
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::Materialized);
        assert_eq!(ack.source, FilesystemWriteAckSource::Filesystem);
        assert_eq!(
            ack.client_durability_target,
            ClientDurabilityTarget::RemoteBackend
        );
    }

    #[test]
    fn filesystem_write_ack_materialized_with_writeback_targets_local_ssd() {
        let ack = resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::Materialized);
        assert_eq!(
            ack.client_durability_target,
            ClientDurabilityTarget::LocalSsd
        );
    }

    #[test]
    fn filesystem_write_ack_explicit_volatile_normalizes_budget() {
        let ack = resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(ack.source, FilesystemWriteAckSource::Filesystem);
        assert_eq!(ack.volatile_memory_bytes, 2_000_000_000);
        assert_eq!(ack.volatile_max_operations, DEFAULT_VOLATILE_MAX_OPERATIONS);
        assert_eq!(
            ack.client_durability_target,
            ClientDurabilityTarget::LocalSsd
        );
    }

    #[test]
    fn filesystem_write_ack_volatile_op_cap_is_positive_and_bounded() {
        let explicit = resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
volatile_max_operations = 1024
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();
        assert_eq!(explicit.volatile_max_operations, 1024);

        for invalid in [0usize, MAX_VOLATILE_MAX_OPERATIONS + 1] {
            let error = resolve_write_ack(&format!(
                r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
volatile_max_operations = {invalid}
{ENABLED_WRITEBACK}"#
            ))
            .unwrap_err();
            assert!(
                format!("{error:#}").contains("volatile_max_operations"),
                "unexpected error for {invalid}: {error:#}"
            );
        }
    }

    #[test]
    fn filesystem_write_ack_legacy_nbd_only_normalizes_to_shared_volatile() {
        let ack = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(ack.source, FilesystemWriteAckSource::LegacyNbd);
        assert_eq!(ack.volatile_memory_bytes, 2_000_000_000);
        assert_eq!(
            ack.client_durability_target,
            ClientDurabilityTarget::LocalSsd
        );
    }

    #[test]
    fn legacy_nbd_inputs_normalize_without_exclusivity() {
        let ack = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[servers.ninep]
addresses = ["127.0.0.1:5564"]
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(ack.source, FilesystemWriteAckSource::LegacyNbd);
        assert_eq!(ack.volatile_memory_bytes, 2_000_000_000);
    }

    #[test]
    fn filesystem_write_ack_matching_dual_forms_normalize_once() {
        let ack = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(
            ack.source,
            FilesystemWriteAckSource::MatchingFilesystemAndLegacyNbd
        );
        assert_eq!(ack.volatile_memory_bytes, 2_000_000_000);
    }

    #[test]
    fn filesystem_write_ack_conflicting_dual_budgets_fail() {
        let error = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 1.0

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("must agree exactly"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn filesystem_write_ack_conflicting_dual_modes_fail() {
        let error = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

[filesystem]
write_ack_mode = "materialized"
{ENABLED_WRITEBACK}"#
        ))
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("conflicts"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn filesystem_write_ack_legacy_materialized_defers_to_explicit_setting() {
        // A legacy NBD section left at its materialized default must not
        // veto (or dilute the source of) an explicitly configured shared
        // setting.
        let ack = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(ack.source, FilesystemWriteAckSource::Filesystem);
    }

    #[test]
    fn filesystem_write_ack_rejects_invalid_budgets() {
        for budget in ["0.0", "-1.0", "inf", "nan"] {
            let error = resolve_write_ack(&format!(
                r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = {budget}
{ENABLED_WRITEBACK}"#
            ))
            .unwrap_err();
            assert!(
                format!("{error:#}").contains("volatile_memory_gb"),
                "unexpected error for {budget}: {error:#}"
            );
        }

        // A budget without volatile mode is a configuration mistake, exactly
        // like the legacy NBD rule.
        let error = resolve_write_ack(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "materialized"
volatile_memory_gb = 1.0"#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("only valid"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn filesystem_write_ack_volatile_requires_enabled_writeback() {
        for writeback in ["", "[writeback]\nenabled = false"] {
            let error = resolve_write_ack(&format!(
                r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

{writeback}"#
            ))
            .unwrap_err();
            assert!(
                format!("{error:#}").contains("writeback"),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn filesystem_write_ack_volatile_requires_a_read_write_server() {
        let settings = parse_settings(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ));

        for mode in [
            WritebackAccessMode::ReadOnly,
            WritebackAccessMode::Checkpoint,
        ] {
            let error = settings.filesystem_write_ack_settings(mode).unwrap_err();
            assert!(
                format!("{error:#}").contains("read-write"),
                "unexpected error: {error:#}"
            );
        }
        settings
            .filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
            .unwrap();
    }

    #[test]
    fn filesystem_write_ack_volatile_rejects_replication() {
        let error = resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}
[replication]
node_id = "node-a"
role = "leader""#
        ))
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("[replication]"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn filesystem_write_ack_volatile_rejects_ignore_fsync() {
        let error = resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
ignore_fsync = true
{ENABLED_WRITEBACK}"#
        ))
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("ignore_fsync"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn filesystem_write_ack_volatile_budget_must_hold_largest_protocol_write() {
        // 0.05 GB < the 128 MiB maximum NBD WRITE.
        let error = resolve_write_ack(&format!(
            r#"[servers.nbd]
addresses = ["127.0.0.1:10809"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 0.05
{ENABLED_WRITEBACK}"#
        ))
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("NBD"),
            "unexpected error: {error:#}"
        );

        // 0.005 GB < the 10 MiB maximum 9P message.
        let error = resolve_write_ack(&format!(
            r#"[servers.ninep]
addresses = ["127.0.0.1:5564"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 0.005
{ENABLED_WRITEBACK}"#
        ))
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("9P"),
            "unexpected error: {error:#}"
        );

        // 0.002 GB >= the 1 MiB NFS wtmax: small budgets are fine when every
        // enabled protocol's maximum write fits.
        resolve_write_ack(&format!(
            r#"[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 0.002
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();
    }

    #[test]
    fn filesystem_write_ack_generated_config_selects_materialized() {
        let ack = Settings::generate_default()
            .filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
            .unwrap();
        assert_eq!(ack.mode, FilesystemWriteAckMode::Materialized);
        assert_eq!(ack.source, FilesystemWriteAckSource::DefaultMaterialized);

        let rendered = Settings::render_default_config().unwrap();
        assert!(
            rendered.contains("write_ack_mode = \"materialized\""),
            "generated config no longer shows the materialized default"
        );
    }

    #[test]
    fn filesystem_write_ack_valid_volatile_config_loads_from_file() {
        let settings = write_and_load(&format!(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0
{ENABLED_WRITEBACK}"#
        ))
        .unwrap();

        let ack = settings
            .filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
            .unwrap();
        assert_eq!(ack.mode, FilesystemWriteAckMode::VolatileMemory);
        assert_eq!(ack.volatile_memory_bytes, 2_000_000_000);
    }
}
