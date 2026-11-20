//! The storage contract a metadata-survival replica must provide.
//!
//! Survival logic (generations, authentication, ordering, retention, recovery
//! selection) lives above this trait and is backend-independent. A backend
//! owns one *survival region* on one physical device and stores small keyed
//! objects in it. It makes no filesystem assumptions visible to the caller:
//! no directory rename atomicity, directory fsync, hard links, path markers
//! or mounted-filesystem free-space probes.
//!
//! `MemberRegion` in `member_region.rs` keeps a region in a raw pool member's
//! object log. `LocalDirectoryBackend` in `replica.rs` keeps one in a
//! directory, and the acceptance contracts run on it. See
//! `docs/metadata-survival.md`.
//!
//! Keys are normalized relative slash paths such as `commits/1/7.json`,
//! `objects/<sha256>` or `identity.json`. Values are opaque bytes; the caller
//! authenticates and hashes everything it reads.

use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Presence {
    /// The expected device is present and its region header matches.
    Present,
    /// No device answers at this location: a lost device. Never recreate.
    Absent,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReplicaError {
    #[error("replica device is absent")]
    Absent,
    #[error("replica survival region is full")]
    Full,
    #[error("replica survival region is fenced by another writer")]
    Locked,
    #[error("replica survival region is not initialized")]
    Uninitialized,
    #[error("replica I/O failed: {0}")]
    Io(String),
}

/// A writer or reader fence. Dropping it releases the fence.
pub(crate) trait Fence: Send + Sync + std::fmt::Debug {}

/// Bytes written under a key but not yet visible to `get` or `list`.
#[derive(Debug)]
pub(crate) struct Staged {
    pub(crate) key: String,
    /// Backend-private handle for the staged bytes.
    pub(crate) token: String,
}

#[async_trait::async_trait]
pub(crate) trait ReplicaBackend: Send + Sync + std::fmt::Debug {
    /// Human-readable location for errors and status.
    fn describe(&self) -> String;

    /// Is the expected device here? Must distinguish a missing device
    /// (`Absent`) from an empty one. Performs no writes.
    async fn probe(&self) -> Result<Presence, ReplicaError>;

    /// Is the survival region initialized on this device?
    async fn initialized(&self) -> Result<bool, ReplicaError>;

    /// Create an empty survival region. Only for first activation or an
    /// explicit offline replacement; refuses a region that already exists.
    async fn initialize(&self) -> Result<(), ReplicaError>;

    /// Exclusive writer fence held for the writer lifetime. A second writer,
    /// in this process or another, gets `Locked`. Must not be breakable by a
    /// reader and must not depend on deleting a possibly held lock object.
    async fn fence_exclusive(&self) -> Result<Box<dyn Fence>, ReplicaError>;

    /// Shared fence for offline recovery; fails with `Locked` while a writer
    /// holds the exclusive fence. Never creates or modifies region state.
    async fn fence_shared(&self) -> Result<Box<dyn Fence>, ReplicaError>;

    /// A visible object's complete bytes, or `None`.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ReplicaError>;

    /// A visible object's length, or `None`.
    async fn len(&self, key: &str) -> Result<Option<u64>, ReplicaError>;

    /// Names of visible objects directly under `prefix` (no trailing slash),
    /// sorted. Staged or torn objects are never listed.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ReplicaError>;

    /// Write `bytes` for `key` without making them visible. The bytes must be
    /// durable on the device before `stage` returns (a crash after it leaves
    /// at most an unreferenced staged object the backend may reclaim).
    async fn stage(&self, key: &str, bytes: Bytes) -> Result<Staged, ReplicaError>;

    /// Atomically make staged bytes visible under their key, replacing any
    /// previous version: every reader sees the old or the new complete
    /// object, never a mixture or a torn object.
    async fn publish(&self, staged: Staged) -> Result<(), ReplicaError>;

    /// Durable barrier: every `publish` and `delete` that returned before
    /// this call survives power loss once it returns.
    async fn flush(&self, key_hint: &str) -> Result<(), ReplicaError>;

    /// Remove a visible object. Missing objects are not an error.
    async fn delete(&self, key: &str) -> Result<(), ReplicaError>;

    /// Bytes the region's visible objects occupy. When the region cannot
    /// take another object, `stage` or `publish` returns `Full` and writes
    /// nothing.
    async fn usage(&self) -> Result<u64, ReplicaError>;
}
