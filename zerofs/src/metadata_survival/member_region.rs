//! A survival region inside a raw pool member.
//!
//! On the raw-device topology the authority NVMe holds the namespace
//! database, the placement catalog and the key envelope, and every HDD member
//! is one `RawStore` object log behind `RawMemberStore`. This adapter keeps a
//! member's recovery generations in that same log under the reserved key
//! namespace `.zerofs-survival/v1/`. Payload keys all start with
//! `.zerofs-pool/`, so the two never collide.
//!
//! How the [`ReplicaBackend`] contract is met:
//!
//! - **publish** is one object-store PUT. `RawMemberStore` appends the record
//!   and flushes both label slots before it returns, so a reader sees the
//!   complete old or the complete new object and an acknowledged PUT survives
//!   power loss.
//! - **stage** holds the bytes in memory until `publish`. Nothing staged is
//!   ever visible, and a crash before `publish` leaves nothing behind. One
//!   object is staged at a time, so an abandoned one is replaced by the next.
//! - **flush** has nothing left to do: every PUT and DELETE is already durable
//!   when it returns.
//! - **fence**: the pool holds the exclusive device claim for the life of the
//!   process, which excludes every other process. Within the process one
//!   holder per device is enforced here.
//! - **identity** is the member's label binding (pool UUID and member UUID),
//!   which `RawStore::open` verified against the device.
//! - **bounded region**: the region shares the member's log, so it has no
//!   size of its own. Exhaustion is the member's exact space admission
//!   refusing the PUT, reported as `Full`. Nothing is written in that case.

use super::backend::{Fence, Presence, ReplicaBackend, ReplicaError, Staged};
use super::format::ROOT;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode};
use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};

/// Marks an initialized region. It is adapter-private and never listed.
const MARKER: &str = "region";

static HELD: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

#[derive(Debug)]
struct MemberFence(String);

impl Fence for MemberFence {}

impl Drop for MemberFence {
    fn drop(&mut self) {
        HELD.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.0);
    }
}

#[derive(Debug)]
pub(crate) struct MemberRegion {
    member_id: String,
    identity: String,
    store: Arc<dyn ObjectStore>,
    presence: Presence,
    staged: Mutex<Option<(String, Bytes)>>,
}

impl MemberRegion {
    /// A region on an opened member. `identity` is the member's label binding.
    pub(crate) fn new(member_id: &str, identity: &str, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            member_id: member_id.to_owned(),
            identity: identity.to_owned(),
            store,
            presence: Presence::Present,
            staged: Mutex::default(),
        }
    }

    /// A member whose device did not answer at startup. It is lost, never an
    /// empty region to recreate.
    pub(crate) fn absent(member_id: &str, identity: &str, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            presence: Presence::Absent,
            ..Self::new(member_id, identity, store)
        }
    }

    fn path(&self, key: &str) -> Result<Path, ReplicaError> {
        let relative =
            super::format::safe_relative(key).map_err(|e| ReplicaError::Io(format!("{e:#}")))?;
        let mut path = Path::from(ROOT);
        for part in relative.components() {
            path = path.join(part.as_os_str().to_string_lossy().as_ref());
        }
        Ok(path)
    }

    fn require_present(&self) -> Result<(), ReplicaError> {
        match self.presence {
            Presence::Present => Ok(()),
            Presence::Absent => Err(ReplicaError::Absent),
        }
    }

    fn fence(&self) -> Result<Box<dyn Fence>, ReplicaError> {
        self.require_present()?;
        if !HELD
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(self.identity.clone())
        {
            return Err(ReplicaError::Locked);
        }
        Ok(Box::new(MemberFence(self.identity.clone())))
    }
}

fn io(error: object_store::Error) -> ReplicaError {
    if crate::retrying_object_store::is_no_space(&error) {
        ReplicaError::Full
    } else {
        ReplicaError::Io(error.to_string())
    }
}

#[async_trait::async_trait]
impl ReplicaBackend for MemberRegion {
    fn describe(&self) -> String {
        format!("pool member {} ({})", self.member_id, self.identity)
    }

    async fn probe(&self) -> Result<Presence, ReplicaError> {
        Ok(self.presence)
    }

    async fn initialized(&self) -> Result<bool, ReplicaError> {
        Ok(self.len(MARKER).await?.is_some())
    }

    async fn initialize(&self) -> Result<(), ReplicaError> {
        self.require_present()?;
        if self.len("identity.json").await?.is_some() {
            return Err(ReplicaError::Io(
                "survival region already holds an identity".into(),
            ));
        }
        match self
            .store
            .put_opts(
                &self.path(MARKER)?,
                Bytes::from_static(b"zerofs-survival-region-v1").into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    async fn fence_exclusive(&self) -> Result<Box<dyn Fence>, ReplicaError> {
        self.fence()
    }

    /// Offline readers hold the device claim themselves, so a shared fence
    /// is the same in-process exclusion a writer takes.
    async fn fence_shared(&self) -> Result<Box<dyn Fence>, ReplicaError> {
        if !self.initialized().await? {
            return Err(ReplicaError::Uninitialized);
        }
        self.fence()
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ReplicaError> {
        self.require_present()?;
        match self.store.get(&self.path(key)?).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(io)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    async fn len(&self, key: &str) -> Result<Option<u64>, ReplicaError> {
        self.require_present()?;
        match self.store.head(&self.path(key)?).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(io(error)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ReplicaError> {
        self.require_present()?;
        let directory = if prefix.is_empty() {
            Path::from(ROOT)
        } else {
            self.path(prefix)?
        };
        let listing = self
            .store
            .list_with_delimiter(Some(&directory))
            .await
            .map_err(io)?;
        let mut names: Vec<String> = listing
            .objects
            .iter()
            .filter_map(|meta| meta.location.filename().map(str::to_owned))
            .filter(|name| !(prefix.is_empty() && name == MARKER))
            .collect();
        names.sort();
        Ok(names)
    }

    async fn stage(&self, key: &str, bytes: Bytes) -> Result<Staged, ReplicaError> {
        self.require_present()?;
        self.path(key)?;
        let token = uuid::Uuid::new_v4().to_string();
        *self
            .staged
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some((token.clone(), bytes));
        Ok(Staged {
            key: key.to_owned(),
            token,
        })
    }

    async fn publish(&self, staged: Staged) -> Result<(), ReplicaError> {
        self.require_present()?;
        let bytes = self
            .staged
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .filter(|(token, _)| *token == staged.token)
            .map(|(_, bytes)| bytes)
            .ok_or_else(|| ReplicaError::Io("staged object is unknown".into()))?;
        self.store
            .put(&self.path(&staged.key)?, bytes.into())
            .await
            .map(|_| ())
            .map_err(io)
    }

    async fn flush(&self, _key_hint: &str) -> Result<(), ReplicaError> {
        self.require_present()
    }

    async fn delete(&self, key: &str) -> Result<(), ReplicaError> {
        self.require_present()?;
        match self.store.delete(&self.path(key)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    async fn usage(&self) -> Result<u64, ReplicaError> {
        self.require_present()?;
        let objects: Vec<object_store::ObjectMeta> = self
            .store
            .list(Some(&Path::from(ROOT)))
            .try_collect()
            .await
            .map_err(io)?;
        Ok(objects.iter().map(|meta| meta.size).sum())
    }
}
