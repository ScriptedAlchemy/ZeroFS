//! Catalog hooks for metadata survival: complete logical export, payload
//! retention for retained recovery generations, and a side-effect-free
//! read-only open of an exactly imported catalog in recovery scratch.

use super::catalog::{self, Catalog, CatalogExport, Physical};
use super::{PoolMember, PoolObjectStore};
use crate::config::SegmentPlacement;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

impl PoolObjectStore {
    pub(super) fn survival_retains(&self, physical: &Physical) -> bool {
        if self
            .inner
            .survival_hold_all
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return true;
        }
        let generation = catalog::physical_generation(physical);
        self.inner
            .survival_retention
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .any(|retained| retained.contains(&generation))
    }

    /// The authority object store and the database prefix within it. The
    /// survival owner enumerates and reads authority objects only through
    /// this object-store interface.
    pub(crate) fn survival_authority(&self) -> (Arc<dyn ObjectStore>, Path) {
        (
            Arc::clone(&self.inner.authority),
            self.inner.authority_prefix.clone(),
        )
    }

    /// Export every catalog row from one consistent redb snapshot.
    pub(crate) async fn export_catalog(&self) -> object_store::Result<CatalogExport> {
        self.catalog(Catalog::export_logical).await
    }

    /// Pin the physical generations a survival generation depends on.
    pub(crate) fn retain_survival_generation(
        &self,
        sequence: u64,
        export: &CatalogExport,
    ) -> anyhow::Result<usize> {
        let generations = catalog::exported_physical_generations(export)?;
        let count = generations.len();
        self.inner
            .survival_retention
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(sequence, generations);
        Ok(count)
    }

    /// Drop pins for every generation except `keep`.
    pub(crate) fn release_survival_generations(&self, keep: &BTreeSet<u64>) {
        self.inner
            .survival_retention
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .retain(|sequence, _| keep.contains(sequence));
    }

    /// Reclaim garbage that no retained generation still needs. Retained
    /// garbage stays charged and is skipped.
    pub(crate) async fn cleanup_released_survival_garbage(&self) -> object_store::Result<usize> {
        let (_, garbage) = self.catalog(Catalog::recovery).await?;
        let mut cleaned = 0;
        for debt in garbage {
            if self.survival_retains(&debt.entry.physical) {
                continue;
            }
            self.cleanup_garbage(&debt).await?;
            cleaned += 1;
        }
        Ok(cleaned)
    }

    /// Open an exact catalog import for offline read-only recovery.
    ///
    /// This bypasses member configuration, marker creation, initialization
    /// and pending-debt recovery, all of which write catalog or member state.
    /// Members whose storage is unavailable stay configured with a store that
    /// fails every request, so their ranges read as I/O errors while the
    /// catalog still names them.
    pub(crate) async fn open_recovered_read_only(
        authority: Arc<dyn ObjectStore>,
        authority_prefix: Path,
        catalog_root: PathBuf,
        export: CatalogExport,
        members: Vec<PoolMember>,
        segment_placement: SegmentPlacement,
    ) -> anyhow::Result<Self> {
        let catalog =
            tokio::task::spawn_blocking(move || Catalog::import_recovered(&catalog_root, &export))
                .await??;
        anyhow::ensure!(
            catalog.identity.authority_prefix == authority_prefix.to_string(),
            "recovered catalog belongs to authority prefix {:?}, not {:?}",
            catalog.identity.authority_prefix,
            authority_prefix.to_string()
        );
        Ok(Self::assemble(
            authority,
            authority_prefix,
            catalog,
            members,
            segment_placement,
            None,
        ))
    }

    /// Member `(identity, capacity)` by id, decoded from an export.
    pub(crate) fn member_records_from_export(
        export: &CatalogExport,
    ) -> anyhow::Result<std::collections::BTreeMap<String, (String, u64)>> {
        let mut records = std::collections::BTreeMap::new();
        for bytes in export
            .tables
            .get("pool_members")
            .ok_or_else(|| anyhow::anyhow!("catalog export has no member table"))?
            .values()
        {
            let record: catalog::MemberRecord = bincode::deserialize(bytes)?;
            records.insert(record.id, (record.identity, record.capacity));
        }
        Ok(records)
    }
}

/// A raw pool member, which carries a survival region in its object log.
pub(crate) struct SurvivalMember {
    pub id: String,
    /// The label binding `RawStore::open` verified: pool and member UUID.
    pub identity: String,
    pub store: Arc<dyn ObjectStore>,
    /// False when the device did not answer at startup.
    pub available: bool,
}

/// Raw members are identified by their label locator.
fn is_raw_member(member: &PoolMember) -> bool {
    member.identity.starts_with("raw:v1:")
}

impl PoolObjectStore {
    /// Every raw member of the pool, in configuration order.
    pub(crate) fn survival_members(&self) -> Vec<SurvivalMember> {
        self.inner
            .members
            .iter()
            .filter(|member| is_raw_member(member))
            .map(|member| SurvivalMember {
                id: member.id.clone(),
                identity: member.identity.clone(),
                store: Arc::clone(&member.store),
                available: self.inner.unavailable.as_deref() != Some(member.id.as_str()),
            })
            .collect()
    }

    /// Publish the survival owner's latest status for `pool status`.
    pub(crate) fn set_survival_status(&self, status: crate::metadata_survival::SurvivalStatus) {
        *self
            .inner
            .survival_status
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(status);
    }

    pub(super) fn survival_status(&self) -> Option<crate::metadata_survival::SurvivalStatus> {
        self.inner
            .survival_status
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// The survival owner has re-established every retention pin from the
    /// authenticated records, so the startup hold is no longer needed.
    pub(crate) fn survival_retention_reconciled(&self) {
        self.inner
            .survival_hold_all
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Rebuild the catalog of a lost authority on a blank raw catalog region
    /// and assemble a pool around it without startup side effects. Returns
    /// the pool and the authority marker of the rebound catalog identity.
    pub(crate) async fn open_recovered_raw(
        authority: Arc<dyn ObjectStore>,
        authority_prefix: Path,
        region: zerofs_raw_device::journal_region::TransactionRegion,
        export: CatalogExport,
        members: Vec<PoolMember>,
        segment_placement: SegmentPlacement,
    ) -> anyhow::Result<(Self, Vec<u8>)> {
        let catalog =
            tokio::task::spawn_blocking(move || Catalog::restore_raw(region, &export)).await??;
        anyhow::ensure!(
            catalog.identity.authority_prefix == authority_prefix.to_string(),
            "recovered catalog belongs to authority prefix {:?}, not {:?}",
            catalog.identity.authority_prefix,
            authority_prefix.to_string()
        );
        let marker = catalog.identity.marker()?;
        Ok((
            Self::assemble(
                authority,
                authority_prefix,
                catalog,
                members,
                segment_placement,
                None,
            ),
            marker,
        ))
    }

    /// Load the payload pins recorded by retained survival generations.
    ///
    /// A directory authority keeps them in its own survival region. On the
    /// raw-device topology they live in each raw member's region. Records are
    /// not authenticated here, and keeping bytes is always safe. When raw
    /// members exist but none of their regions can be read, every cleanup
    /// debt is held until the survival owner reconciles.
    pub(super) async fn load_durable_survival_retention(&self) {
        let retention = Path::from(".zerofs-survival").join("v1").join("retention");
        let authority = self
            .inner
            .authority_prefix
            .clone()
            .join(".zerofs-survival")
            .join("v1")
            .join("retention");
        let _ = self
            .load_retention_records(self.inner.authority.as_ref(), &authority)
            .await;
        let mut raw_members = 0;
        let mut readable = 0;
        for member in self.inner.members.iter().filter(|m| is_raw_member(m)) {
            raw_members += 1;
            if self.inner.unavailable.as_deref() == Some(member.id.as_str()) {
                continue;
            }
            match self
                .load_retention_records(member.store.as_ref(), &retention)
                .await
            {
                Ok(()) => readable += 1,
                Err(error) => {
                    tracing::warn!(member = %member.id, %error, "survival retention records are unreadable on this member");
                }
            }
        }
        if raw_members > 0 && readable == 0 {
            tracing::warn!(
                "no raw member's survival retention could be read; holding every cleanup debt"
            );
            self.inner
                .survival_hold_all
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn load_retention_records(
        &self,
        store: &dyn ObjectStore,
        directory: &Path,
    ) -> object_store::Result<()> {
        let listed: Vec<object_store::ObjectMeta> =
            futures::TryStreamExt::try_collect(store.list(Some(directory))).await?;
        for meta in listed {
            let Some(sequence) = meta
                .location
                .filename()
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|stem| stem.parse::<u64>().ok())
            else {
                continue;
            };
            let bytes = store.get(&meta.location).await?.bytes().await?;
            let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                tracing::warn!(location = %meta.location, "unreadable survival retention record; refusing to reclaim nothing extra");
                continue;
            };
            let generations: BTreeSet<String> = record
                .get("physical_generations")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            self.inner
                .survival_retention
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .entry(sequence)
                .or_default()
                .extend(generations);
        }
        Ok(())
    }
}
