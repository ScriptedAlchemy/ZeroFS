//! Explicit synthetic device binding. The persisted locator is independent
//! of Linux's ephemeral loop number and tmpfs staging paths.
use anyhow::{Context, Result, bail, ensure};
use std::path::{Component, PathBuf};
use url::Url;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub(crate) struct RawBinding {
    pub node: PathBuf,
    pub backing: PathBuf,
    pub pool: Uuid,
    pub member: Uuid,
    pub layout: Option<[u64; 6]>,
}

fn path(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(
        path.is_absolute()
            && path.file_name().is_some()
            && !path.components().any(|c| matches!(c, Component::ParentDir))
            && !value.contains('\0'),
        "expected absolute raw device or backing path without parent traversal"
    );
    Ok(path)
}

impl RawBinding {
    pub(crate) fn parse(url: &Url) -> Result<Self> {
        ensure!(
            cfg!(target_os = "linux")
                && matches!(url.scheme(), "raw+loop" | "raw+p3-v2")
                && url.host().is_none()
                && url.fragment().is_none()
                && !url.path().contains('%'),
            "raw+loop requires an absolute Linux device path without host or fragment"
        );
        let node = path(url.path())?;
        let mut backing = None;
        let mut layout = None;
        let mut pool = None;
        let mut member = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "backing" if backing.is_none() => backing = Some(path(&value)?),
                "pool" if pool.is_none() => pool = Some(Uuid::parse_str(&value)?),
                "member" if member.is_none() => member = Some(Uuid::parse_str(&value)?),
                "layout_mib" if url.scheme() == "raw+p3-v2" && layout.is_none() => {
                    let lengths = value
                        .split(',')
                        .map(|n| -> Result<_> {
                            n.parse::<u64>()?
                                .checked_mul(1 << 20)
                                .filter(|len| *len > 0)
                                .context("invalid P3 role length")
                        })
                        .collect::<Result<Vec<_>>>()?;
                    layout = Some(
                        lengths
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("P3 layout requires six lengths"))?,
                    );
                }
                _ => bail!("duplicate or unrecognized raw device binding field"),
            }
        }
        let pool = pool.context("missing raw pool UUID")?;
        let member = member.context("missing raw member UUID")?;
        ensure!(
            !pool.is_nil() && !member.is_nil(),
            "nil raw device binding UUID"
        );
        ensure!(
            url.scheme() != "raw+p3-v2" || layout.is_some(),
            "missing explicit P3 layout_mib"
        );
        Ok(Self {
            layout,
            node,
            backing: backing.context("missing raw backing path")?,
            pool,
            member,
        })
    }

    pub(crate) fn locator(&self) -> String {
        format!("raw:v1:{}:{}:data", self.pool, self.member)
    }
}

#[cfg(target_os = "linux")]
pub(super) async fn open_member(
    config: &crate::config::StoragePoolMemberConfig,
    binding: RawBinding,
    exact_size: bool,
) -> Result<crate::pool_object_store::PoolMember> {
    use crate::pool_object_store::{FilesystemSpace, FreeSpace, PoolMember};
    use crate::raw_member::RawMemberStore;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use zerofs_raw_device::{Identity, RawStore, Role, claim::Claim};

    #[derive(Debug)]
    struct RawSpace {
        device: u64,
        store: RawMemberStore,
    }

    impl FreeSpace for RawSpace {
        fn observe(&self) -> std::io::Result<FilesystemSpace> {
            let state = self.store.capacity().map_err(std::io::Error::other)?;
            Ok(FilesystemSpace {
                filesystem: self.device,
                committed: state.delete_debt_bytes,
                available: state.free_bytes,
                total: state.device_bytes,
            })
        }

        fn put_cost(&self, bytes: u64) -> u64 {
            RawMemberStore::put_cost(bytes)
                .and_then(|cost| cost.checked_add(RawMemberStore::delete_cost()))
                .unwrap_or(u64::MAX)
        }
    }

    let id = config.id.clone();
    let capacity = config.capacity_bytes()?;
    let reserve = config.reserve_free_bytes()?;
    tokio::task::spawn_blocking(move || {
        let claim = Claim::synthetic(&binding.node, &binding.backing)
            .with_context(|| format!("claim raw member {id}"))?;
        ensure!(
            !exact_size || claim.size == capacity,
            "held raw device for {id} does not match admitted capacity"
        );
        let device = claim.device.metadata()?.rdev();
        let store = RawStore::open(
            claim.device.try_clone()?,
            claim.size,
            Identity {
                pool_uuid: *binding.pool.as_bytes(),
                member_uuid: *binding.member.as_bytes(),
                role: Role::Data,
            },
        )
        .with_context(|| format!("verify raw member {id}"))?;
        // The raw log is append-only. A logical overwrite or a catalog delete
        // cannot make already-appended physical bytes available for reuse.
        let physical_floor = reserve.max(claim.size.saturating_sub(capacity));
        let store = RawMemberStore::new(store, claim, physical_floor)?;
        let min_free_bytes = store.capacity()?.reserve_bytes;
        Ok(PoolMember {
            id,
            identity: binding.locator(),
            store: Arc::new(store.clone()),
            capacity_bytes: capacity,
            local_path: None,
            min_free_bytes,
            health_device: None,
            free_space: Some(Arc::new(RawSpace { device, store })),
        })
    })
    .await?
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn open_member(
    _: &crate::config::StoragePoolMemberConfig,
    _: RawBinding,
    _: bool,
) -> Result<crate::pool_object_store::PoolMember> {
    bail!("raw+loop requires Linux")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    const POOL: &str = "1a284960-8b06-41c2-9de0-9f66ac96819d";
    const MEMBER: &str = "35457f1e-df44-4446-b681-0662a88cec47";

    #[test]
    fn locator_survives_loop_reassignment_and_binding_requires_all_identities() {
        let query = format!("backing=%2Fdev%2Fshm%2Fowned%2Fa&pool={POOL}&member={MEMBER}");
        let parse = |s: String| RawBinding::parse(&Url::parse(&s).unwrap());
        let a = parse(format!("raw+loop:///dev/loop1?{query}")).unwrap();
        let b = parse(format!("raw+loop:///dev/loop9?{query}")).unwrap();
        assert_eq!(a.locator(), b.locator());
        assert_ne!(a.node, b.node);
        for invalid in [
            format!("raw+loop://host/dev/loop1?{query}"),
            format!("raw+loop:///dev/loop1?{query}&pool={POOL}"),
            format!("raw+loop:///dev/loop1?{query}#ignored"),
            format!("raw+loop:///dev/loop1?backing=relative&pool={POOL}&member={MEMBER}"),
            format!("raw+loop:///dev/loop1?backing=/a/../b&pool={POOL}&member={MEMBER}"),
            format!("raw+loop:///dev/loop1?backing=/a&pool={POOL}"),
        ] {
            assert!(parse(invalid).is_err());
        }
    }
}

pub(super) struct OpenP3 {
    pub authority: crate::parse_object_store::ParsedStore,
    pub catalog: crate::pool_object_store::CatalogSource,
    pub journal: zerofs_raw_device::journal_region::TransactionRegion,
}

#[cfg(target_os = "linux")]
pub(super) async fn open_p3(binding: RawBinding) -> Result<OpenP3> {
    use crate::parse_object_store::ParsedStore;
    use crate::pool_object_store::CatalogSource;
    use crate::raw_member::RawMemberStore;
    use object_store::path::Path;
    use std::sync::Arc;
    use zerofs_raw_device::{claim::Claim, p3::P3Layout};
    tokio::task::spawn_blocking(move || {
        let claim = Arc::new(Claim::synthetic(&binding.node, &binding.backing)?);
        let layout = P3Layout::new(claim.size, binding.layout.context("missing raw P3 layout")?)?;
        let roles = layout.open_v2(
            claim.device.try_clone()?,
            *binding.pool.as_bytes(),
            *binding.member.as_bytes(),
        )?;
        let path = Path::from("db");
        let envelope = path
            .clone()
            .join(crate::key_management::WRAPPED_KEY_FILENAME);
        let key = envelope.as_ref().as_bytes();
        ensure!(
            !roles.metadata.contains_key(key) && roles.keys.keys().all(|stored| stored == key),
            "raw authority and key-envelope regions contain misplaced objects"
        );
        let store = super::raw_authority::RawAuthority {
            envelope,
            metadata: RawMemberStore::new(roles.metadata, claim.clone(), 0)?,
            keys: RawMemberStore::new(roles.keys, claim.clone(), 0)?,
        };
        Ok(OpenP3 {
            authority: ParsedStore {
                store: Box::new(store),
                path,
                sftp_pool: None,
            },
            catalog: CatalogSource::Raw(roles.catalog.retain_claim(claim.clone())),
            journal: roles.journal.retain_claim(claim),
        })
    })
    .await?
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn open_p3(_: RawBinding) -> Result<OpenP3> {
    bail!("raw+p3-v2 requires Linux")
}
