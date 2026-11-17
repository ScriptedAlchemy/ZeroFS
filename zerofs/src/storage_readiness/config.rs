use super::devices::{DeviceDeclaration, RoleName};
use crate::config::Settings;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiskProfile {
    Nvme,
    HddCmr,
    HddSmr,
}

pub(super) use super::identity::FilesystemIdentity;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilesystemDeclaration {
    pub(super) id: String,
    pub(super) profile: DiskProfile,
    #[serde(deserialize_with = "crate::config::deserialize_expandable_path")]
    pub(super) mountpoint: PathBuf,
    pub(super) identity: FilesystemIdentity,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReserveDeclaration {
    filesystem: String,
    bytes: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadinessConfig {
    pub(super) strict: bool,
    pub(super) authority: String,
    pub(super) catalog: String,
    pub(super) cache: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) journal: Option<String>,
    pub(super) members: BTreeMap<String, String>,
    pub(super) filesystems: Vec<FilesystemDeclaration>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) devices: Vec<DeviceDeclaration>,
    reserves: Vec<ReserveDeclaration>,
}

pub(super) struct StorageRoot<'a> {
    pub(super) role: RoleName,
    pub(super) path: PathBuf,
    pub(super) filesystem: &'a FilesystemDeclaration,
    pub(super) reserve_bytes: u64,
}

pub(super) fn absolute_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        bail!(
            "storage readiness paths must be absolute without parent components: {}",
            path.display()
        );
    }
    Ok(())
}

fn local_url(raw: &str) -> Result<PathBuf> {
    let url: url::Url = raw.parse()?;
    if url.scheme() != "file" || url.query().is_some() || url.fragment().is_some() {
        bail!("storage readiness requires local file:// storage for every role");
    }
    url.to_file_path()
        .map_err(|_| anyhow::anyhow!("storage readiness requires a local file:// path"))
}

impl ReadinessConfig {
    pub(crate) fn validate(&self, settings: &Settings) -> Result<()> {
        self.validated_roots(settings).map(|_| ())
    }

    pub(super) fn validated_roots<'a>(
        &'a self,
        settings: &Settings,
    ) -> Result<Vec<StorageRoot<'a>>> {
        let pool = settings
            .storage
            .pool
            .as_ref()
            .context("[storage.readiness] requires [storage.pool]")?;
        if settings.wal.is_some() {
            bail!("storage readiness does not support a separate WAL store");
        }
        let mut ids = HashSet::new();
        for filesystem in &self.filesystems {
            if filesystem.id.is_empty()
                || !filesystem
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                || !ids.insert(&filesystem.id)
            {
                bail!("readiness filesystem IDs must be distinct nonempty ASCII labels");
            }
            absolute_path(&filesystem.mountpoint)?;
            match &filesystem.identity {
                FilesystemIdentity::Uuid { uuid } => {
                    let parsed = uuid::Uuid::parse_str(uuid).context("invalid filesystem UUID")?;
                    if parsed.is_nil() || parsed.hyphenated().to_string() != *uuid {
                        bail!("filesystem UUID must be nonzero, lowercase and hyphenated");
                    }
                }
                FilesystemIdentity::Zfs {
                    pool_guid,
                    dataset_guid,
                } => {
                    for guid in [pool_guid, dataset_guid] {
                        let value = guid.parse::<u64>().context("invalid ZFS GUID")?;
                        if value == 0 || value.to_string() != *guid {
                            bail!(
                                "ZFS pool and dataset GUIDs must be canonical nonzero decimal strings"
                            );
                        }
                    }
                }
            }
        }
        let resolve = |id: &str| {
            self.filesystems
                .iter()
                .find(|f| f.id == id)
                .with_context(|| format!("unknown readiness filesystem {id}"))
        };
        let mut roots = vec![
            StorageRoot {
                role: RoleName::Authority,
                path: local_url(&settings.storage.url)?,
                filesystem: resolve(&self.authority)?,
                reserve_bytes: 0,
            },
            StorageRoot {
                role: RoleName::Catalog,
                path: pool.catalog_dir.clone(),
                filesystem: resolve(&self.catalog)?,
                reserve_bytes: 0,
            },
            StorageRoot {
                role: RoleName::Cache,
                path: settings.cache.dir.clone(),
                filesystem: resolve(&self.cache)?,
                reserve_bytes: 0,
            },
        ];
        match settings.writeback.as_ref().filter(|w| w.enabled) {
            Some(writeback) => {
                roots.push(StorageRoot {
                    role: RoleName::Journal,
                    path: writeback.dir.clone(),
                    filesystem: resolve(self.journal.as_deref().context(
                        "readiness journal binding is required when writeback is enabled",
                    )?)?,
                    reserve_bytes: 0,
                })
            }
            None if self.journal.is_some() => {
                bail!("readiness journal binding requires enabled writeback")
            }
            None => {}
        }
        if self.members.len() != pool.members.len() {
            bail!("readiness must bind exactly every configured pool member");
        }
        let mut member_domains = HashSet::new();
        for member in &pool.members {
            let filesystem =
                resolve(self.members.get(&member.id).with_context(|| {
                    format!("missing readiness binding for member {}", member.id)
                })?)?;
            if filesystem.profile == DiskProfile::Nvme {
                bail!(
                    "readiness member {} must declare hdd-cmr or hdd-smr",
                    member.id
                );
            }
            if !member_domains.insert(filesystem.identity.capacity_domain()) {
                bail!(
                    "readiness members must not alias one capacity domain (including a shared ZFS pool)"
                );
            }
            roots.push(StorageRoot {
                role: RoleName::Member(member.id.clone()),
                path: local_url(&member.url)?,
                filesystem,
                reserve_bytes: 0,
            });
        }
        for root in &roots {
            absolute_path(&root.path)?;
            if !root.path.starts_with(&root.filesystem.mountpoint) {
                bail!(
                    "readiness {} root must be under its declared mountpoint",
                    root.role
                );
            }
            match (&root.role, root.filesystem.profile) {
                (RoleName::Journal, DiskProfile::Nvme) => {}
                (RoleName::Journal, _) => bail!("readiness journal must declare nvme"),
                (RoleName::Authority | RoleName::Catalog, DiskProfile::Nvme) => {
                    bail!("readiness {} must declare hdd-cmr or hdd-smr", root.role)
                }
                _ => {}
            }
        }
        for (index, left) in self.filesystems.iter().enumerate() {
            if !roots.iter().any(|root| root.filesystem.id == left.id) {
                bail!("unused readiness filesystem {}", left.id);
            }
            for right in &self.filesystems[index + 1..] {
                if left.mountpoint == right.mountpoint || left.identity == right.identity {
                    bail!("readiness filesystem aliases must use one declaration");
                }
                if left.identity.capacity_domain() == right.identity.capacity_domain()
                    && left.profile != right.profile
                {
                    bail!("one readiness capacity domain cannot declare conflicting profiles");
                }
            }
        }
        self.validate_devices(&roots)?;
        let reserves = self.reserves_by_domain()?;
        for root in &mut roots {
            root.reserve_bytes = reserves[&root.filesystem.identity.capacity_domain()];
        }
        self.validate_reserves(settings, &roots)?;
        Ok(roots)
    }

    fn validate_devices(&self, roots: &[StorageRoot<'_>]) -> Result<()> {
        let mut serials = HashSet::new();
        let mut paths = HashSet::new();
        for device in &self.devices {
            if !serials.insert(&device.serial) {
                bail!("duplicate serial in readiness devices");
            }
            if !paths.insert(&device.by_id) {
                bail!("duplicate by-id in readiness devices");
            }
            device.validate(roots)?;
        }
        Ok(())
    }

    fn reserves_by_domain(&self) -> Result<BTreeMap<String, u64>> {
        let mut reserves = BTreeMap::new();
        for reserve in &self.reserves {
            let filesystem = self
                .filesystems
                .iter()
                .find(|filesystem| filesystem.id == reserve.filesystem)
                .context("reserve references an unknown readiness filesystem")?;
            if reserves
                .insert(filesystem.identity.capacity_domain(), reserve.bytes)
                .is_some()
            {
                bail!("readiness requires exactly one reserve per capacity domain");
            }
        }
        for filesystem in &self.filesystems {
            if !reserves.contains_key(&filesystem.identity.capacity_domain()) {
                bail!("missing reserve for readiness filesystem {}", filesystem.id);
            }
        }
        Ok(reserves)
    }

    fn validate_reserves(&self, settings: &Settings, roots: &[StorageRoot<'_>]) -> Result<()> {
        let declared_for = |role: &RoleName| -> Result<u64> {
            roots
                .iter()
                .find(|root| root.role == *role)
                .map(|root| root.reserve_bytes)
                .context("missing reserve role binding")
        };
        let pool = settings
            .storage
            .pool
            .as_ref()
            .context("missing readiness pool")?;
        let mut capacities = BTreeMap::<String, u128>::new();
        for member in &pool.members {
            let root = roots
                .iter()
                .find(|root| root.role == RoleName::Member(member.id.clone()))
                .context("missing reserve member binding")?;
            let capacity = capacities
                .entry(root.filesystem.identity.capacity_domain())
                .or_default();
            *capacity = capacity
                .checked_add(u128::from(member.capacity_bytes()?))
                .context("configured member capacity sum overflow")?;
            if declared_for(&RoleName::Member(member.id.clone()))? < member.reserve_free_bytes()? {
                bail!(
                    "declared domain reserve is below enforced reserve_free_gb for member {}",
                    member.id
                );
            }
        }
        for (domain, capacity) in capacities {
            let reserve = roots
                .iter()
                .find(|root| root.filesystem.identity.capacity_domain() == domain)
                .map(|root| root.reserve_bytes)
                .expect("member capacity domain has a stamped reserve");
            if u128::from(reserve) > capacity {
                bail!("declared domain reserve exceeds summed configured member capacity");
            }
        }
        if let Some(writeback) =
            settings.writeback_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)?
            && declared_for(&RoleName::Journal)? < writeback.min_free_bytes
        {
            bail!("declared domain reserve is below enforced writeback min_free_gb");
        }
        Ok(())
    }
}
