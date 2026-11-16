use super::config::{DiskProfile, StorageRoot};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

pub(super) use super::identity::Serial;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub(super) enum RoleName {
    Authority,
    Catalog,
    Cache,
    Journal,
    Member(String),
}

impl TryFrom<String> for RoleName {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "authority" => Ok(Self::Authority),
            "catalog" => Ok(Self::Catalog),
            "cache" => Ok(Self::Cache),
            "journal" => Ok(Self::Journal),
            _ => match value.strip_prefix("member ") {
                Some(id) if !id.is_empty() => Ok(Self::Member(id.to_owned())),
                _ => Err("unknown storage role"),
            },
        }
    }
}

impl From<RoleName> for String {
    fn from(value: RoleName) -> Self {
        value.to_string()
    }
}

impl fmt::Display for RoleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authority => f.write_str("authority"),
            Self::Catalog => f.write_str("catalog"),
            Self::Cache => f.write_str("cache"),
            Self::Journal => f.write_str("journal"),
            Self::Member(id) => write!(f, "member {id}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RatedHostWrites {
    pub(super) bytes: u64,
    pub(super) source: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeviceDeclaration {
    pub(super) serial: Serial,
    pub(super) by_id: PathBuf,
    pub(super) profile: DiskProfile,
    pub(super) roles: Vec<RoleName>,
    pub(super) boot: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rated_host_writes: Option<RatedHostWrites>,
}

impl DeviceDeclaration {
    pub(super) fn rated_host_writes(&self) -> Result<&RatedHostWrites, &'static str> {
        self.rated_host_writes
            .as_ref()
            .ok_or("rated host writes unavailable because no lifetime rating was declared")
    }

    pub(super) fn validate(&self, roots: &[StorageRoot<'_>]) -> Result<()> {
        validate_by_id(&self.by_id)?;
        if let Ok(rating) = self.rated_host_writes() {
            if rating.bytes == 0 {
                bail!("rated_host_writes bytes must be greater than zero");
            }
            if rating.source.trim().is_empty() {
                bail!("rated_host_writes source must be nonempty");
            }
        }
        if self.boot && !self.roles.is_empty() {
            bail!("a boot device must not carry storage roles");
        }
        if self.roles.contains(&RoleName::Journal) && self.profile != DiskProfile::Nvme {
            bail!("readiness journal must declare nvme on its device");
        }
        let mut roles = HashSet::new();
        for role in &self.roles {
            if !roles.insert(role) {
                bail!("duplicate device role {role}");
            }
            let Some(root) = roots.iter().find(|root| root.role == *role) else {
                bail!("device references undeclared role {role}");
            };
            if self.profile != root.filesystem.profile {
                bail!("device profile conflicts with the declared filesystem for {role}");
            }
        }
        for root in roots {
            let shares_domain = roots.iter().any(|other| {
                self.roles.contains(&other.role)
                    && other.filesystem.identity.capacity_domain()
                        == root.filesystem.identity.capacity_domain()
            });
            if shares_domain && !self.roles.contains(&root.role) {
                bail!(
                    "device must declare every role sharing its capacity domain, including {}",
                    root.role
                );
            }
        }
        Ok(())
    }
}

fn validate_by_id(path: &Path) -> Result<()> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        bail!("device by-id path must name a stable whole device");
    };
    if Path::new("/dev/disk/by-id").join(name).as_os_str() != path.as_os_str()
        || !["ata-", "nvme-", "scsi-", "wwn-", "virtio-", "usb-"]
            .iter()
            .any(|prefix| name.starts_with(prefix) && name.len() > prefix.len())
    {
        bail!("device by-id must be a stable path directly under /dev/disk/by-id");
    }
    if name.rsplit_once("-part").is_some_and(|(_, suffix)| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        bail!("device by-id must name a whole device, not a partition");
    }
    Ok(())
}
