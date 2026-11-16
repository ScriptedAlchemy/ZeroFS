use super::{DetectedLimits, MemorySelector, SelectedBudget};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::collections::BTreeMap;

/// Acquire multiple pools in declaration order, before taking filesystem locks.
/// The protected progress pool belongs to drain and control jobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolKind {
    Runtime,
    Metadata,
    Dirty,
    Protocol,
    Io,
    Progress,
    Clean,
    Headroom,
}

impl PoolKind {
    pub const ALL: [Self; 8] = [
        Self::Runtime,
        Self::Metadata,
        Self::Dirty,
        Self::Protocol,
        Self::Io,
        Self::Progress,
        Self::Clean,
        Self::Headroom,
    ];
}

#[derive(Clone, Debug, Serialize)]
pub struct MemoryRequirement {
    pub name: String,
    pub bytes: u64,
}

/// Callers must derive these requirements from enabled features and dependency
/// geometry. This API does not qualify any production allocation strategy.
#[derive(Clone, Debug)]
pub struct PoolRequest {
    pub kind: PoolKind,
    pub requirements: Vec<MemoryRequirement>,
    /// A soft maximum for extra capacity. Explicit dirty maxima belong in requirements.
    pub maximum_bytes: u64,
    pub weight: u32,
    pub max_entries: u64,
    pub max_waiters: usize,
}

#[derive(Clone, Debug)]
pub struct PlanRequirements {
    pub pools: Vec<PoolRequest>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct VolatileBudget {
    pub bytes_per_stage: u64,
    pub source: String,
}

impl From<crate::fs::mutation::config::FilesystemWriteAckSettings> for VolatileBudget {
    fn from(settings: crate::fs::mutation::config::FilesystemWriteAckSettings) -> Self {
        use crate::fs::mutation::config::FilesystemWriteAckSource;
        Self {
            bytes_per_stage: settings.volatile_memory_bytes,
            source: match settings.source {
                FilesystemWriteAckSource::DefaultMaterialized => "default materialized",
                FilesystemWriteAckSource::Filesystem => "[filesystem]",
                FilesystemWriteAckSource::LegacyNbd => "legacy [servers.nbd]",
                FilesystemWriteAckSource::MatchingFilesystemAndLegacyNbd => {
                    "matching [filesystem] and legacy [servers.nbd]"
                }
            }
            .into(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PoolCapacity {
    pub kind: PoolKind,
    pub minimum_bytes: u64,
    pub capacity_bytes: u64,
    pub max_entries: u64,
    pub max_waiters: usize,
    pub requirements: Vec<MemoryRequirement>,
}

/// The single planning receipt. It is immutable and records that enforcement is
/// inactive until every production allocation path is connected and qualified.
#[derive(Clone, Debug, Serialize)]
pub struct ResolvedMemoryPlan {
    schema_version: u32,
    enforcement_active: bool,
    budget: SelectedBudget,
    detected: DetectedLimits,
    volatile: VolatileBudget,
    pools: Vec<PoolCapacity>,
}

impl ResolvedMemoryPlan {
    pub fn resolve(
        selector: MemorySelector,
        detected: &DetectedLimits,
        requested: &PlanRequirements,
        volatile: VolatileBudget,
    ) -> Result<Self> {
        let budget = selector.resolve(detected)?;
        let mut requests = BTreeMap::new();
        for request in &requested.pools {
            if requests.insert(request.kind, request.clone()).is_some() {
                bail!("duplicate memory pool {:?}", request.kind);
            }
        }
        for kind in PoolKind::ALL {
            if !requests.contains_key(&kind) {
                bail!("missing memory pool {kind:?}");
            }
        }
        let dirty = requests
            .get_mut(&PoolKind::Dirty)
            .expect("all pools were checked");
        if volatile.bytes_per_stage != 0 {
            volatile
                .bytes_per_stage
                .checked_mul(2)
                .context("volatile ownership stage accounting overflowed")?;
            for name in ["volatile raw input", "volatile accepted overlay"] {
                dirty.requirements.push(MemoryRequirement {
                    name: name.into(),
                    bytes: volatile.bytes_per_stage,
                });
            }
        }
        let mut required = 0_u64;
        let mut pools = Vec::new();
        for request in requests.values() {
            let mut minimum = 0_u64;
            for requirement in &request.requirements {
                minimum = minimum.checked_add(requirement.bytes).with_context(|| {
                    format!("memory requirement {} overflowed", requirement.name)
                })?;
                required = required.checked_add(requirement.bytes).with_context(|| {
                    format!("total memory requirement {} overflowed", requirement.name)
                })?;
                if required > budget.bytes {
                    bail!(
                        "memory requirement {} in {:?} does not fit; the irreducible working set needs at least {required} bytes, budget is {} bytes",
                        requirement.name,
                        request.kind,
                        budget.bytes
                    );
                }
                if minimum > request.maximum_bytes {
                    bail!(
                        "memory requirement {} in {:?} needs {minimum} bytes, above its requested maximum of {} bytes",
                        requirement.name,
                        request.kind,
                        request.maximum_bytes
                    );
                }
            }
            if matches!(
                request.kind,
                PoolKind::Runtime | PoolKind::Progress | PoolKind::Headroom
            ) && minimum == 0
            {
                bail!(
                    "memory pool {:?} needs an explicit nonzero working-set requirement",
                    request.kind
                );
            }
            if request.kind != PoolKind::Headroom
                && request.maximum_bytes > 0
                && request.max_entries == 0
            {
                bail!(
                    "memory pool {:?} needs a finite positive entry capacity",
                    request.kind
                );
            }
            pools.push(PoolCapacity {
                kind: request.kind,
                minimum_bytes: minimum,
                capacity_bytes: minimum,
                max_entries: request.max_entries,
                max_waiters: request.max_waiters,
                requirements: request.requirements.clone(),
            });
        }
        let remaining = budget
            .bytes
            .checked_sub(required)
            .context("memory minimum exceeded budget")?;
        let total_weight = requests
            .values()
            .try_fold(0_u64, |sum, request| {
                sum.checked_add(u64::from(request.weight))
            })
            .context("memory pool weights overflowed")?;
        let mut allocated = required;
        for pool in &mut pools {
            let request = &requests[&pool.kind];
            let share = if total_weight == 0 {
                0
            } else {
                u64::try_from(
                    u128::from(remaining) * u128::from(request.weight) / u128::from(total_weight),
                )?
            };
            let extra = share.min(
                request
                    .maximum_bytes
                    .checked_sub(pool.minimum_bytes)
                    .context("memory pool minimum exceeded maximum")?,
            );
            pool.capacity_bytes = pool
                .capacity_bytes
                .checked_add(extra)
                .context("memory pool capacity overflowed")?;
            allocated = allocated
                .checked_add(extra)
                .context("memory allocation total overflowed")?;
        }
        // Rounding and unused soft maxima remain unallocated headroom.
        let remainder = budget
            .bytes
            .checked_sub(allocated)
            .context("memory shares exceeded budget")?;
        let headroom = pools
            .iter_mut()
            .find(|pool| pool.kind == PoolKind::Headroom)
            .expect("headroom was checked");
        headroom.capacity_bytes = headroom
            .capacity_bytes
            .checked_add(remainder)
            .context("memory headroom overflowed")?;
        Ok(Self {
            schema_version: 1,
            enforcement_active: false,
            budget,
            detected: detected.clone(),
            volatile,
            pools,
        })
    }

    pub fn pools(&self) -> &[PoolCapacity] {
        &self.pools
    }
    pub fn pool(&self, kind: PoolKind) -> &PoolCapacity {
        self.pools
            .iter()
            .find(|pool| pool.kind == kind)
            .expect("a resolved plan contains every pool")
    }
    pub fn volatile(&self) -> &VolatileBudget {
        &self.volatile
    }
    pub fn total_bytes(&self) -> u64 {
        self.budget.bytes
    }
}
