//! Explicit memory planning and ownership accounting. Production still uses the
//! legacy startup admission policy. No allocation path is switched by this module.

pub(crate) mod cgroup;
mod detect;
mod lease;
mod planner;
mod selector;

pub use detect::{
    DetectedLimits, FiniteLimit, LimitKind, LimitObservation, LimitRole, Observation,
    detect_limits, detect_limits_with,
};
pub use lease::{LeaseError, MemoryBudget, MemoryLease, MemoryPool, PoolUsage};
pub use planner::{
    MemoryRequirement, PlanRequirements, PoolCapacity, PoolKind, PoolRequest, ResolvedMemoryPlan,
    VolatileBudget,
};
pub use selector::{MemorySelector, SelectedBudget};

#[cfg(test)]
mod tests;
