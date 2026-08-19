//! Dirty-SSD admission policy owner.
//!
//! [`SsdAdmission`] is the live byte/operation/physical-claim owner.
//! [`DiskAdmission`] remains the legacy watermark adapter used by the
//! current journaler/remote permit path until paced credits replace it.

pub use crate::coordination::admission::{DiskAdmission, DiskPermit};
pub(crate) use crate::writeback::reservation::SsdAdmission;
