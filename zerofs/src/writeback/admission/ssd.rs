//! Dirty-SSD admission policy owner.
//!
//! [`SsdAdmission`] is the live byte/operation/physical-claim owner used by
//! the journaler, store, and remote cleanup path. [`DiskAdmission`] remains
//! only as the extracted legacy watermark type for its own unit tests.

pub use crate::coordination::admission::{DiskAdmission, DiskPermit};
pub(crate) use crate::writeback::reservation::SsdAdmission;
