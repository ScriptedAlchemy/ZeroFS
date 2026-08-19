//! Dirty-SSD admission policy.
//!
//! Legacy watermark admission remains [`DiskAdmission`]. Exact
//! byte/operation/physical-claim reservations live in
//! [`crate::writeback::reservation`].

pub use crate::coordination::admission::{DiskAdmission, DiskPermit};
