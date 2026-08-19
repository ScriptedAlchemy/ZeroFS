//! Writeback dirty-RAM and dirty-SSD admission.
//!
//! RAM and SSD policy owners live in this module. The shared FIFO gate and
//! waiter lifecycle live in `crate::coordination::admission`.

mod ram;
mod ssd;

pub use crate::coordination::admission::AdmissionError;
pub use ram::{AcceptedAdmission, Admission, AdmissionPermit};
pub use ssd::{DiskAdmission, DiskPermit};

#[cfg(test)]
mod tests;
