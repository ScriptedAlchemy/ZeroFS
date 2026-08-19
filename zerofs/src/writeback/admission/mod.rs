//! Writeback dirty-RAM and dirty-SSD admission.
//!
//! RAM policy lives here. SSD ownership lives in
//! [`crate::writeback::reservation::SsdAdmission`]. The shared FIFO gate
//! lives in `crate::coordination::admission`.

mod ram;

pub use crate::coordination::admission::AdmissionError;
pub use ram::{AcceptedAdmission, Admission, AdmissionPermit};

#[cfg(test)]
mod tests;
