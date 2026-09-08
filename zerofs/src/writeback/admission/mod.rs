//! Writeback dirty-RAM and dirty-SSD admission.
//!
//! RAM policy lives here. SSD ownership lives in
//! [`crate::writeback::reservation::SsdAdmission`]. The shared FIFO gate
//! lives in `crate::coordination::admission`.

mod ram;

#[cfg(test)]
pub(crate) use crate::coordination::admission::AdmissionError;
pub(crate) use ram::{AcceptedAdmission, Admission};

#[cfg(test)]
mod tests;
