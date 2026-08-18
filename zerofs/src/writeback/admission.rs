//! Writeback dirty-RAM and dirty-SSD admission.
//!
//! The implementation lives in `crate::coordination::admission`; this module
//! re-exports the old paths until call sites migrate.

pub use crate::coordination::admission::{
    AcceptedAdmission, Admission, AdmissionError, AdmissionPermit, DiskAdmission, DiskPermit,
};

#[cfg(test)]
mod tests {
    use super::{Admission, AdmissionError, DiskAdmission};

    #[tokio::test]
    async fn reservation_larger_than_the_dirty_ram_budget_fails_immediately() {
        let admission = Admission::new(10);

        let error = admission.reserve(11).await.unwrap_err();

        assert_eq!(
            error,
            AdmissionError::TooLarge {
                requested: 11,
                capacity: 10
            }
        );
        assert_eq!(admission.used_bytes(), 0);
    }

    #[test]
    fn disk_gate_restores_pending_blob_bytes_before_accepting_new_writes() {
        let disk = DiskAdmission::with_used(100, 90, 70, 10, 80, 1_000).unwrap();

        assert_eq!(disk.used_bytes(), 80);
    }
}
