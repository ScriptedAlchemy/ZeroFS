//! Exact SSD cleanup credits that replace 95/85 hysteresis.
//!
//! Burst admits under hard byte/op/physical gates until a request would
//! cross the high watermark. Paced then grants only against
//! [`SsdReleaseCredit`] minted by [`credit_for`] after contiguous remote
//! cleanup. Returning to or below resume clears stale credits so an
//! oversized FIFO head can be reconsidered under the hard gates.

use crate::writeback::space_sample::PhysicalSpaceSample;

/// Admission regime for dirty-SSD reservations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SsdAdmissionMode {
    Burst,
    Paced,
}

/// Sole credit issued after durable remote cleanup.
///
/// The byte field is exactly the journal reservation, never logical payload
/// length. This is the only `SsdReleaseCredit` definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SsdReleaseCredit {
    pub(crate) ssd_reservation_bytes: u64,
    pub(crate) operations: u64,
}

/// Evidence that a remote completion may mint credit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DurableCleanupSteps {
    pub(crate) watermark_committed: bool,
    pub(crate) overlay_retired: bool,
    pub(crate) local_cleanup_committed: bool,
    pub(crate) sample: Option<PhysicalSpaceSample>,
    pub(crate) previous_generation: u64,
    pub(crate) ssd_reservation_bytes: u64,
    pub(crate) operations: u64,
}

/// Mint credit only after watermark, overlay retire, durable cleanup, and a
/// strictly newer physical-space sample. Out-of-order upload completion
/// yields none.
pub(crate) fn credit_for(
    steps: DurableCleanupSteps,
) -> Option<(SsdReleaseCredit, PhysicalSpaceSample)> {
    let sample = steps.sample?;
    if !steps.watermark_committed
        || !steps.overlay_retired
        || !steps.local_cleanup_committed
        || sample.generation <= steps.previous_generation
    {
        return None;
    }
    Some((
        SsdReleaseCredit {
            ssd_reservation_bytes: steps.ssd_reservation_bytes,
            operations: steps.operations,
        },
        sample,
    ))
}

/// Byte/operation credit ledger for one SSD admission owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PacingLedger {
    mode: SsdAdmissionMode,
    credit_bytes: u64,
    credit_ops: u64,
}

impl Default for PacingLedger {
    fn default() -> Self {
        Self {
            mode: SsdAdmissionMode::Burst,
            credit_bytes: 0,
            credit_ops: 0,
        }
    }
}

impl PacingLedger {
    pub(crate) fn mode(&self) -> SsdAdmissionMode {
        self.mode
    }

    pub(crate) fn credit_bytes(&self) -> u64 {
        self.credit_bytes
    }

    pub(crate) fn credit_ops(&self) -> u64 {
        self.credit_ops
    }

    pub(crate) fn enter_paced(&mut self) {
        self.mode = SsdAdmissionMode::Paced;
    }

    /// At or below resume, return to Burst and forget leftover credits.
    pub(crate) fn maybe_resume(&mut self, used_ssd_bytes: u64, resume_bytes: u64) {
        if used_ssd_bytes <= resume_bytes {
            *self = Self::default();
        }
    }

    pub(crate) fn add_credit(&mut self, credit: SsdReleaseCredit) {
        if self.mode != SsdAdmissionMode::Paced {
            return;
        }
        self.credit_bytes = self
            .credit_bytes
            .saturating_add(credit.ssd_reservation_bytes);
        self.credit_ops = self.credit_ops.saturating_add(credit.operations);
    }

    pub(crate) fn can_grant(&self, bytes: u64, operations: u64) -> bool {
        match self.mode {
            SsdAdmissionMode::Burst => true,
            SsdAdmissionMode::Paced => self.credit_bytes >= bytes && self.credit_ops >= operations,
        }
    }

    pub(crate) fn consume_grant(&mut self, bytes: u64, operations: u64) {
        if self.mode == SsdAdmissionMode::Paced {
            self.credit_bytes = self.credit_bytes.saturating_sub(bytes);
            self.credit_ops = self.credit_ops.saturating_sub(operations);
        }
    }

    pub(crate) fn return_grant_credit(&mut self, bytes: u64, operations: u64) {
        self.add_credit(SsdReleaseCredit {
            ssd_reservation_bytes: bytes,
            operations,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DurableCleanupSteps, PacingLedger, SsdAdmissionMode, SsdReleaseCredit, credit_for,
    };
    use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
    use crate::writeback::space_sample::PhysicalSpaceSample;

    fn credit(bytes: u64, operations: u64) -> SsdReleaseCredit {
        SsdReleaseCredit {
            ssd_reservation_bytes: bytes,
            operations,
        }
    }

    fn request(bytes: u64, operations: u64) -> SsdReservationRequest {
        SsdReservationRequest {
            ssd_reservation_bytes: bytes,
            physical_reservation_bytes: bytes,
            operations,
        }
    }

    fn sample(generation: u64, available_bytes: u64) -> PhysicalSpaceSample {
        PhysicalSpaceSample {
            generation,
            available_bytes,
        }
    }

    fn steps(
        watermark: bool,
        overlay: bool,
        cleanup: bool,
        generation: Option<u64>,
        previous: u64,
    ) -> DurableCleanupSteps {
        DurableCleanupSteps {
            watermark_committed: watermark,
            overlay_retired: overlay,
            local_cleanup_committed: cleanup,
            sample: generation.map(|generation| sample(generation, 1_000)),
            previous_generation: previous,
            ssd_reservation_bytes: 8,
            operations: 1,
        }
    }

    #[test]
    fn paced_gate_grants_exact_byte_and_op_credit() {
        let mut pacing = PacingLedger::default();
        pacing.enter_paced();
        pacing.add_credit(credit(40, 2));
        assert!(pacing.can_grant(40, 2));
        assert!(!pacing.can_grant(41, 2));
        assert!(!pacing.can_grant(40, 3));
        pacing.consume_grant(40, 2);
        assert_eq!(pacing.credit_bytes(), 0);
        assert_eq!(pacing.credit_ops(), 0);
        assert!(!pacing.can_grant(1, 1));
    }

    #[test]
    fn credit_accumulates_for_fifo_head() {
        let mut pacing = PacingLedger::default();
        pacing.enter_paced();
        pacing.add_credit(credit(10, 1));
        pacing.add_credit(credit(15, 1));
        assert!(!pacing.can_grant(30, 2));
        pacing.add_credit(credit(5, 0));
        assert!(pacing.can_grant(30, 2));
        pacing.consume_grant(30, 2);
        assert_eq!(pacing.credit_bytes(), 0);
        assert_eq!(pacing.credit_ops(), 0);
    }

    #[test]
    fn cancelled_grant_returns_credit() {
        let mut pacing = PacingLedger::default();
        pacing.enter_paced();
        pacing.add_credit(credit(50, 1));
        pacing.consume_grant(50, 1);
        assert!(!pacing.can_grant(50, 1));
        pacing.return_grant_credit(50, 1);
        assert!(pacing.can_grant(50, 1));
    }

    #[test]
    fn out_of_order_upload_completion_issues_no_credit() {
        assert_eq!(credit_for(steps(false, true, true, Some(2), 1)), None);
    }

    #[test]
    fn watermark_without_overlay_retirement_issues_no_credit() {
        assert_eq!(credit_for(steps(true, false, true, Some(2), 1)), None);
    }

    #[test]
    fn cleanup_without_durable_commit_issues_no_credit() {
        assert_eq!(credit_for(steps(true, true, false, Some(2), 1)), None);
    }

    #[test]
    fn fresh_sample_required_before_credit() {
        assert_eq!(credit_for(steps(true, true, true, None, 1)), None);
        assert_eq!(credit_for(steps(true, true, true, Some(1), 1)), None);
        let (issued, sample) = credit_for(steps(true, true, true, Some(2), 1)).unwrap();
        assert_eq!(issued, credit(8, 1));
        assert_eq!(sample.generation, 2);
    }

    #[test]
    fn resume_is_large_head_escape() {
        let mut pacing = PacingLedger::default();
        pacing.enter_paced();
        pacing.add_credit(credit(20, 1));
        assert_eq!(pacing.mode(), SsdAdmissionMode::Paced);
        pacing.maybe_resume(90, 85);
        assert_eq!(pacing.mode(), SsdAdmissionMode::Paced);
        assert_eq!(pacing.credit_bytes(), 20);
        pacing.maybe_resume(85, 85);
        assert_eq!(pacing.mode(), SsdAdmissionMode::Burst);
        assert_eq!(pacing.credit_bytes(), 0);
        assert!(pacing.can_grant(200, 8));
    }

    #[tokio::test]
    async fn paced_admission_grants_from_cleanup_credit_without_resume_drain() {
        let admission = SsdAdmission::new(100, 8, 90, 70, 10).unwrap();
        let held = admission
            .reserve(request(80, 1), sample(1, 1_000))
            .await
            .unwrap();
        assert_eq!(admission.mode(), SsdAdmissionMode::Burst);

        let waiter = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(request(20, 1), sample(2, 1_000)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        assert_eq!(admission.mode(), SsdAdmissionMode::Paced);
        assert!(admission.used_bytes() > 70, "must not drain to resume");

        admission.apply_release_credit(credit(20, 1)).unwrap();
        let granted = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("paced credit must grant without waiting for 85% resume")
            .expect("waiter must join")
            .expect("waiter must be granted");
        assert_eq!(granted.request().ssd_reservation_bytes, 20);
        assert_eq!(admission.used_bytes(), 100);
        drop(held);
    }
}
