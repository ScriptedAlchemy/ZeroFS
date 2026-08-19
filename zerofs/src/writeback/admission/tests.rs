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

use crate::writeback::reservation::{
    ReservationError, ReservationState, SsdAdmission, SsdReservationRequest,
};
use crate::writeback::space_sample::PhysicalSpaceSample;

fn sample(generation: u64, available_bytes: u64) -> PhysicalSpaceSample {
    PhysicalSpaceSample {
        generation,
        available_bytes,
    }
}

fn request(ssd: u64, physical: u64, operations: u64) -> SsdReservationRequest {
    SsdReservationRequest {
        ssd_reservation_bytes: ssd,
        physical_reservation_bytes: physical,
        operations,
    }
}

fn exact_admission() -> SsdAdmission {
    SsdAdmission::new(100, 4, 100, 50, 10).unwrap()
}

#[tokio::test]
async fn byte_and_operation_fit_is_exact() {
    let admission = exact_admission();
    let first = admission
        .reserve(request(60, 20, 2), sample(1, 1_000))
        .await
        .unwrap();
    assert_eq!(admission.used_bytes(), 60);
    assert_eq!(admission.used_operations(), 2);

    let bytes_blocked = tokio::spawn({
        let admission = admission.clone();
        async move {
            admission
                .reserve(request(50, 20, 1), sample(2, 1_000))
                .await
        }
    });
    let ops_blocked = tokio::spawn({
        let admission = admission.clone();
        async move {
            admission
                .reserve(request(10, 10, 4), sample(3, 1_000))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!bytes_blocked.is_finished());
    assert!(!ops_blocked.is_finished());

    drop(first);
    let granted = bytes_blocked.await.unwrap().unwrap();
    assert_eq!(granted.request().ssd_reservation_bytes, 50);
    assert_eq!(admission.used_bytes(), 50);
    assert_eq!(admission.used_operations(), 1);
    assert!(!ops_blocked.is_finished());

    drop(granted);
    let ops = ops_blocked.await.unwrap().unwrap();
    assert_eq!(ops.request().operations, 4);
    assert_eq!(admission.used_bytes(), 10);
    assert_eq!(admission.used_operations(), 4);
}

#[tokio::test]
async fn reservation_larger_than_byte_or_operation_capacity_fails_immediately() {
    let admission = exact_admission();
    assert_eq!(
        admission
            .reserve(request(101, 1, 1), sample(1, 1_000))
            .await
            .unwrap_err(),
        ReservationError::TooLarge {
            requested: 101,
            capacity: 100
        }
    );
    assert_eq!(
        admission
            .reserve(request(1, 1, 5), sample(1, 1_000))
            .await
            .unwrap_err(),
        ReservationError::TooManyOperations {
            requested: 5,
            capacity: 4
        }
    );
    assert_eq!(admission.used_bytes(), 0);
    assert_eq!(admission.used_operations(), 0);
}

#[tokio::test]
async fn physical_reserve_covers_concurrent_claims() {
    let admission = SsdAdmission::new(1_000, 8, 100, 50, 100).unwrap();
    let first = admission
        .reserve(request(10, 40, 1), sample(1, 150))
        .await
        .unwrap();
    assert_eq!(admission.outstanding_physical_claims(), 40);

    let blocked = tokio::spawn({
        let admission = admission.clone();
        async move { admission.reserve(request(10, 20, 1), sample(1, 150)).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!blocked.is_finished());
    assert_eq!(admission.outstanding_physical_claims(), 40);

    drop(first);
    let second = blocked.await.unwrap().unwrap();
    assert_eq!(second.request().physical_reservation_bytes, 20);
    assert_eq!(admission.outstanding_physical_claims(), 20);
}

#[tokio::test]
async fn cancellation_rolls_back_bytes_ops_and_physical_claims_once() {
    let admission = exact_admission();
    let token = admission
        .reserve(request(25, 15, 2), sample(1, 1_000))
        .await
        .unwrap();
    assert_eq!(admission.used_bytes(), 25);
    assert_eq!(admission.used_operations(), 2);
    assert_eq!(admission.outstanding_physical_claims(), 15);

    drop(token);
    assert_eq!(admission.used_bytes(), 0);
    assert_eq!(admission.used_operations(), 0);
    assert_eq!(admission.outstanding_physical_claims(), 0);

    let mut kept = admission
        .reserve(request(25, 15, 2), sample(2, 1_000))
        .await
        .unwrap();
    kept.disarm();
    assert_eq!(kept.state(), ReservationState::Disarmed);
    drop(kept);
    assert_eq!(admission.used_bytes(), 25);
    assert_eq!(admission.used_operations(), 2);
    assert_eq!(admission.outstanding_physical_claims(), 15);
}

#[tokio::test]
async fn checked_underflow_and_overflow_poison_admission() {
    let overflow = SsdAdmission::recover(
        100,
        4,
        100,
        50,
        10,
        [request(u64::MAX, 0, 1), request(1, 0, 1)],
        sample(1, 1_000),
    )
    .unwrap_err();
    assert!(
        matches!(overflow, ReservationError::Poisoned(message) if message.contains("overflow"))
    );

    let admission = exact_admission();
    let _token = admission
        .reserve(request(10, 5, 1), sample(1, 1_000))
        .await
        .unwrap();
    admission.force_release(request(11, 5, 1));
    let error = admission
        .reserve(request(1, 1, 1), sample(2, 1_000))
        .await
        .unwrap_err();
    assert!(matches!(error, ReservationError::Poisoned(message) if message.contains("underflow")));
}

#[tokio::test]
async fn stale_sample_is_rejected() {
    let admission = exact_admission();
    let _token = admission
        .reserve(request(10, 5, 1), sample(2, 1_000))
        .await
        .unwrap();
    let error = admission
        .reserve(request(10, 5, 1), sample(1, 1_000))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ReservationError::StaleSample {
            sample: 1,
            latest: 2
        }
    );
}

#[tokio::test]
async fn close_and_poison_wake_waiters() {
    let admission = exact_admission();
    let held = admission
        .reserve(request(80, 10, 1), sample(1, 1_000))
        .await
        .unwrap();
    let waiter = tokio::spawn({
        let admission = admission.clone();
        async move {
            admission
                .reserve(request(30, 10, 1), sample(2, 1_000))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    admission.close();
    assert_eq!(waiter.await.unwrap().unwrap_err(), ReservationError::Closed);

    let poisoned = SsdAdmission::new(100, 4, 100, 50, 10).unwrap();
    let _held = poisoned
        .reserve(request(80, 10, 1), sample(1, 1_000))
        .await
        .unwrap();
    let waiter = tokio::spawn({
        let poisoned = poisoned.clone();
        async move { poisoned.reserve(request(30, 10, 1), sample(2, 1_000)).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    poisoned.poison("disk failed");
    assert_eq!(
        waiter.await.unwrap().unwrap_err(),
        ReservationError::Poisoned("disk failed".into())
    );
    drop(held);
}

#[tokio::test]
async fn recovery_seeds_exact_pending_bytes_and_operations() {
    let admission = SsdAdmission::recover(
        1_000,
        16,
        90,
        70,
        10,
        [request(40, 12, 1), request(25, 8, 2)],
        sample(7, 500),
    )
    .unwrap();
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.used_ssd_bytes, 65);
    assert_eq!(snapshot.used_operations, 3);
    assert_eq!(snapshot.outstanding_physical_claims, 20);
    assert_eq!(snapshot.available_bytes, 500);
    assert_eq!(snapshot.sample_generation, 7);
}
