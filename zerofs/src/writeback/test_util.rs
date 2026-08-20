//! Shared fixtures for writeback tests: `MutationRecord` builders for `Put`
//! and `Delete` mutations.
//!
//! `journal.rs`, `journaler.rs`, and `overlay.rs` each need slightly
//! different defaults (mutation mode, fence class, and operation-id
//! namespace) for their own tests, so those details are parameters here
//! rather than baked-in constants. Each module keeps a thin local
//! `put_record`/`delete_record` wrapper that supplies its own defaults, so a
//! `MutationRecord` field change only needs to be made once, in this file.

use super::model::{FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WriteAdmissionTestPhase {
    Created,
    Queued,
    AllocationWaiting,
    Allocated,
}

#[derive(Debug)]
struct WriteAdmissionTestState {
    phase: WriteAdmissionTestPhase,
    queued: bool,
    allocation_released: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteAdmissionTestControl {
    state: Arc<Mutex<WriteAdmissionTestState>>,
    changed: Arc<Notify>,
}

impl WriteAdmissionTestControl {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(WriteAdmissionTestState {
                phase: WriteAdmissionTestPhase::Created,
                queued: false,
                allocation_released: false,
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WriteAdmissionTestState> {
        self.state.lock().unwrap()
    }

    async fn wait_for_phase(&self, target: WriteAdmissionTestPhase) {
        loop {
            let changed = self.changed.notified();
            if self.lock().phase >= target {
                return;
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_until_registered(&self) {
        self.wait_for_phase(WriteAdmissionTestPhase::Queued).await;
    }

    pub(crate) async fn wait_until_allocation_waits(&self) {
        self.wait_for_phase(WriteAdmissionTestPhase::AllocationWaiting)
            .await;
    }

    pub(crate) async fn wait_until_allocated(&self) {
        self.wait_for_phase(WriteAdmissionTestPhase::Allocated)
            .await;
    }

    pub(crate) fn release_allocation(&self) {
        self.lock().allocation_released = true;
        self.changed.notify_waiters();
    }

    pub(crate) fn mark_queued(&self) {
        let mut state = self.lock();
        state.queued = true;
        state.phase = state.phase.max(WriteAdmissionTestPhase::Queued);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn was_queued(&self) -> bool {
        self.lock().queued
    }

    pub(crate) async fn wait_for_allocation_release(&self) {
        {
            let mut state = self.lock();
            state.phase = state.phase.max(WriteAdmissionTestPhase::AllocationWaiting);
        }
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            if self.lock().allocation_released {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn mark_allocated(&self) {
        self.lock().phase = WriteAdmissionTestPhase::Allocated;
        self.changed.notify_waiters();
    }
}

/// Build a `Put` `MutationRecord`.
///
/// `operation_id_base` and `accepted_at_base_ms` are added to `sequence` to
/// derive the record's `operation_id` and `accepted_at_unix_ms`, keeping
/// records built by different callers in disjoint, deterministic ranges.
pub(super) fn put_record(
    sequence: u64,
    path: &str,
    payload: &[u8],
    mode: MutationMode,
    fence: FenceClass,
    operation_id_base: u128,
    accepted_at_base_ms: u64,
) -> MutationRecord {
    MutationRecord {
        format_version: 1,
        sequence,
        operation_id: Uuid::from_u128(operation_id_base + sequence as u128),
        path: path.to_owned(),
        kind: MutationKind::Put {
            mode,
            expected_visible_version: None,
            payload_len: payload.len() as u64,
            payload_sha256: Sha256::digest(payload).into(),
            blob_path: String::new(),
        },
        local_etag: LocalEtag::new(Uuid::nil(), sequence),
        accepted_at_unix_ms: accepted_at_base_ms + sequence,
        remote_predecessor_etag: None,
        remote_result_etag: None,
        fence,
        retry_count: 0,
        last_error: None,
    }
}

/// Build a `Delete` `MutationRecord`. See [`put_record`] for the meaning of
/// `operation_id_base` and `accepted_at_base_ms`.
pub(super) fn delete_record(
    sequence: u64,
    path: &str,
    fence: FenceClass,
    operation_id_base: u128,
    accepted_at_base_ms: u64,
) -> MutationRecord {
    MutationRecord {
        format_version: 1,
        sequence,
        operation_id: Uuid::from_u128(operation_id_base + sequence as u128),
        path: path.to_owned(),
        kind: MutationKind::Delete,
        local_etag: LocalEtag::new(Uuid::nil(), sequence),
        accepted_at_unix_ms: accepted_at_base_ms + sequence,
        remote_predecessor_etag: None,
        remote_result_etag: None,
        fence,
        retry_count: 0,
        last_error: None,
    }
}
