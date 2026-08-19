//! Bounded replay ownership for identified writes in materialized mode.

use super::overlay::{IdentifiedWrite, WriteAckReceipt};
use super::request_cache::{AcceptedRequest, RequestCache, RequestLookup};
use super::types::{
    PrepareWriteMember, PrepareWriteRequest, PreparedBatchResult, RequestFingerprint,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::ops::write::{apply_prepared_batch, prepare_write};

/// Completion ownership transferred atomically with the canonical transaction.
/// Dropping it before submission retracts the request-cache entry; after
/// submission only the commit worker can publish the terminal outcome.
pub(crate) struct MaterializedReplayCompletion {
    cache: RequestCache,
    accepted: AcceptedRequest,
    result: PreparedBatchResult,
}

impl MaterializedReplayCompletion {
    fn new(cache: RequestCache, accepted: AcceptedRequest, result: PreparedBatchResult) -> Self {
        Self {
            cache,
            accepted,
            result,
        }
    }

    pub(crate) fn finish(self, outcome: Result<(), FsError>) {
        let Self {
            cache,
            accepted,
            result,
        } = self;
        cache.complete(accepted, outcome.map(|()| result));
    }
}

impl ZeroFS {
    pub(super) async fn write_materialized_nfs_identified(
        &self,
        write: IdentifiedWrite<'_>,
        fingerprint: RequestFingerprint,
    ) -> Result<WriteAckReceipt, FsError> {
        let IdentifiedWrite {
            auth,
            id,
            offset,
            data,
            op_id,
            check_permissions,
            identity,
            request_lifetime,
            fingerprint_context: _,
        } = write;
        let pending = match self
            .materialized_request_cache
            .lookup_or_reserve(identity, fingerprint, request_lifetime)
            .map_err(|_| FsError::IoError)?
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            RequestLookup::Joined(retained) => {
                let result = retained.wait().await?;
                return receipt(result);
            }
            RequestLookup::FingerprintMismatch => return Err(FsError::InvalidArgument),
            RequestLookup::Backpressured => return Err(FsError::RetryLater),
        };
        let retained = pending.retained();
        let accepted = pending.accept();
        let request = PrepareWriteRequest {
            members: vec![PrepareWriteMember {
                id,
                offset,
                data: data.clone(),
            }],
            auth: auth.clone(),
            op_id,
            check_permissions,
        };
        let mut batch = match prepare_write(&self.write_prepare_context(), request).await {
            Ok(batch) => batch,
            Err(error) => {
                self.materialized_request_cache
                    .complete(accepted, Err(error));
                return receipt(retained.wait().await?);
            }
        };
        let result = PreparedBatchResult {
            members: batch
                .members
                .iter()
                .map(|member| (member.id, member.post_attrs.clone()))
                .collect(),
            cutoff: Some(self.capture_mutation_cutoff()),
        };
        batch.commit_ownership = crate::fs::write_coordinator::CommitOwnership::Materialized(
            MaterializedReplayCompletion::new(
                self.materialized_request_cache.clone(),
                accepted,
                result,
            ),
        );
        let applied = apply_prepared_batch(&self.write_apply_context(), &mut batch).await;
        if let crate::fs::write_coordinator::CommitOwnership::Materialized(completion) =
            std::mem::take(&mut batch.commit_ownership)
        {
            completion.finish(applied.as_ref().map(|_| ()).map_err(|error| *error));
        }
        receipt(retained.wait().await?)
    }
}

fn receipt(result: PreparedBatchResult) -> Result<WriteAckReceipt, FsError> {
    Ok(WriteAckReceipt {
        attrs: result.primary_attrs(),
        cutoff: result.cutoff.ok_or(FsError::IoError)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::overlay::IdentifiedWrite;
    use crate::fs::mutation::types::{RequestFingerprint, RequestIdentity, RequestLifetime};
    use crate::fs::test_util::test_creds;
    use crate::fs::types::AuthContext;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Duration;

    fn nfs_identity(xid: u32) -> RequestIdentity {
        RequestIdentity::Nfs {
            server_incarnation: uuid::Uuid::nil(),
            connection_incarnation: 17,
            xid,
        }
    }

    fn identified<'a>(
        auth: &'a AuthContext,
        id: crate::fs::inode::InodeId,
        data: &'a Bytes,
        identity: RequestIdentity,
    ) -> IdentifiedWrite<'a> {
        IdentifiedWrite {
            auth,
            id,
            offset: 0,
            data,
            op_id: [0; 16],
            check_permissions: true,
            identity,
            request_lifetime: RequestLifetime::ReplayWindow(Duration::from_secs(60)),
            fingerprint_context: b"nfs3-write-v1",
        }
    }

    #[tokio::test]
    async fn materialized_nfs_identified_write_replays_and_rejects_collision() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"materialized-nfs.txt")
            .await
            .unwrap();
        let identity = nfs_identity(91);
        let data = Bytes::from_static(b"first-payload");
        let first = fs
            .write_ack_identified(identified(&auth, file, &data, identity.clone()))
            .await
            .unwrap();
        let retry = fs
            .write_ack_identified(identified(&auth, file, &data, identity.clone()))
            .await
            .unwrap();
        assert_eq!(format!("{:?}", retry.attrs), format!("{:?}", first.attrs));
        assert_eq!(retry.cutoff, first.cutoff);

        let collision_data = Bytes::from_static(b"changed-payload");
        let collision = fs
            .write_ack_identified(identified(&auth, file, &collision_data, identity))
            .await;
        assert!(matches!(collision, Err(FsError::InvalidArgument)));
        let (visible, eof) = fs.read_file(&auth, file, 0, 64).await.unwrap();
        assert_eq!(visible, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn materialized_nfs_request_cache_pressure_is_retry_later() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.materialized_request_cache = super::super::request_cache::RequestCache::new(1);
        let fs = Arc::new(fs);
        let auth = AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"materialized-pressure.txt")
            .await
            .unwrap();
        let held = match fs
            .materialized_request_cache
            .lookup_or_reserve(
                RequestIdentity::Nbd {
                    connection_incarnation: 1,
                    handle: 1,
                },
                RequestFingerprint::from_parts(&[b"held"]),
                RequestLifetime::InFlightOnly,
            )
            .unwrap()
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            other => panic!("expected vacant materialized request slot, got {other:?}"),
        };
        let empty = Bytes::new();
        let result = fs
            .write_ack_identified(identified(&auth, file, &empty, nfs_identity(7)))
            .await;
        assert!(matches!(result, Err(FsError::RetryLater)));
        held.cancel();
    }

    #[tokio::test]
    async fn materialized_nfs_caller_cancellation_preserves_owned_replay() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"materialized-cancel.txt")
            .await
            .unwrap();
        let identity = nfs_identity(92);
        let data = Bytes::from_static(b"survives-caller-cancellation");
        let commit_block = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();
        let caller = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let data = data.clone();
            let identity = identity.clone();
            async move {
                fs.write_ack_identified(identified(&auth, file, &data, identity))
                    .await
            }
        });
        apply_reached.await.unwrap();
        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("materialized write caller completed before cancellation"),
        }

        let shutdown_drain = tokio::spawn({
            let fs = Arc::clone(&fs);
            async move { fs.stop_mutation_workers().await.unwrap() }
        });
        tokio::task::yield_now().await;
        assert!(
            !shutdown_drain.is_finished(),
            "shutdown must wait for the worker-owned materialized commit"
        );
        drop(commit_block);
        shutdown_drain.await.unwrap();
        let retry = tokio::time::timeout(
            Duration::from_secs(2),
            fs.write_ack_identified(identified(&auth, file, &data, identity)),
        )
        .await
        .expect("owned materialized write did not complete after caller cancellation")
        .unwrap();
        assert_eq!(retry.attrs.size, data.len() as u64);
        let (visible, eof) = fs.read_file(&auth, file, 0, 64).await.unwrap();
        assert_eq!(visible, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn cancelled_caller_keeps_overlap_tail_and_quota_owned_until_commit() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"materialized-settlement.txt")
            .await
            .unwrap();
        let initial_bytes_written = fs
            .stats
            .bytes_written
            .load(std::sync::atomic::Ordering::Relaxed);
        let initial_write_operations = fs
            .stats
            .write_operations
            .load(std::sync::atomic::Ordering::Relaxed);
        let first_data = Bytes::from(vec![b'A'; 8 * 1024]);
        let second_data = Bytes::from(vec![b'B'; 8 * 1024]);

        let commit_block = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();
        let first = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let data = first_data.clone();
            async move {
                fs.write_ack_identified(identified(&auth, file, &data, nfs_identity(95)))
                    .await
            }
        });
        apply_reached.await.unwrap();
        first.abort();
        match first.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("materialized caller completed before cancellation"),
        }

        let second = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let data = second_data.clone();
            async move { fs.write(&auth, file, 4 * 1024, &data).await }
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !second.is_finished(),
            "overlapping write crossed a cancelled caller's worker-owned extent guard"
        );
        assert_eq!(
            fs.quota.pending_bytes(),
            first_data.len() as u64,
            "cancelled caller released the queued write's growth claim"
        );
        assert_eq!(fs.quota.committed_bytes(), 0);

        drop(commit_block);
        let second_attrs = second.await.unwrap().unwrap();
        assert_eq!(second_attrs.size, 12 * 1024);
        assert_eq!(fs.quota.pending_bytes(), 0);
        assert_eq!(fs.quota.committed_bytes(), 12 * 1024);
        assert_eq!(fs.quota.visible_bytes(), 12 * 1024);
        assert_eq!(
            fs.stats
                .bytes_written
                .load(std::sync::atomic::Ordering::Relaxed),
            initial_bytes_written + 16 * 1024
        );
        assert_eq!(
            fs.stats
                .write_operations
                .load(std::sync::atomic::Ordering::Relaxed),
            initial_write_operations + 2
        );

        let (visible, eof) = fs.read_file(&auth, file, 0, 16 * 1024).await.unwrap();
        let mut expected = vec![b'B'; 12 * 1024];
        expected[..4 * 1024].fill(b'A');
        assert_eq!(
            visible, expected,
            "the overlap staged before the first commit published its tail"
        );
        assert!(eof);
    }

    #[tokio::test]
    async fn materialized_nfs_pre_submit_cancellation_retracts_request() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"materialized-pre-submit-cancel.txt")
            .await
            .unwrap();
        let identity = nfs_identity(93);
        let data = Bytes::from_static(b"must-not-be-written");
        let inode_lock = fs.lock_manager.acquire(file).await;
        let caller = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let data = data.clone();
            async move {
                fs.write_ack_identified(identified(&auth, file, &data, identity))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while fs.materialized_request_cache.used_slots() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("materialized request did not reach pre-submit admission");
        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("materialized write caller crossed the held inode lock"),
        }
        assert_eq!(fs.materialized_request_cache.used_slots(), 0);
        assert_eq!(fs.materialized_request_cache.len(), 0);
        drop(inode_lock);

        fs.stop_new_mutation_admission();
        let rejected = fs
            .write_ack_identified(identified(&auth, file, &data, nfs_identity(94)))
            .await;
        assert!(matches!(rejected, Err(FsError::IoError)));

        let (visible, eof) = fs.read_file(&auth, file, 0, 64).await.unwrap();
        assert!(visible.is_empty());
        assert!(eof);
    }
}
