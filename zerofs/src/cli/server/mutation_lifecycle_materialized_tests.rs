use super::*;
use crate::fs::ZeroFS;
use crate::fs::mutation::overlay::IdentifiedWrite;
use crate::fs::mutation::types::{RequestIdentity, RequestLifetime};
use crate::fs::permissions::Credentials;
use crate::fs::types::AuthContext;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn production_close_drains_cancelled_materialized_commit_before_database_close() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let auth = AuthContext::from(&Credentials {
        uid: 1000,
        gid: 1000,
        gid_known: true,
        groups: [1000; 16],
        groups_count: 1,
        groups_complete: true,
    });
    let file = fs
        .create_exclusive(&auth, 0, b"shutdown-materialized.txt")
        .await
        .unwrap();
    let data = Bytes::from_static(b"committed-before-close");
    let commit_block = fs.db.flush_barrier().write_owned().await;
    let apply_reached = fs.write_coordinator.probe_next_apply();
    let caller = tokio::spawn({
        let fs = Arc::clone(&fs);
        let auth = auth.clone();
        let data = data.clone();
        async move {
            fs.write_ack_identified(IdentifiedWrite {
                auth: &auth,
                id: file,
                offset: 0,
                data: &data,
                op_id: [0; 16],
                check_permissions: true,
                identity: RequestIdentity::Nfs {
                    server_incarnation: Uuid::nil(),
                    connection_incarnation: 17,
                    xid: 501,
                },
                request_lifetime: RequestLifetime::ReplayWindow(Duration::from_secs(60)),
                fingerprint_context: b"nfs3-write-v1",
            })
            .await
        }
    });
    apply_reached.await.unwrap();
    caller.abort();
    match caller.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("materialized caller completed before cancellation"),
    }

    let verified_before_close = Arc::new(AtomicBool::new(false));
    let mut owners = LifecycleOwners::for_process(
        tokio_util::sync::CancellationToken::new(),
        crate::ninep::server::P9AcceptedWorkTracker::new(),
        Arc::clone(&fs),
        None,
        None,
        false,
    );
    owners.close_database = {
        let fs = Arc::clone(&fs);
        let auth = auth.clone();
        let data = data.clone();
        let verified_before_close = Arc::clone(&verified_before_close);
        Arc::new(move || {
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let data = data.clone();
            let verified_before_close = Arc::clone(&verified_before_close);
            Box::pin(async move {
                let (visible, eof) = fs
                    .read_file(&auth, file, 0, 64)
                    .await
                    .map_err(|error| ShutdownError::failed(ShutdownPhase::CloseDatabase, error))?;
                if visible != data || !eof {
                    return Err(ShutdownError::failed(
                        ShutdownPhase::CloseDatabase,
                        "worker-owned materialized write was not canonical before close",
                    ));
                }
                if fs.quota.committed_bytes() != data.len() as u64 || fs.quota.pending_bytes() != 0
                {
                    return Err(ShutdownError::failed(
                        ShutdownPhase::CloseDatabase,
                        "worker-owned quota was not settled before close",
                    ));
                }
                verified_before_close.store(true, Ordering::SeqCst);
                close_database(&fs, None, false).await
            })
        })
    };
    let lifecycle = MutationLifecycle::new(owners);
    let closing = tokio::spawn({
        let lifecycle = Arc::clone(&lifecycle);
        async move {
            lifecycle
                .close(
                    Instant::now() + Duration::from_secs(5),
                    DurabilityTarget::LocalSsd,
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while lifecycle.phase() != ShutdownPhase::StopMutation {
            assert!(!closing.is_finished(), "shutdown crossed the commit drain");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown did not reach the pre-close mutation drain");
    assert!(!closing.is_finished());
    drop(commit_block);
    let _receipt = closing.await.unwrap().unwrap();
    assert!(verified_before_close.load(Ordering::SeqCst));
    assert_eq!(lifecycle.phase(), ShutdownPhase::Complete);
}
