use super::*;
use crate::fault_store::{FaultControls, FaultStore};
use crate::pool_object_store::{PoolMember, PoolObjectStore};
use crate::retrying_object_store::{RetryingObjectStore, has_permanent_source};
use crate::segment::Segid;
use crate::writeback::journaler::LocalJournaler;
use crate::writeback::model::JournalIdentity;
use bytes::Bytes;
use object_store::memory::InMemory;

struct Fixture {
    directory: tempfile::TempDir,
    pool: PoolObjectStore,
    first: Arc<FaultStore>,
    controls: Arc<FaultControls>,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let (first, controls) = FaultStore::new(Arc::new(InMemory::new()));
        let members = [
            ("first", first.clone() as Arc<dyn ObjectStore>),
            ("second", Arc::new(InMemory::new()) as Arc<dyn ObjectStore>),
        ]
        .into_iter()
        .map(|(id, store)| PoolMember {
            id: id.into(),
            identity: format!("memory://{id}"),
            store,
            capacity_bytes: 1024 * 1024,
            local_path: None,
            min_free_bytes: 0,
            health_device: None,
            free_space: None,
        })
        .collect();
        let pool = PoolObjectStore::initialize(
            Arc::new(InMemory::new()),
            Path::from("root"),
            directory.path().join("catalog"),
            members,
        )
        .await
        .unwrap();

        // Initialize the first member's identity before gating its next PUT,
        // so the pause falls inside a physical payload write, not marker setup.
        let warmup = Path::from(format!("root/{}", Segid::new(1, 0).object_key()));
        pool.put_opts(
            &warmup,
            Bytes::from_static(b"warmup").into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
        pool.delete(&warmup).await.unwrap();
        controls.block_puts();
        Self {
            directory,
            pool,
            first,
            controls,
        }
    }

    async fn quarantine_started_put(&self, previous_puts: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.controls.put_count() == previous_puts {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first member must start the payload write");
        self.pool
            .quarantine("first", "quarantined while its payload write was in flight")
            .await
            .unwrap();
        // The paused PUT has already passed the write partition check. It
        // completes, but cleanup fails and must retain this generation's debt.
        self.controls.partition_writes(true);
        self.controls.release_puts();
    }

    async fn assert_cleanup_debt(&self, payload: &Bytes) {
        let status = self.pool.status().await.unwrap();
        let first = status.members.iter().find(|m| m.id == "first").unwrap();
        assert_eq!(first.used_bytes, 0);
        assert_eq!(first.reserved_bytes, payload.len() as u64);
        assert_eq!(first.pending_uploads, 1);
        assert_eq!(first.objects, 0);
        assert!(!first.safe_to_remove);
        let physical = self.controls.put_paths().pop().unwrap();
        assert!(physical.contains("/objects/"));
        assert_eq!(
            self.first
                .get(&Path::from(physical))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            *payload
        );
    }
}

#[tokio::test]
async fn direct_quarantine_refusal_remains_permanent() {
    let fixture = Fixture::new().await;
    let remote = RetryingObjectStore::new(Arc::new(fixture.pool.clone()));
    let path = Path::from(format!("root/{}", Segid::new(1, 1).object_key()));
    let payload = Bytes::from_static(b"direct request interrupted by quarantine");
    let previous_puts = fixture.controls.put_count();
    let bytes = payload.clone();
    let key = path.clone();
    let writer = tokio::spawn(async move {
        remote
            .put_opts(&key, bytes.into(), PutMode::Create.into())
            .await
    });
    fixture.quarantine_started_put(previous_puts).await;
    let error = tokio::time::timeout(Duration::from_secs(3), writer)
        .await
        .expect("direct requests must return the quarantine refusal")
        .unwrap()
        .unwrap_err();
    assert!(has_permanent_source(&error));
    assert!(crate::pool_object_store::is_prepublication_allocation_refusal(&error));
    assert!(!is_terminal_remote_error(&error));
    assert_eq!(fixture.controls.put_count(), previous_puts + 1);
    assert!(fixture.pool.placements().await.unwrap().is_empty());
    fixture.assert_cleanup_debt(&payload).await;
    fixture.pool.settle().await;
}

#[tokio::test]
async fn quarantined_replay_reselects_member_and_keeps_cleanup_debt() {
    let fixture = Fixture::new().await;
    let root = fixture.directory.path().join("journal");
    let journal = Arc::new(
        Journal::open(
            &root,
            JournalIdentity {
                format_version: 1,
                bucket_id: "placement-recovery".into(),
                backend_endpoint: "memory://placement-recovery".into(),
                database_prefix: "root".into(),
                backend_kind: "pool".into(),
                encryption_key_identity_sha256: [0x42; 32],
            },
        )
        .unwrap(),
    );
    let remote: Arc<dyn ObjectStore> =
        Arc::new(RetryingObjectStore::new(Arc::new(fixture.pool.clone())));
    let path = Path::from(format!("root/{}", Segid::new(1, 1).object_key()));
    let payload = Bytes::from_static(b"acknowledged bytes interrupted by quarantine");
    let record = crate::writeback::test_util::put_record(
        1,
        path.as_ref(),
        &payload,
        MutationMode::Create,
        FenceClass::ImmutableCreate,
        0,
        0,
    );
    let record = journal.commit_put(record, &payload).unwrap();
    let reservation = SsdReservationRequest::from_pending_record(&record).unwrap();
    let admission = Admission::new(1024 * 1024);
    let ssd =
        Arc::new(SsdAdmission::recover(1024 * 1024, 100, 90, 70, 1, [reservation], None).unwrap());
    let space = Arc::new(PhysicalSpaceSampler::new(root));
    let overlay = OverlayIndex::recover(remote.clone(), journal.clone())
        .await
        .unwrap();
    let journaler = LocalJournaler::start_with_observer_and_space(
        journal.clone(),
        admission.clone(),
        4,
        1,
        None,
        space.clone(),
    )
    .unwrap();
    let previous_puts = fixture.controls.put_count();
    let scheduler = RemoteScheduler::start(
        remote,
        journal.clone(),
        overlay,
        admission.clone(),
        ssd.clone(),
        space,
        journaler.barrier(),
        1,
    )
    .unwrap();
    fixture.quarantine_started_put(previous_puts).await;

    tokio::time::timeout(Duration::from_secs(3), scheduler.barrier().wait_remote(1))
        .await
        .expect("durable replay must choose another healthy member")
        .expect("a prepublication quarantine refusal must not poison the journal");
    assert_eq!(
        fixture
            .pool
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        payload
    );
    let placements = fixture.pool.placements().await.unwrap();
    assert_eq!(placements.len(), 1);
    assert_eq!(placements[0].member_id, "second");
    fixture.assert_cleanup_debt(&payload).await;
    assert_eq!(journal.progress().unwrap().remote_seq, 1);
    assert_eq!(ssd.used_bytes(), 0);
    assert!(scheduler.terminal_error().is_none());
    drop(
        admission
            .reserve(1)
            .await
            .expect("RAM admission must remain usable"),
    );
    scheduler.shutdown().await.unwrap();
    journaler.shutdown().await.unwrap();
    fixture.pool.settle().await;
}
