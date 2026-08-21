use super::{ObjectHeader, SftpCapabilities, SftpObjectStore};
use crate::sftp_transport::{
    RemoteDirectoryEntry, RemoteEntryKind, RemoteObjectRead, SessionFactory, TransportError,
    TransportSession,
};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Debug, Default)]
struct ListingRaceState {
    dials: AtomicUsize,
    closes: AtomicUsize,
    vanished_started: Notify,
    release_vanished: Notify,
    survivor_started: Notify,
    release_survivor: Notify,
}

#[derive(Debug, Clone)]
struct ListingRaceFactory(Arc<ListingRaceState>);

#[async_trait]
impl SessionFactory for ListingRaceFactory {
    async fn open(
        &self,
        _force: tokio_util::sync::CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError> {
        self.0.dials.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(ListingRaceSession(Arc::clone(&self.0))))
    }
}

#[derive(Debug)]
struct ListingRaceSession(Arc<ListingRaceState>);

#[async_trait]
impl TransportSession for ListingRaceSession {
    fn capabilities(&self) -> SftpCapabilities {
        SftpCapabilities {
            fsync: true,
            hardlink: true,
            posix_rename: true,
        }
    }

    async fn list_directory(
        &self,
        path: &Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        assert_eq!(path, Path::new("root"));
        Ok(vec![
            RemoteDirectoryEntry {
                filename: "vanished".into(),
                kind: RemoteEntryKind::File,
            },
            RemoteDirectoryEntry {
                filename: "survivor".into(),
                kind: RemoteEntryKind::File,
            },
        ])
    }

    async fn read_object(
        &self,
        path: &Path,
        _range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        assert!(head);
        if path == Path::new("root/vanished") {
            self.0.vanished_started.notify_one();
            self.0.release_vanished.notified().await;
            return Err(TransportError::NotFound(path.display().to_string()));
        }
        assert_eq!(path, Path::new("root/survivor"));
        self.0.survivor_started.notify_one();
        self.0.release_survivor.notified().await;
        Ok(RemoteObjectRead {
            header: ObjectHeader {
                generation: Uuid::nil(),
                logical_len: 7,
            },
            modified: SystemTime::UNIX_EPOCH,
            range: 0..7,
            payload: Bytes::new(),
        })
    }

    async fn close(
        &self,
        _force: tokio_util::sync::CancellationToken,
    ) -> Result<(), TransportError> {
        self.0.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn listed_child_vanishing_does_not_cancel_other_metadata_or_retire_the_session() {
    let state = Arc::new(ListingRaceState::default());
    let pool = crate::sftp_transport::SftpSessionPool::new_writable(
        Arc::new(ListingRaceFactory(Arc::clone(&state))),
        2,
        2,
        1,
    )
    .await
    .unwrap();
    let store = Arc::new(SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap());
    let listing = tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.collect_recursive(ObjectPath::from("root")).await }
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            state.vanished_started.notified(),
            state.survivor_started.notified()
        );
    })
    .await
    .expect("both snapshot metadata lookups were not concurrently active");
    let dials_while_both_lookups_are_active = state.dials.load(Ordering::SeqCst);
    state.release_vanished.notify_one();
    tokio::task::yield_now().await;
    assert!(
        !listing.is_finished(),
        "a vanished snapshot child must not abort and cancel the surviving lookup"
    );

    state.release_survivor.notify_one();
    let objects = tokio::time::timeout(Duration::from_secs(1), listing)
        .await
        .expect("listing did not settle after surviving metadata completed")
        .unwrap()
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].location, ObjectPath::from("root/survivor"));
    assert_eq!(
        state.dials.load(Ordering::SeqCst),
        dials_while_both_lookups_are_active,
        "settling the listing must not dial a replacement session"
    );
    assert_eq!(state.closes.load(Ordering::SeqCst), 0);

    drop(store);
    pool.shutdown().await.unwrap();
}
