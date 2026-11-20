use super::*;
use crate::pool_object_store::space::{FilesystemSpace, FreeSpace};
use std::sync::{Mutex, mpsc};

#[derive(Debug)]
struct PausedSpace {
    pause: AtomicBool,
    entered: Notify,
    release: Mutex<mpsc::Receiver<()>>,
}

impl FreeSpace for PausedSpace {
    fn observe(&self) -> std::io::Result<FilesystemSpace> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("the test must release the space observation");
        }
        Ok(FilesystemSpace {
            filesystem: 1,
            committed: 0,
            available: 1024,
            total: 1024,
        })
    }
}

async fn cancelled_admission(survival: bool) {
    let fixture = Fixture::new(1024);
    let (release, receive) = mpsc::sync_channel(1);
    let space = Arc::new(PausedSpace {
        pause: AtomicBool::new(false),
        entered: Notify::new(),
        release: Mutex::new(receive),
    });
    let member = fixture.member(true).with_free_space(space.clone());
    let pool = fixture.open_balanced(vec![member], true).await;
    space.pause.store(true, Ordering::SeqCst);
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move {
            if survival {
                pool.put_survival(
                    "hdd-a",
                    &Path::from(".zerofs-survival/v1/cancelled"),
                    Bytes::from(vec![1; 600]).into(),
                    PutOptions::default(),
                )
                .await
                .map(|_| ())
            } else {
                pool.admit(600, None).await.map(drop)
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(10), space.entered.notified())
        .await
        .expect("admission never reached the space observation");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    pool.inner.tasks.close();
    tokio::time::timeout(Duration::from_secs(10), pool.inner.tasks.wait())
        .await
        .expect("cancelled admission did not settle");

    // The blocking catalog task finishes after its caller is gone. Its
    // abandoned result must return all capacity to the next WRITE.
    let claim = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match pool.admit(1024, None).await {
                Ok(claim) => return claim,
                Err(error) => {
                    assert!(crate::retrying_object_store::is_no_space(&error));
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("cancelled admission leaked its capacity claim");
    assert_eq!(claim.member(), "hdd-a");
    assert!(matches!(
        fixture
            .first
            .head(&Path::from(".zerofs-survival/v1/cancelled"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn cancelled_write_admission_releases_its_claim() {
    cancelled_admission(false).await;
}

#[tokio::test]
async fn cancelled_survival_admission_releases_its_claim() {
    cancelled_admission(true).await;
}

#[tokio::test]
async fn cancelled_survival_write_holds_capacity_until_the_put_finishes() {
    let fixture = Fixture::new(1024);
    let (_release, receive) = mpsc::sync_channel(1);
    let member = fixture.member(true).with_free_space(Arc::new(PausedSpace {
        pause: AtomicBool::new(false),
        entered: Notify::new(),
        release: Mutex::new(receive),
    }));
    let pool = fixture.open_balanced(vec![member], true).await;
    fixture
        .first
        .control
        .gate_payload_len
        .store(600, Ordering::SeqCst);
    let location = Path::from(".zerofs-survival/v1/in-flight");
    let caller = tokio::spawn({
        let pool = pool.clone();
        let location = location.clone();
        async move {
            pool.put_survival(
                "hdd-a",
                &location,
                Bytes::from(vec![2; 600]).into(),
                PutOptions::default(),
            )
            .await
        }
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        fixture.first.control.entered.notified(),
    )
    .await
    .expect("survival PUT never started");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let admission = pool.admit(600, None).await;
    fixture.first.control.release.notify_one();
    pool.inner.tasks.close();
    tokio::time::timeout(Duration::from_secs(10), pool.inner.tasks.wait())
        .await
        .expect("survival PUT did not settle");

    assert!(crate::retrying_object_store::is_no_space(
        &admission.expect_err("the in-flight PUT released its room too early")
    ));
    assert_eq!(read(fixture.first.as_ref(), &location).await, vec![2; 600]);
    pool.admit(1024, None).await.unwrap();
}
