//! Canonical materialization of accepted write batches.
//!
//! Same-inode work is FIFO. Distinct inodes run concurrently. A striped
//! batch becomes visible to [`MutationProgress`] only after every member
//! has been applied. The first post-ack failure poisons progress and
//! freezes the overlay so clients keep the last coherent view.

use crate::fs::ZeroFS;
use crate::fs::mutation::overlay::FilesystemVolatileOverlay;
use crate::fs::mutation::progress::MutationProgress;
use crate::fs::mutation::types::{
    MutationCutoff, MutationError, MutationIncarnation, PreparedWriteBatch,
};
use crate::fs::ops::write::apply_prepared_batch;
use futures::FutureExt;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
#[cfg(test)]
use std::sync::Condvar;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub(crate) type ApplyHook = Arc<
    dyn Fn(
            MutationCutoff,
            PreparedWriteBatch,
        ) -> Pin<Box<dyn Future<Output = Result<(), MutationError>> + Send>>
        + Send
        + Sync,
>;

enum LaneJob {
    Apply {
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        reply: oneshot::Sender<Result<(), MutationError>>,
    },
    Hold {
        acquired: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

struct MaterializerInner {
    incarnation: MutationIncarnation,
    progress: MutationProgress,
    fs: Weak<ZeroFS>,
    overlay: Weak<FilesystemVolatileOverlay>,
    apply_hook: Option<ApplyHook>,
    lanes: Mutex<HashMap<u64, mpsc::UnboundedSender<LaneJob>>>,
    hold_enqueue: Mutex<()>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    closed: Mutex<bool>,
    #[cfg(test)]
    hold_enqueue_interleave: HoldEnqueueInterleave,
}

#[cfg(test)]
struct HoldEnqueueInterleave {
    enabled: AtomicBool,
    arrivals: AtomicUsize,
    second_attempted: Mutex<bool>,
    changed: Condvar,
}

/// Owns per-inode worker loops that apply accepted batches in order.
#[derive(Clone)]
pub(crate) struct Materializer {
    inner: Arc<MaterializerInner>,
}

impl Materializer {
    pub(crate) fn start(
        incarnation: MutationIncarnation,
        fs: Weak<ZeroFS>,
        overlay: Option<Arc<FilesystemVolatileOverlay>>,
    ) -> Arc<Self> {
        Self::start_with_hook(incarnation, fs, overlay, None)
    }

    pub(crate) fn start_with_hook(
        incarnation: MutationIncarnation,
        fs: Weak<ZeroFS>,
        overlay: Option<Arc<FilesystemVolatileOverlay>>,
        apply_hook: Option<ApplyHook>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MaterializerInner {
                incarnation,
                progress: MutationProgress::new(incarnation),
                fs,
                overlay: overlay
                    .map(|overlay| Arc::downgrade(&overlay))
                    .unwrap_or_default(),
                apply_hook,
                lanes: Mutex::new(HashMap::new()),
                hold_enqueue: Mutex::new(()),
                workers: Mutex::new(Vec::new()),
                closed: Mutex::new(false),
                #[cfg(test)]
                hold_enqueue_interleave: HoldEnqueueInterleave {
                    enabled: AtomicBool::new(false),
                    arrivals: AtomicUsize::new(0),
                    second_attempted: Mutex::new(false),
                    changed: Condvar::new(),
                },
            }),
        })
    }

    pub(crate) fn incarnation(&self) -> MutationIncarnation {
        self.inner.incarnation
    }

    pub(crate) fn progress(&self) -> MutationProgress {
        self.inner.progress.clone()
    }

    pub(crate) async fn dispatch_through(
        self: &Arc<Self>,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
    ) -> Result<(), MutationError> {
        if cutoff.mutation_incarnation != self.inner.incarnation {
            return Err(MutationError::StaleIncarnation);
        }
        if *lock(&self.inner.closed) {
            return Err(MutationError::Closed);
        }
        self.inner.progress.check()?;

        let mut inodes = batch
            .members
            .iter()
            .map(|member| member.id)
            .collect::<BTreeSet<_>>();
        if let Some(replayed) = &batch.replayed {
            inodes.extend(replayed.members.iter().map(|(id, _)| *id));
        }
        if inodes.is_empty() {
            self.record_success(cutoff)?;
            return Ok(());
        }

        if inodes.len() == 1 {
            let inode = *inodes.iter().next().expect("one inode");
            let (reply_tx, reply_rx) = oneshot::channel();
            self.lane(inode)
                .send(LaneJob::Apply {
                    cutoff,
                    batch,
                    reply: reply_tx,
                })
                .map_err(|_| self.poison("inode worker dropped"))?;
            let result = reply_rx
                .await
                .unwrap_or_else(|_| Err(self.poison("inode worker dropped")));
            if result.is_ok() {
                self.retire_inodes(&inodes);
            }
            return result;
        }

        let mut holds = Vec::with_capacity(inodes.len());
        #[cfg(test)]
        let interleave_position = self.hold_enqueue_interleave_position_for_test();
        {
            // All lanes must observe multi-inode batches in one global order.
            // Hold only this synchronous enqueue lock; the potentially slow
            // acquisition and apply phases remain concurrent.
            let _enqueue = lock(&self.inner.hold_enqueue);
            for inode in &inodes {
                #[cfg(test)]
                let is_first = holds.is_empty();
                let (acquired_tx, acquired_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                self.lane(*inode)
                    .send(LaneJob::Hold {
                        acquired: acquired_tx,
                        release: release_rx,
                    })
                    .map_err(|_| self.poison("inode worker dropped"))?;
                holds.push((acquired_rx, release_tx));
                #[cfg(test)]
                if is_first && interleave_position == Some(0) {
                    let attempted = lock(&self.inner.hold_enqueue_interleave.second_attempted);
                    drop(
                        self.inner
                            .hold_enqueue_interleave
                            .changed
                            .wait_while(attempted, |attempted| !*attempted)
                            .unwrap_or_else(|error| error.into_inner()),
                    );
                }
            }
        }
        for (acquired, _) in &mut holds {
            acquired
                .await
                .map_err(|_| self.poison("inode worker dropped"))?;
        }

        let result = self.apply_caught(cutoff, batch).await;
        for (_, release) in holds {
            let _ = release.send(());
        }
        if result.is_ok() {
            self.retire_inodes(&inodes);
        }
        result
    }

    pub(crate) async fn stop(&self) {
        {
            *lock(&self.inner.closed) = true;
        }
        let senders = {
            let mut lanes = lock(&self.inner.lanes);
            lanes.drain().map(|(_, sender)| sender).collect::<Vec<_>>()
        };
        drop(senders);
        let workers = {
            let mut workers = lock(&self.inner.workers);
            std::mem::take(&mut *workers)
        };
        for worker in workers {
            let _ = worker.await;
        }
    }

    fn lane(self: &Arc<Self>, inode: u64) -> mpsc::UnboundedSender<LaneJob> {
        let mut lanes = lock(&self.inner.lanes);
        if let Some(sender) = lanes.get(&inode) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let worker = spawn_lane(Arc::clone(self), receiver);
        lock(&self.inner.workers).push(worker);
        lanes.insert(inode, sender.clone());
        sender
    }

    #[cfg(test)]
    fn interleave_next_hold_enqueues_for_test(&self) {
        let interleave = &self.inner.hold_enqueue_interleave;
        *lock(&interleave.second_attempted) = false;
        interleave.arrivals.store(0, Ordering::Release);
        interleave.enabled.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn hold_enqueue_interleave_position_for_test(&self) -> Option<usize> {
        let interleave = &self.inner.hold_enqueue_interleave;
        if !interleave.enabled.load(Ordering::Acquire) {
            return None;
        }
        let position = interleave.arrivals.fetch_add(1, Ordering::AcqRel);
        if position == 1 {
            interleave.enabled.store(false, Ordering::Release);
            *lock(&interleave.second_attempted) = true;
            interleave.changed.notify_all();
        }
        Some(position)
    }

    async fn apply_caught(
        &self,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
    ) -> Result<(), MutationError> {
        let apply = AssertUnwindSafe(self.apply_batch(cutoff, batch)).catch_unwind();
        match apply.await {
            Ok(Ok(())) => self.record_success(cutoff),
            Ok(Err(error)) => {
                let message = error.to_string();
                Err(self.poison(message))
            }
            Err(_) => Err(self.poison("materializer worker panicked")),
        }
    }

    async fn apply_batch(
        &self,
        cutoff: MutationCutoff,
        mut batch: PreparedWriteBatch,
    ) -> Result<(), MutationError> {
        if let Some(hook) = &self.inner.apply_hook {
            return hook(cutoff, batch).await;
        }
        let Some(fs) = self.inner.fs.upgrade() else {
            return Err(MutationError::Closed);
        };
        apply_prepared_batch(&fs.write_apply_context(), &mut batch)
            .await
            .map(|_| ())
            .map_err(|error| MutationError::Poisoned(error.to_string()))
    }

    fn record_success(&self, cutoff: MutationCutoff) -> Result<(), MutationError> {
        self.inner.progress.record_materialized(cutoff)
    }

    fn retire_inodes(&self, inodes: &BTreeSet<u64>) {
        if let Some(overlay) = self.inner.overlay.upgrade() {
            for inode in inodes {
                overlay.retire_inode(*inode);
            }
        }
    }

    fn poison(&self, message: impl Into<String>) -> MutationError {
        let message = message.into();
        self.inner.progress.poison(message.clone());
        if let Some(overlay) = self.inner.overlay.upgrade() {
            overlay.freeze_terminal();
        }
        MutationError::Poisoned(message)
    }
}

fn spawn_lane(
    materializer: Arc<Materializer>,
    mut receiver: mpsc::UnboundedReceiver<LaneJob>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(job) = receiver.recv().await {
            match job {
                LaneJob::Apply {
                    cutoff,
                    batch,
                    reply,
                } => {
                    let result = materializer.apply_caught(cutoff, batch).await;
                    let _ = reply.send(result);
                }
                LaneJob::Hold { acquired, release } => {
                    let _ = acquired.send(());
                    let _ = release.await;
                }
            }
        }
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::config::{
        ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
        FilesystemWriteAckSource,
    };
    use crate::fs::mutation::types::{PrepareWriteMember, PrepareWriteRequest};
    use crate::fs::ops::write::prepare_write;
    use crate::fs::test_util::test_creds;
    use crate::fs::types::{AuthContext, SetAttributes};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    fn cutoff(incarnation: MutationIncarnation, sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: incarnation,
            sequence,
        }
    }

    fn volatile_settings() -> FilesystemWriteAckSettings {
        FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::VolatileMemory,
            volatile_memory_bytes: 8 * 1024 * 1024,
            volatile_max_operations: 1024,
            source: FilesystemWriteAckSource::Filesystem,
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        }
    }

    async fn filesystem() -> (Arc<ZeroFS>, AuthContext) {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();
        (fs, AuthContext::from(&test_creds()))
    }

    async fn create_file(fs: &ZeroFS, _auth: &AuthContext, name: &[u8]) -> u64 {
        fs.create(&test_creds(), 0, name, &SetAttributes::default())
            .await
            .unwrap()
            .0
    }

    async fn prepare(
        fs: &ZeroFS,
        auth: AuthContext,
        id: u64,
        data: &'static [u8],
    ) -> PreparedWriteBatch {
        let mut batch = prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id,
                    offset: 0,
                    data: Bytes::from_static(data),
                }],
                auth,
                op_id: [0u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap();
        batch.guards = None;
        batch
    }

    async fn prepare_striped(
        fs: &ZeroFS,
        auth: AuthContext,
        first: u64,
        second: u64,
    ) -> PreparedWriteBatch {
        prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![
                    PrepareWriteMember {
                        id: first,
                        offset: 0,
                        data: Bytes::from_static(b"AAAA"),
                    },
                    PrepareWriteMember {
                        id: second,
                        offset: 0,
                        data: Bytes::from_static(b"BBBB"),
                    },
                ],
                auth,
                op_id: [1u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn same_inode_applies_fifo() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"fifo.txt").await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let incarnation = MutationIncarnation::new();
        let hook_order = Arc::clone(&order);
        let hook: ApplyHook = Arc::new(move |cutoff, _batch| {
            let order = Arc::clone(&hook_order);
            Box::pin(async move {
                order.lock().unwrap().push(("start", cutoff.sequence));
                tokio::time::sleep(Duration::from_millis(40)).await;
                order.lock().unwrap().push(("end", cutoff.sequence));
                Ok(())
            })
        });
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            fs.volatile_overlay.get().cloned(),
            Some(hook),
        );

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), inode, b"one").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), inode, b"two").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(
            *order.lock().unwrap(),
            vec![("start", 1), ("end", 1), ("start", 2), ("end", 2)]
        );
        materializer.stop().await;
    }

    #[tokio::test]
    async fn different_inodes_apply_concurrently() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"a.txt").await;
        let second_id = create_file(&fs, &auth, b"b.txt").await;
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_cutoff, _batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), first_id, b"aaa").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), second_id, b"bbb").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("distinct inodes must be in flight together");
        release.notify_waiters();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        materializer.stop().await;
    }

    #[tokio::test]
    async fn striped_batch_completes_after_all_members() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"stripe0.bin").await;
        let second_id = create_file(&fs, &auth, b"stripe1.bin").await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let seen = Arc::clone(&seen);
            Arc::new(move |_cutoff, batch| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    let members = batch
                        .members
                        .iter()
                        .map(|member| member.id)
                        .collect::<Vec<_>>();
                    seen.lock().unwrap().push(members);
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let mut batch = prepare_striped(&fs, auth, first_id, second_id).await;
        batch.guards = None;
        materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "striped batch is one apply");
        assert_eq!(seen[0].len(), 2);
        assert_eq!(materializer.progress().materialized_through(), 1);
        materializer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_striped_batches_cannot_split_lane_holds() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"overlap0.bin").await;
        let second_id = create_file(&fs, &auth, b"overlap1.bin").await;
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = Arc::new(|_cutoff, _batch| Box::pin(async { Ok(()) }));
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let mut first_batch = prepare_striped(&fs, auth.clone(), first_id, second_id).await;
        first_batch.guards = None;
        let mut second_batch = prepare_striped(&fs, auth, first_id, second_id).await;
        second_batch.guards = None;
        materializer.interleave_next_hold_enqueues_for_test();
        let start = Arc::new(tokio::sync::Barrier::new(3));

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let start = Arc::clone(&start);
            async move {
                start.wait().await;
                materializer
                    .dispatch_through(cutoff(incarnation, 1), first_batch)
                    .await
            }
        });
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let start = Arc::clone(&start);
            async move {
                start.wait().await;
                materializer
                    .dispatch_through(cutoff(incarnation, 2), second_batch)
                    .await
            }
        });
        start.wait().await;

        tokio::time::timeout(Duration::from_secs(1), async {
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
        })
        .await
        .expect("overlapping striped batches split their inode holds and deadlocked");
        materializer.stop().await;
    }

    #[tokio::test]
    async fn global_prefix_waits_for_gap() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"gap1.txt").await;
        let second_id = create_file(&fs, &auth, b"gap2.txt").await;
        let release_first = Arc::new(Notify::new());
        let second_done = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let release_first = Arc::clone(&release_first);
            let second_done = Arc::clone(&second_done);
            Arc::new(move |cutoff, _batch| {
                let release_first = Arc::clone(&release_first);
                let second_done = Arc::clone(&second_done);
                Box::pin(async move {
                    if cutoff.sequence == 1 {
                        release_first.notified().await;
                    } else {
                        second_done.notify_waiters();
                    }
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));

        let later = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), second_id, b"two").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), second_done.notified())
            .await
            .expect("sequence 2 must apply without waiting for 1");
        assert_eq!(materializer.progress().materialized_through(), 0);

        let waiter = tokio::spawn({
            let progress = materializer.progress();
            async move { progress.wait_materialized(cutoff(incarnation, 2)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        let earlier = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), first_id, b"one").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        release_first.notify_waiters();
        earlier.await.unwrap().unwrap();
        later.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(materializer.progress().materialized_through(), 2);
        materializer.stop().await;
    }

    #[tokio::test]
    async fn overlay_retires_after_canonical_visibility() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"retire.txt").await;
        let overlay = fs.volatile_overlay.get().cloned().expect("overlay");
        let batch = prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id: inode,
                    offset: 0,
                    data: Bytes::from_static(b"visible"),
                }],
                auth: auth.clone(),
                op_id: [2u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap();
        let attrs = batch.members[0].post_attrs.clone();
        overlay.preview_attrs(inode, attrs.clone());
        assert_eq!(overlay.visible_size(inode, 0), attrs.size);

        let incarnation = MutationIncarnation::new();
        let materializer =
            Materializer::start(incarnation, Arc::downgrade(&fs), Some(Arc::clone(&overlay)));
        materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .unwrap();

        let persisted = fs.inode_store.get(inode).await.unwrap();
        let canonical = match persisted {
            crate::fs::inode::Inode::File(file) => file.size,
            other => panic!("expected file, got {other:?}"),
        };
        assert_eq!(canonical, attrs.size);
        assert_eq!(
            overlay.visible_size(inode, canonical),
            canonical,
            "overlay must retire after canonical apply owns the bytes"
        );
        let (data, _) = fs.read_file(&auth, inode, 0, 16).await.unwrap();
        assert_eq!(data.as_ref(), b"visible");
        materializer.stop().await;
    }

    #[tokio::test]
    async fn panic_poison_retains_frozen_view() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"poison.txt").await;
        let overlay = fs.volatile_overlay.get().cloned().expect("overlay");
        let batch = prepare(&fs, auth.clone(), inode, b"frozen").await;
        let attrs = batch.members[0].post_attrs.clone();
        overlay.preview_attrs(inode, attrs.clone());

        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = Arc::new(|_cutoff, _batch| {
            Box::pin(async move {
                panic!("canonical apply exploded");
            })
        });
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            Some(Arc::clone(&overlay)),
            Some(hook),
        );
        let error = materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .expect_err("panic must poison");
        assert!(matches!(error, MutationError::Poisoned(_)));
        assert!(overlay.is_frozen());
        assert_eq!(
            overlay.visible_size(inode, 0),
            attrs.size,
            "poison must keep the last coherent overlay view"
        );
        let later = prepare(&fs, auth, inode, b"later").await;
        let closed = materializer
            .dispatch_through(cutoff(incarnation, 2), later)
            .await
            .expect_err("poisoned materializer rejects later work");
        assert!(matches!(closed, MutationError::Poisoned(_)));
        materializer.stop().await;
    }

    #[tokio::test]
    async fn stop_joins_worker_loops() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"stop.txt").await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_cutoff, _batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_waiters();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let dispatched = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth, inode, b"stop").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let stopping = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            async move { materializer.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !stopping.is_finished(),
            "stop must join the in-flight worker, not detach it"
        );
        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), stopping)
            .await
            .expect("stop must join")
            .unwrap();
        dispatched.await.unwrap().unwrap();
        assert!(
            lock(&materializer.inner.workers).is_empty(),
            "stop must take and join every worker handle"
        );
    }
}
