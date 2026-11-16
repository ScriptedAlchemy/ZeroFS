//! A reusable fault-injecting object store for tests: a pass-through decorator
//! over any inner store whose writes can be partitioned or made to fail
//! transiently, and whose reads can fail or return short bodies. Generalizes the
//! one-off wrappers in `replication::zombie` (write partition) and
//! `length_checked_object_store`'s `ShortBodyStore` (truncated reads) into one
//! programmable harness, driven through a shared [`FaultControls`] handle.

use async_trait::async_trait;
#[cfg(test)]
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Knobs shared with the test driving the store. Everything defaults to "no fault".
#[derive(Debug, Default)]
pub struct FaultControls {
    /// While true, every write (put/delete/copy) fails fast: a write partition.
    partition_writes: AtomicBool,
    /// Fail the next N `get` calls with a transient error, then resume.
    fail_next_gets: AtomicUsize,
    /// Fail the next N `put` calls before applying them, then resume.
    fail_next_puts: AtomicUsize,
    /// Apply the next N `put` calls, then return a transient response error.
    fail_after_puts: AtomicUsize,
    /// Return a body `truncate_bytes` short of the claimed length on the next N gets.
    truncate_next_gets: AtomicUsize,
    truncate_bytes: AtomicUsize,
    gets: AtomicUsize,
    puts: AtomicUsize,
    put_paths: StdMutex<Vec<String>>,
    block_puts: AtomicBool,
    #[cfg(test)]
    block_deletes: AtomicBool,
    #[cfg(test)]
    delete_activity: Arc<Notify>,
    released_put_paths: StdMutex<HashSet<String>>,
    active_puts: AtomicUsize,
    max_active_puts: AtomicUsize,
    put_activity: Arc<Notify>,
    put_release: Arc<Notify>,
    #[cfg(test)]
    block_heads: AtomicBool,
    #[cfg(test)]
    heads: AtomicUsize,
    #[cfg(test)]
    caller_owned_put_attempts: AtomicUsize,
    #[cfg(test)]
    head_activity: Arc<Notify>,
    #[cfg(test)]
    head_release: Arc<Notify>,
    forced_put_etag: StdMutex<Option<String>>,
    /// Return the next N non-head bodies with the right length and wrong bytes.
    #[cfg(test)]
    corrupt_next_gets: AtomicUsize,
    /// Flip the bytes inside one absolute object range on the next N
    /// non-head bodies, leaving the rest of each body intact.
    #[cfg(test)]
    corrupt_range_next_gets: AtomicUsize,
    #[cfg(test)]
    corrupt_range: StdMutex<std::ops::Range<u64>>,
    /// Hold the next N non-head bodies until `release_get_bodies`.
    #[cfg(test)]
    hold_next_get_bodies: AtomicUsize,
    #[cfg(test)]
    get_bodies_released: AtomicBool,
    #[cfg(test)]
    get_body_release: Arc<Notify>,
}

impl FaultControls {
    pub(crate) fn partition_writes(&self, on: bool) {
        self.partition_writes.store(on, Ordering::SeqCst);
    }
    pub(crate) fn fail_gets(&self, n: usize) {
        self.fail_next_gets.store(n, Ordering::SeqCst);
    }
    pub(crate) fn fail_puts(&self, n: usize) {
        self.fail_next_puts.store(n, Ordering::SeqCst);
    }
    pub(crate) fn fail_puts_after_apply(&self, n: usize) {
        self.fail_after_puts.store(n, Ordering::SeqCst);
    }
    /// Make the next `n` gets return a body `by` bytes short of the claimed length.
    pub(crate) fn truncate_gets(&self, n: usize, by: usize) {
        self.truncate_bytes.store(by, Ordering::SeqCst);
        self.truncate_next_gets.store(n, Ordering::SeqCst);
    }
    /// Make the next `n` non-head gets return a same-length body rotated by
    /// half its length, the shape of a corrupted ranged response that passes a
    /// length check.
    #[cfg(test)]
    pub(crate) fn corrupt_gets(&self, n: usize) {
        self.corrupt_next_gets.store(n, Ordering::SeqCst);
    }
    /// Make the next `n` non-head gets flip every byte that falls inside the
    /// absolute object range `range`. A get that [`Self::corrupt_gets`]
    /// already corrupts does not use one of these.
    #[cfg(test)]
    pub(crate) fn corrupt_range_in_gets(&self, n: usize, range: std::ops::Range<u64>) {
        *self.corrupt_range.lock().unwrap() = range;
        self.corrupt_range_next_gets.store(n, Ordering::SeqCst);
    }
    /// Hold the bodies of the next `n` non-head gets until
    /// [`Self::release_get_bodies`]. The request and its metadata return first,
    /// so the caller can register the fetch as in flight.
    #[cfg(test)]
    pub(crate) fn hold_get_bodies(&self, n: usize) {
        self.get_bodies_released.store(false, Ordering::SeqCst);
        self.hold_next_get_bodies.store(n, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) fn release_get_bodies(&self) {
        self.get_bodies_released.store(true, Ordering::SeqCst);
        self.get_body_release.notify_waiters();
    }
    pub(crate) fn get_count(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
    pub(crate) fn put_count(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }
    pub(crate) fn put_paths(&self) -> Vec<String> {
        self.put_paths.lock().unwrap().clone()
    }
    pub(crate) fn block_puts(&self) {
        self.block_puts.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) fn block_deletes(&self) {
        self.block_deletes.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) fn delete_activity(&self) -> Arc<Notify> {
        self.delete_activity.clone()
    }
    pub(crate) fn release_puts(&self) {
        self.block_puts.store(false, Ordering::SeqCst);
        self.put_release.notify_waiters();
    }
    pub(crate) fn release_put_path(&self, path: &str) {
        self.released_put_paths
            .lock()
            .unwrap()
            .insert(path.to_owned());
        self.put_release.notify_waiters();
    }
    pub(crate) fn max_active_puts(&self) -> usize {
        self.max_active_puts.load(Ordering::SeqCst)
    }
    pub(crate) fn put_activity(&self) -> Arc<Notify> {
        self.put_activity.clone()
    }
    #[cfg(test)]
    pub(crate) fn block_heads(&self) {
        self.block_heads.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub(crate) fn release_heads(&self) {
        self.block_heads.store(false, Ordering::SeqCst);
        self.head_release.notify_waiters();
    }
    #[cfg(test)]
    pub(crate) fn head_count(&self) -> usize {
        self.heads.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    pub(crate) fn caller_owned_put_attempt_count(&self) -> usize {
        self.caller_owned_put_attempts.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    fn head_activity(&self) -> Arc<Notify> {
        self.head_activity.clone()
    }
    pub(crate) fn force_put_etag(&self, e_tag: String) {
        *self.forced_put_etag.lock().unwrap() = Some(e_tag);
    }
}

/// Decrement `counter` if positive; returns whether a unit was taken.
fn take_one(counter: &AtomicUsize) -> bool {
    let mut cur = counter.load(Ordering::SeqCst);
    while cur > 0 {
        match counter.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

#[derive(Debug)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    ctl: Arc<FaultControls>,
}

impl FaultStore {
    /// Returns the store and its shared controls (default: no fault).
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<FaultControls>) {
        let ctl = Arc::new(FaultControls::default());
        (
            Arc::new(Self {
                inner,
                ctl: ctl.clone(),
            }),
            ctl,
        )
    }

    fn transient(op: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "FaultStore",
            source: format!("injected transient fault on {op}").into(),
        }
    }

    fn check_writable(&self, op: &'static str) -> object_store::Result<()> {
        if self.ctl.partition_writes.load(Ordering::SeqCst) {
            return Err(Self::transient(op));
        }
        Ok(())
    }
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.ctl.puts.fetch_add(1, Ordering::SeqCst);
        self.ctl
            .put_paths
            .lock()
            .unwrap()
            .push(location.to_string());
        self.check_writable("put")?;
        #[cfg(test)]
        if opts
            .extensions
            .get::<crate::retrying_object_store::CallerOwnsRetries>()
            .is_some()
        {
            self.ctl
                .caller_owned_put_attempts
                .fetch_add(1, Ordering::SeqCst);
        }
        if take_one(&self.ctl.fail_next_puts) {
            return Err(Self::transient("put"));
        }
        let _active = ActivePut::enter(self.ctl.clone());
        while self.ctl.block_puts.load(Ordering::SeqCst) {
            let notified = self.ctl.put_release.notified();
            if self
                .ctl
                .released_put_paths
                .lock()
                .unwrap()
                .remove(location.as_ref())
            {
                break;
            }
            if !self.ctl.block_puts.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        if let Some(e_tag) = self.ctl.forced_put_etag.lock().unwrap().clone() {
            result.e_tag = Some(e_tag);
        }
        if take_one(&self.ctl.fail_after_puts) {
            return Err(Self::transient("put response"));
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check_writable("put_multipart")?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let head = options.head;
        #[cfg(test)]
        if head {
            self.ctl.heads.fetch_add(1, Ordering::SeqCst);
            self.ctl.head_activity.notify_waiters();
            while self.ctl.block_heads.load(Ordering::SeqCst) {
                let notified = self.ctl.head_release.notified();
                if !self.ctl.block_heads.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
        self.ctl.gets.fetch_add(1, Ordering::SeqCst);
        if take_one(&self.ctl.fail_next_gets) {
            return Err(Self::transient("get"));
        }
        let result = self.inner.get_opts(location, options).await?;
        #[cfg(test)]
        let result = if head {
            result
        } else {
            self.corrupt_or_hold(result)
        };
        // Head replies carry no body; only short-circuit truncation for them.
        if head || !take_one(&self.ctl.truncate_next_gets) {
            return Ok(result);
        }
        let by = self.ctl.truncate_bytes.load(Ordering::SeqCst);
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let full = result.bytes().await?;
        let short = full.slice(0..full.len().saturating_sub(by));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(short) }).boxed()),
            meta,
            range,
            attributes,
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        #[cfg(test)]
        if self.ctl.block_deletes.load(Ordering::SeqCst) {
            let controls = self.ctl.clone();
            return locations
                .then(move |location| {
                    let controls = controls.clone();
                    async move {
                        let _path = location?;
                        controls.delete_activity.notify_one();
                        std::future::pending::<object_store::Result<Path>>().await
                    }
                })
                .boxed();
        }
        // A write partition fails every delete fast (one yield per input, as the
        // provided `ObjectStoreExt::delete` and bulk callers expect).
        if self.ctl.partition_writes.load(Ordering::SeqCst) {
            return locations
                .map(|loc| loc.and_then(|_| Err(Self::transient("delete"))))
                .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check_writable("copy")?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
impl FaultStore {
    fn corrupt_or_hold(&self, result: GetResult) -> GetResult {
        let corrupt = take_one(&self.ctl.corrupt_next_gets);
        let flip = (!corrupt && take_one(&self.ctl.corrupt_range_next_gets))
            .then(|| self.ctl.corrupt_range.lock().unwrap().clone());
        let hold = take_one(&self.ctl.hold_next_get_bodies);
        if !corrupt && flip.is_none() && !hold {
            return result;
        }
        let controls = self.ctl.clone();
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let body_start = range.start;
        let body = async move {
            while hold && !controls.get_bodies_released.load(Ordering::SeqCst) {
                let released = controls.get_body_release.notified();
                if controls.get_bodies_released.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
            let bytes = result.bytes().await?;
            if let Some(flip) = flip {
                let mut flipped = bytes.to_vec();
                let from = flip.start.saturating_sub(body_start) as usize;
                let to = (flip.end.saturating_sub(body_start) as usize).min(flipped.len());
                for byte in flipped.iter_mut().take(to).skip(from) {
                    *byte ^= 0xFF;
                }
                return Ok(Bytes::from(flipped));
            }
            if !corrupt || bytes.len() < 2 {
                return Ok(bytes);
            }
            let mut rotated = bytes.to_vec();
            rotated.rotate_left(bytes.len() / 2);
            Ok(Bytes::from(rotated))
        };
        GetResult {
            payload: GetResultPayload::Stream(stream::once(body).boxed()),
            meta,
            range,
            attributes,
            extensions: Default::default(),
        }
    }
}

struct ActivePut {
    controls: Arc<FaultControls>,
}

impl ActivePut {
    fn enter(controls: Arc<FaultControls>) -> Self {
        let active = controls.active_puts.fetch_add(1, Ordering::SeqCst) + 1;
        controls.max_active_puts.fetch_max(active, Ordering::SeqCst);
        controls.put_activity.notify_waiters();
        Self { controls }
    }
}

impl Drop for ActivePut {
    fn drop(&mut self) {
        self.controls.active_puts.fetch_sub(1, Ordering::SeqCst);
        self.controls.put_activity.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[tokio::test]
    async fn fail_gets_then_recovers() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"hello".to_vec().into()).await.unwrap();

        ctl.fail_gets(2);
        assert!(store.get(&path).await.is_err());
        assert!(store.get(&path).await.is_err());

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"hello");
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            5
        );
        assert_eq!(ctl.get_count(), 4);
    }

    #[tokio::test]
    async fn partition_blocks_writes_but_not_reads() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"v".to_vec().into()).await.unwrap();

        ctl.partition_writes(true);
        assert!(store.put(&path, b"v2".to_vec().into()).await.is_err());
        // Reads keep working while writes are partitioned.
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"v");

        ctl.partition_writes(false);
        store.put(&path, b"v3".to_vec().into()).await.unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"v3");
    }

    #[tokio::test]
    async fn fail_puts_then_recovers() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");

        ctl.fail_puts(1);
        assert!(store.put(&path, b"a".to_vec().into()).await.is_err());
        store.put(&path, b"b".to_vec().into()).await.unwrap();

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"b");
        assert_eq!(ctl.put_count(), 2);
    }

    #[tokio::test]
    async fn truncates_a_bounded_number_of_gets() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, vec![7u8; 100].into()).await.unwrap();

        ctl.truncate_gets(2, 40);
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            60
        );
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            60
        );
        // Budget exhausted: the full body comes back.
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap().len(),
            100
        );
    }

    #[tokio::test]
    async fn corrupts_a_bounded_number_of_gets_without_changing_length() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        let body: Vec<u8> = (0..100u8).collect();
        store.put(&path, body.clone().into()).await.unwrap();

        ctl.corrupt_gets(1);
        let corrupt = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(corrupt.len(), body.len());
        assert_ne!(corrupt.as_ref(), body.as_slice());
        let clean = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(clean.as_ref(), body.as_slice());
    }

    #[tokio::test]
    async fn held_get_bodies_wait_for_release() {
        let (store, ctl) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("k");
        store.put(&path, b"held".to_vec().into()).await.unwrap();

        ctl.hold_get_bodies(1);
        let held = store.get(&path).await.unwrap();
        let body = tokio::spawn(async move { held.bytes().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!body.is_finished());
        ctl.release_get_bodies();
        let got = tokio::time::timeout(Duration::from_secs(1), body)
            .await
            .expect("released body did not finish")
            .unwrap()
            .unwrap();
        assert_eq!(&got[..], b"held");
    }

    #[tokio::test]
    async fn blocked_heads_wait_for_release_and_are_counted() {
        let (store, controls) = FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("blocked-head");
        store
            .put(&path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
        controls.block_heads();
        let head_activity = controls.head_activity();
        let head_started = head_activity.notified();
        let store_for_head = store.clone();
        let path_for_head = path.clone();
        let head = tokio::spawn(async move { store_for_head.head(&path_for_head).await });

        tokio::time::timeout(Duration::from_secs(1), head_started)
            .await
            .expect("blocked HEAD did not reach the fault store");
        assert_eq!(controls.head_count(), 1);
        assert!(!head.is_finished());

        controls.release_heads();
        tokio::time::timeout(Duration::from_secs(1), head)
            .await
            .expect("released HEAD did not finish")
            .unwrap()
            .unwrap();
    }
}
