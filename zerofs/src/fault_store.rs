//! A reusable fault-injecting object store for tests: a pass-through decorator
//! over any inner store whose writes can be partitioned or made to fail
//! transiently, and whose reads can fail or return short bodies. Generalizes the
//! one-off wrappers in `replication::zombie` (write partition) and
//! `length_checked_object_store`'s `ShortBodyStore` (truncated reads) into one
//! programmable harness, driven through a shared [`FaultControls`] handle.

use async_trait::async_trait;
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
    head_activity: Arc<Notify>,
    #[cfg(test)]
    head_release: Arc<Notify>,
}

impl FaultControls {
    pub fn partition_writes(&self, on: bool) {
        self.partition_writes.store(on, Ordering::SeqCst);
    }
    pub fn fail_gets(&self, n: usize) {
        self.fail_next_gets.store(n, Ordering::SeqCst);
    }
    pub fn fail_puts(&self, n: usize) {
        self.fail_next_puts.store(n, Ordering::SeqCst);
    }
    pub fn fail_puts_after_apply(&self, n: usize) {
        self.fail_after_puts.store(n, Ordering::SeqCst);
    }
    /// Make the next `n` gets return a body `by` bytes short of the claimed length.
    pub fn truncate_gets(&self, n: usize, by: usize) {
        self.truncate_bytes.store(by, Ordering::SeqCst);
        self.truncate_next_gets.store(n, Ordering::SeqCst);
    }
    pub fn get_count(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
    pub fn put_count(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }
    pub fn put_paths(&self) -> Vec<String> {
        self.put_paths.lock().unwrap().clone()
    }
    pub fn block_puts(&self) {
        self.block_puts.store(true, Ordering::SeqCst);
    }
    pub fn release_puts(&self) {
        self.block_puts.store(false, Ordering::SeqCst);
        self.put_release.notify_waiters();
    }
    pub fn release_put_path(&self, path: &str) {
        self.released_put_paths
            .lock()
            .unwrap()
            .insert(path.to_owned());
        self.put_release.notify_waiters();
    }
    pub fn max_active_puts(&self) -> usize {
        self.max_active_puts.load(Ordering::SeqCst)
    }
    pub fn put_activity(&self) -> Arc<Notify> {
        self.put_activity.clone()
    }
    #[cfg(test)]
    pub fn block_heads(&self) {
        self.block_heads.store(true, Ordering::SeqCst);
    }
    #[cfg(test)]
    pub fn release_heads(&self) {
        self.block_heads.store(false, Ordering::SeqCst);
        self.head_release.notify_waiters();
    }
    #[cfg(test)]
    pub fn head_count(&self) -> usize {
        self.heads.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    pub fn head_activity(&self) -> Arc<Notify> {
        self.head_activity.clone()
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
    pub fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<FaultControls>) {
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
        let result = self.inner.put_opts(location, payload, opts).await?;
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
    use bytes::Bytes;
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
