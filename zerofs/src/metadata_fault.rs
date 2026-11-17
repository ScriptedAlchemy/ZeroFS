//! Local I/O failures of the metadata store.
//!
//! The metadata store is SlateDB over a local `file://` directory: the pool
//! authority, or the whole store when no pool is configured. Upstream
//! `LocalFileSystem` reports every I/O failure as `Error::Generic`. Two retry
//! layers sit above it, ZeroFS's `RetryingObjectStore` and SlateDB's own, and
//! both retry every unmarked `Generic` error without limit. A bad block under
//! an SST therefore produced a retry loop that never ended.
//!
//! [`LocalMetadataStore`] wraps the local metadata store and is the one place
//! that decides which local errors are transient. An error whose errno is not
//! transient on a local filesystem (EIO, ENODEV, EROFS, EACCES and the rest)
//! leaves this store marked with `PermanentError`, so ZeroFS's retry layer
//! returns it at once. SlateDB cannot be told the same thing: it still retries
//! the error. The wrapper therefore also records the first such failure in
//! [`MetadataFault`]. Startup returns that failure as its error, and a serving
//! process exits with it, naming the file and the errno.
//!
//! Errors that end on their own stay retryable: an interrupted or would-block
//! call, a full disk or quota, and exhausted memory or descriptors. A full
//! metadata disk keeps waiting for space, as it did before this wrapper.

use crate::retrying_object_store::PermanentError;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result, UploadPart,
};
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use tokio_util::sync::CancellationToken;

const STORE: &str = "LocalMetadataStore";
const DELETE_CONCURRENCY: usize = 10;

/// Errno values a local filesystem returns for a condition that can clear
/// without operator action. Every other errno means the device or the
/// filesystem refused the operation, and a retry gets the same answer.
fn transient_local_errno(errno: i32) -> bool {
    matches!(
        errno,
        libc::EINTR
            | libc::EAGAIN
            | libc::ENOSPC
            | libc::EDQUOT
            | libc::ENOMEM
            | libc::ENFILE
            | libc::EMFILE
            | libc::EBUSY
            | libc::ETIMEDOUT
    )
}

fn errno_name(errno: i32) -> String {
    let name = match errno {
        libc::EIO => "EIO",
        libc::ENODEV => "ENODEV",
        libc::ENXIO => "ENXIO",
        libc::EROFS => "EROFS",
        libc::EACCES => "EACCES",
        libc::EPERM => "EPERM",
        libc::ENOENT => "ENOENT",
        libc::ENODATA => "ENODATA",
        libc::EBADMSG => "EBADMSG",
        #[cfg(target_os = "linux")]
        libc::EUCLEAN => "EUCLEAN",
        #[cfg(target_os = "linux")]
        libc::EREMOTEIO => "EREMOTEIO",
        _ => return format!("errno {errno}"),
    };
    name.to_owned()
}

/// The errno of the first native I/O error in a `Generic` error's chain.
/// Typed errors (`NotFound`, `AlreadyExists`, `Precondition`) keep their
/// meaning, and errors without an errno keep today's retry behavior.
fn native_errno(error: &Error) -> Option<(i32, String)> {
    if !matches!(error, Error::Generic { .. }) {
        return None;
    }
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(cause) = current {
        if let Some(io) = cause.downcast_ref::<std::io::Error>()
            && let Some(errno) = io.raw_os_error()
        {
            return Some((errno, io.to_string()));
        }
        current = cause.source();
    }
    None
}

/// A metadata store operation refused by the local filesystem.
#[derive(Debug)]
pub(crate) struct LocalIoError {
    operation: &'static str,
    path: String,
    errno: i32,
    message: String,
    source: Error,
}

impl Display for LocalIoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadata store {} of {} failed with {}: {}",
            self.operation,
            self.path,
            errno_name(self.errno),
            self.message
        )
    }
}

impl std::error::Error for LocalIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The first non-transient local I/O failure of the metadata store.
#[derive(Clone, Debug, Default)]
pub(crate) struct MetadataFault(Arc<FaultState>);

#[derive(Debug, Default)]
struct FaultState {
    failure: OnceLock<String>,
    tripped: CancellationToken,
}

impl MetadataFault {
    fn record(&self, failure: &LocalIoError) {
        if self.0.failure.set(failure.to_string()).is_ok() {
            tracing::error!("{failure}; SlateDB retries this error without limit, so ZeroFS stops");
            self.0.tripped.cancel();
        }
    }

    /// Resolves with the recorded failure once one is recorded.
    pub(crate) async fn tripped(&self) -> anyhow::Error {
        self.0.tripped.cancelled().await;
        anyhow::anyhow!(
            "{}",
            self.0
                .failure
                .get()
                .expect("a fault records its failure before it trips")
        )
    }

    /// Run `operation` until it finishes or the metadata store fails.
    pub(crate) async fn bound<T>(
        &self,
        operation: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        tokio::select! {
            biased;
            error = self.tripped() => Err(error),
            result = operation => result,
        }
    }

    /// Exit the process with the recorded failure. A serving process cannot
    /// unwind SlateDB's retry loops, and every final flush would write to the
    /// failed store, so this stops the way `exit_on_write_error` does.
    pub(crate) async fn exit_when_tripped(self) {
        let error = self.tripped().await;
        tracing::error!("Fatal metadata store failure, exiting: {error}");
        std::process::exit(1);
    }

    fn classify(
        &self,
        operation: &'static str,
        path: impl FnOnce() -> String,
        error: Error,
    ) -> Error {
        let Some((errno, message)) = native_errno(&error) else {
            return error;
        };
        if transient_local_errno(errno) {
            return error;
        }
        let failure = LocalIoError {
            operation,
            path: path(),
            errno,
            message,
            source: error,
        };
        self.record(&failure);
        Error::Generic {
            store: STORE,
            source: Box::new(PermanentError::new(failure)),
        }
    }
}

/// Native local stores are rooted at `/`, so a location is an absolute path.
fn filesystem_path(location: &Path) -> String {
    format!("/{location}")
}

/// The local metadata store, with its errors classified (see the module docs).
#[derive(Debug)]
pub(crate) struct LocalMetadataStore {
    inner: Arc<dyn ObjectStore>,
    fault: MetadataFault,
}

impl LocalMetadataStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, fault: MetadataFault) -> Self {
        Self { inner, fault }
    }

    fn fail(&self, operation: &'static str, location: &Path, error: Error) -> Error {
        self.fault
            .classify(operation, || filesystem_path(location), error)
    }
}

impl Display for LocalMetadataStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE}({})", self.inner)
    }
}

fn classified_stream<T: Send + 'static>(
    fault: MetadataFault,
    operation: &'static str,
    path: String,
    items: BoxStream<'static, Result<T>>,
) -> BoxStream<'static, Result<T>> {
    items
        .map_err(move |error| fault.classify(operation, || path.clone(), error))
        .boxed()
}

#[async_trait]
impl ObjectStore for LocalMetadataStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner
            .put_opts(location, payload, opts)
            .await
            .map_err(|error| self.fail("write", location, error))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self
            .inner
            .put_multipart_opts(location, opts)
            .await
            .map_err(|error| self.fail("write", location, error))?;
        Ok(Box::new(LocalMetadataUpload {
            inner: upload,
            fault: self.fault.clone(),
            path: filesystem_path(location),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let result = self
            .inner
            .get_opts(location, options)
            .await
            .map_err(|error| self.fail("read", location, error))?;
        if head {
            return Ok(result);
        }
        let GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions,
        } = result;
        let payload = match payload {
            // The local backend reads a file body after `get_opts` returns.
            // Read it here, with the same single read every caller already
            // makes through `GetResult::bytes`, so its error is classified.
            file @ GetResultPayload::File(..) => {
                let bytes = GetResult {
                    payload: file,
                    meta: meta.clone(),
                    range: range.clone(),
                    attributes: attributes.clone(),
                    extensions: extensions.clone(),
                }
                .bytes()
                .await
                .map_err(|error| self.fail("read", location, error))?;
                GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed())
            }
            GetResultPayload::Stream(body) => GetResultPayload::Stream(classified_stream(
                self.fault.clone(),
                "read",
                filesystem_path(location),
                body,
            )),
        };
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions,
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        self.inner
            .get_ranges(location, ranges)
            .await
            .map_err(|error| self.fail("read", location, error))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let fault = self.fault.clone();
        locations
            .map(move |location| {
                let inner = Arc::clone(&inner);
                let fault = fault.clone();
                async move {
                    let location = location?;
                    match inner.delete(&location).await {
                        Ok(()) => Ok(location),
                        Err(error) => {
                            Err(fault.classify("delete", || filesystem_path(&location), error))
                        }
                    }
                }
            })
            .buffered(DELETE_CONCURRENCY)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        classified_stream(
            self.fault.clone(),
            "list",
            filesystem_path(prefix.unwrap_or(&Path::default())),
            self.inner.list(prefix),
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        classified_stream(
            self.fault.clone(),
            "list",
            filesystem_path(prefix.unwrap_or(&Path::default())),
            self.inner.list_with_offset(prefix, offset),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let directory = prefix.cloned().unwrap_or_default();
        self.inner
            .list_with_delimiter(prefix)
            .await
            .map_err(|error| self.fail("list", &directory, error))
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner
            .copy_opts(from, to, options)
            .await
            .map_err(|error| {
                self.fault.classify(
                    "copy",
                    || format!("{} to {}", filesystem_path(from), filesystem_path(to)),
                    error,
                )
            })
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner
            .rename_opts(from, to, options)
            .await
            .map_err(|error| {
                self.fault.classify(
                    "rename",
                    || format!("{} to {}", filesystem_path(from), filesystem_path(to)),
                    error,
                )
            })
    }
}

#[derive(Debug)]
struct LocalMetadataUpload {
    inner: Box<dyn MultipartUpload>,
    fault: MetadataFault,
    path: String,
}

#[async_trait]
impl MultipartUpload for LocalMetadataUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = self.inner.put_part(data);
        let fault = self.fault.clone();
        let path = self.path.clone();
        Box::pin(async move {
            part.await
                .map_err(|error| fault.classify("write", || path, error))
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let path = self.path.clone();
        self.inner
            .complete()
            .await
            .map_err(|error| self.fault.classify("write", || path, error))
    }

    async fn abort(&mut self) -> Result<()> {
        let path = self.path.clone();
        self.inner
            .abort()
            .await
            .map_err(|error| self.fault.classify("write", || path, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retrying_object_store::has_permanent_source;

    fn native(errno: i32) -> Error {
        Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(std::io::Error::from_raw_os_error(errno)),
        }
    }

    #[test]
    fn errors_that_clear_on_their_own_stay_retryable() {
        for errno in [
            libc::EINTR,
            libc::EAGAIN,
            libc::ENOSPC,
            libc::EDQUOT,
            libc::ENOMEM,
            libc::ENFILE,
            libc::EMFILE,
            libc::EBUSY,
            libc::ETIMEDOUT,
        ] {
            let fault = MetadataFault::default();
            let error = fault.classify("write", || "/metadata/a.sst".to_owned(), native(errno));
            assert!(!has_permanent_source(&error), "errno {errno} must retry");
            assert!(
                !fault.0.tripped.is_cancelled(),
                "errno {errno} must not trip"
            );
        }
    }

    #[test]
    fn device_and_filesystem_refusals_are_final_and_named() {
        for (errno, name) in [
            (libc::EIO, "EIO"),
            (libc::ENODEV, "ENODEV"),
            (libc::ENXIO, "ENXIO"),
            (libc::EROFS, "EROFS"),
            (libc::EACCES, "EACCES"),
            (libc::ENODATA, "ENODATA"),
            (libc::EBADMSG, "EBADMSG"),
        ] {
            let fault = MetadataFault::default();
            let error = fault.classify("read", || "/metadata/a.sst".to_owned(), native(errno));
            assert!(has_permanent_source(&error), "{name} must not retry");
            let recorded = fault.0.failure.get().expect("the failure is recorded");
            assert!(
                recorded.contains("/metadata/a.sst") && recorded.contains(name),
                "{recorded}"
            );
            assert!(fault.0.tripped.is_cancelled());
        }
    }

    #[test]
    fn typed_and_errno_free_errors_keep_their_meaning() {
        let fault = MetadataFault::default();
        let not_found = fault.classify(
            "read",
            || "/metadata/a.sst".to_owned(),
            Error::NotFound {
                path: "metadata/a.sst".to_owned(),
                source: Box::new(std::io::Error::from_raw_os_error(libc::ENOENT)),
            },
        );
        assert!(matches!(not_found, Error::NotFound { .. }));
        let short = fault.classify(
            "read",
            || "/metadata/a.sst".to_owned(),
            Error::Generic {
                store: "LocalFileSystem",
                source: "range read size check failed".into(),
            },
        );
        assert!(!has_permanent_source(&short));
        assert!(!fault.0.tripped.is_cancelled());
    }

    #[tokio::test]
    async fn bound_returns_the_recorded_failure_while_the_operation_still_waits() {
        let fault = MetadataFault::default();
        let waiting = fault.bound(std::future::pending::<anyhow::Result<()>>());
        let recorder = fault.clone();
        tokio::spawn(async move {
            recorder.classify("write", || "/metadata/b.sst".to_owned(), native(libc::EIO));
        });
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("the bound ends when the store fails")
            .expect_err("a pending operation cannot succeed");
        assert!(error.to_string().contains("/metadata/b.sst"), "{error:#}");
    }
}
