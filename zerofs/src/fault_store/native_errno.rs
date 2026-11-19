//! Native errno injection for the local stores that `storage_backend` builds
//! from configuration.
//!
//! A test arms a rule for a filesystem path prefix and an object-name suffix.
//! Every local store built from a `file://` URL consults the armed rules on each
//! call, so a rule can be armed before startup or while the filesystem serves.
//! Failures have the shape `LocalFileSystem` gives them: an `Error::Generic`
//! whose source is the raw `std::io::Error`. A read fails in its body, as a bad
//! block under an open file does.

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    Read,
    Write,
}

#[derive(Debug)]
struct Rule {
    root: PathBuf,
    operation: Operation,
    suffix: &'static str,
    errno: i32,
    /// `None` fails forever; `Some` fails that many more calls, then passes.
    remaining: Option<AtomicUsize>,
}

impl Rule {
    fn matches(&self, operation: Operation, location: &Path) -> bool {
        let path = PathBuf::from(format!("/{location}"));
        self.operation == operation
            && path.starts_with(&self.root)
            && location.as_ref().ends_with(self.suffix)
    }

    fn fire(&self) -> bool {
        self.remaining.as_ref().is_none_or(super::take_one)
    }
}

static RULES: LazyLock<Mutex<Vec<Arc<Rule>>>> = LazyLock::new(Mutex::default);

/// Removes its rule when dropped.
#[must_use = "the rule is removed when the guard drops"]
pub(crate) struct Armed(Arc<Rule>);

impl Drop for Armed {
    fn drop(&mut self) {
        RULES
            .lock()
            .unwrap()
            .retain(|rule| !Arc::ptr_eq(rule, &self.0));
    }
}

/// Fail `operation` on objects under `root` whose name ends with `suffix`
/// with `errno`, `times` more times or forever.
pub(crate) fn arm(
    root: impl Into<PathBuf>,
    operation: Operation,
    suffix: &'static str,
    errno: i32,
    times: Option<usize>,
) -> Armed {
    let rule = Arc::new(Rule {
        root: root.into(),
        operation,
        suffix,
        errno,
        remaining: times.map(AtomicUsize::new),
    });
    RULES.lock().unwrap().push(Arc::clone(&rule));
    Armed(rule)
}

fn injected(operation: Operation, location: &Path) -> Option<object_store::Error> {
    let rules = RULES.lock().unwrap();
    let rule = rules
        .iter()
        .find(|rule| rule.matches(operation, location) && rule.fire())?;
    Some(object_store::Error::Generic {
        store: "LocalFileSystem",
        source: Box::new(std::io::Error::from_raw_os_error(rule.errno)),
    })
}

/// Wrap a configured local store so armed rules apply to it.
pub(crate) fn wrap(store: Box<dyn ObjectStore>) -> Box<dyn ObjectStore> {
    Box::new(NativeErrnoStore {
        inner: Arc::from(store),
    })
}

#[derive(Debug)]
struct NativeErrnoStore {
    inner: Arc<dyn ObjectStore>,
}

impl Display for NativeErrnoStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "NativeErrnoStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for NativeErrnoStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if let Some(error) = injected(Operation::Write, location) {
            return Err(error);
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if let Some(error) = injected(Operation::Write, location) {
            return Err(error);
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if head {
            return Ok(result);
        }
        let Some(error) = injected(Operation::Read, location) else {
            return Ok(result);
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Err(error) }).boxed()),
            meta: result.meta,
            range: result.range,
            attributes: result.attributes,
            extensions: result.extensions,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
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
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
