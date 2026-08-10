use crate::sftp_object_store::SftpCapabilities;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Write,
    Metadata,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("invalid SFTP session limits: {0}")]
    InvalidLimits(String),
    #[error("SFTP server lacks required {0} extension")]
    MissingCapability(&'static str),
    #[error("failed to open SFTP session: {0}")]
    Open(String),
    #[error("failed to close SFTP session: {0}")]
    Close(String),
    #[error("SFTP session pool is closed")]
    PoolClosed,
}

#[async_trait]
pub trait TransportSession: fmt::Debug + Send + Sync + 'static {
    fn capabilities(&self) -> SftpCapabilities;
    async fn close(self: Box<Self>) -> Result<(), TransportError>;
}

#[async_trait]
pub trait SessionFactory: fmt::Debug + Send + Sync + 'static {
    async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError>;
}

pub struct OpenSshSessionFactory {
    endpoint: crate::config::SftpEndpoint,
    known_hosts: PathBuf,
    authentication_config: Arc<tempfile::NamedTempFile>,
}

impl OpenSshSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        known_hosts: PathBuf,
    ) -> Result<Self, TransportError> {
        let mut authentication_config = tempfile::Builder::new()
            .prefix("zerofs-ssh-")
            .suffix(".conf")
            .tempfile()
            .map_err(|_| TransportError::Open("could not create SSH policy file".to_owned()))?;
        authentication_config
            .write_all(
                b"Host *\n\
                  PasswordAuthentication no\n\
                  KbdInteractiveAuthentication no\n\
                  ChallengeResponseAuthentication no\n\
                  PreferredAuthentications publickey\n\
                  PubkeyAuthentication yes\n",
            )
            .map_err(|_| TransportError::Open("could not write SSH policy file".to_owned()))?;
        authentication_config
            .flush()
            .map_err(|_| TransportError::Open("could not flush SSH policy file".to_owned()))?;
        Ok(Self {
            endpoint,
            known_hosts,
            authentication_config: Arc::new(authentication_config),
        })
    }
}

impl fmt::Debug for OpenSshSessionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSshSessionFactory")
            .field("host", &self.endpoint.host)
            .field("port", &self.endpoint.port)
            .field("known_hosts", &self.known_hosts)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SessionFactory for OpenSshSessionFactory {
    async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError> {
        let mut builder = openssh::SessionBuilder::default();
        builder
            .user(self.endpoint.username.clone())
            .port(self.endpoint.port)
            .known_hosts_check(openssh::KnownHosts::Strict)
            .user_known_hosts_file(&self.known_hosts)
            .config_file(self.authentication_config.path());
        let ssh = builder.connect(&self.endpoint.host).await.map_err(|_| {
            TransportError::Open(format!(
                "OpenSSH connection to {}:{} failed",
                self.endpoint.host, self.endpoint.port
            ))
        })?;
        let sftp = openssh_sftp_client::Sftp::from_session(
            ssh,
            openssh_sftp_client::SftpOptions::default(),
        )
        .await
        .map_err(|_| {
            TransportError::Open(format!(
                "SFTP handshake with {}:{} failed",
                self.endpoint.host, self.endpoint.port
            ))
        })?;
        Ok(Box::new(OpenSshTransportSession { sftp: Some(sftp) }))
    }
}

pub struct OpenSshTransportSession {
    sftp: Option<openssh_sftp_client::Sftp>,
}

impl fmt::Debug for OpenSshTransportSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSshTransportSession")
            .finish_non_exhaustive()
    }
}

impl OpenSshTransportSession {
    pub async fn from_streams<W, R>(stdin: W, stdout: R) -> Result<Self, TransportError>
    where
        W: AsyncWrite + Send + 'static,
        R: AsyncRead + Send + 'static,
    {
        let sftp = openssh_sftp_client::Sftp::new(
            stdin,
            stdout,
            openssh_sftp_client::SftpOptions::default(),
        )
        .await
        .map_err(|_| TransportError::Open("SFTP stream handshake failed".to_owned()))?;
        Ok(Self { sftp: Some(sftp) })
    }
}

#[async_trait]
impl TransportSession for OpenSshTransportSession {
    fn capabilities(&self) -> SftpCapabilities {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        SftpCapabilities {
            fsync: sftp.support_fsync(),
            hardlink: sftp.support_hardlink(),
            posix_rename: sftp.support_posix_rename(),
        }
    }

    async fn close(mut self: Box<Self>) -> Result<(), TransportError> {
        self.sftp
            .take()
            .expect("open transport owns SFTP client")
            .close()
            .await
            .map_err(|_| TransportError::Close("OpenSSH SFTP shutdown failed".to_owned()))
    }
}

struct PhysicalSession {
    transport: Box<dyn TransportSession>,
    _lifetime: OwnedSemaphorePermit,
}

impl PhysicalSession {
    async fn close(self) -> Result<(), TransportError> {
        self.transport.close().await
    }
}

struct PoolInner {
    factory: Arc<dyn SessionFactory>,
    shared: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    writes: Arc<Semaphore>,
    idle: Mutex<VecDeque<PhysicalSession>>,
    idle_available: Notify,
    writable: bool,
}

#[derive(Clone)]
pub struct SftpSessionPool {
    inner: Arc<PoolInner>,
}

impl fmt::Debug for SftpSessionPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SftpSessionPool")
            .field("writable", &self.inner.writable)
            .finish_non_exhaustive()
    }
}

impl SftpSessionPool {
    pub async fn from_config_writable(
        factory: Arc<dyn SessionFactory>,
        config: &crate::config::SftpConfig,
    ) -> Result<Self, TransportError> {
        Self::new_writable(
            factory,
            config.max_connections,
            config.read_concurrency,
            config.write_concurrency,
        )
        .await
    }

    pub async fn new_writable(
        factory: Arc<dyn SessionFactory>,
        shared: usize,
        reads: usize,
        writes: usize,
    ) -> Result<Self, TransportError> {
        Self::validate_limits(shared, reads, writes)?;
        let pool = Self {
            inner: Arc::new(PoolInner {
                factory,
                shared: Arc::new(Semaphore::new(shared)),
                reads: Arc::new(Semaphore::new(reads)),
                writes: Arc::new(Semaphore::new(writes)),
                idle: Mutex::new(VecDeque::new()),
                idle_available: Notify::new(),
                writable: true,
            }),
        };

        let session = pool.open_physical().await?;
        if let Err(error) = require_publication_capabilities(session.transport.capabilities()) {
            let _ = session.close().await;
            return Err(error);
        }
        pool.inner.idle.lock().await.push_back(session);
        Ok(pool)
    }

    fn validate_limits(shared: usize, reads: usize, writes: usize) -> Result<(), TransportError> {
        if !(1..=8).contains(&shared) {
            return Err(TransportError::InvalidLimits(
                "shared sessions must be between 1 and 8".to_owned(),
            ));
        }
        for (name, value) in [("read", reads), ("write", writes)] {
            if !(1..=7).contains(&value) {
                return Err(TransportError::InvalidLimits(format!(
                    "{name} sessions must be between 1 and 7"
                )));
            }
            if value > shared {
                return Err(TransportError::InvalidLimits(format!(
                    "{name} sessions must not exceed shared sessions"
                )));
            }
        }
        Ok(())
    }

    pub async fn checkout(&self, kind: OperationKind) -> Result<SessionLease, TransportError> {
        let admission = match kind {
            OperationKind::Read | OperationKind::Metadata => {
                self.inner.reads.clone().acquire_owned().await
            }
            OperationKind::Write => self.inner.writes.clone().acquire_owned().await,
        }
        .map_err(|_| TransportError::PoolClosed)?;

        let session = self.checkout_physical().await?;
        Ok(SessionLease {
            pool: self.inner.clone(),
            session: Some(session),
            admission: Some(admission),
        })
    }

    async fn checkout_physical(&self) -> Result<PhysicalSession, TransportError> {
        loop {
            let idle_available = self.inner.idle_available.notified();
            tokio::pin!(idle_available);
            if let Some(session) = self.inner.idle.lock().await.pop_front() {
                return Ok(session);
            }

            tokio::select! {
                biased;
                permit = self.inner.shared.clone().acquire_owned() => {
                    let permit = permit.map_err(|_| TransportError::PoolClosed)?;
                    let transport = self.inner.factory.open().await?;
                    if self.inner.writable {
                        if let Err(error) = require_publication_capabilities(transport.capabilities()) {
                            let _ = transport.close().await;
                            return Err(error);
                        }
                    }
                    return Ok(PhysicalSession { transport, _lifetime: permit });
                }
                () = &mut idle_available => {}
            }
        }
    }

    async fn open_physical(&self) -> Result<PhysicalSession, TransportError> {
        let permit = self
            .inner
            .shared
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::PoolClosed)?;
        let transport = self.inner.factory.open().await?;
        Ok(PhysicalSession {
            transport,
            _lifetime: permit,
        })
    }
}

fn require_publication_capabilities(capabilities: SftpCapabilities) -> Result<(), TransportError> {
    for (present, extension) in [
        (capabilities.fsync, "fsync@openssh.com"),
        (capabilities.hardlink, "hardlink@openssh.com"),
        (capabilities.posix_rename, "posix-rename@openssh.com"),
    ] {
        if !present {
            return Err(TransportError::MissingCapability(extension));
        }
    }
    Ok(())
}

pub struct SessionLease {
    pool: Arc<PoolInner>,
    session: Option<PhysicalSession>,
    admission: Option<OwnedSemaphorePermit>,
}

impl fmt::Debug for SessionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionLease")
            .finish_non_exhaustive()
    }
}

impl SessionLease {
    pub fn capabilities(&self) -> SftpCapabilities {
        self.session
            .as_ref()
            .expect("lease always owns a session until completion")
            .transport
            .capabilities()
    }

    pub async fn complete(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until completion");
        self.pool.idle.lock().await.push_back(session);
        self.pool.idle_available.notify_one();
        self.admission.take();
        Ok(())
    }

    pub async fn retire(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until retirement");
        let result = session.close().await;
        self.admission.take();
        result
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let admission = self.admission.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = session.close().await;
                drop(admission);
            });
        } else {
            // Releasing a physical permit before an async close would make a
            // ninth connection possible. Without a runtime, retain both.
            std::mem::forget(session);
            std::mem::forget(admission);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OpenSshSessionFactory, OpenSshTransportSession, OperationKind, SessionFactory,
        SftpSessionPool, TransportError, TransportSession,
    };
    use crate::config::SftpEndpoint;
    use crate::sftp_object_store::SftpCapabilities;
    use async_trait::async_trait;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct RecordingFactory {
        state: Arc<FactoryState>,
        capabilities: SftpCapabilities,
    }

    struct FactoryState {
        dials: AtomicUsize,
        live: AtomicUsize,
        peak: AtomicUsize,
        close_started: Notify,
        allow_close: Notify,
        block_close: AtomicUsize,
    }

    impl fmt::Debug for RecordingFactory {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_struct("RecordingFactory").finish()
        }
    }

    impl RecordingFactory {
        fn new(capabilities: SftpCapabilities) -> Self {
            Self {
                state: Arc::new(FactoryState {
                    dials: AtomicUsize::new(0),
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    close_started: Notify::new(),
                    allow_close: Notify::new(),
                    block_close: AtomicUsize::new(0),
                }),
                capabilities,
            }
        }

        fn fully_capable() -> Self {
            Self::new(SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            })
        }

        fn dials(&self) -> usize {
            self.state.dials.load(Ordering::SeqCst)
        }

        fn live(&self) -> usize {
            self.state.live.load(Ordering::SeqCst)
        }

        fn peak(&self) -> usize {
            self.state.peak.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SessionFactory for RecordingFactory {
        async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError> {
            let id = self.state.dials.fetch_add(1, Ordering::SeqCst) + 1;
            let live = self.state.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.state.peak.fetch_max(live, Ordering::SeqCst);
            Ok(Box::new(RecordingSession {
                id,
                state: self.state.clone(),
                capabilities: self.capabilities,
            }))
        }
    }

    struct RecordingSession {
        id: usize,
        state: Arc<FactoryState>,
        capabilities: SftpCapabilities,
    }

    impl fmt::Debug for RecordingSession {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecordingSession")
                .field("id", &self.id)
                .finish()
        }
    }

    #[async_trait]
    impl TransportSession for RecordingSession {
        fn capabilities(&self) -> SftpCapabilities {
            self.capabilities
        }

        async fn close(self: Box<Self>) -> Result<(), TransportError> {
            if self.state.block_close.load(Ordering::SeqCst) != 0 {
                self.state.close_started.notify_waiters();
                self.state.allow_close.notified().await;
            }
            self.state.live.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn pool(
        factory: RecordingFactory,
        shared: usize,
        reads: usize,
        writes: usize,
    ) -> SftpSessionPool {
        SftpSessionPool::new_writable(Arc::new(factory), shared, reads, writes)
            .await
            .expect("fully capable writable pool")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn thirty_two_waiters_observe_exact_shared_and_directional_caps() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut held = Vec::new();
        for _ in 0..7 {
            held.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        held.push(pool.checkout(OperationKind::Write).await.unwrap());
        let mut tasks = Vec::new();

        for index in 0..32 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let kind = if index % 8 == 7 {
                    OperationKind::Write
                } else {
                    OperationKind::Read
                };
                let lease = pool.checkout(kind).await.unwrap();
                lease.complete().await.unwrap();
            }));
        }

        tokio::task::yield_now().await;
        assert!(tasks.iter().all(|task| !task.is_finished()));
        assert_eq!(factory.live(), 8);
        assert_eq!(factory.peak(), 8);
        for lease in held {
            lease.complete().await.unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(factory.peak(), 8);
    }

    #[tokio::test]
    async fn seven_reads_leave_one_shared_slot_for_a_write() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut reads = Vec::new();
        for _ in 0..7 {
            reads.push(pool.checkout(OperationKind::Read).await.unwrap());
        }

        let eighth_read = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Read).await }
        });
        tokio::task::yield_now().await;
        assert!(!eighth_read.is_finished());

        let write = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Write),
        )
        .await
        .expect("write uses reserved slot")
        .unwrap();
        assert_eq!(factory.live(), 8);

        write.complete().await.unwrap();
        reads.pop().unwrap().complete().await.unwrap();
        eighth_read
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        for read in reads {
            read.complete().await.unwrap();
        }
    }

    #[tokio::test]
    async fn metadata_uses_fifo_read_admission_without_starvation() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory, 2, 1, 1).await);
        let first = pool.checkout(OperationKind::Read).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (position, kind) in [
            OperationKind::Metadata,
            OperationKind::Read,
            OperationKind::Metadata,
        ]
        .into_iter()
        .enumerate()
        {
            let pool = pool.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let lease = pool.checkout(kind).await.unwrap();
                order.lock().unwrap().push(position);
                lease.complete().await.unwrap();
            }));
            tokio::task::yield_now().await;
        }

        first.complete().await.unwrap();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_dial_or_consume_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Metadata).await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(factory.dials(), 1);

        held.complete().await.unwrap();
        pool.checkout(OperationKind::Read)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 1);
    }

    #[tokio::test]
    async fn canceled_operation_retires_session_and_next_operation_reconnects() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let checked_out = Arc::new(Notify::new());
        let task = tokio::spawn({
            let pool = pool.clone();
            let checked_out = checked_out.clone();
            async move {
                let _lease = pool.checkout(OperationKind::Write).await.unwrap();
                checked_out.notify_one();
                std::future::pending::<()>().await;
            }
        });
        checked_out.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 2);
    }

    #[tokio::test]
    async fn blocking_close_keeps_shared_lifetime_permit_until_close_finishes() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        factory.state.block_close.store(1, Ordering::SeqCst);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let retiring = tokio::spawn(async move { lease.retire().await });
        factory.state.close_started.notified().await;

        let reconnect = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::task::yield_now().await;
        assert!(!reconnect.is_finished());
        assert_eq!(factory.live(), 1);
        assert_eq!(factory.peak(), 1);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        retiring.await.unwrap().unwrap();
        reconnect.await.unwrap().unwrap().complete().await.unwrap();
        assert_eq!(factory.peak(), 1);
    }

    #[tokio::test]
    async fn ambiguous_mutation_error_retires_session_before_reconnect() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .retire()
            .await
            .unwrap();
        assert_eq!(factory.live(), 0);

        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 2);
    }

    #[tokio::test]
    async fn missing_each_publication_capability_refuses_writable_pool() {
        for (capabilities, missing) in [
            (
                SftpCapabilities {
                    fsync: false,
                    hardlink: true,
                    posix_rename: true,
                },
                "fsync@openssh.com",
            ),
            (
                SftpCapabilities {
                    fsync: true,
                    hardlink: false,
                    posix_rename: true,
                },
                "hardlink@openssh.com",
            ),
            (
                SftpCapabilities {
                    fsync: true,
                    hardlink: true,
                    posix_rename: false,
                },
                "posix-rename@openssh.com",
            ),
        ] {
            let error = SftpSessionPool::new_writable(
                Arc::new(RecordingFactory::new(capabilities)),
                8,
                7,
                7,
            )
            .await
            .expect_err("writable construction must fail closed");
            assert_eq!(
                error.to_string(),
                format!("SFTP server lacks required {missing} extension")
            );
        }
    }

    #[tokio::test]
    async fn writable_pool_accepts_limits_from_sftp_config() {
        let factory = RecordingFactory::fully_capable();
        let config = crate::config::SftpConfig {
            known_hosts: "/tmp/known-hosts".into(),
            max_connections: 3,
            read_concurrency: 2,
            write_concurrency: 1,
        };
        let pool = SftpSessionPool::from_config_writable(Arc::new(factory.clone()), &config)
            .await
            .unwrap();
        let mut leases = Vec::new();
        for _ in 0..2 {
            leases.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        leases.push(pool.checkout(OperationKind::Write).await.unwrap());
        assert_eq!(factory.peak(), 3);
        for lease in leases {
            lease.complete().await.unwrap();
        }
    }

    #[test]
    fn openssh_factory_debug_redacts_the_username() {
        let factory = OpenSshSessionFactory::new(
            SftpEndpoint {
                host: "storage.example.test".to_owned(),
                port: 2222,
                username: "account-secret-name".to_owned(),
            },
            "/tmp/known-hosts".into(),
        )
        .unwrap();

        let debug = format!("{factory:?}");
        assert!(debug.contains("storage.example.test"));
        assert!(debug.contains("2222"));
        assert!(!debug.contains("account-secret-name"));
    }

    #[tokio::test]
    async fn local_openssh_sftp_server_advertises_publication_extensions() {
        let Some(server) = ["/usr/libexec/sftp-server", "/usr/lib/openssh/sftp-server"]
            .into_iter()
            .find(|path| std::path::Path::new(path).is_file())
        else {
            eprintln!(
                "skipped: neither /usr/libexec/sftp-server nor /usr/lib/openssh/sftp-server exists"
            );
            return;
        };
        let mut child = tokio::process::Command::new(server)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("launch local OpenSSH sftp-server");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let session = OpenSshTransportSession::from_streams(stdin, stdout)
            .await
            .expect("complete the real SFTP extension handshake");
        assert_eq!(
            session.capabilities(),
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        );
        Box::new(session).close().await.unwrap();
        assert!(child.wait().await.unwrap().success());
    }
}
