use super::{attach_cleanup_errors, finish_with_sftp_cleanup};
use crate::cache_metrics::{CacheMetrics, FoyerMetricsRegistry};
use crate::checkpoint_manager::CheckpointManager;
use crate::config::{NbdConfig, NfsConfig, NinePConfig, RpcConfig, Settings};
use crate::db::SlateDbHandle;
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use crate::fs::{CacheConfig, GarbageCollector, ZeroFS};
use crate::length_checked_object_store::LengthCheckedObjectStore;
use crate::nbd::{NBDServer, NbdExportGates};
use crate::ninep::server::P9AcceptedWorkTracker;
use crate::object_store_prefetch::PrefetchingObjectStore;
use crate::parse_object_store::{ParsedStore, parse_url_opts};
use crate::storage_class_object_store::with_storage_class;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    S3FifoConfig, Spawner,
};
use futures::stream::{FuturesUnordered, StreamExt};
use slatedb::admin::AdminBuilder;
use slatedb::config::GarbageCollectorDirectoryOptions;
use slatedb::config::GarbageCollectorOptions;
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::object_store::path::Path;
use slatedb::{BlockTransformer, CompactorBuilder, DbBuilder, DbReader, DbReaderMode};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

mod mutation_lifecycle;
use mutation_lifecycle::{LifecycleOwners, MutationLifecycle};

const SFTP_FINAL_DATABASE_CLOSE_TIMEOUT: Duration = Duration::from_secs(20);
const SFTP_FINAL_WORKER_ABORT_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_AUTHORITY_FINISH_TIMEOUT: Duration = Duration::from_secs(10);
const SFTP_CHECKPOINT_POOL_CLEANUP: &str = "Failed to shut down SFTP checkpoint pool";

/// Parse a WAL config into an object store rooted at the full URL path.
pub(crate) async fn parse_wal_object_store(
    wal_config: &crate::config::WalConfig,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let env_vars = wal_config.cloud_provider_env_vars();
    let url: url::Url = wal_config.url.parse()?;
    // The WAL path has no owner for an SFTP pool's lifecycle; refuse rather
    // than leak a transport.
    if url.scheme() == "sftp" {
        anyhow::bail!("the WAL object store does not support sftp:// URLs");
    }
    let ParsedStore { store, path, .. } = parse_url_opts(&url, env_vars, None).await?;
    let path_str: &str = path.as_ref();
    let store: Arc<dyn object_store::ObjectStore> = if path_str.is_empty() {
        Arc::from(store)
    } else {
        Arc::new(object_store::prefix::PrefixStore::new(store, path))
    };
    Ok(with_storage_class(
        store,
        wal_config.storage_class.as_deref(),
    ))
}

#[derive(Debug, Clone, Copy)]
pub enum DatabaseMode {
    ReadWrite,
    ReadOnly,
    Checkpoint(uuid::Uuid),
}

impl DatabaseMode {
    pub fn is_read_only(&self) -> bool {
        !matches!(self, DatabaseMode::ReadWrite)
    }
}

/// Access mode used to resolve the shared write-acknowledgement contract:
/// volatile acknowledgement requires a read-write single-writer server.
fn write_ack_access_mode(db_mode: DatabaseMode) -> crate::writeback::config::WritebackAccessMode {
    match db_mode {
        DatabaseMode::ReadWrite => crate::writeback::config::WritebackAccessMode::ReadWrite,
        DatabaseMode::ReadOnly => crate::writeback::config::WritebackAccessMode::ReadOnly,
        DatabaseMode::Checkpoint(_) => crate::writeback::config::WritebackAccessMode::Checkpoint,
    }
}

fn validate_nbd_database_mode(config: Option<&NbdConfig>, db_mode: DatabaseMode) -> Result<()> {
    if db_mode.is_read_only()
        && config
            .is_some_and(|nbd| nbd.write_ack_mode == crate::config::NbdWriteAckMode::VolatileMemory)
    {
        anyhow::bail!(
            "[servers.nbd] volatile_memory acknowledgement is incompatible with read-only / checkpoint database modes"
        );
    }
    Ok(())
}

async fn resolve_checkpoint_name(settings: &Settings, name: &str) -> Result<uuid::Uuid> {
    let env_vars = settings.cloud_provider_env_vars();
    let ParsedStore {
        store: object_store,
        path: path_from_url,
        sftp_pool,
    } = parse_url_opts(
        &settings.storage.url.parse()?,
        env_vars,
        settings.sftp.as_ref(),
    )
    .await?;
    let object_store = with_storage_class(
        Arc::from(object_store),
        settings.storage.storage_class.as_deref(),
    );
    let db_path = Path::from(path_from_url.to_string());

    let mut admin_builder = AdminBuilder::new(db_path, object_store);
    if let Some(wal_config) = &settings.wal {
        let wal_object_store = match parse_wal_object_store(wal_config).await {
            Ok(store) => store,
            Err(error) => {
                return finish_with_sftp_cleanup(
                    sftp_pool.as_ref(),
                    SFTP_CHECKPOINT_POOL_CLEANUP,
                    Err(error),
                )
                .await;
            }
        };
        admin_builder = admin_builder.with_wal_object_store(wal_object_store);
    }
    let admin = admin_builder.build();

    let checkpoints = admin.list_checkpoints(Some(name)).await;
    drop(admin);
    let checkpoints = finish_with_sftp_cleanup(
        sftp_pool.as_ref(),
        SFTP_CHECKPOINT_POOL_CLEANUP,
        checkpoints.context("Failed to list checkpoints"),
    )
    .await?;

    checkpoints
        .into_iter()
        .find(|cp| cp.name.as_deref() == Some(name))
        .map(|cp| cp.id)
        .ok_or_else(|| anyhow::anyhow!("Checkpoint '{}' not found", name))
}

async fn start_nfs_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NfsConfig>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting NFS server on {}", addr);
            let fs_clone = Arc::clone(&fs);
            let addr = *addr;
            let shutdown_clone = shutdown.clone();
            let shared_identity = config.shared_identity;
            handles.push(spawn_named("nfs-server", async move {
                match crate::nfs::start_nfs_server_with_config(
                    fs_clone,
                    addr,
                    shutdown_clone,
                    shared_identity,
                )
                .await
                {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
    }

    handles
}

fn start_ninep_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NinePConfig>,
    shutdown: CancellationToken,
    accepted_work: P9AcceptedWorkTracker,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!("Starting 9P server on {}", addr);
            let mut ninep_tcp_server = crate::ninep::NinePServer::new(Arc::clone(&fs), *addr);
            if let Some(identity) = config.shared_identity {
                ninep_tcp_server =
                    ninep_tcp_server.with_credential_override(identity.uid, identity.gid);
            }
            let shutdown_clone = shutdown.clone();
            let accepted_work = accepted_work.clone();
            handles.push(spawn_named("9p-server", async move {
                ninep_tcp_server
                    .start_with_accepted_work(shutdown_clone, accepted_work)
                    .await
            }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting 9P server on Unix socket: {}",
            socket_path.display()
        );
        let ninep_unix_fs = Arc::clone(&fs);
        let mut ninep_unix_server =
            crate::ninep::NinePServer::new_unix(ninep_unix_fs, socket_path.clone());
        if let Some(identity) = config.shared_identity {
            ninep_unix_server =
                ninep_unix_server.with_credential_override(identity.uid, identity.gid);
        }
        let shutdown_clone = shutdown.clone();
        let accepted_work = accepted_work.clone();
        handles.push(spawn_named("9p-unix-server", async move {
            ninep_unix_server
                .start_with_accepted_work(shutdown_clone, accepted_work)
                .await
        }));
    }

    handles
}

async fn ensure_nbd_directory(fs: &Arc<ZeroFS>) -> Result<()> {
    let creds = Credentials {
        uid: 0,
        gid: 0,
        gid_known: true,
        groups: [0; 16],
        groups_count: 1,
        groups_complete: true,
    };
    let nbd_name = b".nbd";

    match fs.lookup(&creds, 0, nbd_name).await {
        Ok(_) => info!(".nbd directory already exists"),
        Err(e) => {
            debug!(".nbd directory lookup returned: {:?}, will create it", e);
            let attr = SetAttributes {
                mode: crate::fs::types::SetMode::Set(0o755),
                uid: crate::fs::types::SetUid::Set(0),
                gid: crate::fs::types::SetGid::Set(0),
                ..Default::default()
            };
            fs.mkdir(&creds, 0, nbd_name, &attr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create .nbd directory: {e:?}"))?;
            info!("Created .nbd directory for NBD device management");
        }
    }
    Ok(())
}

fn nbd_volatile_budget(write_ack: crate::fs::mutation::config::FilesystemWriteAckSettings) -> u64 {
    match write_ack.mode {
        crate::fs::mutation::config::FilesystemWriteAckMode::VolatileMemory => {
            write_ack.volatile_memory_bytes
        }
        crate::fs::mutation::config::FilesystemWriteAckMode::Materialized => 0,
    }
}

async fn start_nbd_servers(
    fs: Arc<ZeroFS>,
    config: Option<&NbdConfig>,
    shutdown: CancellationToken,
) -> anyhow::Result<(
    Vec<JoinHandle<Result<(), std::io::Error>>>,
    Option<Arc<NbdExportGates>>,
)> {
    let config = match config {
        Some(c) => c,
        None => return Ok((Vec::new(), None)),
    };
    let mut handles = Vec::new();
    fs.install_volatile_overlay();
    let volatile_memory_bytes = nbd_volatile_budget(fs.write_ack);
    let volatile_enabled = volatile_memory_bytes > 0;
    metrics::gauge!("zerofs_nbd_volatile_memory_enabled").set(f64::from(volatile_enabled));
    if volatile_enabled {
        warn!(
            volatile_memory_bytes,
            "NBD volatile-memory acknowledgement is enabled: ordinary WRITE replies are unsafe across process or power loss until FLUSH/FUA completes"
        );
    } else {
        info!("NBD materialized write acknowledgement is enabled");
    }
    let export_gates = Arc::new(match fs.volatile_budget() {
        Some(budget) => NbdExportGates::with_budget(Some(budget)),
        None => NbdExportGates::new(volatile_memory_bytes),
    });

    if let Some(addresses) = &config.addresses {
        for addr in addresses {
            info!(
                "Starting NBD server on {} (devices dynamically discovered from .nbd/)",
                addr
            );
            let nbd_tcp_server =
                NBDServer::new_tcp(Arc::clone(&fs), Arc::clone(&export_gates), *addr);
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nbd-server", async move {
                if let Err(e) = nbd_tcp_server.start(shutdown_clone).await {
                    Err(e)
                } else {
                    Ok(())
                }
            }));
        }
    }

    if let Some(socket_path) = config.unix_socket.as_ref() {
        info!(
            "Starting NBD server on Unix socket {} (devices dynamically discovered from .nbd/)",
            socket_path.display()
        );
        let nbd_unix_server =
            NBDServer::new_unix(Arc::clone(&fs), Arc::clone(&export_gates), socket_path);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix-server", async move {
            if let Err(e) = nbd_unix_server.start(shutdown_clone).await {
                Err(e)
            } else {
                Ok(())
            }
        }));
    }

    Ok((handles, volatile_enabled.then_some(export_gates)))
}

async fn start_rpc_servers(
    config: Option<&RpcConfig>,
    checkpoint_manager: Arc<CheckpointManager>,
    fs: Arc<ZeroFS>,
    shutdown: CancellationToken,
    protect_nbd_exports: bool,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let config = match config {
        Some(c) => c,
        None => return Vec::new(),
    };

    let service = crate::rpc::server::AdminRpcServer::new(checkpoint_manager, fs, shutdown.clone())
        .with_nbd_export_protection(protect_nbd_exports);
    let mut handles = Vec::new();

    if let Some(addresses) = &config.addresses {
        for &addr in addresses {
            info!("Starting RPC server on {}", addr);
            let service = service.clone();
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("rpc-server", async move {
                crate::rpc::server::serve_tcp(addr, service, shutdown_clone)
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))
            }));
        }
    }

    if let Some(socket_path) = &config.unix_socket {
        info!(
            "Starting RPC server on Unix socket: {}",
            socket_path.display()
        );
        let socket_path = socket_path.clone();
        let service = service.clone();
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("rpc-unix-server", async move {
            crate::rpc::server::serve_unix(socket_path, service, shutdown_clone)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        }));
    }

    handles
}

fn start_stats_reporting(fs: Arc<ZeroFS>, shutdown: CancellationToken) -> JoinHandle<()> {
    spawn_named("stats-reporting", async move {
        info!("Starting stats reporting task (reports to debug every 5 seconds)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Stats reporting task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    fs.stats.output_report_debug();
                }
            }
        }
    })
}

fn start_periodic_flush(
    fs: Arc<ZeroFS>,
    interval_secs: u64,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_named("periodic-flush", async move {
        info!(
            "Starting periodic flush task (flushes every {} seconds)",
            interval_secs
        );
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("Periodic flush task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = fs.flush_coordinator.flush().await {
                        tracing::error!("Periodic flush failed: {:?}", e);
                    }
                }
            }
        }
    })
}

async fn join_or_abort_tasks(
    handles: Vec<JoinHandle<()>>,
    deadline: Duration,
    on_timeout: impl FnOnce(usize),
) {
    let mut handles: FuturesUnordered<_> = handles.into_iter().collect();
    if tokio::time::timeout(deadline, async { while handles.next().await.is_some() {} })
        .await
        .is_ok()
    {
        return;
    }

    on_timeout(handles.len());
    for handle in handles.iter() {
        handle.abort();
    }
    while handles.next().await.is_some() {}
}

async fn join_or_abort_background_tasks(handles: Vec<JoinHandle<()>>, deadline: Duration) {
    join_or_abort_tasks(handles, deadline, |count| {
        tracing::warn!(
            count,
            timeout_secs = deadline.as_secs(),
            "background tasks did not stop before final close; aborting them"
        );
    })
    .await;
}

fn leadership_lost_error() -> anyhow::Error {
    anyhow::anyhow!("HA writer was fenced or superseded; restart required")
}

async fn abort_final_flush_after_leadership_loss(fs: &ZeroFS) {
    match tokio::time::timeout(
        SFTP_FINAL_WORKER_ABORT_TIMEOUT,
        fs.flush_coordinator.abort_close_worker(),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(
            ?error,
            "failed to abort final flush worker after leadership loss"
        ),
        Err(error) => tracing::error!(
            %error,
            "timed out aborting final flush worker after leadership loss"
        ),
    }
}

fn listener_exit_error(
    result: std::result::Result<std::result::Result<(), std::io::Error>, tokio::task::JoinError>,
    unexpected_exit: bool,
) -> Option<anyhow::Error> {
    match result {
        Ok(Ok(())) if unexpected_exit => {
            Some(anyhow::anyhow!("server listener exited unexpectedly"))
        }
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(anyhow::Error::new(error).context("server listener failed")),
        Err(error) => Some(anyhow::Error::new(error).context("server listener task failed")),
    }
}

type ServerHandle = JoinHandle<std::result::Result<(), std::io::Error>>;

#[derive(Debug)]
enum ServingStopCause {
    Signal,
    LeadershipLost,
    ListenerFailure(anyhow::Error),
}

impl ServingStopCause {
    fn is_signal(&self) -> bool {
        matches!(self, Self::Signal)
    }

    fn is_leadership_lost(&self) -> bool {
        matches!(self, Self::LeadershipLost)
    }

    fn into_error(self) -> Option<anyhow::Error> {
        match self {
            Self::Signal => None,
            Self::LeadershipLost => Some(leadership_lost_error()),
            Self::ListenerFailure(error) => Some(error),
        }
    }
}

fn merge_cleanup_results(
    mut existing: Vec<anyhow::Error>,
    result: anyhow::Result<()>,
) -> anyhow::Result<()> {
    if let Err(error) = result {
        existing.push(error);
    }
    if existing.is_empty() {
        return Ok(());
    }

    let primary = existing.remove(0);
    Err(attach_cleanup_errors(primary, existing))
}

fn finish_serving_shutdown(
    cause: ServingStopCause,
    cleanup: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match (cause.into_error(), cleanup) {
        (None, cleanup) => cleanup,
        (Some(primary), Ok(())) => Err(primary),
        (Some(primary), Err(cleanup)) => Err(attach_cleanup_errors(primary, vec![cleanup])),
    }
}

fn finish_process_shutdown(
    server: anyhow::Result<()>,
    writeback: anyhow::Result<()>,
    sftp: anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut cleanup = Vec::new();
    if let Err(error) = writeback {
        cleanup.push(error.context("writeback shutdown failed"));
    }
    if let Err(error) = sftp {
        cleanup.push(error.context("SFTP shutdown failed"));
    }

    match server {
        Err(primary) => Err(attach_cleanup_errors(primary, cleanup)),
        Ok(()) if cleanup.is_empty() => Ok(()),
        Ok(()) => {
            let primary = cleanup.remove(0);
            Err(attach_cleanup_errors(primary, cleanup))
        }
    }
}

fn retain_listener_failure(
    cause: &mut ServingStopCause,
    cleanup_errors: &mut Vec<anyhow::Error>,
    error: anyhow::Error,
) {
    if cause.is_signal() {
        *cause = ServingStopCause::ListenerFailure(error);
    } else {
        cleanup_errors.push(error);
    }
}

fn retain_leadership_loss(cause: &mut ServingStopCause, cleanup_errors: &mut Vec<anyhow::Error>) {
    let previous = std::mem::replace(cause, ServingStopCause::LeadershipLost);
    if let ServingStopCause::ListenerFailure(error) = previous {
        cleanup_errors.push(error);
    }
}

fn finish_serving_cleanup(
    mut cause: ServingStopCause,
    mut cleanup_errors: Vec<anyhow::Error>,
    cleanup_result: anyhow::Result<()>,
    leadership_lost: bool,
) -> anyhow::Result<()> {
    if leadership_lost && !cause.is_leadership_lost() {
        retain_leadership_loss(&mut cause, &mut cleanup_errors);
    }
    finish_serving_shutdown(cause, merge_cleanup_results(cleanup_errors, cleanup_result))
}

async fn drain_server_handles_for_stop(
    mut cause: ServingStopCause,
    handles: &mut FuturesUnordered<ServerHandle>,
    leadership_deposed: &CancellationToken,
) -> (ServingStopCause, Vec<anyhow::Error>) {
    let mut cleanup_errors = Vec::new();
    let grace = tokio::time::sleep(crate::replication::RESPONSE_DRAIN_TIMEOUT);
    tokio::pin!(grace);
    let timed_out = loop {
        if handles.is_empty() {
            break false;
        }
        tokio::select! {
            biased;
            _ = leadership_deposed.cancelled(), if !cause.is_leadership_lost() => {
                retain_leadership_loss(&mut cause, &mut cleanup_errors);
            }
            result = handles.next() => {
                if let Some(error) = listener_exit_error(
                    result.expect("non-empty server handles must yield a result"),
                    false,
                ) {
                    retain_listener_failure(&mut cause, &mut cleanup_errors, error);
                }
            }
            _ = &mut grace => break true,
        }
    };

    if timed_out {
        tracing::warn!(
            count = handles.len(),
            timeout_secs = crate::replication::RESPONSE_DRAIN_TIMEOUT.as_secs(),
            "server listeners did not stop within the response-drain grace; aborting them"
        );
        for handle in handles.iter() {
            handle.abort();
        }
        while let Some(result) = handles.next().await {
            if result
                .as_ref()
                .is_err_and(tokio::task::JoinError::is_cancelled)
            {
                continue;
            }
            if let Some(error) = listener_exit_error(result, false) {
                retain_listener_failure(&mut cause, &mut cleanup_errors, error);
            }
        }
        cleanup_errors.push(anyhow::anyhow!(
            "server listener shutdown exceeded the {}s response-drain grace",
            crate::replication::RESPONSE_DRAIN_TIMEOUT.as_secs()
        ));
    }

    (cause, cleanup_errors)
}

/// Walk an error's source chain looking for an open-file-descriptor exhaustion
/// (EMFILE/ENFILE). foyer reports these as an opaque `I/O error => coding error`
/// whose only clue is the wrapped os error code, so detection has to go by the
/// raw code rather than the (libc-dependent) message text.
fn is_fd_exhaustion(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(err);
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && matches!(io.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
        {
            return true;
        }
        source = e.source();
    }
    false
}

/// Wrap a foyer cache build failure, keeping the real error visible and adding a
/// `ulimit -n` hint when the cause is fd exhaustion (which foyer otherwise hides
/// behind a useless "coding error").
fn foyer_build_error(context: &str, err: foyer::Error) -> anyhow::Error {
    if is_fd_exhaustion(&err) {
        anyhow::anyhow!(
            "{context}: {err}\n\nZeroFS ran out of open file descriptors while building \
             the on-disk cache. Raise the open-file limit (e.g. `ulimit -n 1048576`, or \
             LimitNOFILE= in the systemd unit) and restart."
        )
    } else {
        anyhow::anyhow!("{context}: {err}")
    }
}

/// Build the foyer hybrid cache used as slatedb's block cache. Shared by the
/// server open path and the warm-metadata integration test.
pub(crate) async fn build_block_hybrid(
    hybrid_cache_root: &std::path::Path,
    memory_bytes: usize,
    disk_bytes: usize,
    foyer_handle: &tokio::runtime::Handle,
    metrics: FoyerMetricsRegistry,
) -> Result<(
    Arc<FoyerHybridCache>,
    foyer::HybridCache<slatedb::db_cache::CachedKey, slatedb::db_cache::CachedEntry>,
)> {
    tokio::fs::create_dir_all(hybrid_cache_root)
        .await
        .with_context(|| {
            format!(
                "creating foyer hybrid cache dir at {}",
                hybrid_cache_root.display()
            )
        })?;

    let hybrid = HybridCacheBuilder::new()
        .with_name("zerofs-slatedb-hybrid")
        .with_metrics_registry(Box::new(metrics))
        .memory(memory_bytes)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_, v: &slatedb::db_cache::CachedEntry| v.size())
        .storage()
        .with_spawner(Spawner::from(foyer_handle.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(hybrid_cache_root)
                    .with_capacity(disk_bytes)
                    .build()
                    .map_err(|e| foyer_build_error("foyer device build failed", e))?,
            )
            .with_block_size(64 * 1024 * 1024),
        )
        .build()
        .await
        .map_err(|e| foyer_build_error("foyer hybrid build failed", e))?;
    Ok((
        Arc::new(FoyerHybridCache::new_with_cache(hybrid.clone())),
        hybrid,
    ))
}

/// Block size of the parts disk cache: foyer's eviction/reclaim unit, and the
/// max cacheable entry size.
const PARTS_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// Disk-engine knobs for the parts cache, scaled to the device.
struct PartsEngineKnobs {
    flushers: usize,
    clean_block_threshold: usize,
    submit_queue_bytes: usize,
    buffer_pool_bytes: usize,
}

fn parts_engine_knobs(disk_bytes: usize) -> PartsEngineKnobs {
    let blocks = disk_bytes / PARTS_BLOCK_SIZE;
    let flushers = (blocks / 8).clamp(1, 4);

    PartsEngineKnobs {
        flushers,
        clean_block_threshold: (blocks / 64).clamp(flushers, 8),
        submit_queue_bytes: (disk_bytes / 4).clamp(16 * 1024 * 1024, 1024 * 1024 * 1024),
        buffer_pool_bytes: flushers * PARTS_BLOCK_SIZE,
    }
}

pub(crate) async fn build_parts_hybrid(
    cache_root: &std::path::Path,
    memory_bytes: usize,
    disk_bytes: usize,
    foyer_handle: &tokio::runtime::Handle,
    metrics: FoyerMetricsRegistry,
) -> Result<foyer::HybridCache<crate::object_store_prefetch::PartKey, bytes::Bytes>> {
    use crate::object_store_prefetch::PartKey;
    use bytes::Bytes;

    let parts_root = cache_root.join("parts_cache");
    tokio::fs::create_dir_all(&parts_root)
        .await
        .with_context(|| format!("creating parts cache dir at {}", parts_root.display()))?;

    let knobs = parts_engine_knobs(disk_bytes);

    HybridCacheBuilder::new()
        .with_name("zerofs-object-prefetch-parts")
        .with_metrics_registry(Box::new(metrics))
        .memory(memory_bytes)
        .with_eviction_config(S3FifoConfig::default())
        .with_weighter(|_: &PartKey, v: &Bytes| v.len())
        .storage()
        .with_spawner(Spawner::from(foyer_handle.clone()))
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(&parts_root)
                    .with_capacity(disk_bytes)
                    .build()
                    .map_err(|e| foyer_build_error("parts foyer device build failed", e))?,
            )
            .with_block_size(PARTS_BLOCK_SIZE)
            .with_submit_queue_size_threshold(knobs.submit_queue_bytes)
            .with_flushers(knobs.flushers)
            .with_reclaimers(knobs.flushers)
            .with_clean_block_threshold(knobs.clean_block_threshold)
            .with_buffer_pool_size(knobs.buffer_pool_bytes),
        )
        .build()
        .await
        .map_err(|e| foyer_build_error("parts foyer hybrid build failed", e))
}

/// Split the configured disk-cache total into
/// (parts_disk_bytes, decoded_blocks_disk_bytes).
///
/// SlateDB holds only metadata — a small working set the raw-parts cache backs
/// anyway (it caches SST object bytes next to segment bytes, so a decoded-cache
/// miss is a parts-cache hit plus a re-decode). The decoded-blocks side gets a
/// bounded slice; the parts cache, where the bulk segment bytes live, gets the
/// rest. Floors keep either side from collapsing on a tiny config.
pub(crate) fn split_disk_budget(total_disk_bytes: usize) -> (usize, usize) {
    const MIN_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB floor per side
    // u64: 16 GiB overflows usize on 32-bit targets
    const MAX_META_BYTES: u64 = 16 * 1024 * 1024 * 1024; // metadata rarely needs more

    let max_meta = usize::try_from(MAX_META_BYTES).unwrap_or(usize::MAX);
    let decoded = (total_disk_bytes / 10).clamp(MIN_BYTES, max_meta);
    let parts = total_disk_bytes.saturating_sub(decoded).max(MIN_BYTES);
    (parts, decoded)
}

/// Split the configured clean memory-cache total into (parts_memory_bytes,
/// decoded_blocks_memory_bytes, decoded_extent_memory_bytes). The metadata
/// block cache retains its existing quarter/cap policy; the data share is split
/// evenly between encrypted segment parts and the extent-read caches (decoded
/// plaintext plus their logical location map).
pub(crate) fn split_memory_budget(total_memory_bytes: usize) -> (usize, usize, usize) {
    const MIN_BYTES: usize = 32 * 1024 * 1024; // 32 MiB floor per consumer
    const MAX_META_BYTES: usize = 2 * 1024 * 1024 * 1024; // metadata blocks rarely need more

    let decoded_blocks = (total_memory_bytes / 4).clamp(MIN_BYTES, MAX_META_BYTES);
    let data = total_memory_bytes.saturating_sub(decoded_blocks);
    let decoded_extents = (data / 2).max(MIN_BYTES);
    let parts = data.saturating_sub(decoded_extents).max(MIN_BYTES);
    (parts, decoded_blocks, decoded_extents)
}

/// Result of opening the ZeroFS database.
pub struct SlateDbOpen {
    pub data: SlateDbHandle,
    pub metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    pub cache_metrics: Arc<CacheMetrics>,
    /// The raw-parts prefetch cache, returned so the segment store reuses it
    /// (one budget; segment objects and SST objects share it, keyed by path).
    pub parts_cache: foyer::HybridCache<crate::object_store_prefetch::PartKey, bytes::Bytes>,
    /// Portion of the configured clean memory cache reserved for extent reads:
    /// read-ready plaintext plus its logical location map. This is separate from
    /// the dirty writeback budget.
    pub decoded_extent_memory_bytes: usize,
}

/// Process-wide runtime for cache, database, and GC maintenance.
fn shared_maintenance_runtime() -> &'static tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("zerofs-maintenance")
                .build()
                .expect("failed to build maintenance runtime")
        })
        .handle()
}

// SlateDB 0.15 validates that max_unflushed_bytes is strictly greater than
// l0_sst_size_bytes. ZeroFS must effectively disable both thresholds because
// only a seal-barrier-controlled flush may make metadata durable.
const BARRIER_CONTROLLED_L0_SST_SIZE_BYTES: usize = usize::MAX - 1;
const BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES: usize = usize::MAX;

fn select_rss_pressure_cap(installed_cap: u64, clean_cache_fallback: u64) -> u64 {
    if installed_cap == 0 {
        clean_cache_fallback
    } else {
        installed_cap
    }
}

fn install_validated_rss_cap(
    memory_budget: Option<&crate::cli::memory_budget::MemoryBudgetReceipt>,
) {
    if let Some(memory_budget) = memory_budget {
        crate::alloc_rss::set_rss_cap_bytes(memory_budget.rss_pressure_cap_bytes);
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn build_slatedb(
    object_store: Arc<dyn object_store::ObjectStore>,
    cache_config: &CacheConfig,
    db_path: String,
    db_mode: DatabaseMode,
    lsm_config: Option<crate::config::LsmConfig>,
    block_transformer: Arc<dyn BlockTransformer>,
    wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    replication: Option<&crate::replication::ReplicationParams>,
) -> Result<SlateDbOpen> {
    #[cfg(test)]
    let _rss_cap_guard = crate::alloc_rss::lock_test_rss_cap().await;

    let total_disk_cache_gb = cache_config.max_cache_size_gb;
    let total_memory_cache_gb = cache_config.memory_cache_size_gb.unwrap_or(0.25);

    let total_disk_bytes = (total_disk_cache_gb * 1_000_000_000.0) as usize;
    let (parts_disk_bytes, hybrid_disk_bytes) = split_disk_budget(total_disk_bytes);
    let total_memory_bytes = (total_memory_cache_gb * 1_000_000_000.0) as usize;
    let (parts_memory_bytes, hybrid_memory_bytes, decoded_extent_memory_bytes) =
        split_memory_budget(total_memory_bytes);

    info!(
        "Cache allocation - Disk: {:.2}GB total ({} MB decoded-blocks + {} MB raw-parts), \
         Memory: {:.2}GB total ({} MB extent-reads + {} MB decoded-blocks + {} MB raw-parts)",
        total_disk_cache_gb,
        hybrid_disk_bytes / 1_000_000,
        parts_disk_bytes / 1_000_000,
        total_memory_cache_gb,
        decoded_extent_memory_bytes / 1_000_000,
        hybrid_memory_bytes / 1_000_000,
        parts_memory_bytes / 1_000_000,
    );

    let l0_max_ssts = lsm_config
        .map(|c| c.l0_max_ssts())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS);
    let max_concurrent_compactions = lsm_config
        .map(|c| c.max_concurrent_compactions())
        .unwrap_or(crate::config::LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS);

    // Replication needs the writer path: reject read-only / checkpoint, and
    // reject reaching here as a standby (a standby opens the data db as writer
    // only on promotion; doing so here would fence the live leader).
    if let Some(repl) = replication {
        if db_mode.is_read_only() {
            anyhow::bail!(
                "[replication] is incompatible with read-only / checkpoint database modes; \
                 node {} must open the data database as a writer",
                repl.node_id
            );
        }
        if !repl.is_leader() {
            anyhow::bail!(
                "internal error: build_slatedb reached as a standby (node {}); a standby must \
                 complete failover and be promoted to leader before opening the data database",
                repl.node_id
            );
        }
    }

    // The WAL is permanently off, a correctness requirement: with it on,
    // SlateDB flushes durably on the write path without taking our seal
    // barrier, so a FrameLoc could become durable while its segment is still
    // the un-PUT open buffer (a dangling pointer after a crash). With it off,
    // the barrier-gated flush — which seals the open segment first — is the
    // only path that makes metadata durable.
    let wal_enabled = false;

    let settings = slatedb::config::Settings {
        wal_enabled,
        l0_max_ssts,
        l0_max_ssts_per_key: l0_max_ssts,
        // Disable SlateDB's write-path memtable size-freeze (`flush_interval:
        // None` does not — that only kills the WAL timer). Left finite, the
        // size check would dispatch a durable L0 flush from a background task
        // that never takes our seal barrier, publishing FrameLocs for a
        // still-un-PUT segment. Keep both size thresholds effectively disabled
        // so the memtable freezes only on our barrier-gated `db.flush()`, which
        // also drains it (RAM-bounded) on every flush. SlateDB requires the
        // backpressure threshold to be strictly greater than the freeze
        // threshold, hence MAX - 1 and MAX rather than MAX for both.
        l0_sst_size_bytes: BARRIER_CONTROLLED_L0_SST_SIZE_BYTES,
        compactor_options: None,
        flush_interval: None,
        // Independent of HA authority checks.
        manifest_poll_interval: std::time::Duration::from_secs(5),
        max_unflushed_bytes: BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES,
        compression_codec: None, // Disable compression as we handle it in encryption layer
        l0_flush_parallelism: 16,
        min_filter_keys: 10,
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            manifest_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            compacted_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            compactions_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_mins(1)),
                min_age: Duration::from_mins(1),
                dry_run: false,
            }),
            detach_options: None,
            // Disable WAL fence GC: it defaults to a dry-run that does nothing
            // but logs a conservative-setting warning every interval. See #352.
            wal_fence_options: None,
            ..Default::default()
        }),
        ..Default::default()
    };

    // HA startup retries share the process-wide maintenance runtime.
    let maintenance_runtime = shared_maintenance_runtime().clone();

    let hybrid_cache_root = cache_config.root_folder.join("hybrid_cache");
    let foyer_metrics = FoyerMetricsRegistry::default();
    let (cache, block_cache_metrics) = build_block_hybrid(
        &hybrid_cache_root,
        hybrid_memory_bytes,
        hybrid_disk_bytes,
        &maintenance_runtime,
        foyer_metrics.clone(),
    )
    .await?;

    let parts_cache = build_parts_hybrid(
        &cache_config.root_folder,
        parts_memory_bytes,
        parts_disk_bytes,
        &maintenance_runtime,
        foyer_metrics.clone(),
    )
    .await?;
    let cache_metrics = Arc::new(CacheMetrics::new(
        parts_cache.clone(),
        block_cache_metrics,
        foyer_metrics,
    ));

    // Length-check the store before the data-db prefetch wrapper is layered on;
    // the compactor uses the length-checked store directly (no prefetch cache).
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LengthCheckedObjectStore::new(object_store));
    let compactor_object_store = object_store.clone();
    let wal_object_store = wal_object_store
        .map(|s| Arc::new(LengthCheckedObjectStore::new(s)) as Arc<dyn object_store::ObjectStore>);
    let installed_cap = crate::alloc_rss::rss_cap_bytes();
    let rss_pressure_cap_bytes = select_rss_pressure_cap(installed_cap, total_memory_bytes as u64);
    if installed_cap == 0 {
        crate::alloc_rss::set_rss_cap_bytes(rss_pressure_cap_bytes);
    }
    info!(
        "Resident-memory pressure cap: {} MB (configured clean cache: {} MB)",
        rss_pressure_cap_bytes / 1_000_000,
        total_memory_bytes / 1_000_000,
    );
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
        PrefetchingObjectStore::new(object_store, parts_cache.clone())
            .with_admission_cap(rss_pressure_cap_bytes),
    );

    let db_path = Path::from(db_path);

    match db_mode {
        DatabaseMode::ReadWrite => {
            info!("Opening database in read-write mode");

            let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());

            let mut builder = DbBuilder::new(db_path.clone(), object_store.clone())
                .with_settings(settings)
                .with_gc_runtime(maintenance_runtime.clone())
                .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
                .with_db_cache(cache)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_metrics_recorder(metrics_recorder.clone())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));

            if let Some(wal_store) = wal_object_store {
                builder = builder.with_wal_object_store(wal_store);
            }

            // The compaction coordinator is bound to the read-write DB, so it
            // runs only on the current leader. SlateDB holds only metadata, so
            // its compaction is light enough to embed in-process.
            {
                let scheduler_options: std::collections::HashMap<String, String> =
                    slatedb::config::SizeTieredCompactionSchedulerOptions {
                        max_compaction_sources: 16,
                        ..Default::default()
                    }
                    .into();
                let worker = Some(slatedb::config::CompactionWorkerOptions {
                    max_sst_size: 256 * 1024 * 1024,
                    max_fetch_tasks: 2,
                    bytes_to_fetch: 8 * 1024 * 1024,
                    // Metadata-only DB now that chunks live outside SlateDB, so
                    // compactions are small. Match the 2-job coordinator cap: the
                    // 256MiB max_sst_size floor keeps a compaction single-range
                    // until its input tops 512MiB, so only a rare large one splits
                    // into a second sub-range instead of running single-threaded.
                    max_subcompactions: 2,
                    ..Default::default()
                });
                let compactor = CompactorBuilder::new(db_path, compactor_object_store)
                    .with_runtime(maintenance_runtime.clone())
                    .with_filter_policies(crate::fs::filter_policy::filter_policies())
                    .with_options(slatedb::config::CompactorOptions {
                        poll_interval: std::time::Duration::from_secs(5),
                        commit_compacted_interval: std::time::Duration::from_secs(5),
                        max_concurrent_compactions,
                        scheduler_options,
                        worker,
                        ..Default::default()
                    });

                builder = builder.with_compactor_builder(compactor);
            }

            let slatedb = Arc::new(
                builder
                    .build()
                    .await
                    .context("Failed to build SlateDB instance")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadWrite(slatedb),
                metrics_recorder: Some(metrics_recorder),
                cache_metrics: cache_metrics.clone(),
                parts_cache: parts_cache.clone(),
                decoded_extent_memory_bytes,
            })
        }
        DatabaseMode::ReadOnly => {
            info!("Opening database in read-only mode");

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_db_cache(cache)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database in read-only mode")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                metrics_recorder: None,
                cache_metrics: cache_metrics.clone(),
                parts_cache: parts_cache.clone(),
                decoded_extent_memory_bytes,
            })
        }
        DatabaseMode::Checkpoint(checkpoint_id) => {
            info!("Opening database from checkpoint ID: {}", checkpoint_id);

            let mut reader_builder = DbReader::builder(db_path, object_store)
                .with_reader_mode(DbReaderMode::Checkpoint(checkpoint_id))
                .with_db_cache(cache)
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor));
            if let Some(wal_store) = wal_object_store {
                reader_builder = reader_builder.with_wal_object_store(wal_store);
            }
            let reader = Arc::new(
                reader_builder
                    .build()
                    .await
                    .context("Failed to open database from checkpoint")?,
            );

            Ok(SlateDbOpen {
                data: SlateDbHandle::ReadOnly(ArcSwap::new(reader)),
                metrics_recorder: None,
                cache_metrics,
                parts_cache: parts_cache.clone(),
                decoded_extent_memory_bytes,
            })
        }
    }
}

pub struct InitResult {
    pub fs: Arc<ZeroFS>,
    pub object_store: Arc<dyn object_store::ObjectStore>,
    pub writeback: Option<crate::writeback::store::WritebackObjectStore>,
    pub sftp_pool: Option<crate::sftp_transport::SftpSessionPool>,
    pub cache_metrics: Arc<CacheMetrics>,
    pub wal_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    pub db_path: String,
    pub db_handle: SlateDbHandle,
    /// HA authority monitors retained through database close.
    pub authority: Option<crate::replication::AuthoritySupervisor>,
}

const STARTUP_BANNER: &str = r#"
⠀⠀⠀⠀⠀⣠⣴⣶⣿⣿⣿⣿⣿⣷⣶⣤⣄
⠀⠀⢀⣴⣿⣿⣿⠿⠛⠛⠋⠉⠙⠻⠿⣿⣿⣿⣦⡀
⠀⣠⣿⣿⡿⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣿⡄
⢰⣿⣿⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣿⣿⡄⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⠀⠀⢠⣶⣿⣿⣿⣿⣶⡆
⣾⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⣿⣷⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⠟⠁⠀⠀⣠⣴⣶⣶⣶⣤⡀⠀⠀⣶⣶⣆⣤⣶⣶⠀⢀⣤⣶⣶⣶⣦⣄⠀⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⣿⣿⣏⠀⠀⠈⠉⠃
⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⠀⠀⠀⠀⠀⢀⣼⣿⡟⠁⠀⠀⠀⣼⣿⣟⣁⣀⣙⣿⣿⡀⠀⣿⣿⣿⠋⠉⠙⢠⣿⣿⠏⠀⠈⢻⣿⣧⠀⠀⣿⣿⣿⣿⣿⣿⡇⠀⠀⠘⠻⠿⣿⣿⣶⣦⣄
⢿⢿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⡿⠀⠀⠀⢀⣴⣿⡿⠋⠀⠀⠀⠀⠀⢿⣿⣟⠛⠛⠛⠛⠛⠃⠀⣿⣿⡇⠀⠀⠀⠸⣿⣿⡄⠀⠀⣸⣿⡿⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⣄⣀⠀⠀⠀⣹⣿⣿
⠈⠈⢿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⣿⣿⠃⠀⠀⠀⣾⣿⣿⣿⣿⣿⣿⣿⣿⠀⠈⠻⢿⣷⣶⣶⣶⠿⠀⠀⣿⣿⡇⠀⠀⠀⠀⠙⠿⣿⣶⣾⡿⠟⠁⠀⠀⣿⣿⡇⠀⠀⠀⠀⠀⠀⠻⠿⣿⣿⣿⣿⠿⠋
⠀⠀⠀⠻⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢐⣽⣿⣿⠋
⠀⠀⠀⠀⠙⢿⣿⣶⣤⣀⣀⣀⣀⣤⣤⣶⣿⣿⠟⠁
⠀⠀⠀⠀⠀⠀⠉⠛⠿⢿⣿⣿⣿⣿⠿⠟⠋
"#;

pub async fn run_server(
    config_path: PathBuf,
    read_only: bool,
    checkpoint_name: Option<String>,
) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    eprintln!("{STARTUP_BANNER}");

    // Default: ZeroFS at info, the embedded LSM engine at warn and above (the
    // metadata-compaction digest task summarizes its routine activity).
    // RUST_LOG replaces this entirely.
    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info,slatedb=warn"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    #[cfg(not(feature = "tokio-console"))]
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    info!("ZeroFS v{}", env!("CARGO_PKG_VERSION"));

    let (settings, memory_budget) = load_and_validate_server_settings(&config_path)?;
    install_validated_rss_cap(memory_budget.as_ref());

    let db_mode = match (read_only, &checkpoint_name) {
        (false, None) => DatabaseMode::ReadWrite,
        (true, None) => DatabaseMode::ReadOnly,
        (false, Some(name)) => {
            let uuid = resolve_checkpoint_name(&settings, name)
                .await
                .with_context(|| format!("Failed to resolve checkpoint '{}'", name))?;
            DatabaseMode::Checkpoint(uuid)
        }
        (true, Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both --read-only and --checkpoint flags"
            ));
        }
    };
    validate_nbd_database_mode(settings.servers.nbd.as_ref(), db_mode)?;
    let write_ack = settings
        .filesystem_write_ack_settings(write_ack_access_mode(db_mode))
        .context("Invalid filesystem write-acknowledgement configuration")?;
    if write_ack.mode == crate::fs::mutation::config::FilesystemWriteAckMode::VolatileMemory {
        warn!(
            volatile_memory_bytes = write_ack.volatile_memory_bytes,
            volatile_max_operations = write_ack.volatile_max_operations,
            "volatile-memory write acknowledgement is enabled: ordinary writes are unsafe across process or power loss until a flush barrier completes"
        );
    }
    let maintenance_runtime = if db_mode.is_read_only() {
        None
    } else {
        Some(shared_maintenance_runtime().clone())
    };

    crate::telemetry::send_startup_event(&settings);

    let init_result = crate::cli::init::initialize_filesystem(&settings, db_mode).await?;
    let writeback_for_shutdown = init_result.writeback.clone();
    let writeback_for_metrics = init_result.writeback.clone();
    let writeback_for_checkpoints = init_result.writeback.clone();
    let writeback_for_lifecycle = writeback_for_shutdown.clone();
    let sftp_pool = init_result.sftp_pool.clone();
    let sftp_pool_for_close = sftp_pool.clone();
    let lifecycle_completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lifecycle_completed_after = std::sync::Arc::clone(&lifecycle_completed);
    let server_result: anyhow::Result<()> = async move {
        let fs = init_result.fs;
        let authority = init_result.authority;
        let leadership_deposed = authority
            .as_ref()
            .map_or_else(CancellationToken::new, |authority| authority.loss_token());
        let shutdown = leadership_deposed.child_token();
        let p9_accepted_work = P9AcceptedWorkTracker::new();

        // Do not start listeners after authority was revoked during initialization.
        if leadership_deposed.is_cancelled() {
            return Err(leadership_lost_error());
        }

        if !db_mode.is_read_only() && settings.servers.nbd.is_some() {
            ensure_nbd_directory(&fs).await?;
        }

        // Register the only fallible signal source before starting any
        // background object-store consumers.
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

        let telemetry_handle = crate::telemetry::start_periodic_reporting(
            &settings,
            Arc::clone(&fs.global_stats),
            shutdown.clone(),
        );

        let prometheus_handles = if let Some(ref prometheus_config) = settings.prometheus {
            let slatedb_registry = fs.db.slatedb_metrics();
            crate::prometheus::start(
                prometheus_config,
                crate::prometheus::CollectorSources {
                    fs_stats: Arc::clone(&fs.stats),
                    global_stats: Arc::clone(&fs.global_stats),
                    segment_gc_stats: fs.extent_store.segment_gc_stats(),
                    dedup: Arc::clone(&fs.dedup),
                    cache_metrics: init_result.cache_metrics.clone(),
                    slatedb_registry,
                    writeback: writeback_for_metrics,
                },
                shutdown.clone(),
            )
        } else {
            Vec::new()
        };

        // Metadata compaction digest: at most one line per interval, only when
        // compaction ran, plus a crossing-only L0 backlog warning. Summarizes the
        // engine's per-compaction lines, which the default filter drops.
        // Read-write mode only: readers run no compaction.
        let digest_handle = match (fs.db.slatedb_metrics(), fs.db.subscribe_status()) {
            (Some(recorder), Some(status)) => Some(crate::metadata_digest::spawn(
                recorder,
                status,
                settings
                    .lsm
                    .map(|c| c.l0_max_ssts())
                    .unwrap_or(crate::config::LsmConfig::DEFAULT_L0_MAX_SSTS),
                shutdown.clone(),
            )),
            _ => None,
        };

        fs.install_volatile_overlay();

        let nfs_handles = start_nfs_servers(
            Arc::clone(&fs),
            settings.servers.nfs.as_ref(),
            shutdown.clone(),
        )
        .await;

        let ninep_handles = start_ninep_servers(
            Arc::clone(&fs),
            settings.servers.ninep.as_ref(),
            shutdown.clone(),
            p9_accepted_work.clone(),
        );

        let (nbd_handles, nbd_runtime_registry) = start_nbd_servers(
            Arc::clone(&fs),
            settings.servers.nbd.as_ref(),
            shutdown.clone(),
        )
        .await?;

        // A read-only admin over the same store for the GC's checkpoint gate; built
        // before the store/path are moved into the checkpoint manager below.
        let gc_admin = if !db_mode.is_read_only() {
            Some(
                AdminBuilder::new(
                    slatedb::object_store::path::Path::from(init_result.db_path.clone()),
                    Arc::clone(&init_result.object_store),
                )
                .build(),
            )
        } else {
            None
        };

        let checkpoint_manager = Arc::new(CheckpointManager::new(
            init_result.db_handle,
            slatedb::object_store::path::Path::from(init_result.db_path),
            init_result.object_store,
            init_result.wal_object_store.clone(),
        ));
        if let Some(writeback) = writeback_for_checkpoints {
            checkpoint_manager.set_post_mutation_durability(Arc::new(move || {
                let writeback = writeback.clone();
                Box::pin(async move {
                    writeback
                        .wait_local_through_accepted()
                        .await
                        .map_err(|error| anyhow::anyhow!("writeback local barrier failed: {error}"))
                })
            }));
        }
        // Checkpoints must not durably publish a FrameLoc whose segment is still in
        // the RAM open buffer: seal + flush under the barrier first (see
        // CheckpointManager::create_checkpoint). Read-only mode has no writer to seal.
        if !db_mode.is_read_only() {
            let fc = fs.flush_coordinator.clone();
            checkpoint_manager.set_pre_flush(Arc::new(move || {
                let fc = fc.clone();
                Box::pin(async move {
                    fc.flush()
                        .await
                        .map_err(|e| anyhow::anyhow!("seal+flush failed: {:?}", e))
                })
            }));
        }
        #[cfg(feature = "webui")]
        let checkpoint_manager_for_webui = Arc::clone(&checkpoint_manager);
        let protect_nbd_exports = nbd_runtime_registry.is_some();
        let rpc_handles = start_rpc_servers(
            settings.servers.rpc.as_ref(),
            checkpoint_manager,
            Arc::clone(&fs),
            shutdown.clone(),
            protect_nbd_exports,
        )
        .await;

        // Keep the metadata block cache warm so the first wave of reads (and the
        // reads right after every compaction, which replaces meta SSTs with cold
        // ones) doesn't serialize on object-store GETs of filters/indexes. Read-only
        // opens get no block cache (see `open_database`), so `subscribe_status`
        // returns `None` and warming is skipped there.
        let warm_metadata_handle = if settings.cache.warm_metadata
            != crate::config::WarmMetadata::Off
            && let Some(status) = fs.db.subscribe_status()
        {
            let fs = Arc::clone(&fs);
            let warm_data = settings.cache.warm_metadata == crate::config::WarmMetadata::Full;
            let shutdown = shutdown.clone();
            let warm = async move {
                fs.db.warm_metadata_watch(warm_data, status, shutdown).await;
            };
            Some(match &maintenance_runtime {
                Some(handle) => handle.spawn(warm),
                None => tokio::spawn(warm),
            })
        } else {
            None
        };

        let gc_handle = if !db_mode.is_read_only() {
            let tuning = crate::fs::gc::GcTuning::from(settings.gc.unwrap_or_default());
            let gc = Arc::new(GarbageCollector::new(
                Arc::clone(&fs.db),
                fs.tombstone_store.clone(),
                fs.extent_store.clone(),
                Arc::clone(&fs.stats),
                gc_admin,
                tuning,
            ));
            Some(gc.start(shutdown.clone(), maintenance_runtime.clone()))
        } else {
            None
        };
        let stats_handle = start_stats_reporting(Arc::clone(&fs), shutdown.clone());
        let flush_handle = if !db_mode.is_read_only() {
            let flush_interval_secs = settings
                .lsm
                .map(|c| c.flush_interval_secs())
                .unwrap_or(crate::config::LsmConfig::DEFAULT_FLUSH_INTERVAL_SECS);
            Some(start_periodic_flush(
                Arc::clone(&fs),
                flush_interval_secs,
                shutdown.clone(),
            ))
        } else {
            None
        };

        #[cfg(feature = "webui")]
        let webui_handles = if let Some(ref webui_config) = settings.servers.webui {
            let webui_rpc_service = crate::rpc::server::AdminRpcServer::new(
                checkpoint_manager_for_webui,
                Arc::clone(&fs),
                shutdown.clone(),
            )
            .with_nbd_export_protection(protect_nbd_exports);
            let webui_lock_manager = Arc::new(crate::ninep::lock_manager::FileLockManager::new());
            crate::webui::start(
                webui_config,
                Arc::clone(&fs),
                webui_lock_manager,
                webui_rpc_service,
                shutdown.clone(),
                p9_accepted_work.clone(),
            )
        } else {
            Vec::new()
        };

        let mut server_handles = Vec::new();
        server_handles.extend(nfs_handles);
        server_handles.extend(ninep_handles);
        server_handles.extend(nbd_handles);
        server_handles.extend(rpc_handles);
        #[cfg(feature = "webui")]
        server_handles.extend(webui_handles);

        let mut server_handles: FuturesUnordered<_> = server_handles.into_iter().collect();
        assert!(
            !server_handles.is_empty(),
            "validated endpoint configuration must start at least one server listener"
        );

        let stop_cause = tokio::select! {
            biased;
            _ = leadership_deposed.cancelled() => {
                tracing::error!(
                    "HA: this serving runtime was fenced or superseded; stopping without flushing \
                     the stale database"
                );
                ServingStopCause::LeadershipLost
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, initiating graceful shutdown...");
                ServingStopCause::Signal
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown...");
                ServingStopCause::Signal
            }
            result = server_handles.next() => {
                let error = listener_exit_error(
                    result.expect("validated server handles cannot be empty"),
                    true,
                )
                .expect("an unexpected listener exit must be an error");
                tracing::error!(error = %error, "server listener stopped; initiating shutdown");
                ServingStopCause::ListenerFailure(error)
            }
        };

        info!("Cancelling all servers and background tasks...");
        shutdown.cancel();
        p9_accepted_work.stop_accepting();
        info!("Waiting for servers to exit...");
        let (stop_cause, serving_cleanup_errors) = drain_server_handles_for_stop(
            stop_cause,
            &mut server_handles,
            &leadership_deposed,
        )
        .await;
        info!("Waiting for accepted 9P work to settle...");
        p9_accepted_work.wait().await;

        if stop_cause.is_leadership_lost() {
            if let Some(registry) = &nbd_runtime_registry {
                registry.fence_abort();
                if registry.stop_and_drain().await.is_err() {
                    tracing::error!(
                        "volatile NBD workers reported a terminal error while joining after leadership loss"
                    );
                }
            }
            return finish_serving_shutdown(
                stop_cause,
                merge_cleanup_results(serving_cleanup_errors, Ok(())),
            );
        }

        let leadership_deposed_after_cleanup = leadership_deposed.clone();
        let cleanup_result: anyhow::Result<()> = async move {
            let mut volatile_cleanup_error = None;
            if let Some(registry) = &nbd_runtime_registry {
                if registry.stop_and_drain().await.is_err() {
                    volatile_cleanup_error =
                        Some(anyhow::anyhow!("volatile NBD materialization failed"));
                }
                if let Err(error) = fs.client_fsync().await {
                    let error = anyhow::anyhow!("volatile NBD final flush failed: {error}");
                    if volatile_cleanup_error.is_none() {
                        volatile_cleanup_error = Some(error);
                    } else {
                        tracing::error!(%error, "additional volatile NBD shutdown failure");
                    }
                }
            }
            info!("Waiting for background tasks to exit...");
            if let Some(gc_handles) = gc_handle {
                join_or_abort_tasks(
                    gc_handles,
                    std::time::Duration::from_secs(15),
                    |_| {
                        info!(
                            "GC tasks are still mid-pass after 15s; aborting them before final flush"
                        );
                    },
                )
                .await;
            }
            if let Some(mut handle) = warm_metadata_handle
                && tokio::time::timeout(std::time::Duration::from_secs(5), &mut handle)
                    .await
                    .is_err()
            {
                info!("metadata warming is still active after 5s; aborting it before final flush");
                handle.abort();
                let _ = handle.await;
            }
            let mut background_handles = vec![stats_handle];
            if let Some(flush_handle) = flush_handle {
                background_handles.push(flush_handle);
            }
            if let Some(handle) = telemetry_handle {
                background_handles.push(handle);
            }
            if let Some(handle) = digest_handle {
                background_handles.push(handle);
            }
            background_handles.extend(prometheus_handles);
            join_or_abort_background_tasks(
                background_handles,
                SFTP_FINAL_WORKER_ABORT_TIMEOUT,
            )
            .await;
            // Flush remains lease-gated while background tasks drain.
            if leadership_deposed.is_cancelled() {
                return Err(leadership_lost_error());
            }
            info!("Performing final flush and closing database...");
            let lifecycle = MutationLifecycle::new(LifecycleOwners::for_process(
                shutdown.clone(),
                mutation_lifecycle::DispatchedCalls::new(),
                Arc::clone(&fs),
                writeback_for_lifecycle.clone(),
                sftp_pool_for_close.clone(),
                db_mode.is_read_only(),
            ));
            let target = crate::fs::mutation::durability::DurabilityTarget::from(
                fs.write_ack.client_durability_target,
            );
            let deadline =
                tokio::time::Instant::now() + SFTP_FINAL_DATABASE_CLOSE_TIMEOUT.saturating_mul(2);
            tokio::select! {
                biased;
                _ = leadership_deposed.cancelled() => {
                    abort_final_flush_after_leadership_loss(&fs).await;
                    return Err(leadership_lost_error());
                }
                result = Arc::clone(&lifecycle).close(deadline, target) => {
                    let _receipt = result.map_err(|error| {
                        anyhow::anyhow!("unified writeback close failed: {error}")
                    })?;
                    lifecycle_completed.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }

            if leadership_deposed.is_cancelled() {
                return Err(leadership_lost_error());
            }

            // Retain authority monitors until the database is closed.
            if let Some(authority) = authority {
                tokio::time::timeout(
                    SERVER_AUTHORITY_FINISH_TIMEOUT,
                    authority.finish_after_close(),
                )
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "authority shutdown timed out after {}s",
                        SERVER_AUTHORITY_FINISH_TIMEOUT.as_secs()
                    )
                })?;
            }
            if leadership_deposed.is_cancelled() {
                return Err(leadership_lost_error());
            }

            if let Some(error) = volatile_cleanup_error {
                return Err(error);
            }

            Ok(())
        }
        .await;
        finish_serving_cleanup(
            stop_cause,
            serving_cleanup_errors,
            cleanup_result,
            leadership_deposed_after_cleanup.is_cancelled(),
        )
    }
    .await;

    let writeback_shutdown = if lifecycle_completed_after.load(std::sync::atomic::Ordering::SeqCst)
    {
        Ok(())
    } else {
        match writeback_for_shutdown {
            Some(writeback) => writeback
                .shutdown()
                .await
                .context("Failed to shut down persistent writeback"),
            None => Ok(()),
        }
    };
    let sftp_shutdown = if lifecycle_completed_after.load(std::sync::atomic::Ordering::SeqCst) {
        Ok(())
    } else {
        match sftp_pool {
            Some(pool) => {
                info!("Waiting for SFTP sessions and lifecycle tasks to exit...");
                pool.shutdown()
                    .await
                    .context("Failed to shut down SFTP session pool")
            }
            None => Ok(()),
        }
    };
    let result = finish_process_shutdown(server_result, writeback_shutdown, sftp_shutdown);
    if result.is_ok() {
        info!("Shutdown complete");
    }
    result
}

fn load_and_validate_server_settings(
    config_path: &std::path::Path,
) -> Result<(Settings, Option<super::memory_budget::MemoryBudgetReceipt>)> {
    let settings = Settings::from_file(config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    settings
        .servers
        .require_listener_endpoint()
        .context("Invalid [servers] configuration")?;
    let volatile_write_bytes = settings
        .servers
        .nbd
        .as_ref()
        .map(crate::config::NbdConfig::volatile_memory_bytes)
        .transpose()?
        .unwrap_or(0);
    let memory_budget =
        super::memory_budget::validate_server_startup(&settings, volatile_write_bytes)
            .context("Invalid startup memory budget")?;
    Ok((settings, memory_budget))
}

#[cfg(test)]
mod tests {
    use super::*;

    enum ReaderModeUnderTest {
        ReadOnly,
        Checkpoint,
    }

    fn mode_cache_probe_key() -> bytes::Bytes {
        crate::fs::key_codec::KeyCodec::new().inode_key(1)
    }

    async fn seeded_mode_cache_store() -> (
        Arc<dyn object_store::ObjectStore>,
        Arc<dyn BlockTransformer>,
        uuid::Uuid,
    ) {
        use slatedb::config::{CheckpointOptions, CheckpointScope, PutOptions, WriteOptions};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let transformer: Arc<dyn BlockTransformer> =
            crate::block_transformer::ZeroFsBlockTransformer::new_arc(
                &[7; 32],
                crate::config::CompressionConfig::default(),
            );
        let db = DbBuilder::new(Path::from("mode-cache-test"), store.clone())
            .with_settings(slatedb::config::Settings {
                wal_enabled: false,
                compactor_options: None,
                ..Default::default()
            })
            .with_sst_block_size(slatedb::SstBlockSize::Block32Kib)
            .with_block_transformer(transformer.clone())
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .expect("seed database");
        db.put_with_options(
            mode_cache_probe_key(),
            vec![3; 64 * 1024],
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .expect("write probe");
        db.flush().await.expect("flush probe");
        let checkpoint = db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await
            .expect("create checkpoint");
        db.close().await.expect("close seed database");

        (store, transformer, checkpoint.id)
    }

    async fn assert_mode_exports_attached_decoded_cache(mode: ReaderModeUnderTest) {
        let (store, transformer, checkpoint_id) = seeded_mode_cache_store().await;
        let mode = match mode {
            ReaderModeUnderTest::ReadOnly => DatabaseMode::ReadOnly,
            ReaderModeUnderTest::Checkpoint => DatabaseMode::Checkpoint(checkpoint_id),
        };
        let cache_root = tempfile::tempdir().expect("cache root");
        let opened = build_slatedb(
            store,
            &crate::fs::CacheConfig {
                root_folder: cache_root.path().to_owned(),
                max_cache_size_gb: 0.0,
                memory_cache_size_gb: Some(0.064),
            },
            "mode-cache-test".to_owned(),
            mode,
            None,
            transformer,
            None,
            None,
        )
        .await
        .expect("open reader mode");
        let reader = match &opened.data {
            SlateDbHandle::ReadOnly(reader) => reader.load_full(),
            SlateDbHandle::ReadWrite(_) => panic!("reader mode opened a writer"),
        };

        assert_eq!(
            reader
                .get(mode_cache_probe_key())
                .await
                .expect("read probe"),
            Some(bytes::Bytes::from(vec![3; 64 * 1024]))
        );
        let snapshot = opened.cache_metrics.snapshot();
        assert!(
            snapshot.decoded_blocks.entries > 0,
            "decoded-block metrics must observe the cache used by the reader: {snapshot:?}"
        );

        reader.close().await.expect("close reader");
        opened.parts_cache.close().await.expect("close parts cache");
    }

    #[tokio::test]
    async fn read_only_open_exports_its_attached_decoded_cache() {
        assert_mode_exports_attached_decoded_cache(ReaderModeUnderTest::ReadOnly).await;
    }

    #[tokio::test]
    async fn checkpoint_open_exports_its_attached_decoded_cache() {
        assert_mode_exports_attached_decoded_cache(ReaderModeUnderTest::Checkpoint).await;
    }

    #[tokio::test]
    async fn validated_rss_cap_survives_startup_and_cache_build_selection() {
        struct ResetRss;
        impl Drop for ResetRss {
            fn drop(&mut self) {
                crate::alloc_rss::set_test_rss_envelope(None);
                crate::alloc_rss::set_rss_cap_bytes(0);
            }
        }

        let _rss_cap_guard = crate::alloc_rss::lock_test_rss_cap().await;
        let _reset = ResetRss;
        let gib = 1024 * 1024 * 1024;
        let validated_service_cap = 56 * gib;
        let configured_clean_cache = 64 * gib;
        let receipt = crate::cli::memory_budget::MemoryBudgetReceipt {
            hard_limit_bytes: 96 * gib,
            required_bytes: 88 * gib,
            remaining_bytes: 8 * gib,
            rss_pressure_cap_bytes: validated_service_cap,
        };

        crate::alloc_rss::set_rss_cap_bytes(0);
        install_validated_rss_cap(Some(&receipt));
        let installed_cap = crate::alloc_rss::rss_cap_bytes();
        assert_eq!(
            installed_cap, validated_service_cap,
            "server startup must install the validated receipt cap"
        );

        assert_eq!(
            select_rss_pressure_cap(installed_cap, configured_clean_cache),
            validated_service_cap,
            "build_slatedb must preserve the startup-validated service envelope"
        );
        assert_eq!(
            select_rss_pressure_cap(0, configured_clean_cache),
            configured_clean_cache,
            "standalone callers without a validated envelope retain the cache fallback"
        );

        crate::alloc_rss::set_test_rss_envelope(Some(validated_service_cap));
        assert!(
            crate::alloc_rss::over_rss_cap(),
            "GC's process-wide brake must consume the installed receipt cap"
        );
        assert!(
            crate::alloc_rss::over_rss_cap_of(select_rss_pressure_cap(
                installed_cap,
                configured_clean_cache,
            )),
            "prefetch admission must consume the same selected cap"
        );
    }

    #[test]
    fn unsafe_budget_fails_before_the_file_backend_is_opened() {
        let temp = tempfile::tempdir().unwrap();
        let backend = temp.path().join("backend-must-not-exist");
        let config = temp.path().join("zerofs.toml");
        std::fs::write(
            &config,
            format!(
                r#"[cache]
dir = {cache:?}
disk_size_gb = 1.0
memory_size_gb = 64.0

[runtime]
memory_limit_gb = 96.0

[storage]
url = {storage:?}
encryption_password = "test-password"

[servers.nfs]
addresses = ["127.0.0.1:20490"]
"#,
                cache = temp.path().join("cache").display().to_string(),
                storage = format!("file://{}", backend.display()),
            ),
        )
        .unwrap();

        let error = load_and_validate_server_settings(&config).unwrap_err();

        assert!(format!("{error:#}").contains("Invalid startup memory budget"));
        assert!(
            !backend.exists(),
            "memory guard reached backend initialization"
        );
    }

    #[test]
    fn volatile_nbd_ack_rejects_read_only_database_modes() {
        let config = NbdConfig {
            addresses: Some(std::collections::HashSet::new()),
            unix_socket: None,
            write_ack_mode: crate::config::NbdWriteAckMode::VolatileMemory,
            volatile_memory_gb: 1.0,
        };

        let error = validate_nbd_database_mode(Some(&config), DatabaseMode::ReadOnly).unwrap_err();
        assert!(
            error.to_string().contains("read-only / checkpoint"),
            "unexpected error: {error:#}"
        );
        assert!(validate_nbd_database_mode(Some(&config), DatabaseMode::ReadWrite).is_ok());
    }

    #[test]
    fn volatile_filesystem_ack_rejects_read_only_database_modes() {
        let settings: Settings = toml::from_str(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nfs]
addresses = ["127.0.0.1:2049"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
"#,
        )
        .unwrap();

        for db_mode in [
            DatabaseMode::ReadOnly,
            DatabaseMode::Checkpoint(uuid::Uuid::nil()),
        ] {
            let error = settings
                .filesystem_write_ack_settings(write_ack_access_mode(db_mode))
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("read-write"),
                "unexpected error: {error:#}"
            );
        }
        settings
            .filesystem_write_ack_settings(write_ack_access_mode(DatabaseMode::ReadWrite))
            .unwrap();
    }

    #[test]
    fn nbd_uses_resolved_filesystem_write_ack_budget() {
        let settings: Settings = toml::from_str(
            r#"
[cache]
dir = "/tmp/cache"
disk_size_gb = 1.0

[storage]
url = "file:///tmp/data"
encryption_password = "test"

[servers.nbd]
addresses = ["127.0.0.1:10809"]

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 2.0

[writeback]
enabled = true
dir = "/var/cache/zerofs-writeback"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0
"#,
        )
        .unwrap();
        let write_ack = settings
            .filesystem_write_ack_settings(write_ack_access_mode(DatabaseMode::ReadWrite))
            .unwrap();
        assert_eq!(
            nbd_volatile_budget(write_ack),
            write_ack.volatile_memory_bytes
        );
        assert!(nbd_volatile_budget(write_ack) > 0);
        let nbd = settings.servers.nbd.as_ref().unwrap();
        assert_eq!(
            nbd.write_ack_mode,
            crate::config::NbdWriteAckMode::Materialized
        );
        assert_eq!(nbd.volatile_memory_bytes().unwrap(), 0);
    }

    #[test]
    fn listener_completion_before_shutdown_is_an_error() {
        let error = listener_exit_error(Ok(Ok(())), true).unwrap();
        assert!(
            error.to_string().contains("exited unexpectedly"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn listener_completion_after_shutdown_is_normal() {
        assert!(listener_exit_error(Ok(Ok(())), false).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn listener_failure_aborts_and_joins_a_stuck_sibling_after_grace() {
        let failed = tokio::spawn(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                "primary listener failure",
            ))
        });
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        // Models an RPC/NFS-style serving future whose transport drain never
        // resolves after the shared cancellation token fires.
        let stuck_rpc_listener = tokio::spawn(async move {
            let _alive = alive_tx;
            std::future::pending::<std::io::Result<()>>().await
        });
        let mut handles: FuturesUnordered<_> = [failed, stuck_rpc_listener].into_iter().collect();

        let first = handles.next().await.unwrap();
        let primary = listener_exit_error(first, true).unwrap();

        let started = tokio::time::Instant::now();
        let (cause, cleanup) = drain_server_handles_for_stop(
            ServingStopCause::ListenerFailure(primary),
            &mut handles,
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(
            started.elapsed(),
            crate::replication::RESPONSE_DRAIN_TIMEOUT
        );
        assert_eq!(cleanup.len(), 1, "missing listener-timeout context");
        assert!(cleanup[0].to_string().contains("shutdown exceeded"));
        let sibling_dropped = tokio::time::timeout(Duration::from_secs(1), alive_rx).await;
        assert!(
            matches!(sibling_dropped, Ok(Err(_))),
            "stuck sibling listener was not aborted and joined"
        );
        let message = format!(
            "{:#}",
            finish_serving_shutdown(cause, merge_cleanup_results(cleanup, Ok(()))).unwrap_err()
        );
        assert!(
            message.contains("primary listener failure"),
            "unexpected primary error: {message}"
        );
    }

    #[tokio::test]
    async fn panicking_listener_is_reported_as_a_listener_task_failure() {
        let result = tokio::spawn(async {
            panic!("listener panic");
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        })
        .await;

        let error = listener_exit_error(result, true).unwrap();
        let message = format!("{error:#}");
        assert!(
            message.contains("server listener task failed"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("listener panic"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn listener_failure_stays_primary_when_database_cleanup_fails() {
        let result = finish_serving_shutdown(
            ServingStopCause::ListenerFailure(anyhow::anyhow!("primary listener failure")),
            Err(anyhow::anyhow!("database close failure")),
        );

        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.starts_with("primary listener failure"),
            "listener failure was masked: {message}"
        );
        assert!(
            message.contains("database close failure"),
            "lost cleanup error: {message}"
        );
    }

    #[test]
    fn listener_failure_stays_primary_when_outer_cleanup_fails() {
        let result = finish_process_shutdown(
            Err(anyhow::anyhow!("primary listener failure")),
            Err(anyhow::anyhow!("writeback failure")),
            Err(anyhow::anyhow!("SFTP failure")),
        );

        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.starts_with("primary listener failure"),
            "listener failure was masked: {message}"
        );
        assert!(
            message.contains("writeback failure"),
            "lost cleanup error: {message}"
        );
        assert!(
            message.contains("SFTP failure"),
            "lost cleanup error: {message}"
        );
    }

    #[tokio::test]
    async fn signal_shutdown_remains_successful_after_clean_listener_drain() {
        let mut handles: FuturesUnordered<_> =
            [tokio::spawn(async { Ok::<(), std::io::Error>(()) })]
                .into_iter()
                .collect();

        let (cause, cleanup) = drain_server_handles_for_stop(
            ServingStopCause::Signal,
            &mut handles,
            &CancellationToken::new(),
        )
        .await;

        finish_serving_shutdown(cause, merge_cleanup_results(cleanup, Ok(()))).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn signal_shutdown_reports_a_stuck_listener_after_grace() {
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        let stuck = tokio::spawn(async move {
            let _alive = alive_tx;
            std::future::pending::<std::io::Result<()>>().await
        });
        let mut handles: FuturesUnordered<_> = [stuck].into_iter().collect();

        let started = tokio::time::Instant::now();
        let drained = tokio::time::timeout(
            crate::replication::RESPONSE_DRAIN_TIMEOUT + Duration::from_secs(1),
            drain_server_handles_for_stop(
                ServingStopCause::Signal,
                &mut handles,
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("signal listener drain exceeded its bounded grace");

        assert_eq!(
            started.elapsed(),
            crate::replication::RESPONSE_DRAIN_TIMEOUT
        );
        assert!(handles.is_empty(), "listener handle was not joined");
        assert!(alive_rx.await.is_err(), "stuck listener was not aborted");
        let error = finish_serving_shutdown(drained.0, merge_cleanup_results(drained.1, Ok(())))
            .unwrap_err();
        assert!(error.to_string().contains("shutdown exceeded"));
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_during_signal_drain_preserves_consumed_listener_failure() {
        let failed = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err(std::io::Error::other("listener failed before fencing"))
        });
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        let stuck = tokio::spawn(async move {
            let _alive = alive_tx;
            std::future::pending::<std::io::Result<()>>().await
        });
        let mut handles: FuturesUnordered<_> = [failed, stuck].into_iter().collect();
        let leadership_deposed = CancellationToken::new();
        let cancel_leadership = leadership_deposed.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel_leadership.cancel();
        });

        let (cause, cleanup) = drain_server_handles_for_stop(
            ServingStopCause::Signal,
            &mut handles,
            &leadership_deposed,
        )
        .await;
        let message = format!(
            "{:#}",
            finish_serving_shutdown(cause, merge_cleanup_results(cleanup, Ok(()))).unwrap_err()
        );

        assert!(
            message.starts_with("HA writer was fenced or superseded"),
            "leadership loss was not primary: {message}"
        );
        assert!(
            message.contains("listener failed before fencing"),
            "consumed listener failure was lost: {message}"
        );
        assert!(alive_rx.await.is_err(), "stuck listener was not joined");
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_lost_during_cleanup_promotes_over_listener_failure() {
        let failed = tokio::spawn(async {
            Err(std::io::Error::other("listener failed during signal drain"))
        });
        let mut handles: FuturesUnordered<_> = [failed].into_iter().collect();
        let leadership_deposed = CancellationToken::new();
        let (cause, cleanup_errors) = drain_server_handles_for_stop(
            ServingStopCause::Signal,
            &mut handles,
            &leadership_deposed,
        )
        .await;

        let cancel_leadership = leadership_deposed.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            cancel_leadership.cancel();
        });
        let cleanup_result = async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        }
        .await;

        let message = format!(
            "{:#}",
            finish_serving_cleanup(
                cause,
                cleanup_errors,
                cleanup_result,
                leadership_deposed.is_cancelled(),
            )
            .unwrap_err()
        );
        assert!(
            message.starts_with("HA writer was fenced or superseded"),
            "leadership loss was not primary: {message}"
        );
        assert!(
            message.contains("listener failed during signal drain"),
            "listener failure was not retained as cleanup context: {message}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn leadership_loss_aborts_and_joins_a_stuck_listener_after_grace() {
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        let stuck = tokio::spawn(async move {
            let _alive = alive_tx;
            std::future::pending::<std::io::Result<()>>().await
        });
        let mut handles: FuturesUnordered<_> = [stuck].into_iter().collect();

        let started = tokio::time::Instant::now();
        let (cause, cleanup) = drain_server_handles_for_stop(
            ServingStopCause::LeadershipLost,
            &mut handles,
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(
            started.elapsed(),
            crate::replication::RESPONSE_DRAIN_TIMEOUT
        );
        assert_eq!(cleanup.len(), 1, "missing listener-timeout context");
        assert!(alive_rx.await.is_err(), "stuck listener was not joined");
        let message = format!("{:#}", finish_serving_shutdown(cause, Ok(())).unwrap_err());
        assert!(message.starts_with("HA writer was fenced or superseded"));
    }

    #[test]
    fn leadership_loss_stays_primary_when_cleanup_also_fails() {
        let result = finish_serving_shutdown(
            ServingStopCause::LeadershipLost,
            Err(anyhow::anyhow!("cleanup failure")),
        );

        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.starts_with("HA writer was fenced or superseded"),
            "leadership failure was masked: {message}"
        );
        assert!(
            message.contains("cleanup failure"),
            "lost cleanup error: {message}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn final_drain_aborts_a_stuck_background_caller() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _alive = alive_tx;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();

        let started = tokio::time::Instant::now();
        join_or_abort_background_tasks(vec![handle], std::time::Duration::from_secs(5)).await;

        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        assert!(
            alive_rx.await.is_err(),
            "stuck task was not aborted and joined"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn final_drain_handles_a_task_that_finishes_while_another_is_stuck() {
        let finished = tokio::spawn(async {});

        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        let stuck = tokio::spawn(async move {
            let _alive = alive_tx;
            std::future::pending::<()>().await;
        });

        join_or_abort_background_tasks(vec![finished, stuck], std::time::Duration::from_secs(5))
            .await;

        assert!(
            alive_rx.await.is_err(),
            "stuck task was not aborted and joined"
        );
    }

    #[test]
    fn barrier_controlled_flush_thresholds_are_valid() {
        let settings = slatedb::config::Settings {
            l0_sst_size_bytes: BARRIER_CONTROLLED_L0_SST_SIZE_BYTES,
            max_unflushed_bytes: BARRIER_CONTROLLED_MAX_UNFLUSHED_BYTES,
            ..Default::default()
        };

        settings
            .validate()
            .expect("barrier-controlled flush thresholds must satisfy SlateDB validation");
    }

    #[test]
    fn maintenance_runtime_is_shared_across_open_attempts() {
        let first = shared_maintenance_runtime();
        let second = shared_maintenance_runtime();
        assert!(std::ptr::eq(first, second));

        let (tx, rx) = std::sync::mpsc::channel();
        first.spawn(async move {
            tx.send(()).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("shared maintenance runtime did not execute a task");
    }

    #[test]
    fn split_disk_budget_favors_segments() {
        let gib = 1024 * 1024 * 1024;
        // 10% to metadata, the rest to segments.
        assert_eq!(split_disk_budget(100 * gib), (90 * gib, 10 * gib));
        // Metadata capped at 16 GiB on a huge budget; segments get everything else.
        assert_eq!(split_disk_budget(4096 * gib), (4080 * gib, 16 * gib));
        // Tiny budgets still floor each side at 1 GiB.
        assert_eq!(split_disk_budget(gib / 2), (gib, gib));
    }

    #[test]
    fn parts_engine_knobs_scale_with_device() {
        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        let large = parts_engine_knobs(583 * gib);
        assert_eq!(large.flushers, 4);
        assert_eq!(large.clean_block_threshold, 8);
        assert_eq!(large.submit_queue_bytes, gib);
        assert_eq!(large.buffer_pool_bytes, 256 * mib);

        let floor = parts_engine_knobs(gib);
        assert_eq!(floor.flushers, 2);
        assert_eq!(floor.clean_block_threshold, 2);
        assert_eq!(floor.submit_queue_bytes, 256 * mib);
        assert_eq!(floor.buffer_pool_bytes, 128 * mib);

        // Degenerate device (below one block): everything at its floor.
        let tiny = parts_engine_knobs(mib);
        assert_eq!(tiny.flushers, 1);
        assert_eq!(tiny.clean_block_threshold, 1);
        assert_eq!(tiny.submit_queue_bytes, 16 * mib);
        assert_eq!(tiny.buffer_pool_bytes, 64 * mib);
    }

    #[test]
    fn split_memory_budget_favors_segments() {
        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        // 25% to metadata blocks, then split the data share evenly between
        // encrypted segment parts and decoded plaintext extents.
        assert_eq!(split_memory_budget(gib), (384 * mib, 256 * mib, 384 * mib));
        // Metadata caps at 2 GiB; large caches overwhelmingly serve data.
        assert_eq!(split_memory_budget(40 * gib), (19 * gib, 2 * gib, 19 * gib));
        // Tiny budgets floor all three clean-cache consumers at 32 MiB.
        assert_eq!(
            split_memory_budget(16 * mib),
            (32 * mib, 32 * mib, 32 * mib)
        );
    }

    // foyer builds the same `I/O error => coding error` wrapping around the os
    // error regardless of the build site, so exercise its own From<io::Error>.
    fn foyer_os_error(raw: i32) -> foyer::Error {
        std::io::Error::from_raw_os_error(raw).into()
    }

    #[test]
    fn emfile_is_detected_and_hinted() {
        let err = foyer_os_error(libc::EMFILE);
        assert!(is_fd_exhaustion(&err));
        let msg = foyer_build_error("foyer hybrid build failed", err).to_string();
        assert!(
            msg.contains("foyer hybrid build failed"),
            "lost context: {msg}"
        );
        assert!(msg.contains("ulimit -n"), "missing fd hint: {msg}");
    }

    #[test]
    fn enfile_is_detected() {
        assert!(is_fd_exhaustion(&foyer_os_error(libc::ENFILE)));
    }

    #[test]
    fn other_io_errors_get_no_hint() {
        let err = foyer_os_error(libc::ENOSPC);
        assert!(!is_fd_exhaustion(&err));
        let msg = foyer_build_error("foyer device build failed", err).to_string();
        assert!(!msg.contains("ulimit"), "spurious fd hint: {msg}");
    }

    mod warm_metadata {
        use super::*;
        use crate::fault_store::{FaultControls, FaultStore};
        use crate::fs::key_codec::KeyCodec;
        use bytes::Bytes;
        use object_store::ObjectStore;
        use slatedb::config::WriteOptions;
        use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
        use slatedb::{SstBlockSize, WriteBatch};
        use std::sync::Arc;

        const INODES: u64 = 8_000;
        // Sample keys spread across the keyspace so the cold reads touch many
        // distinct SST data blocks, not just one.
        const SAMPLE_STRIDE: u64 = 400;

        async fn hybrid(root: &std::path::Path) -> Arc<FoyerHybridCache> {
            build_block_hybrid(
                root,
                64 * 1024 * 1024,
                512 * 1024 * 1024,
                &tokio::runtime::Handle::current(),
                FoyerMetricsRegistry::default(),
            )
            .await
            .expect("foyer hybrid")
            .0
        }

        // Open a writer over `store` with the same segment/filter/block config the
        // server uses, so writes route into the `meta` segment exactly as in prod.
        async fn open(store: Arc<dyn ObjectStore>, cache: Arc<FoyerHybridCache>) -> slatedb::Db {
            // Small L0s so the 8k rows freeze into several SSTs, exercising the
            // warm fan-out over more than one SST.
            // No compactor: the 4 setup L0s meet the default compaction
            // threshold, and a background compaction racing into a measured
            // window charges its GETs there (and un-warms the cache by
            // swapping the manifest to fresh SSTs).
            let settings = slatedb::config::Settings {
                l0_sst_size_bytes: 64 * 1024,
                compactor_options: None,
                ..Default::default()
            };
            slatedb::DbBuilder::new(slatedb::object_store::path::Path::from("db"), store)
                .with_settings(settings)
                .with_db_cache(cache)
                .with_sst_block_size(SstBlockSize::Block32Kib)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .expect("open slatedb")
        }

        // Object-store GETs charged while reading the sample keys cold.
        async fn read_sample_gets(
            db: &crate::db::Db,
            codec: &KeyCodec,
            ctl: &FaultControls,
        ) -> usize {
            let before = ctl.get_count();
            let mut id = 0;
            while id < INODES {
                let v = db
                    .get_bytes(&codec.inode_key(id))
                    .await
                    .expect("get")
                    .expect("inode present");
                assert_eq!(v.len(), 64);
                id += SAMPLE_STRIDE;
            }
            ctl.get_count() - before
        }

        /// A cold `Db` (fresh foyer cache, all metadata on the object store)
        /// pays object-store GETs for SST filters/indexes/data on its first
        /// reads. `warm_metadata` pulls the `meta` segment into cache up front,
        /// so the same reads issue no object-store GETs. The bulk segment, which
        /// these keys don't touch, is irrelevant. Real foyer cache + real
        /// LocalFileSystem store; GETs counted by the FaultStore decorator.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn warm_eliminates_cold_metadata_gets() {
            let dir = tempfile::tempdir().unwrap();
            let store_root = dir.path().join("store");
            std::fs::create_dir_all(&store_root).unwrap();
            let local = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&store_root).unwrap(),
            );
            let (store, ctl) = FaultStore::new(local);
            let store: Arc<dyn ObjectStore> = store;
            let codec = KeyCodec::new();

            // Setup: write the metadata and persist it to SSTs, in several flushes
            // so the meta segment ends up with more than one SST.
            {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_setup")).await).await;
                for extent in 0..4u64 {
                    let mut batch = WriteBatch::new();
                    for i in 0..(INODES / 4) {
                        let id = extent * (INODES / 4) + i;
                        batch.put_bytes(codec.inode_key(id), Bytes::from(vec![id as u8; 64]));
                    }
                    raw.write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                    raw.flush().await.unwrap();
                }
                raw.close().await.unwrap();
            }

            // Cold read, no warm: reopen with a fresh cache and read the samples.
            let cold_gets = {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_cold")).await).await;
                let db = crate::db::Db::new(Arc::new(raw), None);
                let gets = read_sample_gets(&db, &codec, &ctl).await;
                db.close().await.unwrap();
                gets
            };

            // Cold read, warmed: reopen with a fresh cache, warm the meta segment,
            // then read the same samples.
            let (warm_gets, warm_second, warmed) = {
                let raw = open(store.clone(), hybrid(&dir.path().join("c_warm")).await).await;
                let db = crate::db::Db::new(Arc::new(raw), None);
                let warmed = db.warm_metadata(true).await;
                let gets = read_sample_gets(&db, &codec, &ctl).await;
                // A second pass over the same keys: warm + first-touch should have
                // left the whole metadata working set in cache.
                let second = read_sample_gets(&db, &codec, &ctl).await;
                db.close().await.unwrap();
                (gets, second, warmed)
            };

            assert!(
                warmed.ssts >= 2,
                "expected the meta segment to span several SSTs, got {}",
                warmed.ssts
            );
            assert_eq!(warmed.failed, 0, "warm should not fail any SST");
            assert!(
                cold_gets > 0,
                "cold reads must hit the object store, got {cold_gets}"
            );
            // Warming collapses the cold read cost by a wide margin. It is not
            // exactly zero: `warm_sst` reuses the manifest's SST handles and so
            // intentionally skips the per-SST footer `open_sst` GET the read path
            // still pays once on first access (~2 per SST), plus the foyer hybrid
            // cache's async disk tier can require an occasional re-fetch. So both
            // warmed passes must stay far below cold, not necessarily at zero.
            assert!(
                warm_gets * 2 <= cold_gets,
                "warming should cut cold GETs by a wide margin: cold={cold_gets} warm={warm_gets}"
            );
            assert!(
                warm_second * 2 <= cold_gets,
                "reads after warming must stay far below cold: cold={cold_gets} second={warm_second}"
            );
        }
    }
}
