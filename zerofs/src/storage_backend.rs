//! Build the configured storage graph and retain every transport's lifecycle.

pub(crate) mod raw;
#[cfg(target_os = "linux")]
mod raw_authority;

use crate::config::Settings;
use crate::metadata_fault::{LocalMetadataStore, MetadataFault};
use crate::operator_required::operator_required;
use crate::parse_object_store::{ObjectStoreScheme, ParsedStore, parse_url_opts};
use crate::pool_object_store::{CATALOG_FILE_NAME, PoolMember, PoolObjectStore};
use crate::sftp_transport::SftpSessionPool;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use futures::{StreamExt, stream::FuturesUnordered};
use object_store::{ObjectStore, ObjectStoreExt, path::Path, prefix::PrefixStore};
use sha2::{Digest, Sha256};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

/// A pool member failure affects that member, not independent local reads.
/// The old single-backend server retains its fatal-transport behavior.
#[derive(Clone, Debug)]
pub(crate) struct BackendSessions {
    pools: Vec<SftpSessionPool>,
    fatal: bool,
}

impl BackendSessions {
    fn single(pool: SftpSessionPool) -> Self {
        Self {
            pools: vec![pool],
            fatal: true,
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        for pool in &self.pools {
            pool.begin_shutdown();
        }
    }

    pub(crate) async fn wait_closed(&self) {
        if !self.fatal || self.pools.is_empty() {
            std::future::pending::<()>().await;
        }
        let mut closed = self
            .pools
            .iter()
            .map(SftpSessionPool::wait_closed)
            .collect::<FuturesUnordered<_>>();
        let _ = closed.next().await;
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        // Poll every shutdown even if one fails: another member may own live
        // sessions and cancellation cleanup that must still be drained.
        let results =
            futures::future::join_all(self.pools.iter().map(SftpSessionPool::shutdown)).await;
        let failures = results
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            anyhow::bail!("SFTP backend cleanup failed: {}", failures.join("; "));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct OpenedStorage {
    pub(crate) store: Box<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) raw_journal: Option<zerofs_raw_device::journal_region::TransactionRegion>,
    pub(crate) sftp_pool: Option<BackendSessions>,
    pub(crate) pool: Option<PoolObjectStore>,
    /// Trips on the first non-transient local I/O error of the metadata
    /// store. It never trips for a remote metadata store.
    pub(crate) metadata_fault: MetadataFault,
}

/// A `file://` metadata store reports its local I/O failures through
/// [`LocalMetadataStore`]; remote metadata stores keep their own classification.
fn guard_local_metadata(
    url: &url::Url,
    store: Box<dyn ObjectStore>,
    fault: &MetadataFault,
) -> Box<dyn ObjectStore> {
    if !matches!(url.scheme(), "file" | "raw+p3-v2") {
        return store;
    }
    #[cfg(test)]
    let store = crate::fault_store::native_errno::wrap(store);
    Box::new(LocalMetadataStore::new(Arc::from(store), fault.clone()))
}

pub(crate) async fn open_storage(settings: &Settings) -> Result<OpenedStorage> {
    if settings.storage.pool.is_some() {
        return open_pool_storage(settings, false)
            .await
            .map(|(opened, _)| opened);
    }
    let url: url::Url = settings.storage.url.parse()?;
    let ParsedStore {
        store,
        path,
        sftp_pool,
    } = parse_url_opts(
        &url,
        settings.cloud_provider_env_vars(),
        settings.sftp.as_ref(),
    )
    .await?;
    let metadata_fault = MetadataFault::default();
    let store = guard_local_metadata(&url, store, &metadata_fault);
    if url.scheme() == "file" {
        let marker = path.clone().join(".zerofs-pool").join("identity");
        match store.head(&marker).await {
            Ok(_) => {
                return Err(operator_required(anyhow::anyhow!(
                    "this volume has a permanent storage pool; restore [storage.pool] and its original catalog instead of opening the metadata directory alone"
                )));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(OpenedStorage {
        store,
        path,
        sftp_pool: sftp_pool.map(BackendSessions::single),
        pool: None,
        raw_journal: None,
        metadata_fault,
    })
}

/// Explicit initialization is used only by `zerofs pool init`. A regular
/// server or maintenance command must never recreate a lost placement catalog.
pub(crate) async fn open_pool_storage(
    settings: &Settings,
    initialize: bool,
) -> Result<(OpenedStorage, PoolObjectStore)> {
    let config = settings
        .storage
        .pool
        .as_ref()
        .context("[storage.pool] is not configured")?;
    settings.validate()?;
    crate::storage_readiness::verify(settings)?;
    validate_directories(settings)?;
    let authority_url: url::Url = settings.storage.url.parse()?;
    let (authority, catalog_source, raw_journal) = if authority_url.scheme() == "raw+p3-v2" {
        let opened = raw::open_p3(raw::RawBinding::parse(&authority_url)?).await?;
        let journal = if settings
            .writeback
            .as_ref()
            .is_some_and(|config| config.enabled)
        {
            Some(opened.journal)
        } else {
            anyhow::ensure!(
                opened.journal.is_empty()?,
                "raw P3 already contains a writeback database; reopen with the original writeback configuration"
            );
            None
        };
        (opened.authority, opened.catalog, journal)
    } else {
        crate::storage_readiness::verify_open_paths(
            settings,
            &[
                (
                    crate::storage_readiness::RoleName::Catalog,
                    config.catalog_dir.join(CATALOG_FILE_NAME),
                ),
                (
                    crate::storage_readiness::RoleName::Catalog,
                    config.catalog_dir.join("pool.lock"),
                ),
            ],
        )?;
        if !initialize && !config.catalog_dir.join(CATALOG_FILE_NAME).is_file() {
            return Err(operator_required(anyhow::anyhow!(
                "pool catalog is missing: initialize a new pool with `zerofs pool init`, or restore an existing pool's catalog; normal startup never recreates it"
            )));
        }
        let parsed =
            parse_url_opts(&authority_url, settings.cloud_provider_env_vars(), None).await?;
        (
            bind_readiness_root(settings, &authority_url, parsed)?,
            config.catalog_dir.clone().into(),
            None,
        )
    };
    let ParsedStore {
        store: authority,
        path,
        sftp_pool: authority_sessions,
    } = authority;
    let metadata_fault = MetadataFault::default();
    let authority = guard_local_metadata(&authority_url, authority, &metadata_fault);
    debug_assert!(authority_sessions.is_none(), "pool metadata must be local");
    let mut sessions = BackendSessions {
        pools: Vec::new(),
        fatal: false,
    };
    let result: Result<PoolObjectStore> = async {
        let mut members = Vec::with_capacity(config.members.len());
        for member in &config.members {
            let url: url::Url = member.url.parse()?;
            if url.scheme() == "raw+loop" {
                let binding = raw::RawBinding::parse(&url)?;
                members.push(
                    raw::open_member(member, binding, settings.raw_device_p3_candidate()).await?,
                );
                continue;
            }
            let (scheme, member_path) = ObjectStoreScheme::parse(&url)?;
            let mut options = settings.cloud_provider_env_vars();
            options.extend(member.cloud_provider_env_vars());
            let identity = member_identity(&url, &options)?;
            let parsed = if scheme == ObjectStoreScheme::Sftp {
                crate::parse_object_store::build_sftp_store_with_warmup(
                    &url,
                    member_path,
                    member.sftp.as_ref(),
                    false,
                )
                .await?
            } else {
                parse_url_opts(&url, options, None).await?
            };
            let parsed = bind_readiness_root(settings, &url, parsed)?;
            #[cfg(test)]
            let parsed = match scheme {
                ObjectStoreScheme::Local => ParsedStore {
                    store: crate::fault_store::native_errno::wrap(parsed.store),
                    ..parsed
                },
                _ => parsed,
            };
            if let Some(pool) = parsed.sftp_pool {
                sessions.pools.push(pool);
            }
            let store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(parsed.store, parsed.path));
            let store = with_storage_class(store, member.storage_class.as_deref());
            members.push(PoolMember {
                id: member.id.clone(),
                identity,
                store,
                capacity_bytes: member.capacity_bytes()?,
                local_path: if scheme == ObjectStoreScheme::Local {
                    Some(local_directory(&url)?)
                } else {
                    None
                },
                min_free_bytes: member.reserve_free_bytes()?,
                health_device: member.health_device.clone(),
                free_space: None,
            });
        }
        Ok(PoolObjectStore::open_with_preflight(
            Arc::from(authority),
            path.clone(),
            catalog_source,
            members,
            config.segment_placement,
            initialize,
            |pool_id| crate::storage_readiness::verify_pool_paths(settings, pool_id),
        )
        .await?)
    }
    .await;
    match result {
        Ok(pool) => Ok((
            OpenedStorage {
                pool: Some(pool.clone()),
                raw_journal,
                store: Box::new(pool.clone()),
                path,
                sftp_pool: (!sessions.pools.is_empty()).then_some(sessions),
                metadata_fault,
            },
            pool,
        )),
        Err(error) => match sessions.shutdown().await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(crate::cli::attach_cleanup_errors(error, vec![cleanup])),
        },
    }
}

fn bind_readiness_root(
    settings: &Settings,
    url: &url::Url,
    mut parsed: ParsedStore,
) -> Result<ParsedStore> {
    if settings.storage.readiness.is_some() {
        let root = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid local storage URL"))?;
        parsed.store = Box::new(
            crate::local_object_store::LocalConditionalStore::new().with_readiness_root(root),
        );
    }
    Ok(parsed)
}

fn local_directory(url: &url::Url) -> Result<PathBuf> {
    let path = url
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("invalid local storage URL"))?;
    if !path.is_dir() {
        anyhow::bail!(
            "local pool directory {} must already exist on its mounted disk",
            path.display()
        );
    }
    path.canonicalize()
        .with_context(|| format!("cannot resolve local directory {}", path.display()))
}

/// Resolve existing symlinks even when the final catalog/cache directory has
/// not been created yet. Lexical-only overlap checks miss symlink aliases.
fn resolved_path(path: &FsPath) -> Result<PathBuf> {
    if !path.is_absolute() {
        anyhow::bail!(
            "pool and cache directories must be absolute: {}",
            path.display()
        );
    }
    if path
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        anyhow::bail!("pool and cache paths must not contain parent components");
    }
    let mut ancestor = path;
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        suffix.push(
            ancestor
                .file_name()
                .context("path has no existing ancestor")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("path has no existing ancestor")?;
    }
    let mut result = ancestor.canonicalize()?;
    for component in suffix.into_iter().rev() {
        result.push(component);
    }
    Ok(result)
}

fn validate_directories(settings: &Settings) -> Result<()> {
    let config = settings
        .storage
        .pool
        .as_ref()
        .context("pool is not configured")?;
    let mut roots = vec![(
        "clean cache".to_owned(),
        resolved_path(&settings.cache.dir)?,
    )];
    let authority_url: url::Url = settings.storage.url.parse()?;
    if authority_url.scheme() == "file" {
        roots.push(("metadata".to_owned(), local_directory(&authority_url)?));
        roots.push(("catalog".to_owned(), resolved_path(&config.catalog_dir)?));
    }
    if let Some(writeback) = &settings.writeback
        && writeback.enabled
    {
        roots.push((
            "writeback journal".to_owned(),
            resolved_path(&writeback.dir)?,
        ));
    }
    for member in &config.members {
        let url: url::Url = member.url.parse()?;
        if url.scheme() == "file" {
            roots.push((format!("member {}", member.id), local_directory(&url)?));
        }
    }
    for (index, (left_name, left)) in roots.iter().enumerate() {
        for (right_name, right) in &roots[index + 1..] {
            if left.starts_with(right) || right.starts_with(left) {
                anyhow::bail!(
                    "pool directories must not overlap: {left_name} ({}) and {right_name} ({})",
                    left.display(),
                    right.display()
                );
            }
        }
    }
    Ok(())
}

fn member_identity(url: &url::Url, options: &[(String, String)]) -> Result<String> {
    let url = if url.scheme() == "file" {
        url::Url::from_directory_path(local_directory(url)?)
            .map_err(|_| anyhow::anyhow!("invalid local directory"))?
    } else {
        url.clone()
    };
    // Endpoint/account changes must not silently repoint existing placements;
    // credential rotation must remain possible. Persist only a fingerprint.
    let (provider, environment): (&str, &[&str]) = match ObjectStoreScheme::parse(&url)?.0 {
        ObjectStoreScheme::AmazonS3 => (
            "aws_",
            &[
                "AWS_ENDPOINT",
                "AWS_ENDPOINT_URL",
                "AWS_DEFAULT_REGION",
                "AWS_REGION",
            ],
        ),
        ObjectStoreScheme::MicrosoftAzure => {
            ("azure_", &["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_ENDPOINT"])
        }
        ObjectStoreScheme::GoogleCloudStorage => ("google_", &["GOOGLE_BASE_URL"]),
        _ => ("", &[]),
    };
    let mut namespace = std::collections::BTreeMap::new();
    for key in environment {
        if let Ok(value) = std::env::var(key) {
            namespace.insert(key.to_ascii_lowercase(), value);
        }
    }
    for (key, value) in options {
        let key = key.to_ascii_lowercase();
        if !provider.is_empty()
            && key.starts_with(provider)
            && (key.contains("endpoint")
                || key.ends_with("region")
                || key.ends_with("storage_account_name")
                || key.ends_with("base_url"))
        {
            namespace.insert(key, value.clone());
        }
    }
    let fingerprint = Sha256::digest(serde_json::to_vec(&(url.as_str(), namespace))?);
    Ok(format!("{fingerprint:x}"))
}

#[cfg(test)]
mod tests;
