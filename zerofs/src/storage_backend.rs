//! Build the configured storage graph and retain every transport's lifecycle.

use crate::config::Settings;
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
        Self { pools: vec![pool], fatal: true }
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
        let mut closed = self.pools.iter().map(SftpSessionPool::wait_closed).collect::<FuturesUnordered<_>>();
        let _ = closed.next().await;
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        // Poll every shutdown even if one fails: another member may own live
        // sessions and cancellation cleanup that must still be drained.
        let results = futures::future::join_all(self.pools.iter().map(SftpSessionPool::shutdown)).await;
        let failures = results.into_iter().filter_map(Result::err).map(|error| error.to_string()).collect::<Vec<_>>();
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
    pub(crate) sftp_pool: Option<BackendSessions>,
}

pub(crate) async fn open_storage(settings: &Settings) -> Result<OpenedStorage> {
    if settings.storage.pool.is_some() {
        return open_pool_storage(settings, false).await.map(|(opened, _)| opened);
    }
    let url: url::Url = settings.storage.url.parse()?;
    let ParsedStore { store, path, sftp_pool } = parse_url_opts(
        &url,
        settings.cloud_provider_env_vars(),
        settings.sftp.as_ref(),
    ).await?;
    if url.scheme() == "file" {
        let marker = path.clone().join(".zerofs-pool").join("identity");
        match store.head(&marker).await {
            Ok(_) => anyhow::bail!("this volume has a permanent storage pool; restore [storage.pool] and its original catalog instead of opening the metadata directory alone"),
            Err(object_store::Error::NotFound { .. }) => {},
            Err(error) => return Err(error.into()),
        }
    }
    Ok(OpenedStorage { store, path, sftp_pool: sftp_pool.map(BackendSessions::single) })
}

/// Explicit initialization is used only by `zerofs pool init`. A regular
/// server or maintenance command must never recreate a lost placement catalog.
pub(crate) async fn open_pool_storage(
    settings: &Settings,
    initialize: bool,
) -> Result<(OpenedStorage, PoolObjectStore)> {
    let config = settings.storage.pool.as_ref().context("[storage.pool] is not configured")?;
    settings.validate()?;
    validate_directories(settings)?;
    if !initialize && !config.catalog_dir.join(CATALOG_FILE_NAME).is_file() {
        anyhow::bail!("pool catalog is missing: initialize a new pool with `zerofs pool init`, or restore an existing pool's catalog; normal startup never recreates it");
    }
    let ParsedStore { store: authority, path, sftp_pool: authority_sessions } = parse_url_opts(
        &settings.storage.url.parse()?,
        settings.cloud_provider_env_vars(),
        None,
    ).await?;
    debug_assert!(authority_sessions.is_none(), "pool metadata must be local");
    let mut sessions = BackendSessions { pools: Vec::new(), fatal: false };
    let result: Result<PoolObjectStore> = async {
        let mut members = Vec::with_capacity(config.members.len());
        for member in &config.members {
            let url: url::Url = member.url.parse()?;
            let (scheme, member_path) = ObjectStoreScheme::parse(&url)?;
            let mut options = settings.cloud_provider_env_vars();
            options.extend(member.cloud_provider_env_vars());
            let identity = member_identity(&url, &options)?;
            let parsed = if scheme == ObjectStoreScheme::Sftp {
                crate::parse_object_store::build_sftp_store_with_warmup(&url, member_path, member.sftp.as_ref(), false).await?
            } else {
                parse_url_opts(&url, options, None).await?
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
            });
        }
        Ok(if initialize {
            PoolObjectStore::initialize(Arc::from(authority), path.clone(), config.catalog_dir.clone(), members).await?
        } else {
            PoolObjectStore::open(Arc::from(authority), path.clone(), config.catalog_dir.clone(), members).await?
        })
    }.await;
    match result {
        Ok(pool) => Ok((OpenedStorage {
            store: Box::new(pool.clone()),
            path,
            sftp_pool: (!sessions.pools.is_empty()).then_some(sessions),
        }, pool)),
        Err(error) => match sessions.shutdown().await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(crate::cli::attach_cleanup_errors(error, vec![cleanup])),
        },
    }
}

fn local_directory(url: &url::Url) -> Result<PathBuf> {
    let path = url.to_file_path().map_err(|_| anyhow::anyhow!("invalid local storage URL"))?;
    if !path.is_dir() {
        anyhow::bail!("local pool directory {} must already exist on its mounted disk", path.display());
    }
    path.canonicalize().with_context(|| format!("cannot resolve local directory {}", path.display()))
}

/// Resolve existing symlinks even when the final catalog/cache directory has
/// not been created yet. Lexical-only overlap checks miss symlink aliases.
fn resolved_path(path: &FsPath) -> Result<PathBuf> {
    if !path.is_absolute() {
        anyhow::bail!("pool and cache directories must be absolute: {}", path.display());
    }
    if path.components().any(|part| matches!(part, std::path::Component::ParentDir)) {
        anyhow::bail!("pool and cache paths must not contain parent components");
    }
    let mut ancestor = path;
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        suffix.push(ancestor.file_name().context("path has no existing ancestor")?.to_owned());
        ancestor = ancestor.parent().context("path has no existing ancestor")?;
    }
    let mut result = ancestor.canonicalize()?;
    for component in suffix.into_iter().rev() {
        result.push(component);
    }
    Ok(result)
}

fn validate_directories(settings: &Settings) -> Result<()> {
    let config = settings.storage.pool.as_ref().context("pool is not configured")?;
    let mut roots = vec![
        ("metadata".to_owned(), local_directory(&settings.storage.url.parse()?)?),
        ("catalog".to_owned(), resolved_path(&config.catalog_dir)?),
        ("clean cache".to_owned(), resolved_path(&settings.cache.dir)?),
    ];
    if let Some(writeback) = &settings.writeback
        && writeback.enabled
    {
        roots.push(("writeback journal".to_owned(), resolved_path(&writeback.dir)?));
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
                anyhow::bail!("pool directories must not overlap: {left_name} ({}) and {right_name} ({})", left.display(), right.display());
            }
        }
    }
    Ok(())
}

fn member_identity(
    url: &url::Url,
    options: &[(String, String)],
) -> Result<String> {
    let url = if url.scheme() == "file" {
        url::Url::from_directory_path(local_directory(url)?).map_err(|_| anyhow::anyhow!("invalid local directory"))?
    } else {
        url.clone()
    };
    // Endpoint/account changes must not silently repoint existing placements;
    // credential rotation must remain possible. Persist only a fingerprint.
    let (provider, environment): (&str, &[&str]) = match ObjectStoreScheme::parse(&url)?.0 {
        ObjectStoreScheme::AmazonS3 => ("aws_", &["AWS_ENDPOINT", "AWS_ENDPOINT_URL", "AWS_DEFAULT_REGION", "AWS_REGION"]),
        ObjectStoreScheme::MicrosoftAzure => ("azure_", &["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_ENDPOINT"]),
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
        if !provider.is_empty() && key.starts_with(provider) && (key.contains("endpoint") || key.ends_with("region") || key.ends_with("storage_account_name") || key.ends_with("base_url")) {
            namespace.insert(key, value.clone());
        }
    }
    let fingerprint = Sha256::digest(serde_json::to_vec(&(url.as_str(), namespace))?);
    Ok(format!("{fingerprint:x}"))
}

#[cfg(test)]
mod tests;
