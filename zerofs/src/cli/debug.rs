use crate::block_transformer::ZeroFsBlockTransformer;
use crate::cli::finish_with_sftp_cleanup;
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::fs::CacheConfig;
use crate::fs::key_codec::{EXTENT_DOMAIN, KeyCodec, KeyPrefix, META_DOMAIN, ParsedKey};
use crate::key_management;
use crate::parse_object_store::parse_url_opts;
use crate::storage_class_object_store::with_storage_class;
use anyhow::{Context, Result};
use object_store::ObjectStoreExt;
use sha2::{Digest, Sha256};
use slatedb::BlockTransformer;
use slatedb::config::{DurabilityLevel, ScanOptions};
use slatedb::object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

const U64_SIZE: usize = std::mem::size_of::<u64>();

/// Classify one raw db key the same way the codec routes it: leading
/// `META_DOMAIN`/`EXTENT_DOMAIN` prefix first, then the kind byte. Returns
/// the kind plus a rendered payload, or `None` for keys with no recognized
/// domain + kind prefix (kind Extent is only valid in the extent domain,
/// every other kind only in the meta domain).
fn describe_key(codec: &KeyCodec, key: &[u8]) -> Option<(KeyPrefix, String)> {
    let (in_extent_domain, rest) = if let Some(rest) = key.strip_prefix(EXTENT_DOMAIN) {
        (true, rest)
    } else {
        let rest = key.strip_prefix(META_DOMAIN)?;
        (false, rest)
    };

    let prefix = KeyPrefix::try_from(*rest.first()?).ok()?;
    if (prefix == KeyPrefix::Extent) != in_extent_domain {
        return None;
    }
    let detail = decode_payload(codec, prefix, key).unwrap_or_else(|| format!("raw={:?}", key));
    Some((prefix, detail))
}

/// Render the id portion of a classified key. `None` means the domain + kind
/// prefix is valid but the payload is malformed for that kind, and the caller
/// falls back to printing the raw bytes.
fn decode_payload(codec: &KeyCodec, prefix: KeyPrefix, key: &[u8]) -> Option<String> {
    let id_off = codec.id_offset(prefix);
    let id_u64 = |off: usize| {
        Some(u64::from_be_bytes(
            key.get(off..off + U64_SIZE)?.try_into().ok()?,
        ))
    };
    match prefix {
        KeyPrefix::Inode if key.len() == codec.inode_key_size() => {
            Some(format!("inode_id={}", id_u64(id_off)?))
        }
        KeyPrefix::Extent => codec
            .parse_extent_key_full(key)
            .map(|(inode_id, extent_index)| {
                format!("inode_id={}, extent_index={}", inode_id, extent_index)
            }),
        KeyPrefix::DirEntry if key.len() > id_off + U64_SIZE => {
            let name = String::from_utf8_lossy(&key[id_off + U64_SIZE..]);
            Some(format!("dir_id={}, name=\"{}\"", id_u64(id_off)?, name))
        }
        KeyPrefix::DirScan => match codec.parse_key(key) {
            ParsedKey::DirScan { cookie } => {
                Some(format!("dir_id={}, cookie={}", id_u64(id_off)?, cookie))
            }
            _ => None,
        },
        KeyPrefix::DirCookie if key.len() == id_off + U64_SIZE => {
            Some(format!("dir_id={}", id_u64(id_off)?))
        }
        KeyPrefix::Tombstone => match codec.parse_key(key) {
            ParsedKey::Tombstone { inode_id } => Some(format!(
                "timestamp={}, inode_id={}",
                id_u64(id_off)?,
                inode_id
            )),
            _ => None,
        },
        KeyPrefix::Orphan => match codec.parse_key(key) {
            ParsedKey::Orphan { inode_id } => Some(format!("inode_id={}", inode_id)),
            _ => None,
        },
        KeyPrefix::Stats if key.len() == id_off + U64_SIZE => {
            Some(format!("shard_id={}", id_u64(id_off)?))
        }
        KeyPrefix::System => Some(format!(
            "subtype=0x{:02x}",
            key.get(id_off).copied().unwrap_or(0)
        )),
        KeyPrefix::SegCount => codec
            .parse_segcount_key(key)
            .map(|(epoch, counter)| format!("epoch={}, counter={}", epoch, counter)),
        _ => None,
    }
}

pub async fn list_keys(config_path: PathBuf) -> Result<()> {
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();

    let cache_config = CacheConfig {
        root_folder: settings.cache.dir.clone(),
        max_cache_size_gb: settings.cache.disk_size_gb,
        memory_cache_size_gb: settings.cache.memory_size_gb,
    };

    let env_vars = settings.cloud_provider_env_vars();
    let crate::parse_object_store::ParsedStore {
        store: object_store,
        path: path_from_url,
        sftp_pool,
    } = parse_url_opts(&url.parse()?, env_vars, settings.sftp.as_ref()).await?;
    let command_result: Result<()> = async move {
        let object_store = with_storage_class(
            Arc::from(object_store),
            settings.storage.storage_class.as_deref(),
        );

        let actual_db_path = path_from_url.to_string();

        let bucket =
            crate::bucket_identity::BucketIdentity::get_or_create(&object_store, &actual_db_path)
                .await?;

        let cache_config = CacheConfig {
            root_folder: cache_config.root_folder.join(bucket.cache_directory_name()),
            ..cache_config
        };

        let password = settings.storage.encryption_password.clone();

        crate::cli::password::validate_password(&password)
            .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

        let db_path = Path::from(actual_db_path.clone());
        let encryption_key =
            key_management::load_or_init_encryption_key(&object_store, &db_path, &password, false)
                .await?;

        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

        let wal_object_store: Option<Arc<dyn object_store::ObjectStore>> =
            if let Some(wal_config) = &settings.wal {
                Some(super::server::parse_wal_object_store(wal_config).await?)
            } else {
                None
            };

        let opened = super::server::build_slatedb(
            object_store,
            &cache_config,
            actual_db_path,
            super::server::DatabaseMode::ReadWrite,
            settings.lsm,
            block_transformer,
            wal_object_store,
            None, // debug command never participates in replication
        )
        .await?;

        let db = match opened.data {
            SlateDbHandle::ReadWrite(db) => db,
            SlateDbHandle::ReadOnly(_) => {
                return Err(anyhow::anyhow!(
                    "Expected read-write mode for debug command"
                ));
            }
        };

        println!("Scanning all keys in the database...\n");

        let scan_options = ScanOptions {
            durability_filter: DurabilityLevel::Memory,
            read_ahead_bytes: 1024 * 1024,
            cache_blocks: false,
            max_fetch_tasks: 4,
            ..Default::default()
        };

        let mut iter = db.scan_with_options(.., &scan_options).await?;

        let codec = KeyCodec::new();
        let mut count = 0;
        let mut count_by_prefix: std::collections::HashMap<KeyPrefix, usize> =
            std::collections::HashMap::new();

        loop {
            let kv = match iter.next().await {
                Ok(Some(kv)) => kv,
                Ok(None) => break,
                Err(e) => anyhow::bail!("dump scan failed after {count} keys: {e}"),
            };
            let key = kv.key;

            let (prefix, detail) = match describe_key(&codec, &key) {
                Some(described) => described,
                None => {
                    if key.is_empty() {
                        println!("Empty key found");
                    } else {
                        println!("Unknown key: {:?}", key);
                    }
                    continue;
                }
            };

            *count_by_prefix.entry(prefix).or_insert(0) += 1;

            println!("[{}] {}", prefix.as_str(), detail);

            count += 1;
        }

        println!("\n=== Summary ===");
        println!("Total keys: {}", count);
        println!("\nKeys by type:");

        let mut prefix_counts: Vec<_> = count_by_prefix.iter().collect();
        prefix_counts.sort_by_key(|(prefix, _)| u8::from(**prefix));

        for (prefix, count) in prefix_counts {
            println!("  {}: {}", prefix.as_str(), count);
        }

        drop(iter);
        db.close().await.context("Failed to close debug database")?;

        Ok(())
    }
    .await;

    finish_with_sftp_cleanup(
        sftp_pool.as_ref(),
        "Failed to shut down SFTP debug pool",
        command_result,
    )
    .await
}

pub async fn reseed_writeback_predecessor(
    config_path: PathBuf,
    journal_path: PathBuf,
    path: String,
    sequence: u64,
) -> Result<()> {
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    if !settings
        .writeback
        .as_ref()
        .is_some_and(|writeback| writeback.enabled)
    {
        anyhow::bail!("[writeback] must be enabled in the supplied config");
    }
    let env_vars = settings.cloud_provider_env_vars();
    let crate::parse_object_store::ParsedStore {
        store: remote,
        sftp_pool,
        path: database_path,
    } = parse_url_opts(
        &settings.storage.url.parse()?,
        env_vars,
        settings.sftp.as_ref(),
    )
    .await?;
    let remote = with_storage_class(Arc::from(remote), settings.storage.storage_class.as_deref());
    let location = Path::parse(&path).context("invalid remote predecessor path")?;
    let result: Result<()> = async {
        let expected_identity = crate::cli::init::load_existing_writeback_identity(
            &settings,
            &remote,
            database_path.as_ref(),
        )
        .await?;
        let metadata = remote
            .head(&location)
            .await
            .with_context(|| format!("failed to inspect remote predecessor {path}"))?;
        let e_tag = metadata
            .e_tag
            .context("remote predecessor has no ETag and cannot be safely reseeded")?;
        let journal = crate::writeback::journal::Journal::open_existing_with_identity(
            &journal_path,
            expected_identity,
        )
        .with_context(|| {
            format!(
                "failed to open stopped writeback journal {}",
                journal_path.display()
            )
        })?;
        journal.seed_remote_object_etag(&path, sequence, &e_tag)?;
        println!(
            "seeded_remote_predecessor path={} sequence={} etag={}",
            path, sequence, e_tag
        );
        Ok(())
    }
    .await;
    finish_with_sftp_cleanup(sftp_pool.as_ref(), "Failed to shut down SFTP pool", result).await
}

fn parse_sha256(value: &str, label: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{label} must be exactly 64 hexadecimal characters");
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        digest[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(digest)
}

#[allow(clippy::too_many_arguments)]
pub async fn accept_remote_writeback_branch(
    config_path: PathBuf,
    journal_path: PathBuf,
    expected_remote_sequence: u64,
    expected_local_sequence: u64,
    manifest_path: String,
    expected_local_sha256: String,
    expected_remote_sha256: String,
    confirm_abandon_maintenance_tail: bool,
) -> Result<()> {
    if !confirm_abandon_maintenance_tail {
        anyhow::bail!("--confirm-abandon-maintenance-tail is required");
    }
    let local_sha256 = parse_sha256(&expected_local_sha256, "local SHA-256")?;
    let remote_sha256 = parse_sha256(&expected_remote_sha256, "remote SHA-256")?;
    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
    if !settings
        .writeback
        .as_ref()
        .is_some_and(|writeback| writeback.enabled)
    {
        anyhow::bail!("[writeback] must be enabled in the supplied config");
    }
    let env_vars = settings.cloud_provider_env_vars();
    let crate::parse_object_store::ParsedStore {
        store: remote,
        sftp_pool,
        path: database_path,
    } = parse_url_opts(
        &settings.storage.url.parse()?,
        env_vars,
        settings.sftp.as_ref(),
    )
    .await?;
    let remote = with_storage_class(Arc::from(remote), settings.storage.storage_class.as_deref());
    let location = Path::parse(&manifest_path).context("invalid divergent manifest path")?;
    let result: Result<()> = async {
        let expected_identity = crate::cli::init::load_existing_writeback_identity(
            &settings,
            &remote,
            database_path.as_ref(),
        )
        .await?;
        let remote_payload = remote
            .get(&location)
            .await
            .with_context(|| format!("failed to read remote manifest {manifest_path}"))?
            .bytes()
            .await
            .with_context(|| format!("failed to collect remote manifest {manifest_path}"))?;
        let actual_remote_sha256: [u8; 32] = Sha256::digest(&remote_payload).into();
        if actual_remote_sha256 != remote_sha256 {
            anyhow::bail!(
                "remote manifest changed: expected {}, got {}",
                expected_remote_sha256,
                hex_sha256(actual_remote_sha256)
            );
        }
        let journal = crate::writeback::journal::Journal::open_existing_with_identity(
            &journal_path,
            expected_identity,
        )
        .with_context(|| {
            format!(
                "failed to open stopped writeback journal {}",
                journal_path.display()
            )
        })?;
        let abandoned = journal.abandon_divergent_maintenance_tail(
            expected_remote_sequence,
            expected_local_sequence,
            &manifest_path,
            local_sha256,
            actual_remote_sha256,
        )?;
        println!(
            "accepted_remote_writeback_branch remote_sequence={} local_sequence={} abandoned_operations={} local_sha256={} remote_sha256={}",
            expected_remote_sequence,
            expected_local_sequence,
            abandoned.len(),
            expected_local_sha256,
            expected_remote_sha256
        );
        for record in abandoned {
            println!(
                "abandoned_local_mutation sequence={} path={}",
                record.sequence, record.path
            );
        }
        Ok(())
    }
    .await;
    finish_with_sftp_cleanup(
        sftp_pool.as_ref(),
        "Failed to shut down SFTP pool after writeback branch recovery",
        result,
    )
    .await
}

fn hex_sha256(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_identity::BucketIdentity;
    use crate::config::Settings;
    use crate::key_management;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackConfig};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use object_store::ObjectStore;
    use object_store::ObjectStoreExt;
    use std::fs;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Every key kind the filesystem writes must classify under its KeyPrefix
    // and decode its id payload; keys are built via KeyCodec so this breaks
    // if the parser ever drifts from the codec's segmented layout again.
    #[test]
    fn test_describe_key_decodes_every_kind() {
        let codec = KeyCodec::new();
        let cases: Vec<(bytes::Bytes, KeyPrefix, &str)> = vec![
            (codec.inode_key(42), KeyPrefix::Inode, "inode_id=42"),
            (
                codec.extent_key(7, 99),
                KeyPrefix::Extent,
                "inode_id=7, extent_index=99",
            ),
            (
                codec.dir_entry_key(3, b"hello.txt"),
                KeyPrefix::DirEntry,
                "dir_id=3, name=\"hello.txt\"",
            ),
            (
                codec.dir_scan_key(3, 12),
                KeyPrefix::DirScan,
                "dir_id=3, cookie=12",
            ),
            (
                codec.dir_cookie_counter_key(3),
                KeyPrefix::DirCookie,
                "dir_id=3",
            ),
            (
                codec.tombstone_key(1111, 5),
                KeyPrefix::Tombstone,
                "timestamp=1111, inode_id=5",
            ),
            (codec.orphan_key(8), KeyPrefix::Orphan, "inode_id=8"),
            (codec.stats_shard_key(2), KeyPrefix::Stats, "shard_id=2"),
            (
                codec.system_counter_key(),
                KeyPrefix::System,
                "subtype=0x01",
            ),
            (
                codec.segcount_key(4, 17),
                KeyPrefix::SegCount,
                "epoch=4, counter=17",
            ),
        ];
        for (key, want_prefix, want_detail) in cases {
            let (prefix, detail) = describe_key(&codec, &key)
                .unwrap_or_else(|| panic!("key {:?} not recognized", key));
            assert_eq!(prefix, want_prefix, "kind for {:?}", key);
            assert_eq!(detail, want_detail, "payload for {:?}", key);
        }
    }

    #[test]
    fn test_describe_key_rejects_undecodable_keys() {
        let codec = KeyCodec::new();
        // Empty, no domain prefix, and the pre-segment layout (bare kind byte).
        assert!(describe_key(&codec, b"").is_none());
        assert!(describe_key(&codec, b"garbage").is_none());
        assert!(describe_key(&codec, &[0x01, 0, 0, 0, 0, 0, 0, 0, 42]).is_none());
        // An extent kind byte under the meta domain is misrouted, not a kind.
        let mut misrouted = META_DOMAIN.to_vec();
        misrouted.push(u8::from(KeyPrefix::Extent));
        misrouted.extend_from_slice(&[0; U64_SIZE * 2]);
        assert!(describe_key(&codec, &misrouted).is_none());
        // A recognized kind with a truncated payload still classifies (and
        // counts) but falls back to the raw rendering instead of misdecoding.
        let inode_key = codec.inode_key(1);
        let truncated = &inode_key[..inode_key.len() - 1];
        let (prefix, detail) = describe_key(&codec, truncated).unwrap();
        assert_eq!(prefix, KeyPrefix::Inode);
        assert!(detail.starts_with("raw="));
    }

    fn recovery_identity() -> JournalIdentity {
        JournalIdentity {
            format_version: 1,
            bucket_id: "journal-a".to_owned(),
            backend_endpoint: "sftp://operator@storage-a:22".to_owned(),
            database_prefix: "zerofs/a".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0xa5; 32],
        }
    }

    fn recovery_put(sequence: u64, path: &str, payload: &[u8]) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(0x51_0000 + sequence as u128),
            path: path.to_owned(),
            kind: MutationKind::Put {
                mode: MutationMode::Create,
                expected_visible_version: None,
                payload_len: payload.len() as u64,
                payload_sha256: Sha256::digest(payload).into(),
                blob_path: String::new(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: 1_786_435_200_000 + sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        }
    }

    fn stopped_journal_files(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn collect(
            root: &std::path::Path,
            current: &std::path::Path,
            files: &mut Vec<(std::path::PathBuf, Vec<u8>)>,
        ) {
            for entry in fs::read_dir(current).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    collect(root, &path, files);
                } else if path.is_file() {
                    files.push((
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(path).unwrap(),
                    ));
                }
            }
        }

        let mut files = Vec::new();
        collect(root, root, &mut files);
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    #[test]
    fn checked_existing_open_rejects_each_config_identity_field_without_journal_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let journal_path = temp.path().join("relocated-offline-journal");
        let identity = recovery_identity();
        let journal = Journal::open(&journal_path, identity.clone()).unwrap();
        journal
            .commit_put(
                recovery_put(
                    1,
                    "zerofs/a/manifest/00000000000000000001.manifest",
                    b"payload-a",
                ),
                b"payload-a",
            )
            .unwrap();
        drop(journal);
        let before = stopped_journal_files(&journal_path);

        let mut bucket = identity.clone();
        bucket.bucket_id = "config-b".to_owned();
        let mut endpoint = identity.clone();
        endpoint.backend_endpoint = "sftp://operator@storage-b:22".to_owned();
        let mut prefix = identity.clone();
        prefix.database_prefix = "zerofs/b".to_owned();
        let mut kind = identity.clone();
        kind.backend_kind = "s3".to_owned();
        let mut key = identity.clone();
        key.encryption_key_identity_sha256 = [0xb6; 32];
        let mut version = identity.clone();
        version.format_version = 2;

        for (field, config_identity) in [
            ("bucket", bucket),
            ("endpoint", endpoint),
            ("prefix", prefix),
            ("backend kind", kind),
            ("encryption key", key),
            ("format version", version),
        ] {
            let error = Journal::open_existing_with_identity(&journal_path, config_identity)
                .expect_err("config B must not open journal A");
            assert!(
                format!("{error:#}").contains("identity mismatch"),
                "{field}: {error:#}"
            );
            assert_eq!(
                stopped_journal_files(&journal_path),
                before,
                "{field} changed journal state"
            );
        }
    }

    #[test]
    fn checked_existing_open_never_initializes_missing_journal_state() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing-journal");
        let error =
            Journal::open_existing_with_identity(&missing, recovery_identity()).unwrap_err();
        assert!(format!("{error:#}").contains("database does not exist"));
        assert!(!missing.exists(), "missing journal must not be initialized");

        let missing_identity = temp.path().join("missing-identity");
        fs::create_dir(&missing_identity).unwrap();
        let database_path = missing_identity.join("journal.redb");
        drop(redb::Database::create(&database_path).unwrap());
        #[cfg(unix)]
        fs::set_permissions(
            &database_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        let error = Journal::open_existing_with_identity(&missing_identity, recovery_identity())
            .unwrap_err();
        assert!(format!("{error:#}").contains("journal metadata"));
        assert!(!missing_identity.join("LOCK").exists());
        assert!(!missing_identity.join("blobs").exists());
        assert!(!missing_identity.join("tmp").exists());
    }

    async fn recovery_config(
        temp: &TempDir,
        namespace: &str,
        password: &str,
    ) -> (PathBuf, Arc<dyn ObjectStore>, String, JournalIdentity) {
        let mut settings = Settings::generate_default();
        settings.aws = None;
        settings.cache.dir = temp.path().join(format!("cache-{namespace}"));
        settings.cache.disk_size_gb = 0.01;
        settings.cache.memory_size_gb = Some(0.01);
        settings.storage.url = format!(
            "file://{}",
            temp.path().join("remote").join(namespace).display()
        );
        settings.storage.encryption_password = password.to_owned();
        settings.writeback = Some(WritebackConfig {
            enabled: true,
            dir: temp
                .path()
                .join(format!("configured-writeback-{namespace}")),
            ack_mode: AckMode::Memory,
            memory_size_gb: 0.01,
            disk_size_gb: 0.02,
            min_free_gb: 0.001,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: Some(2),
            local_concurrency: 2,
            shutdown_flush: ShutdownFlush::Local,
        });
        settings.validate().unwrap();
        let crate::parse_object_store::ParsedStore {
            store,
            path,
            sftp_pool,
        } = parse_url_opts(
            &settings.storage.url.parse().unwrap(),
            settings.cloud_provider_env_vars(),
            settings.sftp.as_ref(),
        )
        .await
        .unwrap();
        assert!(sftp_pool.is_none());
        let remote =
            with_storage_class(Arc::from(store), settings.storage.storage_class.as_deref());
        let prefix = path.to_string();
        BucketIdentity::get_or_create(&remote, &prefix)
            .await
            .unwrap();
        key_management::load_or_init_encryption_key(
            &remote,
            &Path::from(prefix.clone()),
            password,
            false,
        )
        .await
        .unwrap();
        let identity =
            crate::cli::init::load_existing_writeback_identity(&settings, &remote, &prefix)
                .await
                .unwrap();
        let config = temp.path().join(format!("config-{namespace}.toml"));
        fs::write(&config, toml::to_string(&settings).unwrap()).unwrap();
        (config, remote, prefix, identity)
    }

    #[tokio::test]
    async fn reseed_rejects_missing_remote_evidence_without_recovering_the_journal() {
        let temp = tempfile::tempdir().unwrap();
        let (config, _remote, prefix, identity) = recovery_config(&temp, "a", "a-password").await;
        let journal_path = temp.path().join("reseed-invalid-evidence-journal");
        drop(Journal::open(&journal_path, identity).unwrap());
        fs::write(journal_path.join("tmp/recoverable"), b"must survive").unwrap();
        let before = stopped_journal_files(&journal_path);

        let error = reseed_writeback_predecessor(
            config,
            journal_path.clone(),
            format!("{prefix}/recovery/missing"),
            1,
        )
        .await
        .expect_err("missing remote evidence must reject reseed");

        assert!(format!("{error:#}").contains("failed to inspect remote predecessor"));
        assert!(
            stopped_journal_files(&journal_path) == before,
            "invalid remote evidence must not recover or normalize the stopped journal"
        );
    }

    #[tokio::test]
    async fn accept_rejects_wrong_remote_hash_without_recovering_the_journal() {
        let temp = tempfile::tempdir().unwrap();
        let (config, remote_store, prefix, identity) =
            recovery_config(&temp, "a", "a-password").await;
        let journal_path = temp.path().join("accept-invalid-evidence-journal");
        let manifest_path = format!("{prefix}/manifest/00000000000000000001.manifest");
        let local = b"local manifest";
        let remote = b"remote manifest";
        remote_store
            .put(
                &Path::from(manifest_path.as_str()),
                remote.as_slice().into(),
            )
            .await
            .unwrap();
        let journal = Journal::open(&journal_path, identity).unwrap();
        journal
            .commit_put(recovery_put(1, &manifest_path, local), local)
            .unwrap();
        drop(journal);
        fs::write(journal_path.join("tmp/recoverable"), b"must survive").unwrap();
        let before = stopped_journal_files(&journal_path);

        let error = accept_remote_writeback_branch(
            config,
            journal_path.clone(),
            0,
            1,
            manifest_path,
            hex_sha256(Sha256::digest(local).into()),
            hex_sha256([0; 32]),
            true,
        )
        .await
        .expect_err("wrong remote digest must reject maintenance-tail acceptance");

        assert!(format!("{error:#}").contains("remote manifest changed"));
        assert!(
            stopped_journal_files(&journal_path) == before,
            "invalid remote evidence must not recover or normalize the stopped journal"
        );
    }

    #[tokio::test]
    async fn recovery_commands_reject_config_b_before_mutating_journal_a() {
        let temp = tempfile::tempdir().unwrap();
        let (config_a, remote_a, prefix_a, identity_a) =
            recovery_config(&temp, "a", "a-password").await;
        let (config_b, remote_b, _prefix_b, _identity_b) =
            recovery_config(&temp, "b", "b-password").await;

        let reseed_journal_path = temp.path().join("relocated-reseed-journal");
        let predecessor = format!("{prefix_a}/recovery/predecessor");
        remote_b
            .put(
                &Path::from(predecessor.as_str()),
                (&b"remote predecessor"[..]).into(),
            )
            .await
            .unwrap();
        let journal = Journal::open(&reseed_journal_path, identity_a.clone()).unwrap();
        journal
            .commit_put(
                recovery_put(1, &predecessor, b"local predecessor"),
                b"local predecessor",
            )
            .unwrap();
        journal.mark_remote(1, None).unwrap();
        drop(journal);
        let reseed_before = stopped_journal_files(&reseed_journal_path);
        let error = reseed_writeback_predecessor(
            config_b.clone(),
            reseed_journal_path.clone(),
            predecessor.clone(),
            1,
        )
        .await
        .expect_err("config B must not seed journal A");
        assert!(format!("{error:#}").contains("identity mismatch"));
        assert_eq!(stopped_journal_files(&reseed_journal_path), reseed_before);
        remote_a
            .put(
                &Path::from(predecessor.as_str()),
                (&b"remote predecessor"[..]).into(),
            )
            .await
            .unwrap();
        reseed_writeback_predecessor(
            config_a.clone(),
            reseed_journal_path.clone(),
            predecessor.clone(),
            1,
        )
        .await
        .expect("matching config must accept relocated offline journal");
        let journal = Journal::open_existing(&reseed_journal_path).unwrap();
        assert!(
            journal
                .remote_object_etag(&predecessor, 1)
                .unwrap()
                .is_some()
        );
        drop(journal);

        let accept_journal_path = temp.path().join("relocated-accept-journal");
        let manifest_path = format!("{prefix_a}/manifest/00000000000000000007.manifest");
        let local = b"local manifest";
        let remote = b"remote manifest";
        remote_b
            .put(
                &Path::from(manifest_path.as_str()),
                remote.as_slice().into(),
            )
            .await
            .unwrap();
        let journal = Journal::open(&accept_journal_path, identity_a).unwrap();
        journal
            .commit_put(recovery_put(1, &manifest_path, local), local)
            .unwrap();
        drop(journal);
        let accept_before = stopped_journal_files(&accept_journal_path);
        let error = accept_remote_writeback_branch(
            config_b,
            accept_journal_path.clone(),
            0,
            1,
            manifest_path.clone(),
            hex_sha256(Sha256::digest(local).into()),
            hex_sha256(Sha256::digest(remote).into()),
            true,
        )
        .await
        .expect_err("config B must not abandon journal A");
        assert!(format!("{error:#}").contains("identity mismatch"));
        assert_eq!(stopped_journal_files(&accept_journal_path), accept_before);
        remote_a
            .put(
                &Path::from(manifest_path.as_str()),
                remote.as_slice().into(),
            )
            .await
            .unwrap();
        accept_remote_writeback_branch(
            config_a,
            accept_journal_path.clone(),
            0,
            1,
            manifest_path,
            hex_sha256(Sha256::digest(local).into()),
            hex_sha256(Sha256::digest(remote).into()),
            true,
        )
        .await
        .expect("matching config must accept relocated offline journal");
        let journal = Journal::open_existing(&accept_journal_path).unwrap();
        assert_eq!(journal.progress().unwrap().remote_seq, 1);
        assert!(journal.snapshot().unwrap().records.is_empty());
    }
}
