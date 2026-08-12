use crate::writeback::config::WritebackSettings;
use crate::writeback::journal::Journal;
use crate::writeback::model::JournalIdentity;
use crate::writeback::store::WritebackObjectStore;
use object_store::ObjectStore;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

pub struct AttachedWriteback {
    pub store: Arc<dyn ObjectStore>,
    pub lifecycle: WritebackObjectStore,
}

pub async fn attach(
    remote: Arc<dyn ObjectStore>,
    mut settings: WritebackSettings,
    identity: JournalIdentity,
    namespace: &str,
) -> anyhow::Result<AttachedWriteback> {
    validate_namespace(namespace)?;
    ensure_base_directory(&settings.dir)?;
    settings.dir = settings.dir.join(namespace);
    let journal = Arc::new(Journal::open(&settings.dir, identity)?);
    let recovery = journal.progress()?;
    let lifecycle = WritebackObjectStore::open(remote, journal, settings).await?;
    if recovery.remote_seq < recovery.local_seq {
        tracing::info!(
            remote_sequence = recovery.remote_seq,
            local_sequence = recovery.local_seq,
            pending_operations = recovery.local_seq - recovery.remote_seq,
            "replaying the locally durable writeback journal before opening the database"
        );
        lifecycle
            .wait_remote(recovery.local_seq)
            .await
            .map_err(|error| anyhow::anyhow!("writeback recovery replay failed: {error}"))?;
    }
    Ok(AttachedWriteback {
        store: Arc::new(lifecycle.clone()),
        lifecycle,
    })
}

fn ensure_base_directory(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                anyhow::bail!("writeback base {} must be a real directory", path.display());
            }
            #[cfg(unix)]
            validate_owner_only_base(path, &metadata)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| {
                anyhow::anyhow!(
                    "failed to create writeback base {}: {error}",
                    path.display()
                )
            })?;
            #[cfg(unix)]
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            if let Some(parent) = path.parent() {
                fs::File::open(parent)?.sync_all()?;
            }
        }
        Err(error) => {
            anyhow::bail!(
                "failed to inspect writeback base {}: {error}",
                path.display()
            );
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_owner_only_base(path: &Path, metadata: &fs::Metadata) -> anyhow::Result<()> {
    let mode = metadata.mode() & 0o777;
    if mode != 0o700 {
        anyhow::bail!(
            "writeback base {} has mode {mode:o}; expected 700",
            path.display()
        );
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        anyhow::bail!(
            "writeback base {} is not owned by the service user",
            path.display()
        );
    }
    Ok(())
}

fn validate_namespace(namespace: &str) -> anyhow::Result<()> {
    if namespace.is_empty()
        || namespace == "."
        || namespace == ".."
        || namespace.contains('/')
        || namespace.contains('\\')
    {
        anyhow::bail!("writeback namespace must be one safe path component");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::fault_store::FaultStore;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::model::JournalIdentity;
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use std::sync::Arc;
    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt};

    #[tokio::test]
    async fn attachment_scopes_the_journal_and_routes_writes_through_the_overlay() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let base = temp.path().join("dirty");
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 16_000_000_000,
            disk_bytes: 512_000_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-123".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x55; 32],
        };

        let attached = super::attach(partitioned, settings, identity, "bucket_12345678")
            .await
            .unwrap();
        let location = Path::from("zerofs/pilot/segments/1");
        attached
            .store
            .put(&location, Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        assert_eq!(
            attached
                .store
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"payload")
        );
        assert!(remote.head(&location).await.is_err());
        assert!(base.join("bucket_12345678").join("journal.redb").exists());
        attached.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovered_journal_replays_before_attachment_becomes_visible() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let settings = WritebackSettings {
            dir: temp.path().join("dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-recovery".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/recovery".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x66; 32],
        };
        let location = Path::from("zerofs/recovery/manifest/00000000000000000001.manifest");

        let first = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_recovery",
        )
        .await
        .unwrap();
        first
            .store
            .put(&location, Bytes::from_static(b"manifest").into())
            .await
            .unwrap();
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);

        let mut recovery = tokio::spawn(super::attach(
            partitioned,
            settings,
            identity,
            "bucket_recovery",
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut recovery)
                .await
                .is_err(),
            "attachment exposed the overlay before recovered mutations reached the backend"
        );

        controls.partition_writes(false);
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(5), recovery)
            .await
            .expect("recovery should finish after the backend heals")
            .unwrap()
            .unwrap();
        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"manifest")
        );
        recovered.lifecycle.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn attachment_rejects_an_existing_permissive_base_without_chmodding_it() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("dirty");
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 1,
            disk_bytes: 1,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 1,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-123".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x55; 32],
        };

        let error = super::attach(
            Arc::new(InMemory::new()),
            settings,
            identity,
            "bucket_12345678",
        )
        .await
        .err()
        .expect("permissive base should be rejected");

        assert!(format!("{error:#}").contains("mode"));
        assert_eq!(
            fs::metadata(&base).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
