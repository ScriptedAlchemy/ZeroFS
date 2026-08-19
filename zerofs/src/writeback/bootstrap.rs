use crate::writeback::config::WritebackSettings;
use crate::writeback::journal::Journal;
use crate::writeback::model::JournalIdentity;
use crate::writeback::reservation::SsdAdmission;
use crate::writeback::space_sample::PhysicalSpaceSampler;
use crate::writeback::store::WritebackObjectStore;
use object_store::ObjectStore;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

pub struct AttachedWriteback {
    pub store: Arc<dyn ObjectStore>,
    pub lifecycle: WritebackObjectStore,
    // WIP on develop: landed but not wired yet.
    #[allow(dead_code)]
    pub(crate) space: Arc<PhysicalSpaceSampler>,
    // WIP on develop: landed but not wired yet.
    #[allow(dead_code)]
    pub(crate) ssd: Arc<SsdAdmission>,
}

/// Recover the local overlay while leaving remote replay paused.
///
/// The caller must invoke [`WritebackObjectStore::activate_remote`] after it
/// finishes opening the database over `store`. This keeps the remote manifest
/// view from changing underneath database recovery without making startup wait
/// for the entire SSD backlog to reach the backend.
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
    let space = Arc::new(PhysicalSpaceSampler::new(settings.dir.clone()));
    let sample = space.sample().await?;
    let pending = journal.pending_ssd_reservations()?;
    let ssd = Arc::new(SsdAdmission::recover(
        settings.disk_bytes,
        1 << 20,
        settings.high_watermark_percent,
        settings.resume_percent,
        settings.min_free_bytes,
        pending,
        Some(sample),
    )?);
    let lifecycle = WritebackObjectStore::open_paused_with_owners(
        remote,
        journal,
        settings,
        Arc::clone(&space),
        Arc::clone(&ssd),
    )
    .await?;
    if recovery.remote_seq < recovery.local_seq {
        tracing::info!(
            remote_sequence = recovery.remote_seq,
            local_sequence = recovery.local_seq,
            pending_operations = recovery.local_seq - recovery.remote_seq,
            "attached a stable recovered writeback overlay; remote replay remains paused until filesystem initialization completes"
        );
    }
    Ok(AttachedWriteback {
        store: Arc::new(lifecycle.clone()),
        lifecycle,
        space,
        ssd,
    })
}

fn ensure_base_directory(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                anyhow::bail!("writeback base {} must be a real directory", path.display());
            }
            super::validate_owner_only(path, &metadata, 0o700, "writeback base")?;
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
    use futures::TryStreamExt;
    use object_store::{ObjectStoreExt, PutMode, PutOptions, memory::InMemory, path::Path};
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
            local_concurrency: 4,
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
    async fn recovered_journal_is_stable_and_available_before_remote_replay() {
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
            local_concurrency: 4,
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
        let first = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_recovery",
        )
        .await
        .unwrap();
        for sequence in 0..32 {
            first
                .store
                .put(
                    &Path::from(format!("zerofs/recovery/segments/{sequence:02}")),
                    Bytes::from(format!("segment-{sequence:02}")).into(),
                )
                .await
                .unwrap();
        }
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);

        let puts_before_recovery = controls.put_count();
        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::attach(partitioned, settings, identity, "bucket_recovery"),
        )
        .await
        .expect("an offline remote must not make recovered attachment wait for a full drain")
        .unwrap();
        let visible = recovered
            .store
            .list(Some(&Path::from("zerofs/recovery/segments")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(visible.len(), 32);
        assert_eq!(
            recovered
                .store
                .get(&Path::from("zerofs/recovery/segments/31"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"segment-31")
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(
            controls.put_count(),
            puts_before_recovery,
            "remote replay must remain paused while the database opens over the recovered overlay"
        );

        recovered.lifecycle.activate_remote().unwrap();
        controls.partition_writes(false);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            recovered.lifecycle.wait_remote(32),
        )
        .await
        .expect("activated recovery should drain after the backend heals")
        .unwrap();
        recovered.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_activated_recovery_stops_workers_and_releases_the_journal() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote);
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
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-cancel".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/cancel".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x67; 32],
        };
        let first = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_cancel",
        )
        .await
        .unwrap();
        first
            .store
            .put(
                &Path::from("zerofs/cancel/segments/1"),
                Bytes::from_static(b"segment").into(),
            )
            .await
            .unwrap();
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);

        controls.partition_writes(false);
        controls.block_puts();
        let recovered = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_cancel",
        )
        .await
        .unwrap();
        let puts_before_activation = controls.put_count();
        recovered.lifecycle.activate_remote().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while controls.put_count() == puts_before_activation {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("activated replay should enter the blocked remote operation");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recovered.lifecycle.shutdown(),
        )
        .await
        .expect("shutdown must cancel an in-flight recovery operation")
        .unwrap();
        drop(recovered);
        controls.release_puts();

        let reopened = super::attach(partitioned, settings, identity, "bucket_cancel")
            .await
            .expect("cancelled recovery must release its journal lock");
        reopened.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovered_create_with_different_remote_bytes_is_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let settings = WritebackSettings {
            dir: temp.path().join("dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-divergence".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/divergence".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x68; 32],
        };
        let location = Path::from("zerofs/divergence/manifest/1");
        let first = super::attach(
            remote.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_divergence",
        )
        .await
        .unwrap();
        first
            .store
            .put_opts(
                &location,
                Bytes::from_static(b"locally-durable").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);
        remote
            .put(&location, Bytes::from_static(b"remote-diverged").into())
            .await
            .unwrap();

        let recovered = super::attach(remote, settings, identity, "bucket_divergence")
            .await
            .unwrap();
        recovered.lifecycle.activate_remote().unwrap();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recovered.lifecycle.wait_remote(1),
        )
        .await
        .expect("verified content divergence must not retry forever")
        .expect_err("different bytes at a create target must be terminal");
        assert!(error.to_string().contains("different bytes"));
        assert!(
            recovered
                .lifecycle
                .status()
                .unwrap()
                .terminal_error
                .is_some(),
            "terminal divergence must remain observable after waking waiters"
        );
        let shutdown_error = recovered
            .lifecycle
            .shutdown()
            .await
            .expect_err("shutdown must preserve the remote durability failure");
        assert!(shutdown_error.to_string().contains("different bytes"));
    }

    #[tokio::test]
    async fn remote_flush_shutdown_activates_a_paused_attachment() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let settings = WritebackSettings {
            dir: temp.path().join("dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Remote,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-remote-shutdown".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/remote-shutdown".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x69; 32],
        };
        let location = Path::from("zerofs/remote-shutdown/segments/1");
        let attached = super::attach(remote.clone(), settings, identity, "bucket_remote_shutdown")
            .await
            .unwrap();
        attached
            .store
            .put(&location, Bytes::from_static(b"segment").into())
            .await
            .unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            attached.lifecycle.shutdown(),
        )
        .await
        .expect("remote-flush shutdown must not wait forever on a paused scheduler")
        .unwrap();
        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"segment")
        );
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
            local_concurrency: 4,
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

    #[tokio::test]
    async fn attachment_samples_the_namespaced_writeback_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("dirty");
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 1,
            disk_bytes: 1,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 1,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-space".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/space".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x70; 32],
        };

        let attached = super::attach(
            Arc::new(InMemory::new()),
            settings,
            identity,
            "bucket_space",
        )
        .await
        .unwrap();
        let namespaced = base.join("bucket_space");
        assert_eq!(attached.space.writeback_dir(), namespaced.as_path());
        assert_eq!(attached.space.latest_generation(), 1);
        let sample = attached.space.sample().await.unwrap();
        assert_eq!(sample.generation, 2);
        assert_eq!(attached.ssd.used_bytes(), 0);
        assert_eq!(attached.ssd.used_operations(), 0);
        assert!(
            std::sync::Arc::ptr_eq(&attached.space, attached.lifecycle.space_sampler()),
            "store must use the attach-time space sampler, not a shadow owner"
        );
        assert!(
            std::sync::Arc::ptr_eq(&attached.ssd, attached.lifecycle.ssd_admission()),
            "store must use the attach-time SSD admission owner, not a shadow owner"
        );
        attached.lifecycle.shutdown().await.unwrap();
    }
}
