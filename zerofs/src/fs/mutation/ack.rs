//! Shared write-acknowledgement and durability waits.
//!
//! Protocol adapters (NFS, 9P, NBD) consume the resolved
//! [`FilesystemWriteAckSettings`](super::config::FilesystemWriteAckSettings)
//! through these methods instead of hard-coding SSD or taking a side-channel
//! copy of the configuration.

use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::mutation::config::ClientDurabilityTarget;

impl ZeroFS {
    /// Barrier used by NFS COMMIT, 9P `Tfsync`/`Tfsyncdur`, and NBD FLUSH.
    ///
    /// The resolved [`ClientDurabilityTarget`] is the only durability choice:
    /// adapters must not hard-code SSD. Both targets currently share the
    /// filesystem flush coordinator (SlateDB + optional local writeback hook).
    /// Direct backends without writeback flush to the remote object store
    /// through that coordinator.
    pub(crate) async fn wait_configured_durability(&self) -> Result<(), FsError> {
        match self.write_ack.client_durability_target {
            ClientDurabilityTarget::LocalSsd | ClientDurabilityTarget::RemoteBackend => {
                self.client_fsync().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::config::{
        ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
        FilesystemWriteAckSource,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn settings_with_target(target: ClientDurabilityTarget) -> FilesystemWriteAckSettings {
        FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::Materialized,
            volatile_memory_bytes: 0,
            volatile_max_operations: super::super::config::DEFAULT_VOLATILE_MAX_OPERATIONS,
            source: FilesystemWriteAckSource::DefaultMaterialized,
            client_durability_target: target,
        }
    }

    async fn blocked_local_barrier(fs: &ZeroFS) -> (Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        fs.flush_coordinator.set_local_durability_barrier({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move || {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        });
        (entered, release)
    }

    #[tokio::test]
    async fn wait_configured_durability_observes_local_ssd_target() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = settings_with_target(ClientDurabilityTarget::LocalSsd);
        let fs = Arc::new(fs);
        let (entered, release) = blocked_local_barrier(&fs).await;

        let mut wait = tokio::spawn({
            let fs = Arc::clone(&fs);
            async move { fs.wait_configured_durability().await }
        });
        entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut wait)
                .await
                .is_err(),
            "LocalSsd wait returned before the local durability barrier completed"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("LocalSsd wait did not resume")
            .expect("LocalSsd wait panicked")
            .expect("LocalSsd wait failed");
    }

    #[tokio::test]
    async fn wait_configured_durability_observes_remote_backend_target() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = settings_with_target(ClientDurabilityTarget::RemoteBackend);
        let fs = Arc::new(fs);
        let (entered, release) = blocked_local_barrier(&fs).await;

        let mut wait = tokio::spawn({
            let fs = Arc::clone(&fs);
            async move { fs.wait_configured_durability().await }
        });
        entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut wait)
                .await
                .is_err(),
            "RemoteBackend wait returned before the flush coordinator completed"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("RemoteBackend wait did not resume")
            .expect("RemoteBackend wait panicked")
            .expect("RemoteBackend wait failed");
    }
}
