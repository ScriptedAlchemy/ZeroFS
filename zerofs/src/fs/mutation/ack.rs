//! Shared write-acknowledgement and durability waits.
//!
//! Protocol adapters (NFS, 9P, NBD) consume the resolved
//! [`FilesystemWriteAckSettings`](super::config::FilesystemWriteAckSettings)
//! through these methods instead of hard-coding SSD or taking a side-channel
//! copy of the configuration.

use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::mutation::durability::{DurabilityReceipt, DurabilityTarget};
use crate::fs::mutation::types::MutationCutoff;

impl ZeroFS {
    /// Barrier used by NFS COMMIT, 9P `Tfsync`/`Tfsyncdur`, and NBD FLUSH.
    ///
    /// Adapters convert the resolved client target with `.into()` and never
    /// choose SSD or remote themselves.
    pub(crate) async fn wait_configured_durability(&self) -> Result<(), FsError> {
        if let Some(overlay) = self.volatile_overlay.get() {
            overlay.wait_all().await.map_err(|_| FsError::IoError)?;
        }
        self.durable_to_configured_target(self.capture_mutation_cutoff())
            .await
    }

    pub(crate) async fn wait_inode_durability(&self, id: InodeId) -> Result<(), FsError> {
        if let Some(overlay) = self.volatile_overlay.get() {
            overlay.wait_inode(id).await.map_err(|_| FsError::IoError)?;
        }
        self.durable_to_configured_target(self.capture_mutation_cutoff())
            .await
    }

    /// FUA-style wait: cover every backing inode of one logical write, then
    /// take the normalized durability target once.
    pub(crate) async fn wait_inodes_durability(&self, ids: &[InodeId]) -> Result<(), FsError> {
        if let Some(overlay) = self.volatile_overlay.get() {
            for id in ids {
                overlay
                    .wait_inode(*id)
                    .await
                    .map_err(|_| FsError::IoError)?;
            }
        }
        self.durable_to_configured_target(self.capture_mutation_cutoff())
            .await
    }

    /// FUA-style durability for the exact accepted mutation. A later writer
    /// cannot extend this wait by racing a new global cutoff capture.
    pub(crate) async fn wait_mutation_durability(
        &self,
        cutoff: MutationCutoff,
    ) -> Result<(), FsError> {
        self.durable_to_configured_target(cutoff).await
    }

    /// Administrative flush is an explicit remote-backend barrier. It is not
    /// a client `fsync`, so neither the configured client target nor
    /// `ignore_fsync` may weaken it.
    pub(crate) async fn administrative_remote_durability(
        &self,
    ) -> Result<DurabilityReceipt, FsError> {
        self.flush_coordinator
            .durable_through(
                self.capture_mutation_cutoff(),
                DurabilityTarget::RemoteBackend,
            )
            .await
            .map_err(FsError::from)
    }

    async fn durable_to_configured_target(&self, cutoff: MutationCutoff) -> Result<(), FsError> {
        if self.ignore_fsync {
            return Ok(());
        }
        let target = DurabilityTarget::from(self.write_ack.client_durability_target);
        self.flush_coordinator
            .durable_through(cutoff, target)
            .await
            .map(|_| ())
            .map_err(FsError::from)
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
