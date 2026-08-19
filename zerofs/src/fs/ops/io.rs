//! File data plane: write, read, trim.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, FallocateMode, FileAttributes, InodeWithId};
use crate::fs::{ZeroFS, get_current_time};
use ::tracing::{debug, error};
use bytes::Bytes;
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Read up to `count` bytes at `offset`. The bool is EOF: true when the
    /// read reached the end of the file; at or past EOF it is `(empty, true)`.
    pub async fn read_file(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        self.read_file_visible(Some(auth), id, offset, count).await
    }

    /// Read through a fid whose read access was already authorized at open.
    #[allow(dead_code)] // Used by the binary-only 9P handler.
    pub(crate) async fn read_file_opened(
        &self,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        self.read_file_visible(None, id, offset, count).await
    }

    pub(crate) async fn read_file_inner_canonical(
        &self,
        auth: Option<&AuthContext>,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        debug!("read_file: id={}, offset={}, count={}", id, offset, count);

        let inode = self.inode_store.get(id).await?;

        if let Some(auth) = auth {
            let creds = Credentials::from_auth_context(auth);
            check_access(&inode, &creds, AccessMode::Read)?;
        }

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    self.tracer.emit(
                        &self.inode_store,
                        id,
                        FileOperation::Read { offset, length: 0 },
                    );
                    return Ok((Bytes::new(), true));
                }

                let read_len = std::cmp::min(count as u64, file.size - offset);
                let result_bytes = self.extent_store.read(id, offset, read_len).await?;
                let eof = offset + read_len >= file.size;

                self.stats
                    .bytes_read
                    .fetch_add(result_bytes.len() as u64, Ordering::Relaxed);
                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    id,
                    FileOperation::Read {
                        offset,
                        length: read_len,
                    },
                );

                Ok((result_bytes, eof))
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    /// Punch a hole: zero `[offset, offset + length)` in place without
    /// changing the file size.
    pub async fn trim(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Result<(), FsError> {
        if length == 0 {
            return Ok(());
        }

        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;
        let creds = Credentials::from_auth_context(auth);

        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(FsError::IsDirectory),
        }

        let file_size = match &inode {
            Inode::File(file) => file.size,
            _ => unreachable!(),
        };
        offset.checked_add(length).ok_or(FsError::InvalidArgument)?;

        let mut txn = self.db.new_transaction()?;
        self.extent_store
            .zero_range(&mut txn, id, offset, length, file_size)
            .await?;
        self.write_coordinator.commit(txn).await.inspect_err(|e| {
            error!("Failed to commit trim batch: {}", e);
        })?;

        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);
        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Trim { offset, length },
        );

        Ok(())
    }

    /// Atomically allocate, punch, or zero a file range through a fid whose
    /// write access was authorized at open.
    ///
    /// ZeroFS represents zero-filled ranges as sparse holes. Its allocation
    /// guarantee comes from charging logical growth against quota, so a later
    /// write inside the file does not consume additional quota.
    #[allow(dead_code)] // Used by the binary-only 9P handler.
    pub(crate) async fn fallocate_opened(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
    ) -> Result<FileAttributes, FsError> {
        self.fallocate_idempotent_inner(auth, id, offset, length, mode, [0u8; 16])
            .await
    }

    /// Idempotent fallocate through an already-authorized opened fid.
    #[allow(dead_code)] // Used by the binary-only 9P handler.
    pub(crate) async fn fallocate_opened_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        self.fallocate_idempotent_inner(auth, id, offset, length, mode, op_id)
            .await
    }

    async fn fallocate_idempotent_inner(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_fallocate)? {
            return Ok(result);
        }
        debug!(
            "Processing fallocate on inode {} at offset {} length {} mode {:?}",
            id, offset, length, mode
        );

        if length == 0 {
            return Err(FsError::InvalidArgument);
        }
        let end = offset.checked_add(length).ok_or(FsError::InvalidArgument)?;

        let _guard = self.lock_manager.acquire(id).await;
        // Direct filesystem callers do not pass through the 9P single-flight.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_fallocate)? {
            return Ok(result);
        }
        let mut inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        if !matches!(&inode, Inode::File(_)) {
            return Err(FsError::IsDirectory);
        }

        let (old_size, parent_name_for_update) = match &inode {
            Inode::File(file) => (file.size, file.parent.zip(file.name.clone())),
            _ => unreachable!(),
        };

        let new_size = match mode {
            FallocateMode::Allocate | FallocateMode::ZeroRange { keep_size: false } => {
                old_size.max(end)
            }
            FallocateMode::PunchHole | FallocateMode::ZeroRange { keep_size: true } => old_size,
        };
        let zeroes_range = !matches!(mode, FallocateMode::Allocate);
        if new_size > old_size {
            let increase = new_size - old_size;
            let (used_bytes, _) = self.global_stats.get_totals();
            if used_bytes.saturating_add(increase) > self.max_bytes {
                return Err(FsError::NoSpace);
            }
        }

        let mut txn = self.db.new_transaction()?;

        if zeroes_range && offset < old_size {
            let zero_length = end.min(old_size) - offset;
            self.extent_store
                .zero_range(&mut txn, id, offset, zero_length, old_size)
                .await?;
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_EXTENTS);

        if let Inode::File(file) = &mut inode {
            file.size = new_size;
            let (now_sec, now_nsec) = get_current_time();
            file.mtime = now_sec;
            file.mtime_nsec = now_nsec;
            file.ctime = now_sec;
            file.ctime_nsec = now_nsec;

            // Match the write path: a non-owner changing file state must not
            // leave a privileged executable carrying stale SUID/SGID bits.
            if creds.uid != file.uid && creds.uid != 0 {
                file.mode &= !0o6000;
            }
        }

        self.inode_store.save(&mut txn, id, &inode)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_INODE);

        if let Some((parent_id, name)) = parent_name_for_update {
            self.directory_store
                .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                .await?;
        }
        let post_attrs: FileAttributes = InodeWithId { inode: &inode, id }.into();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Fallocate {
                attrs: post_attrs.clone(),
            },
        );

        self.write_coordinator.commit(txn).await.inspect_err(|e| {
            error!("Failed to commit fallocate batch: {}", e);
        })?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_COMMIT);

        debug!("Fallocate completed successfully for inode {}", id);

        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Fallocate {
                offset,
                length,
                mode: mode.linux_mode(),
            },
        );

        Ok(post_attrs)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "failpoints")]
    use crate::failpoints as fp;
    use crate::fs::inode::Inode;
    use crate::fs::test_util::test_creds;
    use crate::fs::tracing::FileOperation;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;
    #[cfg(feature = "failpoints")]
    use std::sync::Arc;

    use crate::fs::types::{
        AuthContext, FallocateMode, FileAttributes, InodeWithId, SetAttributes, SetMode, SetSize,
        SetTime, Timestamp,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn test_process_write_and_read() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(data),
            )
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);
        let empty = fs
            .write(&(&test_auth()).into(), file_id, 1_000_000, &Bytes::new())
            .await
            .unwrap();
        assert_eq!(
            empty.size, fattr.size,
            "an empty write beyond EOF must not grow the file"
        );
        assert_eq!(
            empty.mtime, fattr.mtime,
            "an empty write must not update timestamps"
        );

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), data);
        assert!(eof);
    }

    #[tokio::test]
    async fn write_retry_replays_result_without_overwriting_a_later_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"retry.txt", &SetAttributes::default())
            .await
            .unwrap();
        let op_id = [0x41; 16];
        let auth: AuthContext = (&test_auth()).into();

        let original = fs
            .write_idempotent(&auth, file_id, 0, &Bytes::from_static(b"first"), op_id)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later"))
            .await
            .unwrap();

        let replayed = fs
            .write_idempotent(&auth, file_id, 0, &Bytes::from_static(b"first"), op_id)
            .await
            .unwrap();
        assert_eq!(replayed.size, original.size);
        assert_eq!(replayed.mtime, original.mtime);
        let (data, _) = fs.read_file(&auth, file_id, 0, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"later");
    }

    #[tokio::test]
    async fn empty_write_retry_replays_typed_result_without_overwriting_a_later_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(
                &test_creds(),
                0,
                b"empty-retry.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        let op_id = [0x42; 16];
        let auth: AuthContext = (&test_auth()).into();

        let original = fs
            .write_idempotent(&auth, file_id, 1_000_000, &Bytes::new(), op_id)
            .await
            .unwrap();
        assert_eq!(original.size, 0);
        assert!(matches!(
            fs.dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Write { ref attrs })
                if attrs.size == original.size && attrs.mtime == original.mtime
        ));

        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later"))
            .await
            .unwrap();

        let replayed = fs
            .write_idempotent(&auth, file_id, 1_000_000, &Bytes::new(), op_id)
            .await
            .unwrap();
        assert_eq!(replayed.size, original.size);
        assert_eq!(replayed.mtime, original.mtime);
        let (data, _) = fs.read_file(&auth, file_id, 0, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"later");
    }

    /// The observable attribute fields a write can change, for comparison
    /// (`FileAttributes` itself is not `PartialEq`).
    fn attr_fingerprint(attrs: &FileAttributes) -> (u32, u32, u32, u64, Timestamp, Timestamp) {
        (
            attrs.mode,
            attrs.uid,
            attrs.gid,
            attrs.size,
            attrs.mtime,
            attrs.ctime,
        )
    }

    /// Attributes as a fresh reader would see them, straight from the inode.
    async fn persisted_attrs(fs: &ZeroFS, id: InodeId) -> FileAttributes {
        let inode = fs.inode_store.get(id).await.unwrap();
        InodeWithId { inode: &inode, id }.into()
    }

    /// Attributes as `readdir` reports them, i.e. from the parent directory
    /// entry's embedded inode copy.
    async fn directory_entry_attrs(fs: &ZeroFS, dir: InodeId, name: &[u8]) -> FileAttributes {
        let auth: AuthContext = (&test_auth()).into();
        let listing = fs.readdir(&auth, dir, 0, 64).await.unwrap();
        listing
            .entries
            .into_iter()
            .find(|entry| entry.name == name)
            .expect("entry present in listing")
            .attr
    }

    /// Park until just after a wall-clock second boundary so a short burst of
    /// writes lands inside one second.
    async fn wait_for_second_boundary() {
        let (_, nsec) = get_current_time();
        let remaining = 1_000_000_000u32.saturating_sub(nsec);
        tokio::time::sleep(std::time::Duration::from_nanos(
            u64::from(remaining) + 2_000_000,
        ))
        .await;
    }

    /// The NBD steady state: `O_DIRECT` overwrites that change neither the
    /// file size nor the second the timestamps fall in. Those writes must not
    /// republish inode metadata, so the timestamps they report are the ones
    /// already durable.
    #[tokio::test]
    async fn in_place_overwrite_within_one_second_reuses_persisted_timestamps() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"nbd.img", &SetAttributes::default())
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();

        // Grow once so every later write is a pure in-place overwrite.
        fs.write(&auth, file_id, 0, &Bytes::from(vec![0xAA; 4096]))
            .await
            .unwrap();

        let (first, second) = loop {
            wait_for_second_boundary().await;
            let first = fs
                .write(&auth, file_id, 0, &Bytes::from(vec![0xBB; 4096]))
                .await
                .unwrap();
            let second = fs
                .write(&auth, file_id, 0, &Bytes::from(vec![0xCC; 4096]))
                .await
                .unwrap();
            if first.mtime.seconds == second.mtime.seconds {
                break (first, second);
            }
        };

        assert_eq!(
            first.mtime, second.mtime,
            "an in-place overwrite inside one second must not republish mtime"
        );
        assert_eq!(
            first.ctime, second.ctime,
            "an in-place overwrite inside one second must not republish ctime"
        );
        assert_eq!(second.size, 4096);

        // What the write returned is exactly what is durable, and the parent
        // directory entry agrees with it.
        assert_eq!(
            attr_fingerprint(&persisted_attrs(&fs, file_id).await),
            attr_fingerprint(&second)
        );
        assert_eq!(
            attr_fingerprint(&directory_entry_attrs(&fs, 0, b"nbd.img").await),
            attr_fingerprint(&second)
        );

        let (data, _) = fs.read_file(&auth, file_id, 0, 4096).await.unwrap();
        assert_eq!(data.as_ref(), vec![0xCC; 4096].as_slice());
    }

    /// A write that grows the file changes something a reader can see, so it
    /// must publish both the inode and the directory entry copy.
    #[tokio::test]
    async fn a_growing_write_always_republishes_metadata() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"growing.txt", &SetAttributes::default())
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();

        wait_for_second_boundary().await;
        let first = fs
            .write(&auth, file_id, 0, &Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        let second = fs
            .write(&auth, file_id, 0, &Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();

        assert_eq!(second.size, 8);
        assert_ne!(
            first.mtime, second.mtime,
            "a size change must publish a fresh mtime"
        );
        assert_eq!(
            attr_fingerprint(&persisted_attrs(&fs, file_id).await),
            attr_fingerprint(&second)
        );
        assert_eq!(
            directory_entry_attrs(&fs, 0, b"growing.txt").await.size,
            8,
            "readdir must not report a stale size"
        );
    }

    /// Once the clock leaves the second the durable timestamps were taken in,
    /// even a same-size overwrite has to refresh them.
    #[tokio::test]
    async fn an_overwrite_in_a_later_second_refreshes_timestamps() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"stale.img", &SetAttributes::default())
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"abcd"))
            .await
            .unwrap();

        let (now_sec, _) = get_current_time();
        let backdated = Timestamp {
            seconds: now_sec - 60,
            nanoseconds: 0,
        };
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                mtime: SetTime::SetToClientTime(backdated),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let attrs = fs
            .write(&auth, file_id, 0, &Bytes::from_static(b"wxyz"))
            .await
            .unwrap();

        assert!(
            attrs.mtime.seconds >= now_sec,
            "a write in a later second must refresh mtime: {:?}",
            attrs.mtime
        );
        assert_eq!(
            attr_fingerprint(&persisted_attrs(&fs, file_id).await),
            attr_fingerprint(&attrs)
        );
        assert_eq!(
            attr_fingerprint(&directory_entry_attrs(&fs, 0, b"stale.img").await),
            attr_fingerprint(&attrs),
            "the directory entry copy must be refreshed too"
        );
    }

    /// SUID/SGID clearing is a mode change, so it always publishes even when
    /// the size and timestamp second are unchanged.
    #[tokio::test]
    async fn an_in_place_overwrite_still_clears_suid_for_a_non_owner() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"suid.bin", &SetAttributes::default())
            .await
            .unwrap();
        let owner: AuthContext = (&test_auth()).into();
        fs.write(&owner, file_id, 0, &Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                mode: SetMode::Set(0o6777),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let other = AuthContext {
            uid: 1001,
            gid: 1001,
            ..Default::default()
        };
        let attrs = fs
            .write(&other, file_id, 0, &Bytes::from_static(b"wxyz"))
            .await
            .unwrap();

        assert_eq!(
            attrs.mode & 0o6000,
            0,
            "a non-owner write must clear SUID/SGID"
        );
        assert_eq!(
            attr_fingerprint(&persisted_attrs(&fs, file_id).await),
            attr_fingerprint(&attrs)
        );
        assert_eq!(
            directory_entry_attrs(&fs, 0, b"suid.bin").await.mode & 0o6000,
            0,
            "the directory entry copy must lose SUID/SGID too"
        );
    }

    /// Sequential in-place overwrite through `fs.write`, the NBD member-chunk
    /// shape. Run with `--ignored --nocapture`.
    #[tokio::test]
    #[ignore = "benchmark"]
    async fn bench_sequential_in_place_overwrite() {
        const ITERATIONS: usize = 2_000;

        for chunk_size in [512usize, 4096, 64 * 1024] {
            let fs = ZeroFS::new_in_memory().await.unwrap();
            let (file_id, _) = fs
                .create(&test_creds(), 0, b"bench.img", &SetAttributes::default())
                .await
                .unwrap();
            let auth: AuthContext = (&test_auth()).into();
            let chunk = Bytes::from(vec![0x5A; chunk_size]);
            fs.write(&auth, file_id, 0, &chunk).await.unwrap();

            let start = std::time::Instant::now();
            for _ in 0..ITERATIONS {
                fs.write(&auth, file_id, 0, &chunk).await.unwrap();
            }
            let elapsed = start.elapsed();
            println!(
                "in-place overwrite: {ITERATIONS} x {chunk_size}B in {elapsed:?} ({:?}/op, {:.0} ops/s)",
                elapsed / ITERATIONS as u32,
                ITERATIONS as f64 / elapsed.as_secs_f64()
            );
        }
    }

    #[tokio::test]
    async fn fallocate_retry_does_not_zero_a_later_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(
                &test_creds(),
                0,
                b"range-retry.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"first"))
            .await
            .unwrap();
        let op_id = [0x43; 16];
        let mode = FallocateMode::ZeroRange { keep_size: true };

        let original = fs
            .fallocate_opened_idempotent(&auth, file_id, 0, 5, mode, op_id)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later"))
            .await
            .unwrap();

        let replayed = fs
            .fallocate_opened_idempotent(&auth, file_id, 0, 5, mode, op_id)
            .await
            .unwrap();
        assert_eq!(replayed.mtime, original.mtime);
        let (data, _) = fs.read_file(&auth, file_id, 0, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"later");
    }

    #[tokio::test]
    async fn write_offset_length_overflow_is_rejected() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"f.txt", &SetAttributes::default())
            .await
            .unwrap();

        for off in [u64::MAX - 4, u64::MAX - 99, u64::MAX] {
            let r = fs
                .write(
                    &(&test_auth()).into(),
                    file_id,
                    off,
                    &Bytes::from(vec![1u8; 100]),
                )
                .await;
            assert!(
                matches!(r, Err(FsError::InvalidArgument)),
                "write at offset {off} must be EINVAL, got {r:?}"
            );
        }
        // The rejected writes left the file untouched.
        let size = match fs.inode_store.get(file_id).await.unwrap() {
            Inode::File(f) => f.size,
            _ => panic!("expected a file"),
        };
        assert_eq!(size, 0, "a rejected overflow write must not grow the file");

        // trim's offset+length overflow is likewise rejected.
        let r = fs
            .trim(&(&test_auth()).into(), file_id, u64::MAX - 4, 100)
            .await;
        assert!(
            matches!(r, Err(FsError::InvalidArgument)),
            "trim overflow must be EINVAL, got {r:?}"
        );
    }

    #[tokio::test]
    async fn fallocate_allocate_grows_without_overwriting() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"allocate.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"data"))
            .await
            .unwrap();

        let attrs = fs
            .fallocate_opened(&auth, file_id, 8, 8, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_eq!(attrs.size, 16);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert_eq!(&data[..4], b"data");
        assert!(data[4..].iter().all(|&b| b == 0));

        let attrs = fs
            .fallocate_opened(&auth, file_id, 1, 2, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_eq!(attrs.size, 16, "allocation inside EOF must not shrink");
    }

    #[tokio::test]
    async fn fallocate_reserves_logical_quota_for_later_writes() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.max_bytes = 8;
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"quota.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();

        fs.fallocate_opened(&auth, file_id, 0, 8, FallocateMode::Allocate)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"12345678"))
            .await
            .expect("an overwrite inside the reservation consumes no new quota");
        let result = fs
            .fallocate_opened(&auth, file_id, 8, 1, FallocateMode::Allocate)
            .await;
        assert!(matches!(result, Err(FsError::NoSpace)));
    }

    #[tokio::test]
    async fn fallocate_punch_and_zero_range_semantics() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"ranges.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"abcdefghijkl"))
            .await
            .unwrap();

        let attrs = fs
            .fallocate_opened(&auth, file_id, 2, 3, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_eq!(attrs.size, 12);
        let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
        assert_eq!(&data[..2], b"ab");
        assert_eq!(&data[2..5], &[0; 3]);
        assert_eq!(&data[5..], b"fghijkl");

        let attrs = fs
            .fallocate_opened(
                &auth,
                file_id,
                8,
                8,
                FallocateMode::ZeroRange { keep_size: true },
            )
            .await
            .unwrap();
        assert_eq!(attrs.size, 12, "KEEP_SIZE must not cross EOF");

        let attrs = fs
            .fallocate_opened(
                &auth,
                file_id,
                8,
                8,
                FallocateMode::ZeroRange { keep_size: false },
            )
            .await
            .unwrap();
        assert_eq!(attrs.size, 16);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert!(data[8..].iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn fallocate_updates_metadata_when_data_and_size_are_unchanged() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"metadata.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"data"))
            .await
            .unwrap();

        let old_mtime = Timestamp {
            seconds: 1,
            nanoseconds: 0,
        };
        let reset_mtime = SetAttributes {
            mtime: SetTime::SetToClientTime(old_mtime),
            ..SetAttributes::default()
        };

        fs.setattr(&test_creds(), file_id, &reset_mtime)
            .await
            .unwrap();
        let attrs = fs
            .fallocate_opened(&auth, file_id, 0, 1, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_ne!(
            attrs.mtime, old_mtime,
            "allocation inside EOF updates mtime"
        );

        fs.setattr(&test_creds(), file_id, &reset_mtime)
            .await
            .unwrap();
        let attrs = fs
            .fallocate_opened(&auth, file_id, 100, 1, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_ne!(
            attrs.mtime, old_mtime,
            "hole punching beyond EOF still updates mtime"
        );
    }

    #[tokio::test]
    async fn fallocate_by_non_owner_clears_suid_and_sgid() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let attrs = SetAttributes {
            mode: crate::fs::types::SetMode::Set(0o6777),
            ..SetAttributes::default()
        };
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"privileged.bin", &attrs)
            .await
            .unwrap();
        let owner_auth = (&test_auth()).into();
        fs.write(
            &owner_auth,
            file_id,
            0,
            &Bytes::from_static(b"privileged data"),
        )
        .await
        .unwrap();

        let non_owner = AuthContext {
            uid: 2000,
            gid: 2000,
            gid_known: true,
            gids: Vec::new(),
            groups_complete: true,
        };
        let attrs = fs
            .fallocate_opened(&non_owner, file_id, 0, 1, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_eq!(attrs.mode & 0o6000, 0);
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn fallocate_failpoints_cover_transaction_stages() {
        let _scenario = fail::FailScenario::setup();
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let (file_id, _) = fs
            .create(
                &test_creds(),
                0,
                b"fallocate-failpoints.bin",
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        let original = Bytes::from_static(b"abcdefghijkl");
        fs.write(&auth, file_id, 0, &original).await.unwrap();

        for (point, offset) in [
            (fp::FALLOCATE_AFTER_EXTENTS, 0),
            (fp::FALLOCATE_AFTER_INODE, 4),
        ] {
            fail::cfg(point, "panic").unwrap();
            let fs_clone = Arc::clone(&fs);
            let auth_clone = auth.clone();
            let result = tokio::spawn(async move {
                fs_clone
                    .fallocate_opened(&auth_clone, file_id, offset, 2, FallocateMode::PunchHole)
                    .await
            })
            .await;
            fail::cfg(point, "off").unwrap();

            assert!(result.unwrap_err().is_panic(), "{point} must be reached");
            let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
            assert_eq!(
                data, original,
                "pre-commit crash must discard the transaction"
            );
        }

        fail::cfg(fp::FALLOCATE_AFTER_COMMIT, "panic").unwrap();
        let fs_clone = Arc::clone(&fs);
        let auth_clone = auth.clone();
        let result = tokio::spawn(async move {
            fs_clone
                .fallocate_opened(&auth_clone, file_id, 8, 2, FallocateMode::PunchHole)
                .await
        })
        .await;
        fail::cfg(fp::FALLOCATE_AFTER_COMMIT, "off").unwrap();

        assert!(
            result.unwrap_err().is_panic(),
            "post-commit failpoint must be reached"
        );
        let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
        assert_eq!(&data[..8], b"abcdefgh");
        assert_eq!(&data[8..10], &[0, 0]);
        assert_eq!(&data[10..], b"kl");
    }

    #[tokio::test]
    async fn trim_preserves_timestamps_and_zeroes_the_requested_range() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"trim.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();

        let old_mtime = Timestamp {
            seconds: 1,
            nanoseconds: 0,
        };
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                mtime: SetTime::SetToClientTime(old_mtime),
                ..SetAttributes::default()
            },
        )
        .await
        .unwrap();

        fs.trim(&auth, file_id, 1, 2).await.unwrap();
        let inode = fs.inode_store.get(file_id).await.unwrap();
        let attrs: FileAttributes = InodeWithId {
            inode: &inode,
            id: file_id,
        }
        .into();
        assert_eq!(
            attrs.mtime, old_mtime,
            "NBD trim must not rewrite inode metadata"
        );
        let (data, _) = fs.read_file(&auth, file_id, 0, 8).await.unwrap();
        assert_eq!(&data[..], b"a\0\0defgh");

        let mut events = fs.tracer.subscribe();
        fs.fallocate_opened(&auth, file_id, 0, 1, FallocateMode::Allocate)
            .await
            .unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.operation,
            FileOperation::Fallocate {
                offset: 0,
                length: 1,
                mode: 0
            }
        ));
    }

    #[tokio::test]
    async fn test_process_write_partial_extents() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data1),
        )
        .await
        .unwrap();

        let data2 = vec![b'B'; 50];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            50,
            &Bytes::copy_from_slice(&data2),
        )
        .await
        .unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_extents() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"bigfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let extent_size = EXTENT_SIZE;
        let data = vec![b'X'; extent_size * 2 + 1024];

        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(&data),
            )
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), &data[..]);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_read_beyond_truncated_extent() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![b'A'; 300 * 1024];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(100 * 1024),
            ..Default::default()
        };
        fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 200 * 1024, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 0);
    }

    #[tokio::test]
    async fn test_tail_cache_sequential_append_matches() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"seq.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Small sequential appends that cross extent boundaries, so the tail extent
        // fills and rolls over repeatedly: every append into a partially-filled
        // extent takes the cached-tail splice path instead of re-reading.
        let step = 5000usize;
        let total = EXTENT_SIZE * 3 + 1234;
        let mut expected = Vec::with_capacity(total);
        let mut offset = 0u64;
        while expected.len() < total {
            let n = step.min(total - expected.len());
            let extent: Vec<u8> = (0..n)
                .map(|i| ((offset as usize + i) % 251) as u8)
                .collect();
            fs.write(
                &(&test_auth()).into(),
                file_id,
                offset,
                &Bytes::copy_from_slice(&extent),
            )
            .await
            .unwrap();
            expected.extend_from_slice(&extent);
            offset += n as u64;
        }

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, total as u32)
            .await
            .unwrap();
        assert_eq!(&read_data[..], &expected[..]);
    }

    #[tokio::test]
    async fn test_tail_cache_invalidated_by_truncate() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"trunc.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Build a partial tail extent; this populates the tail cache.
        let a = vec![b'A'; 1000];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&a),
        )
        .await
        .unwrap();

        // Shrink into that extent. truncate must drop the cache, else the next
        // append splices onto the stale pre-truncate bytes.
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                size: SetSize::Set(800),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Append past the hole the truncate left. Bytes 800..900 must read back as
        // zeros, not the 'A's a non-invalidated cache would carry forward.
        let b = vec![b'B'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            900,
            &Bytes::copy_from_slice(&b),
        )
        .await
        .unwrap();

        let (gap, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 800, 100)
            .await
            .unwrap();
        assert_eq!(
            gap,
            vec![0u8; 100],
            "truncate must invalidate the tail cache"
        );

        let (tail, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 900, 100)
            .await
            .unwrap();
        assert_eq!(tail, b);
    }

    #[tokio::test]
    async fn test_tail_cache_sparse_write_creates_hole_without_corruption() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"sparse.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Partial tail in extent 0 -> cache holds extent 0.
        let a = vec![b'A'; 1000];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&a),
        )
        .await
        .unwrap();

        // Write far past EOF, leaving a hole. The target extent is beyond_eof, so it
        // must build on zeros, never on the cached extent-0 bytes.
        let far = 6 * EXTENT_SIZE as u64 + 500;
        let b = vec![b'B'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            far,
            &Bytes::copy_from_slice(&b),
        )
        .await
        .unwrap();

        // The bytes before the far write within its own extent are part of the hole:
        // must be zeros, not the cached 'A's.
        let (lead, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 6 * EXTENT_SIZE as u64, 500)
            .await
            .unwrap();
        assert_eq!(
            lead,
            vec![0u8; 500],
            "hole extent must not inherit cached bytes"
        );

        // The hole between the two writes reads as zeros.
        let (hole, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 3 * EXTENT_SIZE as u64, 256)
            .await
            .unwrap();
        assert_eq!(hole, vec![0u8; 256]);

        // The original tail and the far write are both intact.
        let (head, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(head, a);
        let (tail, _) = fs
            .read_file(&(&test_auth()).into(), file_id, far, 100)
            .await
            .unwrap();
        assert_eq!(tail, b);

        // Back-fill into the old extent 0; it must read extent 0 from the store (the
        // cache moved to the far extent), not splice onto a stale entry.
        let c = vec![b'C'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            500,
            &Bytes::copy_from_slice(&c),
        )
        .await
        .unwrap();
        let (head2, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        let mut want = vec![b'A'; 1000];
        want[500..600].fill(b'C');
        assert_eq!(head2, want);
    }

    async fn file_size(fs: &ZeroFS, id: crate::fs::inode::InodeId) -> u64 {
        match fs.inode_store.get(id).await.unwrap() {
            Inode::File(file) => file.size,
            _ => panic!("expected a file inode"),
        }
    }

    /// The write path releases its inode lock at submit, so the size a writer
    /// reads comes from the queued value rather than from the read cache. A
    /// commit that fails before its apply must retract that value and leave
    /// the last committed size standing: a successor that read the retracted
    /// one would compute `max(phantom, its own end)` and drop a real write.
    ///
    /// This is the sequential case, where the failure resolves before the next
    /// writer reads. A successor that is already staging when the failure
    /// lands has necessarily read the queued size and keeps it; that case is
    /// deliberate and pinned by
    /// `queued_batch_that_fails_leaves_the_counter_matching_durable_sizes`.
    #[tokio::test]
    async fn a_failed_commit_between_two_writes_cannot_shrink_the_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let auth: AuthContext = (&test_auth()).into();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"pipelined", &SetAttributes::default())
            .await
            .unwrap();

        let first = Bytes::from(vec![b'A'; 64 * 1024]);
        fs.write(&auth, file_id, 0, &first).await.unwrap();
        assert_eq!(file_size(&fs, file_id).await, 64 * 1024);

        // Inject a pre-apply failure that also carries an inode mutation for
        // this file, exactly as a failing write would: an unreadable segment
        // counter aborts the batch in stage_seg_deltas, before any apply.
        let codec = crate::fs::key_codec::KeyCodec::new();
        let poisoned = codec.segcount_key(9, 9);
        fs.db
            .put_with_options(
                &poisoned,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &slatedb::config::WriteOptions::default(),
            )
            .await
            .unwrap();
        let mut doomed = fs.db.new_transaction().unwrap();
        let mut phantom = match fs.inode_store.get(file_id).await.unwrap() {
            Inode::File(file) => file,
            _ => panic!("expected a file inode"),
        };
        phantom.size = 99;
        fs.inode_store
            .save(&mut doomed, file_id, &Inode::File(phantom))
            .unwrap();
        doomed.add_seg_delta(&poisoned, 1, 1);
        fs.write_coordinator.commit(doomed).await.unwrap_err();

        assert_eq!(
            file_size(&fs, file_id).await,
            64 * 1024,
            "a batch that failed before its apply must not leave its inode visible"
        );

        // A later write below the old EOF must extend nothing and shrink
        // nothing; it would report 4096 if it had read the retracted size.
        let second = Bytes::from(vec![b'B'; 4096]);
        let attrs = fs.write(&auth, file_id, 0, &second).await.unwrap();
        assert_eq!(
            attrs.size,
            64 * 1024,
            "the write after the failed commit lost the committed size"
        );
        assert_eq!(file_size(&fs, file_id).await, 64 * 1024);

        let (head, _) = fs.read_file(&auth, file_id, 0, 4096).await.unwrap();
        assert_eq!(head, second);
        let (tail, _) = fs.read_file(&auth, file_id, 60 * 1024, 1024).await.unwrap();
        assert_eq!(tail, first.slice(0..1024));
    }

    /// The point of releasing the lock at submit: a second write to the same
    /// inode stages and queues while the first write is still awaiting its
    /// commit. Held under the old structure, the first write's lock would keep
    /// the second one from even reading the inode until the apply finished.
    #[tokio::test]
    async fn a_second_write_queues_while_the_first_is_still_committing() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth: AuthContext = (&test_auth()).into();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"striped", &SetAttributes::default())
            .await
            .unwrap();

        const CHUNK: usize = 256 * 1024;
        // Stall the first write inside its apply, at the write permit.
        let stalled_apply = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();

        let first_fs = Arc::clone(&fs);
        let first_auth = auth.clone();
        let first = tokio::spawn(async move {
            first_fs
                .write(&first_auth, file_id, 0, &Bytes::from(vec![b'A'; CHUNK]))
                .await
        });
        apply_reached.await.unwrap();

        // The first write is submitted and its apply is blocked. Its inode
        // lock is already gone, so a disjoint second write must be able to run
        // its whole staging path and queue behind it.
        let second_fs = Arc::clone(&fs);
        let second_auth = auth.clone();
        let second = tokio::spawn(async move {
            second_fs
                .write(
                    &second_auth,
                    file_id,
                    CHUNK as u64,
                    &Bytes::from(vec![b'B'; CHUNK]),
                )
                .await
        });

        // Queueing publishes the second write's inode, which is the observable
        // proof that it got past the lock, read the first write's size, staged
        // its extents and submitted -- all while the first commit is in flight.
        let queued = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(Some(Inode::File(file))) = fs.inode_store.pending_inode(file_id)
                    && file.size == 2 * CHUNK as u64
                {
                    return file.size;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second write never queued while the first was committing");
        assert_eq!(queued, 2 * CHUNK as u64);

        drop(stalled_apply);
        first.await.unwrap().unwrap();
        let second_attrs = second.await.unwrap().unwrap();

        assert_eq!(second_attrs.size, 2 * CHUNK as u64);
        assert_eq!(file_size(&fs, file_id).await, 2 * CHUNK as u64);
        let (head, _) = fs.read_file(&auth, file_id, 0, CHUNK as u32).await.unwrap();
        assert_eq!(head, Bytes::from(vec![b'A'; CHUNK]));
        let (tail, _) = fs
            .read_file(&auth, file_id, CHUNK as u64, CHUNK as u32)
            .await
            .unwrap();
        assert_eq!(tail, Bytes::from(vec![b'B'; CHUNK]));
    }

    // Single-inode write latency, the shape the per-inode lock used to cap.
    //
    // Both arms write the same total bytes to one file at disjoint,
    // extent-aligned offsets. The serial arm awaits each write, so it pays
    // staging plus the full commit round trip every time and is unaffected by
    // the locking structure. The pipelined arm keeps several writes in flight,
    // which is only possible while the inode lock is not held across the
    // commit -- when it is, the concurrent arm collapses onto the serial one.
    // The gap between the two arms is the lock-hold portion of the latency.
    //   cargo test --release --lib -- --ignored --nocapture bench_single_inode
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "latency measurement, run explicitly in release"]
    async fn bench_single_inode_write_latency() {
        const CHUNK: usize = 256 * 1024;
        const WRITES: usize = 128;
        const IN_FLIGHT: usize = 8;

        // `files == 1` puts every in-flight write on one inode and so through
        // one lock; `files == IN_FLIGHT` is the identical workload with the
        // per-inode lock removed from the picture. Task counts, wave shape and
        // total bytes are the same in both, so the difference between them is
        // the per-inode serialisation and nothing else.
        async fn arm(files: usize, sync_writes: bool) -> (std::time::Duration, u64) {
            let fs = Arc::new(
                ZeroFS::new_in_memory_with_sync_writes(sync_writes)
                    .await
                    .unwrap(),
            );
            let auth: AuthContext = (&test_auth()).into();
            let mut ids = Vec::with_capacity(files);
            for index in 0..files {
                let (id, _) = fs
                    .create(
                        &test_creds(),
                        0,
                        format!("bench{index}").as_bytes(),
                        &SetAttributes::default(),
                    )
                    .await
                    .unwrap();
                ids.push(id);
            }
            let payload = Bytes::from(vec![b'Z'; CHUNK]);

            let start = std::time::Instant::now();
            let mut wave = Vec::with_capacity(IN_FLIGHT);
            for index in 0..WRITES {
                let file_id = ids[index % files];
                // Disjoint, extent-aligned offsets within each file.
                let at = (index / files * CHUNK) as u64;
                let task_fs = Arc::clone(&fs);
                let task_auth = auth.clone();
                let task_payload = payload.clone();
                wave.push(tokio::spawn(async move {
                    task_fs
                        .write(&task_auth, file_id, at, &task_payload)
                        .await
                        .unwrap();
                }));
                if wave.len() == IN_FLIGHT {
                    for task in wave.drain(..) {
                        task.await.unwrap();
                    }
                }
            }
            for task in wave {
                task.await.unwrap();
            }
            let elapsed = start.elapsed();
            let apply_nanos = fs.write_coordinator.apply_nanos();
            (elapsed, apply_nanos)
        }

        let report = |label: &str, elapsed: std::time::Duration, apply_nanos: u64| {
            let bytes = (WRITES * CHUNK) as f64;
            println!(
                "{label:>12}: {:>7.0} us/write, {:>7.1} MB/s, commit worker busy {:.0}%",
                elapsed.as_micros() as f64 / WRITES as f64,
                bytes / elapsed.as_secs_f64() / 1e6,
                apply_nanos as f64 / elapsed.as_nanos() as f64 * 100.0,
            );
        };

        // Buffered writes reply as soon as the batch is in the memtable, so the
        // commit round trip is nearly free and staging dominates; durable
        // writes make the reply wait for a flush, which is the shape the
        // production config has and the one the inode lock used to hold
        // across. Both are reported because only the second should move.
        for (label, sync_writes) in [("buffered", false), ("durable", true)] {
            let (one_inode, one_apply) = arm(1, sync_writes).await;
            let (spread, spread_apply) = arm(IN_FLIGHT, sync_writes).await;
            println!("{label}:");
            report("one inode", one_inode, one_apply);
            report("spread", spread, spread_apply);
            println!(
                "  one inode reaches {:.0}% of the unlocked rate at {IN_FLIGHT} in flight",
                spread.as_secs_f64() / one_inode.as_secs_f64() * 100.0
            );
        }
    }

    /// Overlapping writers must not pipeline: the second one rebuilds a
    /// partially overwritten extent and debits the frame it supersedes, and
    /// both reads only see state the commit worker publishes at apply.
    #[tokio::test]
    async fn an_overlapping_second_write_waits_for_the_first_to_apply() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth: AuthContext = (&test_auth()).into();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"overlapping", &SetAttributes::default())
            .await
            .unwrap();

        let stalled_apply = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();

        let first_fs = Arc::clone(&fs);
        let first_auth = auth.clone();
        let first = tokio::spawn(async move {
            first_fs
                .write(&first_auth, file_id, 0, &Bytes::from(vec![b'A'; 8192]))
                .await
        });
        apply_reached.await.unwrap();

        // Starts inside the first write's extent, so it must block until that
        // write has applied and can be spliced onto.
        let second_fs = Arc::clone(&fs);
        let second_auth = auth.clone();
        let second = tokio::spawn(async move {
            second_fs
                .write(&second_auth, file_id, 4096, &Bytes::from(vec![b'B'; 8192]))
                .await
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        // Queueing is what a pipelined write would have reached; the second
        // write grows the file, so its size is distinguishable from the first
        // write's. Seeing it here would mean it staged against unapplied state.
        assert_eq!(
            fs.inode_store
                .pending_inode(file_id)
                .and_then(|inode| match inode {
                    Some(Inode::File(file)) => Some(file.size),
                    _ => None,
                }),
            Some(8192),
            "an overlapping write must not stage against an unapplied write"
        );

        drop(stalled_apply);
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        let (data, _) = fs.read_file(&auth, file_id, 0, 12288).await.unwrap();
        let mut want = vec![b'B'; 12288];
        want[..4096].fill(b'A');
        assert_eq!(data, want, "the overlapping write lost the first write");
    }
}
