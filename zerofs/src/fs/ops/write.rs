//! Canonical write preparation and application.
//!
//! Preparation owns validation, lock order, and exact post-write attributes.
//! Application owns extents, transactions, publication, stats, and tracing.
//! Materialized public methods call prepare then apply immediately.

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::mutation::types::{
    PrepareWriteMember, PrepareWriteRequest, PreparedBatchResult, PreparedWriteBatch,
    PreparedWriteMember,
};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::store::ExtentStore;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, FileAttributes, InodeWithId};
use crate::fs::{ZeroFS, get_current_time};
use ::tracing::debug;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

/// Borrowed filesystem view used by [`prepare_write`].
pub(crate) struct WritePrepareContext<'a> {
    pub(crate) fs: &'a ZeroFS,
}

/// Borrowed filesystem view used by [`apply_prepared_batch`].
pub(crate) struct WriteApplyContext<'a> {
    pub(crate) fs: &'a ZeroFS,
}

impl ZeroFS {
    pub(crate) fn write_prepare_context(&self) -> WritePrepareContext<'_> {
        WritePrepareContext { fs: self }
    }

    pub(crate) fn write_apply_context(&self) -> WriteApplyContext<'_> {
        WriteApplyContext { fs: self }
    }

    /// Write `data` at `offset`, growing the file as needed; growth is
    /// quota-checked against `max_bytes`. Returns the post-write attributes.
    pub async fn write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        self.write_idempotent(auth, id, offset, data, [0u8; 16])
            .await
    }

    /// Idempotent write retaining the original post-write attributes.
    pub async fn write_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        self.write_idempotent_inner(auth, id, offset, data, op_id, true)
            .await
    }

    /// Write through a fid whose access mode was already authorized at open.
    ///
    /// The original credentials are still used for ownership-sensitive
    /// metadata such as clearing SUID/SGID; only the mutable mode-bit access
    /// check is skipped so an open descriptor remains a stable capability.
    #[allow(dead_code)] // Used by the binary-only 9P handler.
    pub(crate) async fn write_opened_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        self.write_idempotent_inner(auth, id, offset, data, op_id, false)
            .await
    }

    async fn write_idempotent_inner(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
        check_permissions: bool,
    ) -> Result<FileAttributes, FsError> {
        let request = PrepareWriteRequest {
            members: vec![PrepareWriteMember {
                id,
                offset,
                data: data.clone(),
            }],
            auth: auth.clone(),
            op_id,
            check_permissions,
        };
        let mut batch = prepare_write(&self.write_prepare_context(), request).await?;
        let result = apply_prepared_batch(&self.write_apply_context(), &mut batch).await?;
        Ok(result.primary_attrs())
    }
}

/// Validate members, acquire inode locks in ascending ID order, and
/// compute exact post-write attributes. Does not publish extents or inodes.
pub(crate) async fn prepare_write(
    context: &WritePrepareContext<'_>,
    request: PrepareWriteRequest,
) -> Result<PreparedWriteBatch, FsError> {
    let fs = context.fs;
    if let Some(result) = replayed_batch(fs, &request)? {
        return Ok(result);
    }

    let mut seen = BTreeSet::new();
    for member in &request.members {
        if !seen.insert(member.id) {
            return Err(FsError::InvalidArgument);
        }
    }
    if request.members.is_empty() {
        return Err(FsError::InvalidArgument);
    }

    let start_time = std::time::Instant::now();
    for member in &request.members {
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            member.data.len(),
            member.id,
            member.offset
        );
    }

    let creds = Credentials::from_auth_context(&request.auth);
    let ids: Vec<InodeId> = request.members.iter().map(|member| member.id).collect();
    let guards = fs.lock_manager.acquire_multi(ids).await;

    for member in &request.members {
        let span = ExtentStore::extent_span(member.offset, member.data.len() as u64);
        if let Some((start_extent, end_extent)) = span {
            fs.extent_store
                .wait_for_inflight_overlap(member.id, start_extent, end_extent)
                .await;
        }
    }

    if let Some(result) = replayed_batch(fs, &request)? {
        drop(guards);
        return Ok(result);
    }

    let (used_bytes, _) = fs.global_stats.get_totals();
    let mut reserved_growth = 0u64;
    let mut prepared_members = Vec::with_capacity(request.members.len());

    for member in &request.members {
        let mut inode = fs.inode_store.get(member.id).await?;
        match &inode {
            Inode::File(file) => {
                if request.check_permissions && creds.uid != file.uid {
                    check_access(&inode, &creds, AccessMode::Write)?;
                }
            }
            _ => return Err(FsError::IsDirectory),
        }

        let span = ExtentStore::extent_span(member.offset, member.data.len() as u64);

        if member.data.is_empty() {
            let post_attrs: FileAttributes = InodeWithId {
                inode: &inode,
                id: member.id,
            }
            .into();
            prepared_members.push(PreparedWriteMember {
                id: member.id,
                offset: member.offset,
                data: member.data.clone(),
                old_size: match &inode {
                    Inode::File(file) => file.size,
                    _ => 0,
                },
                new_size: match &inode {
                    Inode::File(file) => file.size,
                    _ => 0,
                },
                inode,
                post_attrs,
                republish_metadata: false,
                parent_name_for_update: None,
                span,
            });
            continue;
        }

        let Inode::File(file) = &mut inode else {
            return Err(FsError::IsDirectory);
        };
        let old_size = file.size;
        let end_offset = member
            .offset
            .checked_add(member.data.len() as u64)
            .ok_or(FsError::InvalidArgument)?;
        let new_size = std::cmp::max(file.size, end_offset);

        if new_size > old_size {
            let size_increase = new_size - old_size;
            if used_bytes
                .saturating_add(reserved_growth)
                .saturating_add(size_increase)
                > fs.max_bytes
            {
                debug!(
                    "Write would exceed quota: used={}, reserved={}, increase={}, max={}",
                    used_bytes, reserved_growth, size_increase, fs.max_bytes
                );
                return Err(FsError::NoSpace);
            }
            reserved_growth = reserved_growth.saturating_add(size_increase);
        }

        let (now_sec, now_nsec) = get_current_time();
        let clears_setid = creds.uid != file.uid && creds.uid != 0 && file.mode & 0o6000 != 0;
        let republish_metadata =
            new_size != old_size || clears_setid || file.mtime != now_sec || file.ctime != now_sec;

        if republish_metadata {
            file.size = new_size;
            file.mtime = now_sec;
            file.mtime_nsec = now_nsec;
            file.ctime = now_sec;
            file.ctime_nsec = now_nsec;
            if clears_setid {
                file.mode &= !0o6000;
            }
        }

        let parent_name_for_update = republish_metadata
            .then(|| file.parent.zip(file.name.clone()))
            .flatten();
        let post_attrs: FileAttributes = InodeWithId {
            inode: &inode,
            id: member.id,
        }
        .into();

        prepared_members.push(PreparedWriteMember {
            id: member.id,
            offset: member.offset,
            data: member.data.clone(),
            old_size,
            new_size,
            inode,
            post_attrs,
            republish_metadata,
            parent_name_for_update,
            span,
        });
    }

    Ok(PreparedWriteBatch {
        op_id: request.op_id,
        start_time,
        members: prepared_members,
        replayed: None,
        guards: Some(guards),
    })
}

fn replayed_batch(
    fs: &ZeroFS,
    request: &PrepareWriteRequest,
) -> Result<Option<PreparedWriteBatch>, FsError> {
    let Some(attrs) = fs.replay_dedup_result(&request.op_id, DedupResult::into_write)? else {
        return Ok(None);
    };
    let members = request
        .members
        .iter()
        .map(|member| (member.id, attrs.clone()))
        .collect();
    Ok(Some(PreparedWriteBatch::replayed(
        request.op_id,
        PreparedBatchResult { members },
    )))
}

/// Materialize a prepared batch: one transaction, one result boundary.
pub(crate) async fn apply_prepared_batch(
    context: &WriteApplyContext<'_>,
    batch: &mut PreparedWriteBatch,
) -> Result<PreparedBatchResult, FsError> {
    if let Some(result) = batch.replayed.clone() {
        return Ok(result);
    }

    let fs = context.fs;
    let all_empty = batch.members.iter().all(|member| member.data.is_empty());
    if all_empty {
        let result = empty_batch_result(fs, batch).await?;
        batch.guards = None;
        return Ok(result);
    }

    let mut txn = fs.db.new_transaction()?;
    let mut tail_updates = Vec::new();

    for member in &batch.members {
        if member.data.is_empty() {
            continue;
        }
        let tail_update = fs
            .extent_store
            .write(
                &mut txn,
                member.id,
                member.offset,
                &member.data,
                member.old_size,
            )
            .await?;
        tail_updates.push((member.id, tail_update));

        #[cfg(feature = "failpoints")]
        fail_point!(fp::WRITE_AFTER_EXTENT);
    }

    for member in &batch.members {
        if member.republish_metadata {
            fs.inode_store.save(&mut txn, member.id, &member.inode)?;
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::WRITE_AFTER_INODE);

        if let Some((parent_id, name)) = &member.parent_name_for_update {
            fs.directory_store
                .update_inode_in_entry(&mut txn, *parent_id, name, member.id, &member.inode)
                .await?;
        }
    }

    if let Some(first) = batch.members.first() {
        txn.set_dedup_result(
            batch.op_id,
            DedupResult::Write {
                attrs: first.post_attrs.clone(),
            },
        );
    }

    let db_write_start = std::time::Instant::now();
    let mut queued_extents = Vec::new();
    for member in &batch.members {
        if let Some((start_extent, end_extent)) = member.span {
            queued_extents.push(fs.extent_store.register_inflight_write(
                member.id,
                start_extent,
                end_extent,
            ));
        }
    }

    let pending = fs.write_coordinator.submit(txn)?;
    batch.guards = None;
    pending.wait().await?;
    debug!("DB write took: {:?}", db_write_start.elapsed());

    for (id, tail_update) in tail_updates {
        fs.extent_store.apply_tail_update(id, tail_update);
    }
    drop(queued_extents);

    #[cfg(feature = "failpoints")]
    fail_point!(fp::WRITE_AFTER_COMMIT);

    let elapsed = batch.start_time.elapsed();
    for member in &batch.members {
        if member.data.is_empty() {
            continue;
        }
        debug!(
            "Write processed successfully for inode {}, new size: {}, took: {:?}",
            member.id, member.new_size, elapsed
        );
        fs.stats
            .bytes_written
            .fetch_add(member.data.len() as u64, Ordering::Relaxed);
        fs.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        fs.stats.total_operations.fetch_add(1, Ordering::Relaxed);
        fs.tracer.emit(
            &fs.inode_store,
            member.id,
            FileOperation::Write {
                offset: member.offset,
                length: member.data.len() as u64,
            },
        );
    }

    Ok(PreparedBatchResult {
        members: batch
            .members
            .iter()
            .map(|member| (member.id, member.post_attrs.clone()))
            .collect(),
    })
}

async fn empty_batch_result(
    fs: &ZeroFS,
    batch: &PreparedWriteBatch,
) -> Result<PreparedBatchResult, FsError> {
    let result = PreparedBatchResult {
        members: batch
            .members
            .iter()
            .map(|member| (member.id, member.post_attrs.clone()))
            .collect(),
    };
    if crate::dedup::has_op_id(&batch.op_id) {
        let mut txn = fs.db.new_transaction()?;
        if let Some((_, attrs)) = result.members.first() {
            txn.set_dedup_result(
                batch.op_id,
                DedupResult::Write {
                    attrs: attrs.clone(),
                },
            );
        }
        fs.write_coordinator.commit(txn).await?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{apply_prepared_batch, prepare_write};
    use crate::fs::mutation::types::{PrepareWriteMember, PrepareWriteRequest};
    use crate::fs::test_util::test_creds;
    use crate::fs::types::{AuthContext, FileAttributes, InodeWithId, SetAttributes};
    use crate::fs::{ZeroFS, get_current_time};
    use crate::test_helpers::test_helpers_mod::test_auth;
    use bytes::Bytes;

    async fn persisted_attrs(fs: &ZeroFS, id: crate::fs::inode::InodeId) -> FileAttributes {
        let inode = fs.inode_store.get(id).await.unwrap();
        InodeWithId { inode: &inode, id }.into()
    }

    async fn wait_for_second_boundary() {
        let (_, nsec) = get_current_time();
        let remaining = 1_000_000_000u32.saturating_sub(nsec);
        tokio::time::sleep(std::time::Duration::from_nanos(
            u64::from(remaining) + 2_000_000,
        ))
        .await;
    }

    fn auth() -> AuthContext {
        (&test_auth()).into()
    }

    #[tokio::test]
    async fn prepare_write_has_no_canonical_side_effect() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"prepare.txt", &SetAttributes::default())
            .await
            .unwrap();
        let auth = auth();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"old"))
            .await
            .unwrap();
        let before = persisted_attrs(&fs, file_id).await;

        let request = PrepareWriteRequest {
            members: vec![PrepareWriteMember {
                id: file_id,
                offset: 0,
                data: Bytes::from_static(b"new"),
            }],
            auth: auth.clone(),
            op_id: [0u8; 16],
            check_permissions: true,
        };
        let batch = prepare_write(&fs.write_prepare_context(), request)
            .await
            .unwrap();

        let after_prepare = persisted_attrs(&fs, file_id).await;
        assert_eq!(after_prepare.size, before.size);
        assert_eq!(after_prepare.mtime.seconds, before.mtime.seconds);
        assert_eq!(after_prepare.mtime.nanoseconds, before.mtime.nanoseconds);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert_eq!(data.as_ref(), b"old");
        assert!(!batch.is_replayed());
        assert_eq!(batch.members[0].new_size, 3);
        drop(batch);
    }

    #[tokio::test]
    async fn apply_uses_preselected_timestamp_and_attrs() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"attrs.txt", &SetAttributes::default())
            .await
            .unwrap();
        let auth = auth();
        wait_for_second_boundary().await;

        let request = PrepareWriteRequest {
            members: vec![PrepareWriteMember {
                id: file_id,
                offset: 0,
                data: Bytes::from_static(b"hello"),
            }],
            auth: auth.clone(),
            op_id: [0u8; 16],
            check_permissions: true,
        };
        let mut batch = prepare_write(&fs.write_prepare_context(), request)
            .await
            .unwrap();
        let prepared = batch.members[0].post_attrs.clone();
        assert_eq!(prepared.size, 5);

        wait_for_second_boundary().await;
        let result = apply_prepared_batch(&fs.write_apply_context(), &mut batch)
            .await
            .unwrap();

        let persisted = persisted_attrs(&fs, file_id).await;
        assert_eq!(result.primary_attrs().size, prepared.size);
        assert_eq!(persisted.size, prepared.size);
        assert_eq!(persisted.mtime.seconds, prepared.mtime.seconds);
        assert_eq!(persisted.mtime.nanoseconds, prepared.mtime.nanoseconds);
        assert_eq!(persisted.ctime.seconds, prepared.ctime.seconds);
        assert_eq!(persisted.mode, prepared.mode);
        let (now_sec, _) = get_current_time();
        assert_ne!(
            persisted.mtime.seconds, now_sec,
            "apply must not resample the wall clock"
        );
    }

    #[tokio::test]
    async fn zero_length_write_preserves_result() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"empty.txt", &SetAttributes::default())
            .await
            .unwrap();
        let auth = auth();
        let written = fs
            .write(&auth, file_id, 0, &Bytes::from_static(b"payload"))
            .await
            .unwrap();
        let op_id = [0x42; 16];

        let empty = fs
            .write_idempotent(&auth, file_id, 1_000_000, &Bytes::new(), op_id)
            .await
            .unwrap();
        assert_eq!(empty.size, written.size);
        assert_eq!(empty.mtime.seconds, written.mtime.seconds);
        assert_eq!(empty.mtime.nanoseconds, written.mtime.nanoseconds);

        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later!!"))
            .await
            .unwrap();
        let replayed = fs
            .write_idempotent(&auth, file_id, 1_000_000, &Bytes::new(), op_id)
            .await
            .unwrap();
        assert_eq!(replayed.size, empty.size);
        assert_eq!(replayed.mtime.seconds, empty.mtime.seconds);
        assert_eq!(replayed.mtime.nanoseconds, empty.mtime.nanoseconds);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert_eq!(data.as_ref(), b"later!!");
    }

    #[tokio::test]
    async fn materialized_write_waits_for_apply() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"wait.txt", &SetAttributes::default())
            .await
            .unwrap();
        let auth = auth();
        let data = Bytes::from_static(b"durable");
        let attrs = fs.write(&auth, file_id, 0, &data).await.unwrap();

        let persisted = persisted_attrs(&fs, file_id).await;
        assert_eq!(attrs.size, persisted.size);
        assert_eq!(persisted.size, data.len() as u64);
        let (read, eof) = fs
            .read_file(&auth, file_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), data.as_ref());
        assert!(eof);
    }

    #[tokio::test]
    async fn striped_batch_has_one_result_boundary() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (first_id, _) = fs
            .create(&test_creds(), 0, b"stripe0.bin", &SetAttributes::default())
            .await
            .unwrap();
        let (second_id, _) = fs
            .create(&test_creds(), 0, b"stripe1.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = auth();

        let request = PrepareWriteRequest {
            members: vec![
                PrepareWriteMember {
                    id: first_id,
                    offset: 0,
                    data: Bytes::from_static(b"AAAA"),
                },
                PrepareWriteMember {
                    id: second_id,
                    offset: 0,
                    data: Bytes::from_static(b"BBBB"),
                },
            ],
            auth: auth.clone(),
            op_id: [0u8; 16],
            check_permissions: true,
        };
        let mut batch = prepare_write(&fs.write_prepare_context(), request)
            .await
            .unwrap();
        assert_eq!(batch.members.len(), 2);
        assert_eq!(persisted_attrs(&fs, first_id).await.size, 0);
        assert_eq!(persisted_attrs(&fs, second_id).await.size, 0);
        let (first_read, _) = fs.read_file(&auth, first_id, 0, 8).await.unwrap();
        let (second_read, _) = fs.read_file(&auth, second_id, 0, 8).await.unwrap();
        assert!(first_read.is_empty());
        assert!(second_read.is_empty());

        let result = apply_prepared_batch(&fs.write_apply_context(), &mut batch)
            .await
            .unwrap();
        assert_eq!(result.members.len(), 2);
        assert_eq!(result.members[0].0, first_id);
        assert_eq!(result.members[1].0, second_id);
        assert_eq!(persisted_attrs(&fs, first_id).await.size, 4);
        assert_eq!(persisted_attrs(&fs, second_id).await.size, 4);
        let (first_read, _) = fs.read_file(&auth, first_id, 0, 8).await.unwrap();
        let (second_read, _) = fs.read_file(&auth, second_id, 0, 8).await.unwrap();
        assert_eq!(first_read.as_ref(), b"AAAA");
        assert_eq!(second_read.as_ref(), b"BBBB");
    }
}
