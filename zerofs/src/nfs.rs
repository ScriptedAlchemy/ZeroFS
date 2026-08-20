use crate::config::NfsSharedIdentity;
use crate::fs::EXTENT_SIZE;
use crate::fs::ZeroFS;
use crate::fs::inode::Inode;
use crate::fs::mutation::durability::DurabilityTarget;
use crate::fs::mutation::overlay::IdentifiedWrite;
use crate::fs::mutation::types::{RequestIdentity, RequestLifetime};
use crate::fs::permissions::Credentials;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, FileType, InodeWithId, SetAttributes, SetGid, SetUid};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use zerofs_nfsserve::nfs::{
    FSF_CANSETTIME, FSF_HOMOGENEOUS, FSF_LINK, FSF_SYMLINK, fattr3, fileid3, filename3, fsinfo3,
    fsstat3, ftype3, nfspath3, nfsstat3, nfstime3, post_op_attr, sattr3, specdata3, stable_how,
    writeverf3,
};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{
    AuthContext as NfsAuthContext, CommitRequestContext, CommitResult, NFSFileSystem,
    VFSCapabilities, WriteRequestContext, WriteResult,
};

const NFS_REPLAY_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub(crate) struct NfsServiceIdentity {
    server_incarnation: uuid::Uuid,
    write_verifier: writeverf3,
}

impl NfsServiceIdentity {
    pub(crate) fn new() -> Self {
        let server_incarnation = uuid::Uuid::new_v4();
        let write_verifier = loop {
            let candidate = rand::random::<writeverf3>();
            if candidate != [0; 8] {
                break candidate;
            }
        };
        Self {
            server_incarnation,
            write_verifier,
        }
    }
}

/// Adapter struct that implements the NFS trait for ZeroFS.
/// This prevents accidental direct calls to NFS trait methods on ZeroFS.
#[derive(Clone)]
pub struct NFSAdapter {
    fs: Arc<ZeroFS>,
    shared_identity: Option<NfsSharedIdentity>,
    service_identity: NfsServiceIdentity,
}

impl NFSAdapter {
    pub fn new(fs: Arc<ZeroFS>) -> Self {
        Self::with_service_identity(fs, NfsServiceIdentity::new())
    }

    pub(crate) fn with_service_identity(
        fs: Arc<ZeroFS>,
        service_identity: NfsServiceIdentity,
    ) -> Self {
        fs.install_volatile_overlay();
        Self {
            fs,
            shared_identity: None,
            service_identity,
        }
    }

    fn with_shared_identity(mut self, shared_identity: Option<NfsSharedIdentity>) -> Self {
        self.shared_identity = shared_identity;
        self
    }

    fn auth_context(&self, auth: &NfsAuthContext) -> AuthContext {
        match self.shared_identity {
            Some(identity) => AuthContext {
                uid: identity.uid,
                gid: identity.gid,
                gid_known: true,
                gids: Vec::new(),
                groups_complete: true,
            },
            None => auth.into(),
        }
    }

    fn create_attributes(&self, attr: sattr3) -> SetAttributes {
        let mut attr = SetAttributes::from(attr);
        if let Some(identity) = self.shared_identity {
            attr.uid = SetUid::Set(identity.uid);
            attr.gid = SetGid::Set(identity.gid);
        }
        attr
    }

    fn setattr_attributes(&self, attr: sattr3) -> SetAttributes {
        let mut attr = SetAttributes::from(attr);
        if self.shared_identity.is_some() {
            attr.uid = SetUid::NoChange;
            attr.gid = SetGid::NoChange;
        }
        attr
    }

    fn write_fingerprint_context(&self, context: &WriteRequestContext) -> Vec<u8> {
        let target: DurabilityTarget = self.fs.write_ack.client_durability_target.into();
        let client_addr = context.rpc.client_addr.as_bytes();
        let target = format!("{target:?}");
        let mut encoded = Vec::with_capacity(client_addr.len() + target.len() + 32);
        encoded.extend_from_slice(b"nfs3-write-v1");
        encoded.extend_from_slice(&(client_addr.len() as u64).to_le_bytes());
        encoded.extend_from_slice(client_addr);
        encoded.extend_from_slice(&(context.requested_stability as u32).to_le_bytes());
        encoded.extend_from_slice(&(target.len() as u64).to_le_bytes());
        encoded.extend_from_slice(target.as_bytes());
        encoded
    }

    async fn commit_current_cutoff(&self, fileid: fileid3) -> Result<writeverf3, nfsstat3> {
        if self.fs.ignore_fsync {
            tracing::error!(
                fileid,
                "rejecting NFS COMMIT because ignore_fsync disables durability barriers"
            );
            return Err(nfsstat3::NFS3ERR_NOTSUPP);
        }

        let cutoff = self.fs.capture_mutation_cutoff();
        match self.fs.wait_mutation_durability(cutoff).await {
            Ok(()) => {
                debug!("commit successful for file {fileid}");
                self.fs
                    .tracer
                    .emit(&self.fs.inode_store, fileid, FileOperation::Fsync);
                Ok(self.service_identity.write_verifier)
            }
            Err(fs_error) => {
                let nfsstat: nfsstat3 = fs_error.into();
                tracing::error!("commit failed for file {fileid}: {nfsstat:?}");
                Err(nfsstat)
            }
        }
    }
}

#[async_trait]
impl NFSFileSystem for NFSAdapter {
    fn root_dir(&self) -> fileid3 {
        0
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        debug!(
            "lookup called: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);

        let inode_id = self.fs.lookup(&creds, dirid, filename).await?;
        Ok(inode_id)
    }

    async fn getattr(&self, _auth: &NfsAuthContext, id: fileid3) -> Result<fattr3, nfsstat3> {
        debug!("getattr called: id={}", id);
        let inode = self.fs.visible_inode(id).await?;
        Ok(InodeWithId { inode: &inode, id }.into())
    }

    async fn read(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        debug!("read called: id={}, offset={}, count={}", id, offset, count);
        let auth_ctx = self.auth_context(auth);
        self.fs
            .read_file(&auth_ctx, id, offset, count)
            .await
            .map(|(data, eof)| (data.to_vec(), eof))
            .map_err(|e| e.into())
    }

    async fn write(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let auth_ctx = self.auth_context(auth);
        let data_bytes = bytes::Bytes::copy_from_slice(data);
        let file_attrs: crate::fs::types::FileAttributes = self
            .fs
            .write_ack(&auth_ctx, id, offset, &data_bytes)
            .await?;
        Ok((&file_attrs).into())
    }

    async fn write_with_context(
        &self,
        context: &WriteRequestContext,
        auth: &NfsAuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<WriteResult, nfsstat3> {
        debug!(
            xid = context.rpc.xid,
            connection_incarnation = context.rpc.connection_incarnation,
            requested_stability = ?context.requested_stability,
            data_len = data.len(),
            inode = id,
            offset,
            "processing contextual NFS WRITE",
        );

        let auth_ctx = self.auth_context(auth);
        let data = bytes::Bytes::copy_from_slice(data);
        let fingerprint_context = self.write_fingerprint_context(context);
        let receipt = self
            .fs
            .write_ack_identified(IdentifiedWrite {
                auth: &auth_ctx,
                id,
                offset,
                data: &data,
                op_id: [0; 16],
                check_permissions: true,
                identity: RequestIdentity::Nfs {
                    server_incarnation: self.service_identity.server_incarnation,
                    connection_incarnation: context.rpc.connection_incarnation,
                    xid: context.rpc.xid,
                },
                request_lifetime: RequestLifetime::ReplayWindow(NFS_REPLAY_WINDOW),
                fingerprint_context: &fingerprint_context,
            })
            .await?;

        let committed = match context.requested_stability {
            stable_how::UNSTABLE => stable_how::UNSTABLE,
            stable_how::DATA_SYNC | stable_how::FILE_SYNC if self.fs.ignore_fsync => {
                stable_how::UNSTABLE
            }
            stable_how::DATA_SYNC | stable_how::FILE_SYNC => {
                self.fs.wait_mutation_durability(receipt.cutoff).await?;
                stable_how::FILE_SYNC
            }
        };
        Ok(WriteResult {
            attributes: (&receipt.attrs).into(),
            committed,
            verifier: self.service_identity.write_verifier,
        })
    }

    async fn create(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "create called: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = self.create_attributes(attr);

        let (id, file_attrs): (u64, crate::fs::types::FileAttributes) =
            self.fs.create(&creds, dirid, filename, &fs_attr).await?;

        Ok((id, (&file_attrs).into()))
    }

    async fn create_exclusive(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        debug!(
            "create_exclusive called: dirid={}, filename={:?}",
            dirid, filename
        );

        let auth_ctx = self.auth_context(auth);
        let id = self.fs.create_exclusive(&auth_ctx, dirid, filename).await?;

        Ok(id)
    }

    async fn mkdir(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        dirname: &filename3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "mkdir called: dirid={}, dirname={}",
            dirid,
            String::from_utf8_lossy(dirname)
        );

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = self.create_attributes(*attr);
        let (id, file_attrs): (u64, crate::fs::types::FileAttributes) =
            self.fs.mkdir(&creds, dirid, dirname, &fs_attr).await?;
        Ok((id, (&file_attrs).into()))
    }

    async fn remove(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!("remove called: dirid={}, filename={:?}", dirid, filename);

        let auth_ctx = self.auth_context(auth);
        Ok(self.fs.remove(&auth_ctx, dirid, filename).await?)
    }

    async fn rename(
        &self,
        auth: &NfsAuthContext,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "rename called: from_dirid={}, to_dirid={}",
            from_dirid, to_dirid
        );

        let auth_ctx = self.auth_context(auth);
        self.fs
            .rename(&auth_ctx, from_dirid, from_filename, to_dirid, to_filename)
            .await
            .map_err(|e| e.into())
    }

    async fn readdir(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<zerofs_nfsserve::vfs::ReadDirResult, nfsstat3> {
        debug!(
            "readdir called: dirid={}, start_after={}, max_entries={}",
            dirid, start_after, max_entries
        );

        let auth_ctx = self.auth_context(auth);
        let result = self
            .fs
            .readdir(&auth_ctx, dirid, start_after, max_entries)
            .await?;

        Ok(zerofs_nfsserve::vfs::ReadDirResult {
            entries: result
                .entries
                .into_iter()
                .map(|e| zerofs_nfsserve::vfs::DirEntry {
                    fileid: e.fileid,
                    name: e.name.into(),
                    attr: (&e.attr).into(),
                    cookie: e.cookie,
                })
                .collect(),
            end: result.end,
        })
    }

    async fn setattr(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        debug!("setattr called: id={}, setattr={:?}", id, setattr);

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = self.setattr_attributes(setattr);
        let file_attrs = self.fs.setattr(&creds, id, &fs_attr).await?;
        Ok((&file_attrs).into())
    }

    async fn symlink(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "symlink called: dirid={}, linkname={:?}, target={:?}",
            dirid, linkname, symlink
        );

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = self.create_attributes(*attr);
        let (id, file_attrs) = self
            .fs
            .symlink(&creds, dirid, &linkname.0, &symlink.0, &fs_attr)
            .await
            .map_err(|e: crate::fs::errors::FsError| -> nfsstat3 { e.into() })?;

        Ok((id, (&file_attrs).into()))
    }

    async fn readlink(&self, _auth: &NfsAuthContext, id: fileid3) -> Result<nfspath3, nfsstat3> {
        debug!("readlink called: id={}", id);

        let inode = self.fs.inode_store.get(id).await?;

        match inode {
            Inode::Symlink(symlink) => Ok(nfspath3 { 0: symlink.target }),
            _ => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn mknod(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
        ftype: ftype3,
        attr: &sattr3,
        spec: Option<&specdata3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "mknod called: dirid={}, filename={:?}, ftype={:?}",
            dirid, filename, ftype
        );

        let rdev = match ftype {
            ftype3::NF3CHR | ftype3::NF3BLK => spec.map(|s| (s.specdata1, s.specdata2)),
            _ => None,
        };

        let auth_ctx = self.auth_context(auth);
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = self.create_attributes(*attr);
        let fs_type = FileType::from(ftype);
        let (id, file_attrs) = self
            .fs
            .mknod(&creds, dirid, &filename.0, fs_type, &fs_attr, rdev)
            .await?;

        Ok((id, (&file_attrs).into()))
    }

    async fn link(
        &self,
        auth: &NfsAuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "link called: fileid={}, linkdirid={}, linkname={:?}",
            fileid, linkdirid, linkname
        );

        let auth_ctx = self.auth_context(auth);
        Ok(self
            .fs
            .link(&auth_ctx, fileid, linkdirid, &linkname.0)
            .await?)
    }

    async fn commit(
        &self,
        _auth: &NfsAuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        tracing::debug!(
            "commit called: fileid={}, offset={}, count={}",
            fileid,
            offset,
            count
        );
        self.commit_current_cutoff(fileid).await
    }

    async fn commit_with_context(
        &self,
        context: &CommitRequestContext,
        _auth: &NfsAuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<CommitResult, nfsstat3> {
        debug!(
            xid = context.rpc.xid,
            connection_incarnation = context.rpc.connection_incarnation,
            fileid,
            offset,
            count,
            "processing contextual NFS COMMIT",
        );
        Ok(CommitResult {
            verifier: self.commit_current_cutoff(fileid).await?,
        })
    }

    fn get_write_verf(&self) -> writeverf3 {
        self.service_identity.write_verifier
    }

    async fn fsinfo(&self, auth: &NfsAuthContext, id: fileid3) -> Result<fsinfo3, nfsstat3> {
        debug!("fsinfo called: id={}", id);

        let obj_attr = match self.getattr(auth, id).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(e) => {
                debug!("fsinfo: getattr failed for id {}: {:?}", id, e);
                post_op_attr::Void
            }
        };

        // Use configured max_bytes from filesystem config, capped at 8 EiB
        // to avoid breaking NFS clients that can't handle larger values
        const MAX_NFS_BYTES: u64 = 8 * (1 << 60); // 8 EiB
        let maxfilesize = self.fs.max_bytes.min(MAX_NFS_BYTES);

        Ok(fsinfo3 {
            obj_attributes: obj_attr,
            rtmax: 1024 * 1024,
            rtpref: 1024 * 1024,
            rtmult: EXTENT_SIZE as u32,
            wtmax: 1024 * 1024,
            wtpref: 1024 * 1024,
            wtmult: EXTENT_SIZE as u32,
            dtpref: 1024 * 1024,
            maxfilesize,
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1,
            },
            properties: FSF_LINK | FSF_SYMLINK | FSF_HOMOGENEOUS | FSF_CANSETTIME,
        })
    }

    async fn fsstat(&self, auth: &NfsAuthContext, fileid: fileid3) -> Result<fsstat3, nfsstat3> {
        debug!("fsstat called: fileid={}", fileid);

        let obj_attr = match self.getattr(auth, fileid).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(e) => {
                debug!("fsstat: getattr failed for fileid {}: {:?}", fileid, e);
                post_op_attr::Void
            }
        };

        let (used_bytes, used_inodes) = self.fs.global_stats.get_totals();

        // Fixed, signed-safe inode capacity (see fs::TOTAL_INODES): a u64::MAX-based
        // count renders negative under GNU `stat`/`df`. `available` = capacity - in-use.
        let total_inodes = crate::fs::TOTAL_INODES;
        let available_inodes = total_inodes.saturating_sub(used_inodes);

        // Use configured max_bytes from filesystem config, capped at 8 EiB
        // to avoid breaking NFS clients that can't handle larger values
        const MAX_NFS_BYTES: u64 = 8 * (1 << 60); // 8 EiB
        let total_bytes = self.fs.max_bytes.min(MAX_NFS_BYTES);
        let free_bytes = total_bytes.saturating_sub(used_bytes);

        let res = fsstat3 {
            obj_attributes: obj_attr,
            tbytes: total_bytes,
            fbytes: free_bytes,
            abytes: free_bytes,
            tfiles: total_inodes,
            ffiles: available_inodes,
            afiles: available_inodes,
            invarsec: 1,
        };

        Ok(res)
    }
}

pub async fn start_nfs_server_with_config(
    filesystem: Arc<ZeroFS>,
    socket: SocketAddr,
    shutdown: CancellationToken,
    shared_identity: Option<NfsSharedIdentity>,
) -> anyhow::Result<()> {
    start_nfs_server_with_service_identity(
        filesystem,
        socket,
        shutdown,
        shared_identity,
        NfsServiceIdentity::new(),
    )
    .await
}

pub(crate) async fn start_nfs_server_with_service_identity(
    filesystem: Arc<ZeroFS>,
    socket: SocketAddr,
    shutdown: CancellationToken,
    shared_identity: Option<NfsSharedIdentity>,
    service_identity: NfsServiceIdentity,
) -> anyhow::Result<()> {
    if filesystem.ignore_fsync {
        anyhow::bail!(
            "NFS cannot start with filesystem.ignore_fsync=true because stable WRITE and COMMIT require functional durability barriers"
        );
    }

    let adapter = NFSAdapter::with_service_identity(filesystem, service_identity)
        .with_shared_identity(shared_identity);
    let listener = NFSTcpListener::bind(socket, adapter)
        .await
        .map_err(|e| crate::net_util::tcp_bind_error("NFS", socket, &e))?;

    info!("NFS server listening on {}", socket);

    listener.handle_with_shutdown(shutdown).await?;
    Ok(())
}

#[cfg(test)]
#[path = "nfs/typed_durability_tests.rs"]
mod tests_typed_durability;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NfsSharedIdentity;
    use crate::fs::inode::{Inode, test_file_inode};
    use crate::fs::store::directory::COOKIE_FIRST_ENTRY;
    use crate::test_helpers::test_helpers_mod::{filename, test_auth};
    use zerofs_nfsserve::nfs::{
        ftype3, nfspath3, nfsstat3, sattr3, set_atime, set_gid3, set_mode3, set_mtime, set_size3,
        set_uid3,
    };

    async fn seed_directory_entries(fs: &ZeroFS, count: usize) {
        let mut transaction = fs.db.new_transaction().unwrap();

        for index in 0..count {
            let inode_id = fs.inode_store.allocate();
            let name = format!("entry-{index:05}").into_bytes();
            let mut inode = test_file_inode(0);
            let Inode::File(file) = &mut inode else {
                unreachable!("test_file_inode must return a file")
            };
            file.name = Some(name.clone());

            fs.inode_store
                .save(&mut transaction, inode_id, &inode)
                .unwrap();
            fs.directory_store.add(
                &mut transaction,
                0,
                &name,
                inode_id,
                COOKIE_FIRST_ENTRY + index as u64,
                Some(&inode),
            );
        }

        fs.write_coordinator.commit(transaction).await.unwrap();
    }

    #[tokio::test]
    async fn test_nfs_filesystem_trait() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let adapter = NFSAdapter::new(fs);

        assert_eq!(adapter.root_dir(), 0);
        assert!(matches!(adapter.capabilities(), VFSCapabilities::ReadWrite));
    }

    #[tokio::test]
    async fn test_shared_identity_allows_cross_client_mutation_and_ownership() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let default_adapter = NFSAdapter::new(Arc::clone(&fs));
        let shared_adapter =
            NFSAdapter::new(fs).with_shared_identity(Some(NfsSharedIdentity { uid: 501, gid: 20 }));
        let mac_auth = NfsAuthContext {
            uid: 501,
            gid: 20,
            gids: Vec::new(),
        };
        let vm_auth = NfsAuthContext {
            uid: 1000,
            gid: 1000,
            gids: Vec::new(),
        };
        let owner_only = sattr3 {
            mode: set_mode3::mode(0o600),
            uid: set_uid3::uid(9001),
            gid: set_gid3::gid(9002),
            ..sattr3::default()
        };

        let (mac_file, attrs) = shared_adapter
            .create(&mac_auth, 0, &filename(b"mac-owned"), owner_only)
            .await
            .unwrap();
        assert_eq!((attrs.uid, attrs.gid, attrs.mode), (501, 20, 0o600));

        assert!(matches!(
            default_adapter
                .write(&vm_auth, mac_file, 0, b"denied")
                .await
                .unwrap_err(),
            nfsstat3::NFS3ERR_ACCES
        ));
        assert!(matches!(
            default_adapter
                .setattr(
                    &vm_auth,
                    mac_file,
                    sattr3 {
                        mode: set_mode3::mode(0o660),
                        uid: set_uid3::uid(9001),
                        gid: set_gid3::gid(9002),
                        ..sattr3::default()
                    },
                )
                .await
                .unwrap_err(),
            nfsstat3::NFS3ERR_PERM
        ));

        shared_adapter
            .write(&vm_auth, mac_file, 0, b"shared")
            .await
            .unwrap();
        let attrs = shared_adapter
            .setattr(
                &vm_auth,
                mac_file,
                sattr3 {
                    mode: set_mode3::mode(0o660),
                    uid: set_uid3::uid(9001),
                    gid: set_gid3::gid(9002),
                    ..sattr3::default()
                },
            )
            .await
            .unwrap();
        assert_eq!((attrs.uid, attrs.gid, attrs.mode), (501, 20, 0o660));

        let (_, attrs) = shared_adapter
            .create(&vm_auth, 0, &filename(b"vm-created"), sattr3::default())
            .await
            .unwrap();
        assert_eq!((attrs.uid, attrs.gid), (501, 20));

        let root_auth = NfsAuthContext {
            uid: 0,
            gid: 0,
            gids: vec![20],
        };
        let (_, attrs) = shared_adapter
            .create(&root_auth, 0, &filename(b"root-created"), sattr3::default())
            .await
            .unwrap();
        assert_eq!((attrs.uid, attrs.gid), (501, 20));
    }

    #[tokio::test]
    async fn test_lookup() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"test.txt"))
            .await
            .unwrap();

        assert_eq!(found_id, file_id);

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"nonexistent.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_getattr() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let fattr = fs.getattr(&test_auth(), 0).await.unwrap();

        assert!(matches!(fattr.ftype, ftype3::NF3DIR));
        assert_eq!(fattr.fileid, 0);
    }

    #[tokio::test]
    async fn test_read_write() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = b"Hello, NFS!";
        let fattr = fs.write(&test_auth(), file_id, 0, data).await.unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_create_exclusive() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let file_id = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await
            .unwrap();

        assert!(file_id > 0);

        let result = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_EXIST)));
    }

    #[tokio::test]
    async fn test_mkdir_and_readdir() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (dir_id, fattr) = fs
            .mkdir(&test_auth(), 0, &filename(b"mydir"), &sattr3::default())
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));

        let (_file_id, _) = fs
            .create(
                &test_auth(),
                dir_id,
                &filename(b"file_in_dir.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), dir_id, 0, 10).await.unwrap();
        assert!(result.end);

        let names: Vec<&[u8]> = result.entries.iter().map(|e| e.name.0.as_ref()).collect();

        assert!(names.contains(&b".".as_ref()));
        assert!(names.contains(&b"..".as_ref()));
        assert!(names.contains(&b"file_in_dir.txt".as_ref()));
    }

    #[tokio::test]
    async fn readdir_honors_zero_one_and_two_entry_budgets() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let adapter = NFSAdapter::new(Arc::clone(&fs));
        adapter
            .create(&test_auth(), 0, &filename(b"file.txt"), sattr3::default())
            .await
            .unwrap();

        let zero = adapter.readdir(&test_auth(), 0, 0, 0).await.unwrap();
        assert!(zero.entries.is_empty());
        assert!(!zero.end);

        let one = adapter.readdir(&test_auth(), 0, 0, 1).await.unwrap();
        assert_eq!(one.entries.len(), 1);
        assert_eq!(one.entries[0].name.0, b".");
        assert!(!one.end);

        let two = adapter.readdir(&test_auth(), 0, 0, 2).await.unwrap();
        assert_eq!(two.entries.len(), 2);
        assert_eq!(two.entries[0].name.0, b".");
        assert_eq!(two.entries[1].name.0, b"..");
        assert!(!two.end);
    }

    #[tokio::test]
    async fn readdir_huge_budget_is_capped_at_adapter_boundary() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        seed_directory_entries(&fs, 4_097).await;
        let adapter = NFSAdapter::new(fs);

        let result = adapter
            .readdir(&test_auth(), 0, 0, usize::MAX)
            .await
            .unwrap();

        assert_eq!(result.entries.len(), 4_096);
        assert!(!result.end);
    }

    #[tokio::test]
    async fn test_rename() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"original.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"test data")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            0,
            &filename(b"original.txt"),
            0,
            &filename(b"renamed.txt"),
        )
        .await
        .unwrap();

        let result = fs.lookup(&test_auth(), 0, &filename(b"original.txt")).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"renamed.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);
    }

    #[tokio::test]
    async fn test_remove() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"to_remove.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.remove(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await
            .unwrap();

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let result = fs.getattr(&test_auth(), file_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_setattr() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (file_id, initial_fattr) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        // Test changing mode (which any owner can do)
        let setattr_mode = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_mode)
            .await
            .unwrap();
        assert_eq!(fattr.mode, 0o755);

        // Test that uid/gid remain unchanged when not specified
        assert_eq!(fattr.uid, initial_fattr.uid);
        assert_eq!(fattr.gid, initial_fattr.gid);

        // Test changing size (truncate)
        let setattr_size = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(1024),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_size)
            .await
            .unwrap();
        assert_eq!(fattr.size, 1024);
    }

    #[tokio::test]
    async fn test_symlink_and_readlink() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let target = nfspath3 {
            0: b"/path/to/target".to_vec(),
        };
        let attr = sattr3::default();

        let (link_id, fattr) = fs
            .symlink(&test_auth(), 0, &filename(b"mylink"), &target, &attr)
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3LNK));

        let read_target = fs.readlink(&test_auth(), link_id).await.unwrap();
        assert_eq!(read_target.0, target.0);
    }

    #[tokio::test]
    async fn test_complex_filesystem_operations() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let (docs_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"documents"), &sattr3::default())
            .await
            .unwrap();
        let (images_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"images"), &sattr3::default())
            .await
            .unwrap();

        let (file1_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"readme.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file2_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"notes.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file3_id, _) = fs
            .create(
                &test_auth(),
                images_dir,
                &filename(b"photo.jpg"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file1_id, 0, b"This is the readme")
            .await
            .unwrap();
        fs.write(&test_auth(), file2_id, 0, b"These are my notes")
            .await
            .unwrap();
        fs.write(&test_auth(), file3_id, 0, b"JPEG data...")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            docs_dir,
            &filename(b"readme.txt"),
            images_dir,
            &filename(b"readme.txt"),
        )
        .await
        .unwrap();

        let docs_entries = fs.readdir(&test_auth(), docs_dir, 0, 10).await.unwrap();
        assert_eq!(docs_entries.entries.len(), 3);

        let images_entries = fs.readdir(&test_auth(), images_dir, 0, 10).await.unwrap();
        assert_eq!(images_entries.entries.len(), 4);

        let (data, _) = fs.read(&test_auth(), file1_id, 0, 100).await.unwrap();
        assert_eq!(data, b"This is the readme");
    }

    #[tokio::test]
    async fn test_large_directory_pagination() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        // Create a large number of files
        let num_files = 100;
        for i in 0..num_files {
            fs.create(
                &test_auth(),
                0,
                &filename(format!("file_{i:04}.txt").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Test pagination with different page sizes
        let page_sizes = vec![10, 25, 50];

        for page_size in page_sizes {
            let mut all_entries = Vec::new();
            let mut last_cookie = 0u64;
            let mut iterations = 0;

            loop {
                let result = fs
                    .readdir(&test_auth(), 0, last_cookie, page_size)
                    .await
                    .unwrap();

                // Skip . and .. if we're at the beginning
                let start_idx = if last_cookie == 0 { 2 } else { 0 };

                for entry in &result.entries[start_idx..] {
                    all_entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    last_cookie = entry.cookie;
                }

                iterations += 1;

                if result.end {
                    break;
                }

                // Safety check to prevent infinite loops
                assert!(
                    iterations < 50,
                    "Too many iterations for page size {page_size}"
                );
            }

            // Should have all files
            assert_eq!(
                all_entries.len(),
                num_files,
                "Wrong number of entries for page size {page_size}"
            );

            // Verify all files are present and in order
            all_entries.sort();
            for (i, entry) in all_entries.iter().enumerate().take(num_files) {
                assert_eq!(entry, &format!("file_{i:04}.txt"));
            }
        }
    }

    #[tokio::test]
    async fn test_pagination_with_many_hardlinks() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        // Create original files
        let num_files = 5;
        let hardlinks_per_file = 20;

        let mut file_ids = Vec::new();
        for i in 0..num_files {
            let (file_id, _) = fs
                .create(
                    &test_auth(),
                    0,
                    &filename(format!("original_{i}.txt").as_bytes()),
                    sattr3::default(),
                )
                .await
                .unwrap();
            file_ids.push(file_id);
        }

        // Create many hardlinks for each file
        for (i, &file_id) in file_ids.iter().enumerate() {
            for j in 0..hardlinks_per_file {
                fs.link(
                    &test_auth(),
                    file_id,
                    0,
                    &filename(format!("link_{i}_{j:02}.txt").as_bytes()),
                )
                .await
                .unwrap();
            }
        }

        // Test pagination - should handle all entries correctly
        let mut all_entries = Vec::new();
        let mut last_cookie = 0u64;
        let page_size = 20;

        loop {
            let result = fs
                .readdir(&test_auth(), 0, last_cookie, page_size)
                .await
                .unwrap();

            let start_idx = if last_cookie == 0 { 2 } else { 0 };

            for entry in &result.entries[start_idx..] {
                let name = String::from_utf8_lossy(&entry.name).to_string();
                all_entries.push(name);

                // With stable cookies, fileid is the raw inode
                assert!(entry.fileid > 0);
                // cookie is used for pagination
                assert!(entry.cookie > 0);

                last_cookie = entry.cookie;
            }

            if result.end {
                break;
            }
        }

        // Should have all files: originals + all hardlinks
        let expected_count = num_files + (num_files * hardlinks_per_file);
        assert_eq!(all_entries.len(), expected_count);

        // Verify no duplicates
        all_entries.sort();
        for i in 1..all_entries.len() {
            assert_ne!(all_entries[i - 1], all_entries[i], "Found duplicate entry");
        }
    }

    #[tokio::test]
    async fn test_pagination_edge_cases() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        // Test 1: Empty directory (only . and ..)
        let (empty_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"empty"), &sattr3::default())
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), empty_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 2); // Only . and ..
        assert!(result.end);
        assert_eq!(result.entries[0].name.0, b".");
        assert_eq!(result.entries[1].name.0, b"..");

        // Test 2: Single entry directory
        let (single_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"single"), &sattr3::default())
            .await
            .unwrap();
        fs.create(
            &test_auth(),
            single_dir,
            &filename(b"file.txt"),
            sattr3::default(),
        )
        .await
        .unwrap();

        let result = fs.readdir(&test_auth(), single_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 3); // ., .., file.txt
        assert!(result.end);

        // Test 3: Pagination with exactly page_size entries
        let (exact_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"exact"), &sattr3::default())
            .await
            .unwrap();

        // Create 8 files (so with . and .. we have 10 total)
        for i in 0..8 {
            fs.create(
                &test_auth(),
                exact_dir,
                &filename(format!("f{i}").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Read with page size 10 - should get all in one go
        let result = fs.readdir(&test_auth(), exact_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 10);
        assert!(result.end);

        // Read with page size 5 - should need exactly 2 reads
        let result1 = fs.readdir(&test_auth(), exact_dir, 0, 5).await.unwrap();
        assert_eq!(result1.entries.len(), 5);
        assert!(!result1.end);

        let last_cookie = result1.entries.last().unwrap().cookie;
        let result2 = fs
            .readdir(&test_auth(), exact_dir, last_cookie, 5)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 5);
        assert!(result2.end);

        // Test 4: Resume from non-existent cookie (should return no entries)
        let fake_cookie = 999999u64;
        let result = fs.readdir(&test_auth(), 0, fake_cookie, 10).await.unwrap();
        assert!(result.entries.is_empty() || result.end);
    }

    #[tokio::test]
    async fn test_concurrent_readdir_operations() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        // Create some files
        for i in 0..20 {
            fs.create(
                &test_auth(),
                0,
                &filename(format!("file_{i:02}.txt").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Simulate multiple concurrent readdir operations
        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let handle1 = tokio::spawn(async move {
            let mut entries = Vec::new();
            let mut last_cookie = 0u64;

            loop {
                let result = fs1.readdir(&test_auth(), 0, last_cookie, 5).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_cookie = entry.cookie;
                }
                if result.end {
                    break;
                }
            }
            entries
        });

        let handle2 = tokio::spawn(async move {
            let mut entries = Vec::new();
            let mut last_cookie = 0u64;

            loop {
                let result = fs2.readdir(&test_auth(), 0, last_cookie, 7).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_cookie = entry.cookie;
                }
                if result.end {
                    break;
                }
            }
            entries
        });

        let (entries1, entries2) = tokio::join!(handle1, handle2);
        let mut entries1 = entries1.unwrap();
        let mut entries2 = entries2.unwrap();

        // Both should have all 20 files
        assert_eq!(entries1.len(), 20);
        assert_eq!(entries2.len(), 20);

        // Sort and verify they're identical
        entries1.sort();
        entries2.sort();
        assert_eq!(entries1, entries2);
    }

    #[tokio::test]
    async fn nfs_commit_waits_for_configured_write_ack_barrier() {
        use crate::fs::mutation::config::{
            ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
            FilesystemWriteAckSource,
        };
        use std::time::Duration;
        use tokio::sync::Notify;

        let mut filesystem = ZeroFS::new_in_memory().await.unwrap();
        filesystem.write_ack = FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::Materialized,
            volatile_memory_bytes: 0,
            volatile_max_operations: crate::fs::mutation::config::DEFAULT_VOLATILE_MAX_OPERATIONS,
            source: FilesystemWriteAckSource::DefaultMaterialized,
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        };
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        filesystem.flush_coordinator.set_local_durability_barrier({
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
        let filesystem = Arc::new(filesystem);
        let adapter = NFSAdapter::new(Arc::clone(&filesystem));

        let mut commit = tokio::spawn(async move { adapter.commit(&test_auth(), 0, 0, 0).await });
        entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut commit)
                .await
                .is_err(),
            "NFS COMMIT returned before the configured write-ack barrier completed"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), commit)
            .await
            .expect("NFS COMMIT did not resume")
            .expect("NFS COMMIT panicked")
            .expect("NFS COMMIT failed");
    }

    #[tokio::test]
    async fn nfs_write_returns_without_waiting_for_write_ack_barrier() {
        use crate::fs::mutation::config::{
            ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
            FilesystemWriteAckSource,
        };
        use std::time::Duration;
        use tokio::sync::Notify;

        let mut filesystem = ZeroFS::new_in_memory().await.unwrap();
        filesystem.write_ack = FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::Materialized,
            volatile_memory_bytes: 0,
            volatile_max_operations: crate::fs::mutation::config::DEFAULT_VOLATILE_MAX_OPERATIONS,
            source: FilesystemWriteAckSource::DefaultMaterialized,
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        };
        let filesystem = Arc::new(filesystem);
        let adapter = NFSAdapter::new(Arc::clone(&filesystem));
        let file_id = adapter
            .create_exclusive(&test_auth(), 0, &filename(b"ack.txt"))
            .await
            .unwrap();
        filesystem.flush_coordinator.set_local_durability_barrier({
            let release = Arc::new(Notify::new());
            Arc::new(move || {
                let release = Arc::clone(&release);
                Box::pin(async move {
                    release.notified().await;
                    Ok(())
                })
            })
        });

        let fattr = tokio::time::timeout(
            Duration::from_secs(2),
            adapter.write(&test_auth(), file_id, 0, b"hello"),
        )
        .await
        .expect("NFS WRITE waited for the durability barrier")
        .expect("NFS WRITE failed");
        assert_eq!(fattr.size, 5);
    }
}
