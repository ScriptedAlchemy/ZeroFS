use super::error::{CommandError, CommandResult, NBDError, Result};
use super::out_of_bounds;
use super::{
    NBD_STRIPE_MANIFEST_MAX_BYTES, NBD_STRIPE_MARKER, NBD_STRIPE_MAX_BYTES, NBD_STRIPE_MAX_MEMBERS,
    NBD_STRIPE_MIN_BYTES, StripeManifest, is_nbd_provision_staging_name,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::tracing::FileOperation;
use crate::fs::types::AuthContext;
use bytes::{Bytes, BytesMut};
use deku::DekuContainerWrite;
use futures::future::try_join_all;
use nbd_proto::{
    NBD_INFO_EXPORT, NBD_REP_ACK, NBD_REP_ERR_INVALID, NBD_REP_ERR_UNKNOWN, NBD_REP_INFO,
    NBD_REP_SERVER, NBDInfoExport, TRANSMISSION_FLAGS,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tracing::debug;

const NBD_READDIR_DEFAULT_LIMIT: usize = 1000;
/// Response to send back for an option
pub struct OptionReply {
    pub reply_type: u32,
    pub data: Vec<u8>,
}

impl OptionReply {
    pub fn new(reply_type: u32, data: Vec<u8>) -> Self {
        Self { reply_type, data }
    }

    pub fn ack() -> Self {
        Self::new(NBD_REP_ACK, Vec::new())
    }

    pub fn error(reply_type: u32) -> Self {
        Self::new(reply_type, Vec::new())
    }
}

/// Result of processing an option - may return device if negotiation complete
pub enum OptionResult {
    /// Continue negotiation, send these replies
    Continue(Vec<OptionReply>),
    /// Negotiation complete, use this device
    Done(NBDDevice, Vec<OptionReply>),
    /// Error during processing
    Error(NBDError, Vec<OptionReply>),
}

/// NBD device descriptor
#[derive(Clone)]
pub struct NBDDevice {
    pub name: Vec<u8>,
    pub size: u64,
    backing: NbdBacking,
    trace_inode: u64,
    gate: Arc<RwLock<()>>,
}

#[derive(Clone, Debug)]
struct NbdMember {
    inode: u64,
    size: u64,
}

#[derive(Clone, Debug)]
enum NbdBacking {
    Single {
        inode: u64,
        size: u64,
    },
    Striped {
        members: Arc<[NbdMember]>,
        stripe_bytes: u64,
    },
}

fn parse_stripe_manifest(data: &[u8]) -> Result<StripeManifest> {
    let manifest: StripeManifest = serde_json::from_slice(data)
        .map_err(|error| NBDError::Protocol(format!("invalid striped NBD manifest: {error}")))?;
    if manifest.version != 1 {
        return Err(NBDError::Protocol(format!(
            "unsupported striped NBD manifest version {}",
            manifest.version
        )));
    }
    if manifest.members.len() < 2 || manifest.members.len() > NBD_STRIPE_MAX_MEMBERS {
        return Err(NBDError::Protocol(format!(
            "striped NBD requires 2..={NBD_STRIPE_MAX_MEMBERS} members"
        )));
    }
    if !manifest.stripe_bytes.is_power_of_two()
        || !(NBD_STRIPE_MIN_BYTES..=NBD_STRIPE_MAX_BYTES).contains(&manifest.stripe_bytes)
    {
        return Err(NBDError::Protocol(format!(
            "striped NBD stripe_bytes must be a power of two in {NBD_STRIPE_MIN_BYTES}..={NBD_STRIPE_MAX_BYTES}"
        )));
    }
    let mut unique = HashSet::with_capacity(manifest.members.len());
    for member in &manifest.members {
        if member.is_empty()
            || member == "."
            || member == ".."
            || member == NBD_STRIPE_MARKER
            || member.as_bytes().contains(&b'/')
            || !unique.insert(member.as_bytes().to_vec())
        {
            return Err(NBDError::Protocol(
                "striped NBD member names must be unique direct children".to_string(),
            ));
        }
    }
    Ok(manifest)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StripeChunk {
    member_index: usize,
    inode: u64,
    member_offset: u64,
    logical_offset: u64,
    length: u64,
}

fn map_stripe_chunks(
    backing: &NbdBacking,
    offset: u64,
    length: u64,
) -> CommandResult<Vec<StripeChunk>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    match backing {
        NbdBacking::Single { inode, size } => {
            if offset.checked_add(length).is_none_or(|end| end > *size) {
                return Err(CommandError::InvalidArgument);
            }
            Ok(vec![StripeChunk {
                member_index: 0,
                inode: *inode,
                member_offset: offset,
                logical_offset: 0,
                length,
            }])
        }
        NbdBacking::Striped {
            members,
            stripe_bytes,
        } => {
            if members.is_empty() || *stripe_bytes == 0 {
                return Err(CommandError::InvalidArgument);
            }
            let member_count = members.len() as u64;
            let mut chunks = Vec::new();
            let mut logical_offset = 0_u64;
            while logical_offset < length {
                let position = offset
                    .checked_add(logical_offset)
                    .ok_or(CommandError::InvalidArgument)?;
                let stripe = position / stripe_bytes;
                let within_stripe = position % stripe_bytes;
                let member_index = (stripe % member_count) as usize;
                let row = stripe / member_count;
                let member_offset = row
                    .checked_mul(*stripe_bytes)
                    .and_then(|base| base.checked_add(within_stripe))
                    .ok_or(CommandError::InvalidArgument)?;
                let chunk_length = (length - logical_offset).min(*stripe_bytes - within_stripe);
                let member = &members[member_index];
                if member_offset
                    .checked_add(chunk_length)
                    .is_none_or(|end| end > member.size)
                {
                    return Err(CommandError::InvalidArgument);
                }
                chunks.push(StripeChunk {
                    member_index,
                    inode: member.inode,
                    member_offset,
                    logical_offset,
                    length: chunk_length,
                });
                logical_offset += chunk_length;
            }
            Ok(chunks)
        }
    }
}

fn group_stripe_chunks(chunks: Vec<StripeChunk>, member_count: usize) -> Vec<Vec<StripeChunk>> {
    let mut groups = (0..member_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for chunk in chunks {
        groups[chunk.member_index].push(chunk);
    }
    groups
}

impl NBDDevice {
    pub fn info_export(&self) -> NBDInfoExport {
        NBDInfoExport {
            info_type: NBD_INFO_EXPORT,
            size: self.size,
            transmission_flags: TRANSMISSION_FLAGS,
        }
    }
}

#[derive(Default)]
pub struct NbdExportGates {
    gates: StdMutex<HashMap<Vec<u8>, Weak<RwLock<()>>>>,
}

impl NbdExportGates {
    fn for_export(&self, name: &[u8]) -> Arc<RwLock<()>> {
        let mut gates = self
            .gates
            .lock()
            .expect("NBD export gate registry poisoned");
        if let Some(gate) = gates.get(name).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(RwLock::new(()));
        gates.insert(name.to_vec(), Arc::downgrade(&gate));
        gate
    }
}

/// Handler for NBD protocol operations
pub struct NBDHandler {
    filesystem: Arc<ZeroFS>,
    export_gates: Arc<NbdExportGates>,
}

impl NBDHandler {
    pub fn new(filesystem: Arc<ZeroFS>, export_gates: Arc<NbdExportGates>) -> Self {
        Self {
            filesystem,
            export_gates,
        }
    }

    /// Get the .nbd directory inode
    async fn nbd_dir_inode(&self) -> Result<u64> {
        self.filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .map_err(NBDError::from)
    }

    /// List all available NBD devices
    pub async fn list_devices(&self) -> Result<Vec<NBDDevice>> {
        let auth = AuthContext::default();
        let nbd_dir_inode = self.nbd_dir_inode().await?;

        let mut devices = Vec::new();
        let mut start_after = 0;
        loop {
            let page = self
                .filesystem
                .readdir(&auth, nbd_dir_inode, start_after, NBD_READDIR_DEFAULT_LIMIT)
                .await?;
            let next_start = page.entries.last().map(|entry| entry.cookie);
            for entry in &page.entries {
                let name = &entry.name;
                if name == b"." || name == b".." {
                    continue;
                }

                match self.resolve_device(name, entry.fileid).await {
                    Ok(device) => devices.push(device),
                    Err(error) => {
                        debug!(
                            "skipping invalid NBD export '{}': {error}",
                            String::from_utf8_lossy(name)
                        );
                    }
                }
            }
            if page.end {
                break;
            }
            let next_start = next_start.ok_or_else(|| {
                NBDError::Protocol("NBD export listing made no pagination progress".to_string())
            })?;
            if next_start <= start_after {
                return Err(NBDError::Protocol(
                    "NBD export listing returned a non-increasing cookie".to_string(),
                ));
            }
            start_after = next_start;
        }

        Ok(devices)
    }

    /// Rreturns list of all devices
    pub async fn list(&self) -> OptionResult {
        match self.list_devices().await {
            Ok(devices) => {
                let mut replies = Vec::new();
                for device in devices {
                    let mut reply_data = Vec::new();
                    reply_data.extend_from_slice(&(device.name.len() as u32).to_be_bytes());
                    reply_data.extend_from_slice(&device.name);
                    replies.push(OptionReply::new(NBD_REP_SERVER, reply_data));
                }
                replies.push(OptionReply::ack());
                OptionResult::Continue(replies)
            }
            Err(e) => OptionResult::Error(e, vec![]),
        }
    }

    /// Returns device info without completing negotiation
    pub async fn info(&self, data: &[u8]) -> OptionResult {
        if data.len() < 4 {
            return OptionResult::Continue(vec![OptionReply::error(NBD_REP_ERR_INVALID)]);
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid INFO option length".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name = &data[4..4 + name_len];
        debug!(
            "INFO option: requested export name '{}' (name_len: {})",
            String::from_utf8_lossy(name),
            name_len
        );

        match self.get_device(name).await {
            Ok(device) => match device.info_export().to_bytes() {
                Ok(info_bytes) => OptionResult::Continue(vec![
                    OptionReply::new(NBD_REP_INFO, info_bytes),
                    OptionReply::ack(),
                ]),
                Err(e) => OptionResult::Error(
                    NBDError::Protocol(format!("Failed to serialize info: {:?}", e)),
                    vec![],
                ),
            },
            Err(e) => {
                debug!(
                    "INFO option: device '{}' not found: {:?}",
                    String::from_utf8_lossy(name),
                    e
                );
                OptionResult::Continue(vec![OptionReply::error(NBD_REP_ERR_UNKNOWN)])
            }
        }
    }

    /// Returns device info and completes negotiation
    pub async fn go(&self, data: &[u8]) -> OptionResult {
        if data.len() < 4 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid GO option".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len + 2 {
            return OptionResult::Error(
                NBDError::Protocol("Invalid GO option length".to_string()),
                vec![OptionReply::error(NBD_REP_ERR_INVALID)],
            );
        }

        let name = &data[4..4 + name_len];
        debug!(
            "GO option: requested export name '{}' (name_len: {})",
            String::from_utf8_lossy(name),
            name_len
        );
        debug!(
            "GO option data length: {}, expected minimum: {}",
            data.len(),
            4 + name_len + 2
        );

        match self.get_device(name).await {
            Ok(device) => match device.info_export().to_bytes() {
                Ok(info_bytes) => OptionResult::Done(
                    device,
                    vec![
                        OptionReply::new(NBD_REP_INFO, info_bytes),
                        OptionReply::ack(),
                    ],
                ),
                Err(e) => OptionResult::Error(
                    NBDError::Protocol(format!("Failed to serialize info: {:?}", e)),
                    vec![],
                ),
            },
            Err(e) => {
                debug!(
                    "GO option: device '{}' not found: {:?}",
                    String::from_utf8_lossy(name),
                    e
                );
                OptionResult::Error(
                    NBDError::DeviceNotFound(name.to_vec()),
                    vec![OptionReply::error(NBD_REP_ERR_UNKNOWN)],
                )
            }
        }
    }

    /// Get a specific NBD device by name
    pub async fn get_device(&self, name: &[u8]) -> Result<NBDDevice> {
        let nbd_dir_inode = self.nbd_dir_inode().await?;

        let device_inode = self
            .filesystem
            .directory_store
            .get(nbd_dir_inode, name)
            .await
            .map_err(|e| match e {
                FsError::NotFound => NBDError::DeviceNotFound(name.to_vec()),
                e => NBDError::Filesystem(e),
            })?;

        self.resolve_device(name, device_inode).await
    }

    async fn resolve_device(&self, name: &[u8], device_inode: u64) -> Result<NBDDevice> {
        if is_nbd_provision_staging_name(name) {
            return Err(NBDError::DeviceNotFound(name.to_vec()));
        }
        match self.filesystem.inode_store.get(device_inode).await? {
            Inode::File(file_inode) => Ok(NBDDevice {
                name: name.to_vec(),
                size: file_inode.size,
                backing: NbdBacking::Single {
                    inode: device_inode,
                    size: file_inode.size,
                },
                trace_inode: device_inode,
                gate: self.export_gates.for_export(name),
            }),
            Inode::Directory(_) => self.resolve_striped_device(name, device_inode).await,
            _ => Err(NBDError::Protocol(format!(
                "NBD device '{}' is neither a regular file nor a striped export directory",
                String::from_utf8_lossy(name)
            ))),
        }
    }

    async fn resolve_striped_device(&self, name: &[u8], directory_inode: u64) -> Result<NBDDevice> {
        let marker_inode = self
            .filesystem
            .directory_store
            .get(directory_inode, NBD_STRIPE_MARKER.as_bytes())
            .await
            .map_err(NBDError::from)?;
        let marker_size = match self.filesystem.inode_store.get(marker_inode).await? {
            Inode::File(file) if file.size > 0 && file.size <= NBD_STRIPE_MANIFEST_MAX_BYTES => {
                file.size
            }
            _ => {
                return Err(NBDError::Protocol(format!(
                    "striped NBD export '{}' has an invalid manifest file",
                    String::from_utf8_lossy(name)
                )));
            }
        };
        let auth = AuthContext::default();
        let (manifest_bytes, _) = self
            .filesystem
            .read_file(&auth, marker_inode, 0, marker_size as u32)
            .await?;
        let manifest = parse_stripe_manifest(&manifest_bytes)?;
        let mut members = Vec::with_capacity(manifest.members.len());
        let mut member_size = None;
        for member_name in &manifest.members {
            let member_inode = self
                .filesystem
                .directory_store
                .get(directory_inode, member_name.as_bytes())
                .await
                .map_err(NBDError::from)?;
            let size = match self.filesystem.inode_store.get(member_inode).await? {
                Inode::File(file) => file.size,
                _ => {
                    return Err(NBDError::Protocol(format!(
                        "striped NBD member '{member_name}' is not a regular file"
                    )));
                }
            };
            if size == 0 || size % manifest.stripe_bytes != 0 {
                return Err(NBDError::Protocol(format!(
                    "striped NBD member '{member_name}' size must be non-zero and stripe-aligned"
                )));
            }
            if member_size.is_some_and(|expected| expected != size) {
                return Err(NBDError::Protocol(
                    "striped NBD members must have equal sizes".to_string(),
                ));
            }
            member_size = Some(size);
            members.push(NbdMember {
                inode: member_inode,
                size,
            });
        }
        let size = member_size
            .unwrap_or(0)
            .checked_mul(members.len() as u64)
            .ok_or_else(|| NBDError::Protocol("striped NBD size overflow".to_string()))?;
        Ok(NBDDevice {
            name: name.to_vec(),
            size,
            backing: NbdBacking::Striped {
                members: members.into(),
                stripe_bytes: manifest.stripe_bytes,
            },
            trace_inode: directory_inode,
            gate: self.export_gates.for_export(name),
        })
    }

    pub async fn read(&self, device: &NBDDevice, offset: u64, length: u32) -> CommandResult<Bytes> {
        if out_of_bounds(offset, length, device.size) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(Bytes::new());
        }

        match &device.backing {
            NbdBacking::Single { inode, .. } => {
                let auth = AuthContext::default();
                let (data, _) = self
                    .filesystem
                    .read_file(&auth, *inode, offset, length)
                    .await?;
                if data.len() != length as usize {
                    return Err(CommandError::IoError);
                }
                Ok(data)
            }
            NbdBacking::Striped { members, .. } => {
                let groups = group_stripe_chunks(
                    map_stripe_chunks(&device.backing, offset, length as u64)?,
                    members.len(),
                );
                let reads = groups
                    .into_iter()
                    .filter(|group| !group.is_empty())
                    .map(|group| {
                        let filesystem = Arc::clone(&self.filesystem);
                        async move {
                            let auth = AuthContext::default();
                            let mut parts = Vec::with_capacity(group.len());
                            for chunk in group {
                                let (data, _) = filesystem
                                    .read_file(
                                        &auth,
                                        chunk.inode,
                                        chunk.member_offset,
                                        chunk.length as u32,
                                    )
                                    .await
                                    .map_err(CommandError::from)?;
                                if data.len() != chunk.length as usize {
                                    return Err(CommandError::IoError);
                                }
                                parts.push((chunk.logical_offset, data));
                            }
                            Ok::<_, CommandError>(parts)
                        }
                    });
                let mut output = BytesMut::zeroed(length as usize);
                for parts in try_join_all(reads).await? {
                    for (logical_offset, data) in parts {
                        let start = logical_offset as usize;
                        output[start..start + data.len()].copy_from_slice(&data);
                    }
                }
                Ok(output.freeze())
            }
        }
    }

    pub(crate) async fn begin_mutation(&self, device: &NBDDevice) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&device.gate).read_owned().await
    }

    pub(crate) async fn write_admitted(
        &self,
        device: &NBDDevice,
        offset: u64,
        data: &Bytes,
        fua: bool,
        admission: OwnedRwLockReadGuard<()>,
    ) -> CommandResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        if offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > device.size)
        {
            return Err(CommandError::NoSpace);
        }

        match &device.backing {
            NbdBacking::Single { inode, .. } => {
                let auth = AuthContext::default();
                self.filesystem.write(&auth, *inode, offset, data).await?;
            }
            NbdBacking::Striped { members, .. } => {
                let groups = group_stripe_chunks(
                    map_stripe_chunks(&device.backing, offset, data.len() as u64)?,
                    members.len(),
                );
                let writes = groups
                    .into_iter()
                    .filter(|group| !group.is_empty())
                    .map(|group| {
                        let filesystem = Arc::clone(&self.filesystem);
                        let data = data.clone();
                        async move {
                            let auth = AuthContext::default();
                            for chunk in group {
                                let start = chunk.logical_offset as usize;
                                let part = data.slice(start..start + chunk.length as usize);
                                filesystem
                                    .write(&auth, chunk.inode, chunk.member_offset, &part)
                                    .await?;
                            }
                            Ok::<_, FsError>(())
                        }
                    });
                try_join_all(writes).await?;
            }
        }
        drop(admission);

        if fua {
            self.flush(device).await?;
        }

        Ok(())
    }

    pub async fn trim(
        &self,
        device: &NBDDevice,
        offset: u64,
        length: u32,
        fua: bool,
    ) -> CommandResult<()> {
        if out_of_bounds(offset, length, device.size) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let write_guard = device.gate.read().await;
        let groups = group_stripe_chunks(
            map_stripe_chunks(&device.backing, offset, length as u64)?,
            match &device.backing {
                NbdBacking::Single { .. } => 1,
                NbdBacking::Striped { members, .. } => members.len(),
            },
        );
        let trims = groups
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|group| {
                let filesystem = Arc::clone(&self.filesystem);
                async move {
                    let auth = AuthContext::default();
                    for chunk in group {
                        filesystem
                            .trim(&auth, chunk.inode, chunk.member_offset, chunk.length)
                            .await?;
                    }
                    Ok::<_, FsError>(())
                }
            });
        try_join_all(trims).await?;
        drop(write_guard);

        if fua {
            self.flush(device).await?;
        }

        Ok(())
    }

    pub async fn cache(&self, offset: u64, length: u32, device_size: u64) -> CommandResult<()> {
        if out_of_bounds(offset, length, device_size) {
            return Err(CommandError::InvalidArgument);
        }
        Ok(())
    }

    pub async fn flush(&self, device: &NBDDevice) -> CommandResult<()> {
        let _flush_guard = device.gate.write().await;
        self.filesystem
            .client_fsync()
            .await
            .map_err(|_| CommandError::IoError)?;

        self.filesystem.tracer.emit(
            &self.filesystem.inode_store,
            device.trace_inode,
            FileOperation::Fsync,
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommandError, NBDError, NBDHandler, NbdBacking, NbdExportGates, NbdMember, OptionResult,
        map_stripe_chunks, parse_stripe_manifest,
    };
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{AuthContext, SetAttributes, SetSize};
    use bytes::Bytes;
    use nbd_proto::{NBD_REP_ACK, NBD_REP_ERR_UNKNOWN, NBD_REP_INFO, NBD_REP_SERVER};
    use std::sync::Arc;

    const PUBLISHED_EXPORT: &[u8] = b"vm100";
    const STAGING_EXPORT: &[u8] = b".zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000000";
    const NEAR_PREFIX_EXPORT: &[u8] = b".zerofs-nbd-provision-v1-archive";

    fn root_credentials() -> Credentials {
        Credentials {
            uid: 0,
            gid: 0,
            gid_known: true,
            groups: [0; 16],
            groups_count: 1,
            groups_complete: true,
        }
    }

    async fn create_striped_export(filesystem: &Arc<ZeroFS>, nbd_dir: u64, name: &[u8]) {
        let credentials = root_credentials();
        let (export_dir, _) = filesystem
            .mkdir(&credentials, nbd_dir, name, &SetAttributes::default())
            .await
            .expect("create striped export directory");
        let manifest =
            br#"{"version":1,"stripe_bytes":4096,"members":["lane-0","lane-1","lane-2","lane-3"]}"#;
        let (marker_inode, _) = filesystem
            .create(
                &credentials,
                export_dir,
                super::NBD_STRIPE_MARKER.as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .expect("create stripe manifest");
        filesystem
            .write(
                &AuthContext::default(),
                marker_inode,
                0,
                &Bytes::from_static(manifest),
            )
            .await
            .expect("write stripe manifest");

        for name in [b"lane-0", b"lane-1", b"lane-2", b"lane-3"] {
            let (inode, _) = filesystem
                .create(&credentials, export_dir, name, &SetAttributes::default())
                .await
                .expect("create stripe member");
            filesystem
                .setattr(
                    &credentials,
                    inode,
                    &SetAttributes {
                        size: SetSize::Set(16 * 1024),
                        ..Default::default()
                    },
                )
                .await
                .expect("size stripe member");
        }
    }

    async fn striped_export() -> (Arc<ZeroFS>, NBDHandler, super::NBDDevice) {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let credentials = root_credentials();
        let (nbd_dir, _) = filesystem
            .mkdir(&credentials, 0, b".nbd", &SetAttributes::default())
            .await
            .expect("create .nbd directory");
        create_striped_export(&filesystem, nbd_dir, b"striped-test").await;

        let handler = NBDHandler::new(Arc::clone(&filesystem), Arc::new(NbdExportGates::default()));
        let device = handler
            .get_device(b"striped-test")
            .await
            .expect("discover striped export");
        (filesystem, handler, device)
    }

    async fn single_file_export() -> (Arc<ZeroFS>, NBDHandler, super::NBDDevice, u64) {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let credentials = root_credentials();
        let (nbd_dir, _) = filesystem
            .mkdir(&credentials, 0, b".nbd", &SetAttributes::default())
            .await
            .expect("create .nbd directory");
        let (inode, _) = filesystem
            .create(
                &credentials,
                nbd_dir,
                b"single-file-test",
                &SetAttributes::default(),
            )
            .await
            .expect("create single-file export");
        filesystem
            .setattr(
                &credentials,
                inode,
                &SetAttributes {
                    size: SetSize::Set(4096),
                    ..Default::default()
                },
            )
            .await
            .expect("size single-file export");

        let handler = NBDHandler::new(Arc::clone(&filesystem), Arc::new(NbdExportGates::default()));
        let device = handler
            .get_device(b"single-file-test")
            .await
            .expect("discover single-file export");
        (filesystem, handler, device, inode)
    }

    async fn handler_with_published_and_staging_exports() -> NBDHandler {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let (nbd_dir, _) = filesystem
            .mkdir(&root_credentials(), 0, b".nbd", &SetAttributes::default())
            .await
            .expect("create .nbd directory");
        create_striped_export(&filesystem, nbd_dir, PUBLISHED_EXPORT).await;
        create_striped_export(&filesystem, nbd_dir, STAGING_EXPORT).await;
        create_striped_export(&filesystem, nbd_dir, NEAR_PREFIX_EXPORT).await;
        NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()))
    }

    fn export_option_payload(name: &[u8]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(4 + name.len() + 2);
        payload.extend_from_slice(&(name.len() as u32).to_be_bytes());
        payload.extend_from_slice(name);
        payload.extend_from_slice(&0_u16.to_be_bytes());
        payload
    }

    #[test]
    fn striped_mapping_covers_each_logical_byte_once_across_rows() {
        let backing = NbdBacking::Striped {
            members: Arc::from([
                NbdMember {
                    inode: 10,
                    size: 32,
                },
                NbdMember {
                    inode: 11,
                    size: 32,
                },
                NbdMember {
                    inode: 12,
                    size: 32,
                },
                NbdMember {
                    inode: 13,
                    size: 32,
                },
            ]),
            stripe_bytes: 8,
        };

        let chunks = map_stripe_chunks(&backing, 6, 36).expect("valid mapping");
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (
                    chunk.member_index,
                    chunk.member_offset,
                    chunk.logical_offset,
                    chunk.length,
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, 6, 0, 2),
                (1, 0, 2, 8),
                (2, 0, 10, 8),
                (3, 0, 18, 8),
                (0, 8, 26, 8),
                (1, 8, 34, 2),
            ]
        );
        assert_eq!(chunks.iter().map(|chunk| chunk.length).sum::<u64>(), 36);
    }

    #[test]
    fn stripe_manifest_requires_a_bounded_aligned_unique_layout() {
        let manifest = parse_stripe_manifest(
            br#"{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}"#,
        )
        .expect("valid manifest");
        assert_eq!(manifest.stripe_bytes, 1024 * 1024);
        assert_eq!(manifest.members.len(), 4);

        for invalid in [
            br#"{"version":2,"stripe_bytes":1048576,"members":["a","b"]}"#.as_slice(),
            br#"{"version":1,"stripe_bytes":1000,"members":["a","b"]}"#.as_slice(),
            br#"{"version":1,"stripe_bytes":1048576,"members":["same","same"]}"#.as_slice(),
            br#"{"version":1,"stripe_bytes":1048576,"members":["only"]}"#.as_slice(),
        ] {
            assert!(parse_stripe_manifest(invalid).is_err());
        }
    }

    #[tokio::test]
    async fn striped_export_discovers_and_roundtrips_cross_lane_io() {
        let (_filesystem, handler, device) = striped_export().await;
        assert_eq!(device.size, 64 * 1024);
        let payload = Bytes::from(
            (0..22 * 1024)
                .map(|index| ((index * 31 + 7) % 251) as u8)
                .collect::<Vec<_>>(),
        );

        let admission = handler.begin_mutation(&device).await;
        handler
            .write_admitted(&device, 2048, &payload, false, admission)
            .await
            .expect("write across stripe rows");
        let read_back = handler
            .read(&device, 2048, payload.len() as u32)
            .await
            .expect("read across stripe rows");

        assert_eq!(read_back, payload);
    }

    #[tokio::test]
    async fn striped_read_returns_io_error_when_a_member_is_shorter_than_resolved() {
        let (filesystem, handler, device) = striped_export().await;
        let nbd_dir = filesystem
            .directory_store
            .get(0, b".nbd")
            .await
            .expect("find .nbd directory");
        let export_dir = filesystem
            .directory_store
            .get(nbd_dir, b"striped-test")
            .await
            .expect("find striped export");
        let first_member = filesystem
            .directory_store
            .get(export_dir, b"lane-0")
            .await
            .expect("find first stripe member");
        filesystem
            .setattr(
                &root_credentials(),
                first_member,
                &SetAttributes {
                    size: SetSize::Set(2048),
                    ..Default::default()
                },
            )
            .await
            .expect("truncate member after resolving the NBD device");

        assert!(matches!(
            handler.read(&device, 0, 4096).await,
            Err(CommandError::IoError)
        ));
    }

    #[tokio::test]
    async fn single_file_read_returns_io_error_when_backing_file_is_shorter_than_resolved() {
        let (filesystem, handler, device, inode) = single_file_export().await;
        filesystem
            .setattr(
                &root_credentials(),
                inode,
                &SetAttributes {
                    size: SetSize::Set(2048),
                    ..Default::default()
                },
            )
            .await
            .expect("truncate backing file after resolving the NBD device");

        assert!(matches!(
            handler.read(&device, 0, 4096).await,
            Err(CommandError::IoError)
        ));
    }

    #[tokio::test]
    async fn striped_trim_preserves_unaffected_bytes() {
        let (_filesystem, handler, device) = striped_export().await;
        let payload = Bytes::from(vec![0x5a; 24 * 1024]);
        let admission = handler.begin_mutation(&device).await;
        handler
            .write_admitted(&device, 0, &payload, false, admission)
            .await
            .expect("seed striped export");

        handler
            .trim(&device, 3 * 1024, 6 * 1024, false)
            .await
            .expect("trim across stripes");
        let read_back = handler
            .read(&device, 0, payload.len() as u32)
            .await
            .expect("read modified striped export");

        assert!(read_back[..3 * 1024].iter().all(|byte| *byte == 0x5a));
        assert!(read_back[3 * 1024..9 * 1024].iter().all(|byte| *byte == 0));
        assert!(
            read_back[9 * 1024..13 * 1024]
                .iter()
                .all(|byte| *byte == 0x5a)
        );
        assert!(read_back[13 * 1024..].iter().all(|byte| *byte == 0x5a));
    }

    #[tokio::test]
    async fn list_devices_pages_past_the_first_thousand_directory_entries() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let credentials = root_credentials();
        let (nbd_dir, _) = filesystem
            .mkdir(&credentials, 0, b".nbd", &SetAttributes::default())
            .await
            .expect("create .nbd directory");
        const DEVICE_COUNT: usize = 1005;
        for index in 0..DEVICE_COUNT {
            let name = format!("device-{index:04}");
            filesystem
                .create(
                    &credentials,
                    nbd_dir,
                    name.as_bytes(),
                    &SetAttributes::default(),
                )
                .await
                .expect("create NBD export");
        }

        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        let devices = handler.list_devices().await.expect("list all NBD exports");

        assert_eq!(devices.len(), DEVICE_COUNT);
        assert!(devices.iter().any(|device| device.name == b"device-1004"));
    }

    #[tokio::test]
    async fn list_hides_a_completed_provisioning_staging_export() {
        let handler = handler_with_published_and_staging_exports().await;

        let replies = match handler.list().await {
            OptionResult::Continue(replies) => replies,
            OptionResult::Done(_, _) => panic!("LIST unexpectedly completed negotiation"),
            OptionResult::Error(_, _) => panic!("LIST unexpectedly failed"),
        };
        let names = replies
            .into_iter()
            .filter(|reply| reply.reply_type == NBD_REP_SERVER)
            .map(|reply| {
                let name_length = u32::from_be_bytes(
                    reply.data[..4]
                        .try_into()
                        .expect("LIST reply contains a name length"),
                ) as usize;
                assert_eq!(reply.data.len(), 4 + name_length);
                reply.data[4..].to_vec()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![PUBLISHED_EXPORT.to_vec(), NEAR_PREFIX_EXPORT.to_vec()]
        );
    }

    #[tokio::test]
    async fn info_rejects_a_completed_provisioning_staging_export() {
        let handler = handler_with_published_and_staging_exports().await;

        let replies = match handler.info(&export_option_payload(STAGING_EXPORT)).await {
            OptionResult::Continue(replies) => replies,
            OptionResult::Done(_, _) => panic!("INFO unexpectedly completed negotiation"),
            OptionResult::Error(_, _) => panic!("INFO unexpectedly aborted negotiation"),
        };

        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_type, NBD_REP_ERR_UNKNOWN);
    }

    #[tokio::test]
    async fn go_rejects_a_completed_provisioning_staging_export() {
        let handler = handler_with_published_and_staging_exports().await;

        match handler.go(&export_option_payload(STAGING_EXPORT)).await {
            OptionResult::Error(NBDError::DeviceNotFound(name), replies) => {
                assert_eq!(name, STAGING_EXPORT);
                assert_eq!(replies.len(), 1);
                assert_eq!(replies[0].reply_type, NBD_REP_ERR_UNKNOWN);
            }
            OptionResult::Continue(_) => panic!("GO unexpectedly continued negotiation"),
            OptionResult::Done(_, _) => panic!("GO attached the staging export"),
            OptionResult::Error(_, _) => panic!("GO returned the wrong error"),
        }
    }

    #[tokio::test]
    async fn get_device_rejects_a_completed_provisioning_staging_export() {
        let handler = handler_with_published_and_staging_exports().await;

        assert!(matches!(
            handler.get_device(STAGING_EXPORT).await,
            Err(NBDError::DeviceNotFound(name)) if name == STAGING_EXPORT
        ));
        assert_eq!(
            handler
                .get_device(PUBLISHED_EXPORT)
                .await
                .expect("published export remains attachable")
                .name,
            PUBLISHED_EXPORT
        );
    }

    #[tokio::test]
    async fn info_accepts_a_legitimate_near_prefix_export() {
        let handler = handler_with_published_and_staging_exports().await;

        let replies = match handler
            .info(&export_option_payload(NEAR_PREFIX_EXPORT))
            .await
        {
            OptionResult::Continue(replies) => replies,
            OptionResult::Done(_, _) => panic!("INFO unexpectedly completed negotiation"),
            OptionResult::Error(_, _) => panic!("INFO rejected the legitimate export"),
        };

        assert_eq!(
            replies
                .iter()
                .map(|reply| reply.reply_type)
                .collect::<Vec<_>>(),
            vec![NBD_REP_INFO, NBD_REP_ACK]
        );
    }

    #[tokio::test]
    async fn go_accepts_a_legitimate_near_prefix_export() {
        let handler = handler_with_published_and_staging_exports().await;

        match handler.go(&export_option_payload(NEAR_PREFIX_EXPORT)).await {
            OptionResult::Done(device, replies) => {
                assert_eq!(device.name, NEAR_PREFIX_EXPORT);
                assert_eq!(
                    replies
                        .iter()
                        .map(|reply| reply.reply_type)
                        .collect::<Vec<_>>(),
                    vec![NBD_REP_INFO, NBD_REP_ACK]
                );
            }
            OptionResult::Continue(_) => panic!("GO unexpectedly continued negotiation"),
            OptionResult::Error(_, _) => panic!("GO rejected the legitimate export"),
        }
    }

    #[tokio::test]
    async fn get_device_accepts_a_legitimate_near_prefix_export() {
        let handler = handler_with_published_and_staging_exports().await;

        assert_eq!(
            handler
                .get_device(NEAR_PREFIX_EXPORT)
                .await
                .expect("near-prefix export remains attachable")
                .name,
            NEAR_PREFIX_EXPORT
        );
    }
}
