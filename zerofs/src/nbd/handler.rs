use super::error::{CommandError, CommandResult, NBDError, Result};
use super::out_of_bounds;
use super::{
    NBD_STRIPE_MANIFEST_MAX_BYTES, NBD_STRIPE_MARKER, StripeManifest, is_nbd_provision_staging_name,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::mutation::volatile_overlay::{VolatileAdmission, VolatileBudget};
use crate::fs::tracing::FileOperation;
use crate::fs::types::AuthContext;
use bytes::{Bytes, BytesMut};
use deku::DekuContainerWrite;
use futures::future::try_join_all;
use nbd_proto::{
    NBD_FLAG_SEND_TRIM, NBD_INFO_EXPORT, NBD_REP_ACK, NBD_REP_ERR_INVALID, NBD_REP_ERR_UNKNOWN,
    NBD_REP_INFO, NBD_REP_SERVER, NBDInfoExport, TRANSMISSION_FLAGS,
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
    backing: NbdBacking,
    state: Arc<NbdExportState>,
}

pub(crate) struct MutationAdmission {
    gate: OwnedRwLockReadGuard<()>,
    reserved: Option<VolatileAdmission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NbdMember {
    inode: u64,
    size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum NbdBacking {
    Single {
        inode: u64,
        size: u64,
    },
    Striped {
        directory_inode: u64,
        members: Arc<[NbdMember]>,
        stripe_bytes: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ExportIdentity {
    backing: NbdBacking,
}

struct NbdExportState {
    identity: ExportIdentity,
    gate: Arc<RwLock<()>>,
    volatile_mode: bool,
}

fn parse_stripe_manifest(data: &[u8]) -> Result<StripeManifest> {
    let manifest: StripeManifest = serde_json::from_slice(data)
        .map_err(|error| NBDError::Protocol(format!("invalid striped NBD manifest: {error}")))?;
    manifest.validate().map_err(NBDError::Protocol)?;
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
            ..
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
            size: self.size(),
            transmission_flags: self.transmission_flags(),
        }
    }

    pub(crate) fn transmission_flags(&self) -> u16 {
        if self.state.volatile_mode {
            TRANSMISSION_FLAGS & !NBD_FLAG_SEND_TRIM
        } else {
            TRANSMISSION_FLAGS
        }
    }

    /// Size presented to the NBD client, derived from the backing geometry.
    /// Striped overflow is rejected when the device is resolved.
    pub fn size(&self) -> u64 {
        match &self.backing {
            NbdBacking::Single { size, .. } => *size,
            NbdBacking::Striped { members, .. } => members
                .first()
                .map_or(0, |member| member.size * members.len() as u64),
        }
    }

    /// Inode reported to the file-operation tracer for this export.
    fn trace_inode(&self) -> u64 {
        match &self.backing {
            NbdBacking::Single { inode, .. } => *inode,
            NbdBacking::Striped {
                directory_inode, ..
            } => *directory_inode,
        }
    }
}

pub struct NbdExportGates {
    registry: StdMutex<ExportRegistry>,
    materialized_gates: StdMutex<HashMap<Vec<u8>, Weak<RwLock<()>>>>,
    volatile_budget: Option<Arc<VolatileBudget>>,
}

#[derive(Default)]
struct ExportRegistry {
    names: HashMap<Vec<u8>, ExportIdentity>,
    exports: HashMap<ExportIdentity, Arc<NbdExportState>>,
    inode_owners: HashMap<u64, ExportIdentity>,
}

impl Default for NbdExportGates {
    fn default() -> Self {
        Self::new(0)
    }
}

impl NbdExportGates {
    pub fn new(volatile_memory_bytes: u64) -> Self {
        Self::with_budget(
            (volatile_memory_bytes > 0).then(|| VolatileBudget::new(volatile_memory_bytes, 65_536)),
        )
    }

    pub fn with_budget(budget: Option<std::sync::Arc<VolatileBudget>>) -> Self {
        Self {
            registry: StdMutex::new(ExportRegistry::default()),
            materialized_gates: StdMutex::new(HashMap::new()),
            volatile_budget: budget,
        }
    }

    fn for_export(
        &self,
        name: &[u8],
        backing: &NbdBacking,
        filesystem: &Arc<ZeroFS>,
    ) -> Result<Arc<NbdExportState>> {
        let identity = ExportIdentity {
            backing: backing.clone(),
        };
        if self.volatile_budget.is_none() {
            let mut gates = self
                .materialized_gates
                .lock()
                .expect("NBD materialized export gate registry poisoned");
            let gate = gates.get(name).and_then(Weak::upgrade).unwrap_or_else(|| {
                let gate = Arc::new(RwLock::new(()));
                gates.insert(name.to_vec(), Arc::downgrade(&gate));
                gate
            });
            return Ok(Arc::new(NbdExportState {
                identity,
                gate,
                volatile_mode: false,
            }));
        }
        let mut registry = self
            .registry
            .lock()
            .expect("NBD export gate registry poisoned");
        if let Some(existing) = registry.names.get(name)
            && existing != &identity
        {
            return Err(NBDError::Protocol(format!(
                "NBD export '{}' changed backing identity while active",
                String::from_utf8_lossy(name)
            )));
        }
        if let Some(state) = registry.exports.get(&identity).cloned() {
            debug_assert_eq!(state.identity, identity);
            registry.names.insert(name.to_vec(), identity);
            return Ok(state);
        }

        let backing_inodes = backing.inodes();
        for inode in &backing_inodes {
            if let Some(owner) = registry.inode_owners.get(inode)
                && owner != &identity
            {
                return Err(NBDError::Protocol(format!(
                    "NBD backing inode {inode} is already owned by another active export"
                )));
            }
        }

        let state = Arc::new(NbdExportState {
            identity: identity.clone(),
            gate: Arc::new(RwLock::new(())),
            volatile_mode: true,
        });
        for inode in backing_inodes {
            registry.inode_owners.insert(inode, identity.clone());
        }
        registry.names.insert(name.to_vec(), identity.clone());
        registry.exports.insert(identity, Arc::clone(&state));
        Ok(state)
    }

    fn unactivated(&self, backing: &NbdBacking) -> Arc<NbdExportState> {
        Arc::new(NbdExportState {
            identity: ExportIdentity {
                backing: backing.clone(),
            },
            gate: Arc::new(RwLock::new(())),
            volatile_mode: self.volatile_budget.is_some(),
        })
    }

    pub(crate) async fn stop_and_drain(&self) -> CommandResult<()> {
        let states = {
            let registry = self
                .registry
                .lock()
                .expect("NBD export gate registry poisoned");
            registry.exports.values().cloned().collect::<Vec<_>>()
        };
        let mut exclusive = Vec::with_capacity(states.len());
        for state in &states {
            exclusive.push(state.gate.write().await);
        }
        drop(exclusive);
        Ok(())
    }

    pub(crate) fn fence_abort(&self) {}

    #[cfg(test)]
    fn runtimes(&self) -> Vec<()> {
        Vec::new()
    }
}

impl NbdBacking {
    fn inodes(&self) -> Vec<u64> {
        match self {
            Self::Single { inode, .. } => vec![*inode],
            Self::Striped { members, .. } => members.iter().map(|member| member.inode).collect(),
        }
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

                match self.resolve_device(name, entry.fileid, false).await {
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

        match self.get_device_with_activation(name, false).await {
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
        self.get_device_with_activation(name, true).await
    }

    async fn get_device_with_activation(&self, name: &[u8], activate: bool) -> Result<NBDDevice> {
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

        self.resolve_device(name, device_inode, activate).await
    }

    async fn resolve_device(
        &self,
        name: &[u8],
        device_inode: u64,
        activate: bool,
    ) -> Result<NBDDevice> {
        if is_nbd_provision_staging_name(name) {
            return Err(NBDError::DeviceNotFound(name.to_vec()));
        }
        match self.filesystem.inode_store.get(device_inode).await? {
            Inode::File(file_inode) => {
                let backing = NbdBacking::Single {
                    inode: device_inode,
                    size: file_inode.size,
                };
                let state = if activate {
                    self.export_gates
                        .for_export(name, &backing, &self.filesystem)?
                } else {
                    self.export_gates.unactivated(&backing)
                };
                Ok(NBDDevice {
                    name: name.to_vec(),
                    backing,
                    state,
                })
            }
            Inode::Directory(_) => {
                self.resolve_striped_device(name, device_inode, activate)
                    .await
            }
            _ => Err(NBDError::Protocol(format!(
                "NBD device '{}' is neither a regular file nor a striped export directory",
                String::from_utf8_lossy(name)
            ))),
        }
    }

    async fn resolve_striped_device(
        &self,
        name: &[u8],
        directory_inode: u64,
        activate: bool,
    ) -> Result<NBDDevice> {
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
        let mut member_inodes = HashSet::with_capacity(manifest.members.len());
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
            if !member_inodes.insert(member_inode) {
                return Err(NBDError::Protocol(
                    "striped NBD members must resolve to distinct backing inodes".to_string(),
                ));
            }
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
        member_size
            .unwrap_or(0)
            .checked_mul(members.len() as u64)
            .ok_or_else(|| NBDError::Protocol("striped NBD size overflow".to_string()))?;
        let backing = NbdBacking::Striped {
            directory_inode,
            members: members.into(),
            stripe_bytes: manifest.stripe_bytes,
        };
        let state = if activate {
            self.export_gates
                .for_export(name, &backing, &self.filesystem)?
        } else {
            self.export_gates.unactivated(&backing)
        };
        Ok(NBDDevice {
            name: name.to_vec(),
            backing,
            state,
        })
    }

    pub async fn read(&self, device: &NBDDevice, offset: u64, length: u32) -> CommandResult<Bytes> {
        if out_of_bounds(offset, length, device.size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(Bytes::new());
        }

        if device.state.volatile_mode {
            return read_visible(
                Arc::clone(&self.filesystem),
                device.backing.clone(),
                offset,
                length,
            )
            .await;
        }

        read_backing(
            Arc::clone(&self.filesystem),
            device.backing.clone(),
            offset,
            length,
        )
        .await
    }

    pub(crate) async fn begin_mutation(
        &self,
        device: &NBDDevice,
        length: usize,
    ) -> CommandResult<MutationAdmission> {
        let gate = Arc::clone(&device.state.gate).read_owned().await;
        let reserved = if device.state.volatile_mode {
            if let Some(overlay) = self.filesystem.volatile_overlay.get() {
                let inode = match &device.backing {
                    NbdBacking::Single { inode, .. } => *inode,
                    NbdBacking::Striped { members, .. } => {
                        members.first().map(|member| member.inode).unwrap_or(0)
                    }
                };
                Some(overlay.reserve(inode, length).await?)
            } else {
                None
            }
        } else {
            None
        };
        Ok(MutationAdmission { gate, reserved })
    }

    pub(crate) async fn write_admitted(
        &self,
        device: &NBDDevice,
        offset: u64,
        data: Bytes,
        fua: bool,
        admission: MutationAdmission,
    ) -> CommandResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        if offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > device.size())
        {
            return Err(CommandError::NoSpace);
        }

        drop(admission.reserved);
        if device.state.volatile_mode {
            write_visible(&self.filesystem, &device.backing, offset, &data).await?;
        } else {
            write_backing(&self.filesystem, &device.backing, offset, &data).await?;
        }
        drop(admission.gate);

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
        if device.state.volatile_mode {
            // Volatile mode deliberately does not advertise TRIM until trim is
            // represented in the same ordered overlay sequence as WRITE.
            return Err(CommandError::InvalidArgument);
        }
        if out_of_bounds(offset, length, device.size()) {
            return Err(CommandError::InvalidArgument);
        }

        if length == 0 {
            return Ok(());
        }

        let write_guard = device.state.gate.read().await;
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

    pub async fn cache(&self, device: &NBDDevice, offset: u64, length: u32) -> CommandResult<()> {
        if out_of_bounds(offset, length, device.size()) {
            return Err(CommandError::InvalidArgument);
        }
        Ok(())
    }

    pub async fn flush(&self, device: &NBDDevice) -> CommandResult<()> {
        let _flush_guard = device.state.gate.write().await;
        self.filesystem
            .wait_configured_durability()
            .await
            .map_err(|_| CommandError::IoError)?;

        self.filesystem.tracer.emit(
            &self.filesystem.inode_store,
            device.trace_inode(),
            FileOperation::Fsync,
        );

        Ok(())
    }
}

async fn write_visible(
    filesystem: &ZeroFS,
    backing: &NbdBacking,
    offset: u64,
    data: &Bytes,
) -> CommandResult<()> {
    let auth = AuthContext::default();
    for chunk in map_stripe_chunks(backing, offset, data.len() as u64)? {
        let start = chunk.logical_offset as usize;
        let end = start + chunk.length as usize;
        let piece = data.slice(start..end);
        filesystem
            .write_ack(&auth, chunk.inode, chunk.member_offset, &piece)
            .await?;
    }
    Ok(())
}

async fn read_visible(
    filesystem: Arc<ZeroFS>,
    backing: NbdBacking,
    offset: u64,
    length: u32,
) -> CommandResult<Bytes> {
    let auth = AuthContext::default();
    let chunks = map_stripe_chunks(&backing, offset, length as u64)?;
    let mut out = BytesMut::zeroed(length as usize);
    for chunk in chunks {
        let (piece, _) = filesystem
            .read_file_visible(
                Some(&auth),
                chunk.inode,
                chunk.member_offset,
                chunk.length as u32,
            )
            .await?;
        let start = chunk.logical_offset as usize;
        out[start..start + piece.len()].copy_from_slice(&piece);
    }
    Ok(out.freeze())
}

async fn read_backing(
    filesystem: Arc<ZeroFS>,
    backing: NbdBacking,
    offset: u64,
    length: u32,
) -> CommandResult<Bytes> {
    match &backing {
        NbdBacking::Single { inode, .. } => {
            let auth = AuthContext::default();
            let (data, _) = filesystem.read_file(&auth, *inode, offset, length).await?;
            if data.len() != length as usize {
                return Err(CommandError::IoError);
            }
            Ok(data)
        }
        NbdBacking::Striped { members, .. } => {
            let groups = group_stripe_chunks(
                map_stripe_chunks(&backing, offset, length as u64)?,
                members.len(),
            );
            let reads = groups
                .into_iter()
                .filter(|group| !group.is_empty())
                .map(|group| {
                    let filesystem = Arc::clone(&filesystem);
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
            let mut parts: Vec<(u64, Bytes)> =
                try_join_all(reads).await?.into_iter().flatten().collect();
            parts.sort_unstable_by_key(|(logical_offset, _)| *logical_offset);

            let mut output = BytesMut::with_capacity(length as usize);
            let mut expected_offset = 0_u64;
            for (logical_offset, data) in parts {
                if logical_offset != expected_offset {
                    return Err(CommandError::IoError);
                }
                expected_offset += data.len() as u64;
                output.extend_from_slice(&data);
            }
            if expected_offset != length as u64 {
                return Err(CommandError::IoError);
            }
            Ok(output.freeze())
        }
    }
}

async fn write_backing(
    filesystem: &Arc<ZeroFS>,
    backing: &NbdBacking,
    offset: u64,
    data: &Bytes,
) -> CommandResult<()> {
    match backing {
        NbdBacking::Single { inode, .. } => {
            let auth = AuthContext::default();
            filesystem.write(&auth, *inode, offset, data).await?;
        }
        NbdBacking::Striped { members, .. } => {
            let groups = group_stripe_chunks(
                map_stripe_chunks(backing, offset, data.len() as u64)?,
                members.len(),
            );
            let writes = groups
                .into_iter()
                .filter(|group| !group.is_empty())
                .map(|group| {
                    let filesystem = Arc::clone(filesystem);
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
    Ok(())
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
    use deku::DekuContainerRead;
    use nbd_proto::{
        NBD_FLAG_SEND_TRIM, NBD_REP_ACK, NBD_REP_ERR_UNKNOWN, NBD_REP_INFO, NBD_REP_SERVER,
        NBDInfoExport,
    };
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
            directory_inode: 1,
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
        assert_eq!(device.size(), 64 * 1024);
        let payload = Bytes::from(
            (0..22 * 1024)
                .map(|index| ((index * 31 + 7) % 251) as u8)
                .collect::<Vec<_>>(),
        );

        let admission = handler
            .begin_mutation(&device, payload.len())
            .await
            .unwrap();
        handler
            .write_admitted(&device, 2048, payload.clone(), false, admission)
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
    async fn materialized_export_can_be_resolved_again_after_resize() {
        let (filesystem, handler, first, inode) = single_file_export().await;
        assert_eq!(first.size(), 4096);
        drop(first);
        filesystem
            .setattr(
                &root_credentials(),
                inode,
                &SetAttributes {
                    size: SetSize::Set(8192),
                    ..Default::default()
                },
            )
            .await
            .expect("resize inactive materialized export");

        let reopened = handler
            .get_device(b"single-file-test")
            .await
            .expect("materialized mode must not freeze stale export geometry");
        assert_eq!(reopened.size(), 8192);
    }

    #[tokio::test]
    async fn striped_trim_preserves_unaffected_bytes() {
        let (_filesystem, handler, device) = striped_export().await;
        let payload = Bytes::from(vec![0x5a; 24 * 1024]);
        let admission = handler
            .begin_mutation(&device, payload.len())
            .await
            .unwrap();
        handler
            .write_admitted(&device, 0, payload.clone(), false, admission)
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
    async fn volatile_list_does_not_activate_write_runtimes() {
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
        filesystem
            .create(
                &credentials,
                nbd_dir,
                b"list-only",
                &SetAttributes::default(),
            )
            .await
            .expect("create list-only export");
        let gates = Arc::new(NbdExportGates::new(1024 * 1024));
        let handler = NBDHandler::new(filesystem, Arc::clone(&gates));

        let devices = handler.list_devices().await.expect("list exports");

        assert_eq!(devices.len(), 1);
        assert!(gates.runtimes().is_empty(), "LIST must not spawn workers");
    }

    #[tokio::test]
    async fn volatile_info_advertises_mode_without_activating_a_runtime() {
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
        filesystem
            .create(
                &credentials,
                nbd_dir,
                b"info-only",
                &SetAttributes::default(),
            )
            .await
            .expect("create info-only export");
        let gates = Arc::new(NbdExportGates::new(1024 * 1024));
        let handler = NBDHandler::new(filesystem, Arc::clone(&gates));

        let replies = match handler.info(&export_option_payload(b"info-only")).await {
            OptionResult::Continue(replies) => replies,
            OptionResult::Done(_, _) => panic!("INFO unexpectedly completed negotiation"),
            OptionResult::Error(error, _) => panic!("INFO failed: {error}"),
        };
        let info_reply = replies
            .iter()
            .find(|reply| reply.reply_type == NBD_REP_INFO)
            .expect("INFO export reply");
        let (_, info) =
            NBDInfoExport::from_bytes((&info_reply.data, 0)).expect("decode INFO reply");

        assert_eq!(info.transmission_flags & NBD_FLAG_SEND_TRIM, 0);
        assert!(
            gates.runtimes().is_empty(),
            "INFO must not permanently activate write workers"
        );
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
