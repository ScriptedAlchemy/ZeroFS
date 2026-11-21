use super::session::PENDING_REQUEST_TASK_BYTES;
use crate::fs::inode::Inode;
use bytes::{Bytes, BytesMut};
use ninep_proto::{
    DekuBytes, DirEntry, DirEntryPlus, Message, P9_COUNT_FIELD_LEN, P9_HEADER_SIZE, P9_IOHDRSZ,
    P9_MAX_MSIZE, P9_MIN_MESSAGE_SIZE, P9_OP_ENVELOPE_LEN, P9_OP_FLAG_RETRY, P9_OP_ID_LEN,
    P9_READDIR_BATCH_SIZE, P9_RLOPENATREAD_HDR, P9_SIZE_FIELD_LEN, P9Message, P9String, Qid, Stat,
    T_WRITE, Twrite, VERSION_9P2000L_ZEROFS_CI_UMASK, message_type,
};
use std::mem::size_of;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::sync::CancellationToken;

pub(super) const TFLUSH_TYPE: u8 = 108;
pub(super) const TVERSION_TYPE: u8 = 100;
pub(super) const TCLUNK_TYPE: u8 = 120;
pub(super) const P9_RWRITE_MAX_SIZE: usize = P9_HEADER_SIZE + P9_COUNT_FIELD_LEN;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum FidFootprint {
    #[default]
    None,
    One(u32),
    Two(u32, u32),
    All,
}

impl FidFootprint {
    pub(super) fn contains(self, fid: u32) -> bool {
        match self {
            Self::None => false,
            Self::One(first) => first == fid,
            Self::Two(first, second) => first == fid || second == fid,
            Self::All => true,
        }
    }
}

/// Request completion signal and fid footprint.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ShallowFrameMetadata {
    pub(super) fids: FidFootprint,
    pub(super) received_op: Option<([u8; P9_OP_ID_LEN], u8, u64)>,
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn fixed_one_fid(body: &[u8]) -> FidFootprint {
    read_u32_at(body, 0).map_or(FidFootprint::None, FidFootprint::One)
}

fn fixed_two_fids(body: &[u8]) -> FidFootprint {
    match (read_u32_at(body, 0), read_u32_at(body, 4)) {
        (Some(first), Some(second)) => FidFootprint::Two(first, second),
        _ => FidFootprint::None,
    }
}

/// Decode the fid-bearing prefix needed for receive-order barriers.
fn request_fid_footprint(type_byte: u8, body: &[u8]) -> FidFootprint {
    match type_byte {
        TVERSION_TYPE => FidFootprint::All,

        20 | 30 | 70 | 104 | 110 | 226 | 230 | 236 | 238 | 246 | 252 => fixed_two_fids(body),

        // `Trenameat.newdirfid` follows the variable-length old name.
        74 | 235 => {
            let Some(old_dir) = read_u32_at(body, 0) else {
                return FidFootprint::None;
            };
            let Some(name_len) = body
                .get(4..6)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u16::from_le_bytes)
            else {
                return FidFootprint::None;
            };
            let new_dir_offset = 6usize.saturating_add(usize::from(name_len));
            read_u32_at(body, new_dir_offset).map_or(FidFootprint::None, |new_dir| {
                FidFootprint::Two(old_dir, new_dir)
            })
        }

        8 | 12 | 14 | 16 | 18 | 22 | 24 | 26 | 32 | 40 | 50 | 52 | 54 | 72 | 76 | 116 | 118
        | TCLUNK_TYPE | 228 | 232 | 240 | 242 | 244 | 248 | 250 | 254 => fixed_one_fid(body),
        _ => FidFootprint::None,
    }
}

/// Inspect framing, mutation envelope, and fid prefix without copying the body.
pub(super) fn inspect_frame_metadata(frame: &[u8], zerofs_protocol: bool) -> ShallowFrameMetadata {
    let type_byte = frame[4];
    let carries_op = zerofs_protocol && P9Message::carries_op_id(type_byte);
    let body_offset = P9_HEADER_SIZE + usize::from(carries_op) * P9_OP_ENVELOPE_LEN;

    let received_op = if carries_op && frame.len() >= body_offset {
        let mut op_id = [0; P9_OP_ID_LEN];
        op_id.copy_from_slice(&frame[P9_HEADER_SIZE..P9_HEADER_SIZE + P9_OP_ID_LEN]);
        let origin_offset = P9_HEADER_SIZE + P9_OP_ID_LEN + 1;
        let origin_epoch = u64::from_le_bytes(
            frame[origin_offset..origin_offset + 8]
                .try_into()
                .expect("validated operation envelope"),
        );
        Some((op_id, frame[P9_HEADER_SIZE + P9_OP_ID_LEN], origin_epoch))
    } else {
        None
    };

    ShallowFrameMetadata {
        fids: frame.get(body_offset..).map_or(FidFootprint::None, |body| {
            request_fid_footprint(type_byte, body)
        }),
        received_op,
    }
}

/// Decode only the fixed Twrite prefix for an epoch-zero RETRY. Such a retry
/// can only replay the durable result of its original request, so retaining its
/// bulk payload while waiting is unnecessary. Non-zero epochs keep the full
/// payload because promotion grace may allow them to become the applier.
pub(super) fn shallow_epoch_zero_retry_write(
    frame: &[u8],
    metadata: ShallowFrameMetadata,
) -> Option<P9Message> {
    let (op_id, op_flags, origin_epoch) = metadata.received_op?;
    if frame.get(4) != Some(&T_WRITE) || op_flags & P9_OP_FLAG_RETRY == 0 || origin_epoch != 0 {
        return None;
    }

    let body_offset = P9_HEADER_SIZE + P9_OP_ENVELOPE_LEN;
    let body = frame.get(body_offset..)?;
    let fid = read_u32_at(body, 0)?;
    let offset = u64::from_le_bytes(body.get(4..12)?.try_into().ok()?);
    let count = read_u32_at(body, 12)?;
    let payload_end = 16usize.checked_add(count as usize)?;
    body.get(..payload_end)?;
    let tag = u16::from_le_bytes(frame.get(5..7)?.try_into().ok()?);

    Some(P9Message::new_with_op_id_flags_and_origin(
        tag,
        op_id,
        op_flags,
        origin_epoch,
        Message::Twrite(Twrite {
            fid,
            offset,
            count,
            data: DekuBytes::from(Bytes::new()),
        }),
    ))
}

/// Conservative reply charge for a request type when its frame does not
/// carry a usable bound: an unknown or future type, or a malformed count.
pub(super) fn possible_response_bytes(type_byte: u8) -> usize {
    match type_byte {
        T_WRITE => P9_RWRITE_MAX_SIZE,
        // Large read-like handlers retain their source payload while the
        // protocol encoder materializes the wire response. Charge both live
        // buffers; the process-wide byte semaphore remains the hard bound.
        message_type::TREAD
        | message_type::TREADDIR
        | message_type::TLOPENATREAD
        | message_type::TREADDIRATTR => 2 * P9_MAX_MSIZE as usize,
        _ => P9_MAX_MSIZE as usize,
    }
}

/// Every request may be answered with `Rlerror`: header and errno.
const RLERROR_BYTES: usize = P9_HEADER_SIZE + 4;
/// A 9P string on the wire: a u16 length and at most `u16::MAX` bytes.
const MAX_STRING_BYTES: usize = 2 + u16::MAX as usize;
/// Heap behind one filesystem name. Names are at most 255 bytes, and the
/// allocator rounds that allocation to 256.
const NAME_HEAP_BYTES: usize = 256;
/// Largest `Rgetlock`: type, start, length, proc_id and the holder's client id.
const RGETLOCK_MAX_BYTES: usize = P9_HEADER_SIZE + 1 + 8 + 8 + 4 + MAX_STRING_BYTES;
/// A count-bounded request: header, then fid[4] and offset[8] (Tread,
/// Treaddir, Treaddirattr) or fid[4] newfid[4] flags[4] (Tlopenatread), then
/// count[4].
const COUNT_FRAME_LEN: usize = P9_HEADER_SIZE + 12 + P9_COUNT_FIELD_LEN;
/// Per walked name: its wire qid, plus the handler's qid vector and the path
/// vector it extends, each of which can hold twice the entries it uses.
const WALK_BYTES_PER_NAME: usize = Qid::WIRE_SIZE + 2 * size_of::<Qid>() + 2 * size_of::<Bytes>();
/// Directory listings, for each protocol entry type.
const READDIR_BATCH_BYTES: usize = readdir_batch_bytes(size_of::<DirEntry>());
const READDIRATTR_BATCH_BYTES: usize = readdir_batch_bytes(size_of::<DirEntryPlus>());
/// `Txattrwalk` reads the value or the name list before it takes the xattr
/// permit that keeps the value on the fid. A value is at most
/// `XATTR_VALUE_MAX`. A list is at most `XATTR_COUNT_MAX` names of 256 bytes
/// (1 MiB) in a vector that may have doubled to 2 MiB, plus the listed keys
/// and their handles. Twice the value maximum covers either.
const XATTRWALK_SOURCE_BYTES: usize = 2 * crate::fs::XATTR_VALUE_MAX;

/// What a readdir handler holds for one batch besides its reply payload.
///
/// `readdir_opened` returns at most `P9_READDIR_BATCH_SIZE` rows plus "." and
/// "..", whatever the request count, and the handler then trims them to the
/// count. Per row it holds the scanned row with its decoded inode, the inode
/// looked up for a row that had none, the lookup indices, the filesystem
/// entry, and the protocol entry. Each vector can hold twice the rows it uses.
/// The entry name moves from row to protocol entry, and an inode can own a
/// second name. A symlink inode's target is not counted here.
const fn readdir_batch_bytes(protocol_entry: usize) -> usize {
    let rows = (P9_READDIR_BATCH_SIZE + 2).next_power_of_two();
    let per_row = size_of::<(u64, Vec<u8>, u64, Option<Inode>)>()
        + size_of::<(usize, Option<Inode>)>()
        + size_of::<(usize, u64)>()
        + size_of::<usize>()
        + size_of::<crate::fs::types::DirEntry>()
        + protocol_entry
        + 2 * NAME_HEAP_BYTES;
    rows * per_row
}

/// The request count of a well-formed count-bounded frame, or `None` when the
/// frame does not have exactly that layout.
fn count_field(frame: &[u8]) -> Option<usize> {
    if frame.len() != COUNT_FRAME_LEN || read_u32_at(frame, 0) != Some(COUNT_FRAME_LEN as u32) {
        return None;
    }
    read_u32_at(frame, COUNT_FRAME_LEN - P9_COUNT_FIELD_LEN).map(|count| count as usize)
}

/// A count clamped the way the handler clamps it, using the protocol maximum
/// rather than the negotiated msize, which a concurrent Tversion may change.
fn clamped_payload(count: usize, reply_header: usize) -> usize {
    count.min(P9_MAX_MSIZE as usize - reply_header)
}

/// `nwname` as declared by a walk frame (fid[4] newfid[4] nwname[2] ...).
fn declared_walk_names(frame: &[u8]) -> Option<usize> {
    let offset = P9_HEADER_SIZE + 8;
    let bytes = frame.get(offset..offset + 2)?;
    Some(usize::from(u16::from_le_bytes([bytes[0], bytes[1]])))
}

/// Qids a walk reply can carry: no more than the names that parse, and each
/// name takes at least its two-byte length on the wire.
fn walk_reply_qids(frame: &[u8]) -> Option<usize> {
    let names_on_wire = frame.len().checked_sub(P9_HEADER_SIZE + 10)? / 2;
    Some(declared_walk_names(frame)?.min(names_on_wire))
}

/// Bytes the decoded request holds besides its frame. `Twrite` decodes
/// without copying its payload. Every other request copies its variable
/// fields out of the frame, which stays alive until decoding ends, so it holds
/// at most the frame again. A walk also allocates one string header per
/// declared name, where the wire spends two bytes, in a vector that may be
/// sized by the declared count before the names are read.
pub(super) fn decoded_request_bytes(frame: &[u8]) -> usize {
    match frame.get(4).copied().unwrap_or_default() {
        T_WRITE => 0,
        message_type::TWALK | message_type::TWALKGETATTR | message_type::TWALKGETATTREXACT => {
            frame.len() + 2 * declared_walk_names(frame).unwrap_or(0) * size_of::<P9String>()
        }
        _ => frame.len(),
    }
}

/// Handler state one request holds besides its protocol buffers. The request
/// task is the spawned request future, which debug assertions in
/// `start_9p_request` keep below `PENDING_REQUEST_TASK_BYTES`. The heap a
/// handler loads is bounded by what one request touches. A rename, the widest, holds five inodes at once (both
/// directories, source, target, and one at a time on its cycle check), each
/// of which may own a 256-byte name, and clones two fids whose path vectors
/// take 32 bytes per directory level. `REQUEST_HEAP_BYTES` covers those
/// inodes and two paths 48 levels deep. A symlink target held in a loaded
/// inode is not counted.
const REQUEST_HEAP_BYTES: usize = 5 * NAME_HEAP_BYTES + 2 * 48 * size_of::<Bytes>();
pub(super) const REQUEST_STATE_BYTES: usize = PENDING_REQUEST_TASK_BYTES + REQUEST_HEAP_BYTES;

/// Everything one admitted frame may hold: the frame, its decoded request,
/// its reply charge, and the request's task and handler state.
pub(super) fn frame_admission_bytes(frame: &[u8]) -> usize {
    frame.len()
        + decoded_request_bytes(frame)
        + possible_frame_response_bytes(frame)
        + REQUEST_STATE_BYTES
}

/// The reply charge for one request, keyed by its message type.
///
/// Each arm is the largest reply the handler can send, from the reply's wire
/// layout, plus any buffer the handler builds to produce it. A fixed-size
/// reply is built on the request task and copied once into its wire buffer,
/// so its charge is its wire size. A reply whose payload the handler reads
/// into its own buffer holds that source and the wire copy together, so its
/// payload counts twice. Types without an arm keep the conservative charge.
/// Every reply may instead be `Rlerror`, so no charge is below that.
pub(super) fn possible_frame_response_bytes(frame: &[u8]) -> usize {
    use message_type as t;
    const H: usize = P9_HEADER_SIZE;
    const QID: usize = Qid::WIRE_SIZE;
    const STAT: usize = Stat::WIRE_SIZE;
    const IOHDR: usize = P9_IOHDRSZ as usize;
    const OPENATREAD_HDR: usize = P9_RLOPENATREAD_HDR as usize;

    let type_byte = frame.get(4).copied().unwrap_or_default();
    let reply = match type_byte {
        // Rversion: msize[4] version[s]. The server answers with one of its
        // own dialect names or "unknown".
        t::TVERSION => H + 4 + 2 + VERSION_9P2000L_ZEROFS_CI_UMASK.len(),
        // Rattach and Rrebind: qid[13].
        t::TATTACH | t::TREBIND => H + QID,
        // Rwalk: nwqid[2] qid[13] per name walked. The handler also keeps a
        // qid vector and extends the fid's path vector by one per name.
        t::TWALK => match walk_reply_qids(frame) {
            Some(qids) => H + 2 + qids * WALK_BYTES_PER_NAME,
            None => return possible_response_bytes(type_byte),
        },
        // Rwalkgetattr: Rwalk followed by stat[145].
        t::TWALKGETATTR | t::TWALKGETATTREXACT => match walk_reply_qids(frame) {
            Some(qids) => H + 2 + qids * WALK_BYTES_PER_NAME + STAT,
            None => return possible_response_bytes(type_byte),
        },
        // Rlopen, Rlopenat and Rlcreate: qid[13] iounit[4].
        t::TLOPEN | t::TLOPENAT | t::TLCREATE => H + QID + 4,
        // Rread: count[4] data[count]. The handler clamps count to the
        // negotiated msize. It reads into its own buffer, and a short read
        // inside an extent can keep that whole decoded extent alive, so the
        // source is at least one extent.
        t::TREAD => match count_field(frame) {
            Some(count) => 2 * (IOHDR + clamped_payload(count, IOHDR).max(crate::fs::EXTENT_SIZE)),
            None => return possible_response_bytes(type_byte),
        },
        // Rlopenatread: qid[13] iounit[4] eof[1] count[4] data[count], with
        // the same read source as Tread.
        t::TLOPENATREAD => match count_field(frame) {
            Some(count) => {
                2 * (OPENATREAD_HDR
                    + clamped_payload(count, OPENATREAD_HDR).max(crate::fs::EXTENT_SIZE))
            }
            None => return possible_response_bytes(type_byte),
        },
        // Rreaddir: count[4] data[count]. The handler encodes the entries
        // into a data buffer and then into the wire buffer, and holds the
        // whole batch it listed, which the count does not bound.
        t::TREADDIR => match count_field(frame) {
            Some(count) => 2 * (IOHDR + clamped_payload(count, IOHDR)) + READDIR_BATCH_BYTES,
            None => return possible_response_bytes(type_byte),
        },
        // Rreaddirattr: as Rreaddir, with a stat in every entry.
        t::TREADDIRATTR => match count_field(frame) {
            Some(count) => 2 * (IOHDR + clamped_payload(count, IOHDR)) + READDIRATTR_BATCH_BYTES,
            None => return possible_response_bytes(type_byte),
        },
        // Rwrite: count[4].
        T_WRITE => P9_RWRITE_MAX_SIZE,
        // Empty replies. Tclunk on an xattr fid commits a value that is
        // already charged to that fid's xattr permit, which it keeps until
        // the commit ends. Txattrcreate takes that permit before it
        // allocates the value.
        t::TCLUNK
        | t::TSETATTR
        | t::TFALLOCATE
        | t::TLINK
        | t::TRENAME
        | t::TRENAMEAT
        | t::TRENAMENOREPLACE
        | t::TUNLINKAT
        | t::TFSYNC
        | t::TFSYNCDUR
        | t::TFLUSH
        | t::TXATTRCREATE => H,
        // Rgetattr: valid[8] stat[145].
        t::TGETATTR => H + 8 + STAT,
        // Rmkdir, Rsymlink and Rmknod: qid[13].
        t::TMKDIR | t::TSYMLINK | t::TMKNOD => H + QID,
        // The private attr compounds reply with stat[145].
        t::TSETATTRATTR | t::TMKDIRATTR | t::TSYMLINKATTR | t::TMKNODATTR | t::TLINKATTR => {
            H + STAT
        }
        // Rlcreateattr: iounit[4] stat[145].
        t::TLCREATEATTR => H + 4 + STAT,
        // Rreadlink: target[s]. The handler refuses a target longer than a
        // 9P string can carry, and moves the target out of the inode it read,
        // so it holds the target once besides the wire copy.
        t::TREADLINK => 2 * MAX_STRING_BYTES + H,
        // Rgetlock: the reported holder's client id is a 9P string. The lock
        // manager hands the handler its own copy, which moves into the reply.
        t::TGETLOCK => 2 * RGETLOCK_MAX_BYTES,
        // Rlock: status[1].
        t::TLOCK => H + 1,
        // Rstatfs: type[4] bsize[4] blocks, bfree, bavail, files, ffree and
        // fsid[8 each] namelen[4].
        t::TSTATFS => H + 4 + 4 + 6 * 8 + 4,
        // Rgetlineage: token[8] writer_epoch[8].
        t::TGETLINEAGE => H + 8 + 8,
        // Rxattrwalk: size[8], plus the value or list read before the fid's
        // xattr permit is taken.
        t::TXATTRWALK => H + 8 + XATTRWALK_SOURCE_BYTES,
        _ => return possible_response_bytes(type_byte),
    };
    reply.max(RLERROR_BYTES)
}

/// The fixed fields a reader needs before it commits memory to a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FrameHeader {
    pub(super) len: usize,
    pub(super) type_byte: u8,
}

#[cfg(test)]
pub(super) async fn read_9p_frame<R>(
    reader: &mut R,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    Ok(read_9p_frame_charged(reader, shutdown, async |_| Ok(()))
        .await?
        .map(|(frame, ())| frame))
}

/// Read one frame, calling `charge` with its header before allocating the
/// body. `charge` decides what memory credit covers the frame; shutdown
/// during the header, the charge or the body ends the read cleanly.
pub(super) async fn read_9p_frame_charged<R, C>(
    reader: &mut R,
    shutdown: &CancellationToken,
    charge: impl AsyncFnOnce(FrameHeader) -> anyhow::Result<C>,
) -> anyhow::Result<Option<(Bytes, C)>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; P9_HEADER_SIZE];
    let header_result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(None),
        result = reader.read_exact(&mut header[..P9_SIZE_FIELD_LEN]) => result,
    };
    if let Err(error) = header_result {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(error.into())
        };
    }

    let frame_len = u32::from_le_bytes(
        header[..P9_SIZE_FIELD_LEN]
            .try_into()
            .expect("size field is four bytes"),
    ) as usize;
    if frame_len < P9_MIN_MESSAGE_SIZE as usize || frame_len > P9_MAX_MSIZE as usize {
        anyhow::bail!("invalid 9P frame length {frame_len}");
    }
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(None),
        result = reader.read_exact(&mut header[P9_SIZE_FIELD_LEN..]) => { result?; }
    }
    let charged = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(None),
        charged = charge(FrameHeader {
            len: frame_len,
            type_byte: header[P9_SIZE_FIELD_LEN],
        }) => charged?,
    };

    let mut frame = BytesMut::with_capacity(frame_len);
    frame.extend_from_slice(&header);
    // The body is initialized by AsyncRead, rather than zero-filled and then
    // immediately overwritten. Keep the read bounded even if the allocator
    // gives BytesMut more capacity than requested: the next frame must remain
    // available for its own admission decision.
    let mut body = reader.take((frame_len - P9_HEADER_SIZE) as u64);
    while frame.len() < frame_len {
        let received = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            result = body.read_buf(&mut frame) => result?,
        };
        if received == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated 9P request body",
            )
            .into());
        }
    }
    Ok(Some((frame.freeze(), charged)))
}

#[cfg(test)]
mod reader_tests;
