use bytes::Bytes;
use ninep_proto::{
    DekuBytes, Message, P9_COUNT_FIELD_LEN, P9_HEADER_SIZE, P9_MAX_MSIZE, P9_MIN_MESSAGE_SIZE,
    P9_OP_ENVELOPE_LEN, P9_OP_FLAG_RETRY, P9_OP_ID_LEN, P9_SIZE_FIELD_LEN, P9Message, T_WRITE,
    Twrite, message_type,
};
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
pub(super) fn request_fid_footprint(type_byte: u8, body: &[u8]) -> FidFootprint {
    match type_byte {
        TVERSION_TYPE => FidFootprint::All,

        20 | 30 | 70 | 104 | 110 | 230 | 236 | 238 | 246 | 252 => fixed_two_fids(body),

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

        8 | 12 | 14 | 16 | 18 | 22 | 24 | 26 | 40 | 50 | 52 | 54 | 72 | 76 | 116 | 118
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

pub(super) async fn read_9p_frame<R>(
    reader: &mut R,
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<Bytes>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; P9_SIZE_FIELD_LEN];
    let header_result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(None),
        result = reader.read_exact(&mut header) => result,
    };
    if let Err(error) = header_result {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(error.into())
        };
    }

    let frame_len = u32::from_le_bytes(header) as usize;
    if frame_len < P9_MIN_MESSAGE_SIZE as usize || frame_len > P9_MAX_MSIZE as usize {
        anyhow::bail!("invalid 9P frame length {frame_len}");
    }
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(&header);
    frame.resize(frame_len, 0);
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Ok(None),
        result = reader.read_exact(&mut frame[P9_SIZE_FIELD_LEN..]) => { result?; }
    }
    Ok(Some(Bytes::from(frame)))
}
