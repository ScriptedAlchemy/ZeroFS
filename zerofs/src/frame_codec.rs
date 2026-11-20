//! Shared compress-then-encrypt frame codec.
//!
//! Used by the SlateDB [`BlockTransformer`](crate::block_transformer) (with an
//! empty AAD, preserving the historical on-disk SST block format) and by the
//! data-plane segment writer (with a per-frame AAD that binds a frame to its
//! segment, slot, and logical block so it cannot be lifted elsewhere).
//!
//! Sealed frame wire format:
//!
//! ```text
//! [nonce: 24][ ciphertext( compress(plain) ) + tag: 16 ]
//! ```
//!
//! Compression is auto-detected on `open` from the (decrypted) payload, so a
//! codec configured for one algorithm decodes frames written by the other.

use chacha20poly1305::{
    Key, Tag, XChaCha20Poly1305, XNonce,
    aead::{AeadInPlace, KeyInit},
};
use hkdf::Hkdf;
use rand::{RngCore, thread_rng};
use sha2::Sha256;

use crate::config::CompressionConfig;

const NONCE_SIZE: usize = 24;
const TAG_SIZE: usize = 16;
/// Fixed per-frame expansion of [`FrameCodec::seal_compressed`]: the prepended
/// nonce plus the appended authentication tag. See [`Compressed::sealed_len`].
const SEALED_FRAME_OVERHEAD: usize = NONCE_SIZE + TAG_SIZE;
pub(crate) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("compression failed: {0}")]
    Compress(String),
    #[error("decompression failed: {0}")]
    Decompress(String),
    #[error("encryption failed")]
    Encrypt,
    #[error("frame too short: {0} bytes")]
    TooShort(usize),
    #[error("decryption failed (wrong key, AAD mismatch, or corrupt frame)")]
    Decrypt,
}

/// A frame's compressed (but not encrypted) payload: what sits between
/// [`FrameCodec::compress`] and [`FrameCodec::seal_compressed`], or comes out of
/// [`FrameCodec::open_compressed`]. A distinct type so plaintext can't be sealed
/// as if pre-compressed (double compression on read) nor a compressed payload
/// served as plaintext; the only ways out are `seal_compressed` (re-encrypt
/// as-is, the compaction passthrough) and `decompress`. The allocation includes
/// a nonce-sized prefix and tag capacity, so sealing can consume it in place;
/// the prefix is not part of the compressed payload returned by `len`.
pub(crate) struct Compressed(Vec<u8>);

impl Compressed {
    /// Stored payload size, the unit of compaction's gather accounting.
    #[allow(clippy::len_without_is_empty)] // a compressed payload is never empty
    pub(crate) fn len(&self) -> usize {
        self.0.len() - NONCE_SIZE
    }

    /// Exact byte length [`FrameCodec::seal_compressed`] will return for this
    /// payload. The wire format prepends a fixed-size nonce and appends a
    /// fixed-size tag around a stream cipher, so the sealed size depends only
    /// on the compressed size — never on the key, the AAD, or the nonce draw.
    ///
    /// The extent write path relies on this: it reserves a frame's byte range
    /// in the open segment buffer under a short lock, then runs the AEAD
    /// outside that lock and copies the result into the reserved range.
    pub(crate) fn sealed_len(&self) -> usize {
        self.len() + SEALED_FRAME_OVERHEAD
    }

    fn payload(&self) -> &[u8] {
        &self.0[NONCE_SIZE..]
    }
}

/// Compress-then-encrypt primitive over a single frame.
///
/// Holds a derived XChaCha20-Poly1305 subkey and a compression config. Cheap to
/// `seal`/`open`; the async / `spawn_blocking` policy lives in the callers.
pub struct FrameCodec {
    cipher: XChaCha20Poly1305,
    compression: CompressionConfig,
}

impl FrameCodec {
    /// Derive a subkey from `master_key` via HKDF-SHA256 with `info` and build a codec.
    pub fn new(master_key: &[u8; 32], info: &[u8], compression: CompressionConfig) -> Self {
        let hk = Hkdf::<Sha256>::new(None, master_key);
        let mut subkey = [0u8; 32];
        hk.expand(info, &mut subkey)
            .expect("valid HKDF output length");
        Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&subkey)),
            compression,
        }
    }

    /// Whether encoding is cheap enough to run inline rather than on a blocking thread.
    pub(crate) fn encode_is_cheap(&self) -> bool {
        match self.compression {
            CompressionConfig::Lz4 => true,
            CompressionConfig::Zstd(level) => level <= 12,
        }
    }

    /// Compress then encrypt `plain`, binding `aad`. Returns `[nonce][ct+tag]`.
    pub(crate) fn seal(&self, plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.seal_compressed(self.compress(plain)?, aad)
    }

    /// Encrypt an already-[`Self::compress`]ed payload, binding `aad`.
    /// `seal(plain, aad)` is exactly `seal_compressed(compress(plain), aad)`;
    /// the split exists so callers can run the compression outside a lock whose
    /// critical section must assign the identifiers the AAD binds, and so a
    /// relocation can rebind a frame's AAD from [`Self::open_compressed`]'s
    /// output without a decompress/recompress round trip.
    pub(crate) fn seal_compressed(
        &self,
        Compressed(mut out): Compressed,
        aad: &[u8],
    ) -> Result<Vec<u8>, CodecError> {
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        thread_rng().fill_bytes(&mut nonce_bytes);
        // Compression and verified relocation both leave the nonce prefix
        // and tag capacity in this allocation. Encrypt it directly instead of
        // allocating another buffer and copying every compressed frame.
        out[..NONCE_SIZE].copy_from_slice(&nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let tag = self
            .cipher
            .encrypt_in_place_detached(nonce, aad, &mut out[NONCE_SIZE..])
            .map_err(|_| CodecError::Encrypt)?;
        out.extend_from_slice(tag.as_slice());
        Ok(out)
    }

    /// Decrypt (verifying `aad`) then decompress a frame produced by [`Self::seal`].
    pub(crate) fn open(&self, frame: &[u8], aad: &[u8]) -> Result<Vec<u8>, CodecError> {
        let compressed = self.open_compressed(frame, aad)?;
        self.decompress(compressed.payload())
    }

    /// Decrypt (verifying `aad`) a frame, returning its still-compressed payload.
    /// `open(frame, aad)` is exactly `decompress(open_compressed(frame, aad))`;
    /// the split lets a relocation verify and rebind a frame without ever
    /// materializing (or recompressing) the plaintext.
    pub(crate) fn open_compressed(
        &self,
        frame: &[u8],
        aad: &[u8],
    ) -> Result<Compressed, CodecError> {
        if frame.len() < NONCE_SIZE + TAG_SIZE {
            return Err(CodecError::TooShort(frame.len()));
        }
        let tag_start = frame.len() - TAG_SIZE;
        let nonce = XNonce::from_slice(&frame[..NONCE_SIZE]);
        let tag = Tag::from_slice(&frame[tag_start..]);
        let mut out = frame.to_vec();
        self.cipher
            .decrypt_in_place_detached(nonce, aad, &mut out[NONCE_SIZE..tag_start], tag)
            .map_err(|_| CodecError::Decrypt)?;
        out.truncate(tag_start);
        Ok(Compressed(out))
    }

    /// Largest [`Compressed::sealed_len`] a `plain_len`-byte input can produce:
    /// the length prefix, the compressor's worst case, nonce and tag.
    pub(crate) fn sealed_bound(&self, plain_len: usize) -> usize {
        let payload_bound = match self.compression {
            CompressionConfig::Lz4 => lz4_flex::block::get_maximum_output_size(plain_len),
            CompressionConfig::Zstd(_) => zstd::zstd_safe::compress_bound(plain_len),
        };
        4 + payload_bound + SEALED_FRAME_OVERHEAD
    }

    /// Compress `data` with the configured codec. Pure: the output depends only
    /// on the bytes and the configured level, never on shared state, so it can
    /// run on any thread, outside any lock, and feed [`Self::seal_compressed`].
    pub(crate) fn compress(&self, data: &[u8]) -> Result<Compressed, CodecError> {
        // Both codecs keep the existing LE-u32 plaintext length prefix. The
        // surrounding nonce space and tag capacity are retained until sealing,
        // including when compression and encryption run on different threads.
        let payload_bound = match self.compression {
            CompressionConfig::Lz4 => lz4_flex::block::get_maximum_output_size(data.len()),
            CompressionConfig::Zstd(_) => zstd::zstd_safe::compress_bound(data.len()),
        };
        let payload_start = NONCE_SIZE + 4;
        let mut out = Vec::with_capacity(payload_start + payload_bound + TAG_SIZE);
        out.resize(payload_start, 0);
        out[NONCE_SIZE..payload_start].copy_from_slice(&(data.len() as u32).to_le_bytes());
        let compressed_len = match self.compression {
            CompressionConfig::Lz4 => {
                // The safe LZ4 slice API requires initialized output, as does
                // its own compress_prepend_size implementation.
                out.resize(payload_start + payload_bound, 0);
                lz4_flex::block::compress_into(data, &mut out[payload_start..])
                    .map_err(|e| CodecError::Compress(e.to_string()))?
            }
            CompressionConfig::Zstd(level) => {
                // Zstd's safe WriteBuf API can append directly into a Vec's
                // spare capacity. Avoid zeroing a plaintext-sized destination
                // when the compressed result may be only a few bytes.
                let mut output = std::io::Cursor::new(&mut out);
                output.set_position(payload_start as u64);
                zstd_compress_into(data, level, &mut output)?
            }
        };
        out.truncate(payload_start + compressed_len);
        // Encoded frames can outlive this call in byte-counted caches and
        // queues. Do not retain compression-bound capacity there, especially
        // for highly compressible data; keep only the future tag's capacity.
        let sealed_len = out.len() + TAG_SIZE;
        if matches!(self.compression, CompressionConfig::Zstd(_)) && out.capacity() / 2 > sealed_len
        {
            // Recycling the large zstd scratch allocation is cheaper than
            // repeatedly shrinking it into a tiny allocation size class. Only
            // the small compressed result is copied, still avoiding the old
            // separate size-prefix and encryption output allocations.
            let mut compact = Vec::with_capacity(sealed_len);
            compact.extend_from_slice(&out);
            out = compact;
        } else {
            out.shrink_to(sealed_len);
        }
        Ok(Compressed(out))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        // Zstd frames carry the magic at offset 4 (after our LE size prefix);
        // everything else is treated as lz4 (also size-prefixed by lz4_flex).
        if data.len() >= 8 && data[4..8] == ZSTD_MAGIC {
            let size = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            zstd::bulk::decompress(&data[4..], size)
                .map_err(|e| CodecError::Decompress(e.to_string()))
        } else {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| CodecError::Decompress(e.to_string()))
        }
    }
}

/// Compress via a thread-local reused zstd context: creating a context per
/// 32 KiB frame costs more than low-level compression itself. Keyed by level so
/// codecs at different levels (or a config change) rebuild it; the output is
/// byte-identical to one-shot `zstd::bulk::compress` at the same level.
fn zstd_compress_into(
    data: &[u8],
    level: i32,
    output: &mut impl zstd::zstd_safe::WriteBuf,
) -> Result<usize, CodecError> {
    use std::cell::RefCell;
    thread_local! {
        static ZSTD: RefCell<Option<(i32, zstd::bulk::Compressor<'static>)>> =
            const { RefCell::new(None) };
    }
    ZSTD.with(|slot| {
        let mut slot = slot.borrow_mut();
        if !matches!(&*slot, Some((l, _)) if *l == level) {
            let ctx = zstd::bulk::Compressor::new(level)
                .map_err(|e| CodecError::Compress(e.to_string()))?;
            *slot = Some((level, ctx));
        }
        slot.as_mut()
            .expect("compressor installed above")
            .1
            .compress_to_buffer(data, output)
            .map_err(|e| CodecError::Compress(e.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::aead::{Aead, Payload};

    fn codec() -> FrameCodec {
        FrameCodec::new(&[7u8; 32], b"frame-codec-test", CompressionConfig::Lz4)
    }

    /// Pool admission claims `sealed_bound` before compressing, so no input
    /// may seal larger, whatever it holds and whichever codec seals it.
    #[test]
    fn sealed_bound_covers_every_sealed_frame() {
        for compression in [CompressionConfig::Lz4, CompressionConfig::Zstd(3)] {
            let codec = FrameCodec::new(&[7u8; 32], b"frame-codec-test", compression);
            for plain in [
                Vec::new(),
                vec![0; 32 * 1024],
                noise(32 * 1024),
                noise(28 * 4096),
            ] {
                let sealed = codec.compress(&plain).unwrap().sealed_len();
                assert!(
                    sealed <= codec.sealed_bound(plain.len()),
                    "{compression:?}: {sealed} > {}",
                    codec.sealed_bound(plain.len())
                );
                assert_eq!(
                    codec.seal(&plain, b"aad").unwrap().len(),
                    sealed,
                    "a sealed frame is as long as its compressed payload promised"
                );
            }
        }
    }

    fn noise(size: usize) -> Vec<u8> {
        let mut state = 0x9e3779b97f4a7c15u64;
        (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn legacy_payload(plain: &[u8], compression: CompressionConfig) -> Vec<u8> {
        match compression {
            CompressionConfig::Lz4 => lz4_flex::compress_prepend_size(plain),
            CompressionConfig::Zstd(level) => {
                let mut payload = (plain.len() as u32).to_le_bytes().to_vec();
                payload.extend(zstd::bulk::compress(plain, level).unwrap());
                payload
            }
        }
    }

    #[test]
    fn compression_and_sealing_preserve_the_existing_wire_format() {
        for compression in [
            CompressionConfig::Lz4,
            CompressionConfig::Zstd(1),
            CompressionConfig::Zstd(3),
        ] {
            let c = FrameCodec::new(&[7u8; 32], b"frame-codec-test", compression);
            for size in [0, 1, 17, 4096, 32_768, 131_072] {
                for plain in [vec![0xAB; size], noise(size)] {
                    let expected_payload = legacy_payload(&plain, compression);
                    let compressed = c.compress(&plain).unwrap();
                    assert_eq!(compressed.payload(), expected_payload);
                    assert_eq!(compressed.len(), expected_payload.len());

                    let sealed = c.seal_compressed(compressed, b"wire-aad").unwrap();
                    let expected_ciphertext = c
                        .cipher
                        .encrypt(
                            XNonce::from_slice(&sealed[..NONCE_SIZE]),
                            Payload {
                                msg: &expected_payload,
                                aad: b"wire-aad",
                            },
                        )
                        .unwrap();
                    assert_eq!(&sealed[NONCE_SIZE..], expected_ciphertext);
                    // Decoding remains independent of the configured encoder.
                    assert_eq!(codec().open(&sealed, b"wire-aad").unwrap(), plain);
                }
            }
        }
    }

    #[test]
    fn sealing_reuses_compression_and_relocation_allocations() {
        for compression in [CompressionConfig::Lz4, CompressionConfig::Zstd(3)] {
            let c = FrameCodec::new(&[7u8; 32], b"frame-codec-test", compression);
            for plain in [vec![0xAB; 32_768], noise(32_768)] {
                let compressed = c.compress(&plain).unwrap();
                let pointer = compressed.0.as_ptr();
                let capacity = compressed.0.capacity();
                let predicted = compressed.sealed_len();
                assert!(capacity >= predicted);
                assert_eq!(capacity, predicted);
                let sealed = c.seal_compressed(compressed, b"old-slot").unwrap();
                assert_eq!(sealed.as_ptr(), pointer);
                assert_eq!(sealed.capacity(), capacity);
                assert_eq!(sealed.len(), predicted);

                let relocated = c.open_compressed(&sealed, b"old-slot").unwrap();
                let pointer = relocated.0.as_ptr();
                let capacity = relocated.0.capacity();
                let resealed = c.seal_compressed(relocated, b"new-slot").unwrap();
                assert_eq!(resealed.as_ptr(), pointer);
                assert_eq!(resealed.capacity(), capacity);
                assert_eq!(c.open(&resealed, b"new-slot").unwrap(), plain);
                assert!(matches!(
                    c.open(&resealed, b"old-slot"),
                    Err(CodecError::Decrypt)
                ));
            }
        }
    }

    #[test]
    fn legacy_frames_open_and_corruption_still_fails_authentication() {
        for compression in [CompressionConfig::Lz4, CompressionConfig::Zstd(3)] {
            let c = FrameCodec::new(&[7u8; 32], b"frame-codec-test", compression);
            let plain = noise(32_768);
            let payload = legacy_payload(&plain, compression);
            let nonce = [42u8; NONCE_SIZE];
            let mut legacy = nonce.to_vec();
            legacy.extend(
                c.cipher
                    .encrypt(
                        XNonce::from_slice(&nonce),
                        Payload {
                            msg: &payload,
                            aad: b"legacy-slot",
                        },
                    )
                    .unwrap(),
            );
            assert_eq!(c.open(&legacy, b"legacy-slot").unwrap(), plain);
            for position in [0, NONCE_SIZE, legacy.len() - 1] {
                let mut corrupt = legacy.clone();
                corrupt[position] ^= 1;
                assert!(matches!(
                    c.open_compressed(&corrupt, b"legacy-slot"),
                    Err(CodecError::Decrypt)
                ));
            }
        }
    }

    // seal() must remain exactly seal_compressed(compress(..)): the write path
    // splits the phases around a lock and both halves must roundtrip together.
    #[test]
    fn split_seal_roundtrips_like_fused_seal() {
        for c in [
            codec(),
            FrameCodec::new(&[7u8; 32], b"frame-codec-test", CompressionConfig::Zstd(3)),
        ] {
            let plain = vec![9u8; 40_000];
            let sealed = c
                .seal_compressed(c.compress(&plain).unwrap(), b"aad")
                .unwrap();
            assert_eq!(c.open(&sealed, b"aad").unwrap(), plain);
            assert!(matches!(
                c.open(&sealed, b"other"),
                Err(CodecError::Decrypt)
            ));
        }
    }

    // The reused zstd context must rebuild when the level changes and keep
    // producing output the (level-agnostic) decompressor accepts.
    #[test]
    fn zstd_context_reuse_survives_level_changes() {
        let a = FrameCodec::new(&[7u8; 32], b"t", CompressionConfig::Zstd(1));
        let b = FrameCodec::new(&[7u8; 32], b"t", CompressionConfig::Zstd(19));
        let plain = vec![5u8; 10_000];
        for c in [&a, &b, &a] {
            let sealed = c.seal(&plain, b"aad").unwrap();
            assert_eq!(c.open(&sealed, b"aad").unwrap(), plain);
        }
    }

    // The extent write path reserves each frame's byte range in the open
    // segment before the AEAD runs, so the sealed size must be a pure function
    // of the compressed size. Pin it across codecs, sizes, AAD, and repeated
    // (fresh-nonce) seals: a variable-length sealed frame would silently
    // corrupt every reservation placed after it.
    #[test]
    fn sealed_len_predicts_seal_compressed_output_exactly() {
        for compression in [
            CompressionConfig::Lz4,
            CompressionConfig::Zstd(1),
            CompressionConfig::Zstd(19),
        ] {
            let c = FrameCodec::new(&[9u8; 32], b"t", compression);
            for size in [0usize, 1, 17, 4096, 65_536] {
                for fill in [0u8, 0xAB] {
                    let plain = vec![fill; size];
                    let compressed = c.compress(&plain).unwrap();
                    let predicted = compressed.sealed_len();
                    // Two seals of equal payloads draw different nonces; both
                    // must land on the same predicted length.
                    for aad in [b"aad-1".as_slice(), b"".as_slice()] {
                        let again = c.compress(&plain).unwrap();
                        assert_eq!(again.len(), compressed.len());
                        let sealed = c.seal_compressed(again, aad).unwrap();
                        assert_eq!(
                            sealed.len(),
                            predicted,
                            "sealed length is not predictable for {compression:?} \
                             at {size} bytes"
                        );
                    }
                    let sealed = c.seal_compressed(compressed, b"aad-1").unwrap();
                    assert_eq!(c.open(&sealed, b"aad-1").unwrap(), plain);
                }
            }
        }
    }

    #[test]
    fn seal_open_roundtrips_with_aad() {
        let c = codec();
        let plain = vec![3u8; 1000];
        let sealed = c.seal(&plain, b"aad-1").unwrap();
        assert_eq!(c.open(&sealed, b"aad-1").unwrap(), plain);
    }

    #[test]
    fn aad_mismatch_fails() {
        let c = codec();
        let sealed = c.seal(b"payload", b"aad-1").unwrap();
        assert!(matches!(
            c.open(&sealed, b"aad-2"),
            Err(CodecError::Decrypt)
        ));
        assert!(matches!(c.open(&sealed, b""), Err(CodecError::Decrypt)));
    }

    #[test]
    fn wrong_subkey_fails() {
        let a = FrameCodec::new(&[1u8; 32], b"info-a", CompressionConfig::Lz4);
        let b = FrameCodec::new(&[1u8; 32], b"info-b", CompressionConfig::Lz4);
        let sealed = a.seal(b"payload", b"aad").unwrap();
        // Same master key, different HKDF label -> different subkey -> must not open.
        assert!(matches!(b.open(&sealed, b"aad"), Err(CodecError::Decrypt)));
    }

    #[test]
    fn too_short_frame_fails() {
        let c = codec();
        assert!(matches!(
            c.open(&[0u8; 10], b""),
            Err(CodecError::TooShort(10))
        ));
    }

    #[test]
    fn empty_payload_roundtrips() {
        let c = codec();
        let sealed = c.seal(b"", b"aad").unwrap();
        assert!(c.open(&sealed, b"aad").unwrap().is_empty());
    }
}
