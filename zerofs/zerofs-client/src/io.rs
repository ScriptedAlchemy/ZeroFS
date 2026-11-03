//! `tokio-io` feature: a [`FileCursor`] adapter that turns the positioned
//! [`File`] API into a stateful `AsyncRead + AsyncWrite + AsyncSeek` stream, so
//! `tokio::io::copy` and friends work against a ZeroFS file. Rust-only; never
//! crosses the FFI boundary.

use crate::error::ZeroFsError;
use crate::file::File;
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

#[cfg(not(target_arch = "wasm32"))]
type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
#[cfg(target_arch = "wasm32")]
type BoxFut<T> = Pin<Box<dyn Future<Output = T>>>;

enum State {
    Idle,
    Reading,
    /// A write in flight, plus how many bytes it covers (added to `pos` on success).
    Writing {
        fut: BoxFut<Result<(), ZeroFsError>>,
        len: u64,
    },
    /// A `SeekFrom::End` in flight: resolves the size, then offsets by `delta`.
    SeekingEnd {
        fut: BoxFut<Result<u64, ZeroFsError>>,
        delta: i64,
    },
}

// Bound speculative payloads independently of both the caller's (often 8 KiB)
// copy buffer and a potentially much larger negotiated msize.
const READ_AHEAD_DEPTH: usize = 8;
const READ_AHEAD_CHUNK: u32 = 1024 * 1024;

enum ReadState {
    Pending(BoxFut<Result<Bytes, ZeroFsError>>),
    Ready(Result<Bytes, ZeroFsError>),
}

struct ReadRequest {
    len: u32,
    state: ReadState,
}

async fn read_chunk(file: Arc<File>, offset: u64, len: u32) -> Result<Bytes, ZeroFsError> {
    let mut collected = BytesMut::new();
    while collected.len() < len as usize {
        let remaining = len - collected.len() as u32;
        let data = file
            .read_at(offset + collected.len() as u64, remaining)
            .await?;
        if data.len() > remaining as usize {
            return Err(ZeroFsError::Protocol {
                message: "read reply exceeds the requested range".into(),
            });
        }
        if data.is_empty() {
            break;
        }
        if collected.is_empty() && data.len() == len as usize {
            return Ok(data);
        }
        if collected.is_empty() {
            collected = BytesMut::with_capacity(len as usize);
        }
        collected.extend_from_slice(&data);
    }
    Ok(collected.freeze())
}

/// An owned cursor over a [`File`] implementing `AsyncRead + AsyncWrite +
/// AsyncSeek`, built entirely on [`File::read_at`]/[`File::write_at`]. Multiple
/// cursors over one `File` are safe; each carries its own position and the
/// underlying I/O is positioned, so there is no shared seek pointer.
///
/// After the first successful read, contiguous reads use bounded, ordered
/// read-ahead: up to eight chunks of at most 1 MiB, regardless of the caller's
/// buffer size. Seek and write discard speculation and reset this promotion,
/// keeping isolated small reads small. Starting a seek or write while a demand
/// read is pending is rejected with `io::ErrorKind::Other`.
pub struct FileCursor {
    file: Arc<File>,
    pos: u64,
    state: State,
    read_buffer: Bytes,
    read_ahead: VecDeque<ReadRequest>,
    next_read_offset: Option<u64>,
    read_ahead_enabled: bool,
}

impl FileCursor {
    pub(crate) fn new(file: Arc<File>) -> Self {
        Self {
            file,
            pos: 0,
            state: State::Idle,
            read_buffer: Bytes::new(),
            read_ahead: VecDeque::new(),
            next_read_offset: Some(0),
            read_ahead_enabled: false,
        }
    }

    /// The cursor's current absolute byte offset.
    pub fn position(&self) -> u64 {
        self.pos
    }

    fn discard_read_ahead(&mut self) {
        self.read_buffer = Bytes::new();
        self.read_ahead.clear();
        self.next_read_offset = Some(self.pos);
        self.read_ahead_enabled = false;
    }

    fn poll_read_ahead(&mut self, cx: &mut Context<'_>, initial_len: usize) {
        let buffered = usize::from(!self.read_buffer.is_empty());
        let depth = if self.read_ahead_enabled {
            READ_AHEAD_DEPTH
        } else {
            1
        };
        while self.read_ahead.len() + buffered < depth {
            let Some(offset) = self.next_read_offset else {
                break;
            };
            let chunk = self.file.max_read_chunk().clamp(1, READ_AHEAD_CHUNK);
            let len = if self.read_ahead_enabled {
                chunk
            } else {
                initial_len.min(chunk as usize) as u32
            };
            let len = u64::from(len).min(u64::MAX - offset) as u32;
            if len == 0 {
                self.next_read_offset = None;
                break;
            }
            self.next_read_offset = Some(offset + u64::from(len));
            self.read_ahead.push_back(ReadRequest {
                len,
                state: ReadState::Pending(Box::pin(read_chunk(
                    Arc::clone(&self.file),
                    offset,
                    len,
                ))),
            });
        }
        // Poll every request before waiting on the first one. Responses may
        // arrive in any order, but only the front is exposed to the caller.
        for request in &mut self.read_ahead {
            if let ReadState::Pending(future) = &mut request.state {
                if let Poll::Ready(result) = future.as_mut().poll(cx) {
                    request.state = ReadState::Ready(result);
                }
            }
        }
    }
}

impl AsyncRead for FileCursor {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !matches!(self.state, State::Idle | State::Reading) {
            return Poll::Ready(Err(busy()));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if matches!(self.state, State::Idle) {
            if let Err(error) = self.file.check_open() {
                self.discard_read_ahead();
                return Poll::Ready(Err(error.into()));
            }
        }
        self.poll_read_ahead(cx, buf.remaining());
        if self.read_buffer.is_empty() {
            if let Some(request) = self.read_ahead.front() {
                if matches!(request.state, ReadState::Pending(_)) {
                    self.state = State::Reading;
                    return Poll::Pending;
                }
                let request = self.read_ahead.pop_front().unwrap();
                let ReadState::Ready(result) = request.state else {
                    unreachable!();
                };
                self.state = State::Idle;
                let data = match result {
                    Ok(data) => data,
                    Err(error) => {
                        self.discard_read_ahead();
                        return Poll::Ready(Err(error.into()));
                    }
                };
                if data.len() < request.len as usize {
                    self.read_ahead.clear();
                    self.next_read_offset = None;
                }
                self.read_buffer = data;
            }
            if self.read_buffer.is_empty() {
                // Report EOF once, then allow a later read to observe growth.
                self.next_read_offset = Some(self.pos);
                self.read_ahead_enabled = false;
                self.state = State::Idle;
                return Poll::Ready(Ok(()));
            }
        }
        let n = self.read_buffer.len().min(buf.remaining());
        buf.put_slice(&self.read_buffer.split_to(n));
        self.pos += n as u64;
        // Promote the next call, not this one: a read followed by a seek must
        // not launch speculative ranges that the caller will immediately drop.
        self.read_ahead_enabled = true;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FileCursor {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Writing { fut, len } => {
                    let res = ready!(fut.as_mut().poll(cx));
                    let len = *len;
                    self.state = State::Idle;
                    res.map_err(io::Error::from)?;
                    self.pos += len;
                    self.next_read_offset = Some(self.pos);
                    return Poll::Ready(Ok(len as usize));
                }
                State::Idle => {
                    if buf.is_empty() {
                        return Poll::Ready(Ok(0));
                    }
                    self.discard_read_ahead();
                    // write_at writes all of `data` (chunked internally), so one
                    // poll consumes the whole buffer.
                    let offset = self.pos;
                    let data = buf.to_vec();
                    let len = data.len() as u64;
                    let file = Arc::clone(&self.file);
                    self.state = State::Writing {
                        fut: Box::pin(async move { file.write_at(offset, &data).await }),
                        len,
                    };
                }
                _ => return Poll::Ready(Err(busy())),
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Drive an in-flight write to completion; there is no client-side write
        // buffer beyond that (each write_at awaits the server), so once idle
        // everything sent is acknowledged.
        match &mut self.state {
            State::Writing { fut, len } => {
                let res = ready!(fut.as_mut().poll(cx));
                let len = *len;
                self.state = State::Idle;
                res.map_err(io::Error::from)?;
                self.pos += len;
                self.next_read_offset = Some(self.pos);
                Poll::Ready(Ok(()))
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for FileCursor {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        if !matches!(self.state, State::Idle) {
            return Err(busy());
        }
        self.discard_read_ahead();
        match position {
            SeekFrom::Start(n) => self.pos = n,
            SeekFrom::Current(delta) => {
                self.pos = self
                    .pos
                    .checked_add_signed(delta)
                    .ok_or_else(negative_seek)?;
            }
            SeekFrom::End(delta) => {
                let file = Arc::clone(&self.file);
                self.state = State::SeekingEnd {
                    fut: Box::pin(async move { file.metadata().await.map(|m| m.size) }),
                    delta,
                };
            }
        }
        self.next_read_offset = Some(self.pos);
        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        if let State::SeekingEnd { fut, delta } = &mut self.state {
            let size = ready!(fut.as_mut().poll(cx));
            let delta = *delta;
            self.state = State::Idle;
            let size = size.map_err(io::Error::from)?;
            self.pos = size.checked_add_signed(delta).ok_or_else(negative_seek)?;
            self.next_read_offset = Some(self.pos);
        }
        Poll::Ready(Ok(self.pos))
    }
}

fn busy() -> io::Error {
    io::Error::other("zerofs FileCursor: another I/O operation is already in flight")
}

fn negative_seek() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "zerofs FileCursor: seek to a negative or out-of-range offset",
    )
}
