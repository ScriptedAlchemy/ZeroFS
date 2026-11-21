use super::*;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::ReadBuf;

struct FragmentedReader {
    bytes: Vec<u8>,
    position: usize,
    max_chunk: usize,
    body_start: usize,
    initialized_body_bytes: usize,
    fail_at: Option<usize>,
    pending_at: Option<usize>,
}

impl FragmentedReader {
    fn new(bytes: Vec<u8>, private: bool, max_chunk: usize) -> Self {
        Self {
            bytes,
            position: 0,
            max_chunk,
            body_start: P9_HEADER_SIZE + if private { P9_OP_ENVELOPE_LEN } else { 0 },
            initialized_body_bytes: 0,
            fail_at: None,
            pending_at: None,
        }
    }
}

impl AsyncRead for FragmentedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.fail_at.is_some_and(|at| this.position >= at) {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "test")));
        }
        if this.pending_at.is_some_and(|at| this.position >= at) {
            return Poll::Pending;
        }
        if this.position >= this.body_start {
            this.initialized_body_bytes += output.initialized().len();
        }
        let count = (this.bytes.len() - this.position)
            .min(this.max_chunk)
            .min(output.remaining());
        output.put_slice(&this.bytes[this.position..this.position + count]);
        this.position += count;
        Poll::Ready(Ok(()))
    }
}

struct Credit(Arc<AtomicUsize>);

impl Drop for Credit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn write_frame(payload: usize, private: bool) -> Vec<u8> {
    let data: Vec<u8> = (0..payload).map(|i| (i % 251) as u8).collect();
    P9Message::new_with_op_id_flags_and_origin(
        41,
        [7; P9_OP_ID_LEN],
        0,
        3,
        Message::Twrite(Twrite {
            fid: 12,
            offset: 17,
            count: data.len() as u32,
            data: data.into(),
        }),
    )
    .to_bytes_ctx(private)
    .unwrap()
}

#[tokio::test]
async fn fragmented_frames_initialize_the_body_only_as_it_arrives() {
    for private in [false, true] {
        for chunk in [1, 17, 4096] {
            let expected = write_frame(if chunk == 1 { 4096 } else { 128 * 1024 }, private);
            let mut reader = FragmentedReader::new(expected.clone(), private, chunk);
            let dropped = Arc::new(AtomicUsize::new(0));
            let shutdown = CancellationToken::new();
            let (actual, charge) = read_9p_frame_charged(&mut reader, &shutdown, async |header| {
                assert_eq!(header.len, expected.len());
                assert_eq!(header.type_byte, T_WRITE);
                Ok(Credit(Arc::clone(&dropped)))
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(actual.as_ref(), expected);
            assert_eq!(reader.position, expected.len());
            assert_eq!(
                reader.initialized_body_bytes, 0,
                "the transport should initialize body bytes, without a preceding zero-fill"
            );
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            drop(charge);
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn charged_reader_leaves_a_pipelined_frame_unread() {
    for private in [false, true] {
        let first = write_frame(128 * 1024, private);
        let second = write_frame(97, private);
        let mut stream = first.clone();
        stream.extend_from_slice(&second);
        let mut reader = FragmentedReader::new(stream, private, usize::MAX);
        let shutdown = CancellationToken::new();
        for expected in [&first, &second] {
            let (actual, ()) = read_9p_frame_charged(&mut reader, &shutdown, async |_| Ok(()))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(actual.as_ref(), expected.as_slice());
            if expected.len() == first.len() {
                assert_eq!(reader.position, first.len());
            }
        }
    }
}

#[tokio::test]
async fn truncated_body_returns_eof_and_releases_admission() {
    let mut frame = write_frame(128 * 1024, true);
    frame.pop();
    let mut reader = FragmentedReader::new(frame, true, 4096);
    let dropped = Arc::new(AtomicUsize::new(0));
    let result = read_9p_frame_charged(&mut reader, &CancellationToken::new(), async |_| {
        Ok(Credit(Arc::clone(&dropped)))
    })
    .await;
    let error = result.err().expect("truncated body must fail");
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn body_read_error_releases_admission() {
    let mut reader = FragmentedReader::new(write_frame(128 * 1024, false), false, 4096);
    reader.fail_at = Some(P9_HEADER_SIZE + 4096);
    let dropped = Arc::new(AtomicUsize::new(0));
    let result = read_9p_frame_charged(&mut reader, &CancellationToken::new(), async |_| {
        Ok(Credit(Arc::clone(&dropped)))
    })
    .await;
    let error = result.err().expect("connection reset must fail the read");
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::ConnectionReset
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_during_partial_body_releases_admission() {
    let mut reader = FragmentedReader::new(write_frame(128 * 1024, false), false, 4096);
    reader.pending_at = Some(P9_HEADER_SIZE + 4096);
    let dropped = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let read = read_9p_frame_charged(&mut reader, &shutdown, async |_| {
        Ok(Credit(Arc::clone(&dropped)))
    });
    let mut read = std::pin::pin!(read);
    assert!(futures::poll!(read.as_mut()).is_pending());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    shutdown.cancel();
    assert!(read.await.unwrap().is_none());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admission_failure_does_not_read_the_body() {
    for private in [false, true] {
        let mut reader = FragmentedReader::new(write_frame(128 * 1024, private), private, 4096);
        let result = read_9p_frame_charged(&mut reader, &CancellationToken::new(), async |_| {
            Err::<(), _>(anyhow::anyhow!("admission closed"))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(reader.position, P9_HEADER_SIZE);
    }
}

#[tokio::test]
async fn exact_reader_accepts_both_frame_size_boundaries() {
    for size in [P9_MIN_MESSAGE_SIZE as usize, P9_MAX_MSIZE as usize] {
        let mut frame = vec![0; size];
        frame[..4].copy_from_slice(&(size as u32).to_le_bytes());
        let mut reader = FragmentedReader::new(frame.clone(), false, 64 * 1024);
        let actual = read_9p_frame(&mut reader, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.as_ref(), frame);
    }
    for size in [P9_MIN_MESSAGE_SIZE - 1, P9_MAX_MSIZE + 1] {
        let mut reader = FragmentedReader::new(size.to_le_bytes().to_vec(), false, 4);
        assert!(
            read_9p_frame(&mut reader, &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(reader.position, 4);
    }
}
