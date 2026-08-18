use crate::buffered_send::BufferedSendQueue;
use crate::runtime::{self, sleep};
use crate::{ClientError, ClientResult, Conn, OutboundFrame, SEND_STALL_TIMEOUT};
use bytes::Bytes;
use js_sys::{ArrayBuffer, Uint8Array};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, Event, MessageEvent, WebSocket};

const MAX_BUFFERED_BYTES: u32 = 4 * 1024 * 1024;
const BUFFER_POLL_INTERVAL: Duration = Duration::from_millis(2);

pub(super) struct WebSocketIo {
    socket: WebSocket,
    incoming: mpsc::UnboundedReceiver<ClientResult<Bytes>>,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _close: Closure<dyn FnMut(Event)>,
    _error: Closure<dyn FnMut(Event)>,
}

impl Drop for WebSocketIo {
    fn drop(&mut self) {
        self.socket.set_onmessage(None);
        self.socket.set_onclose(None);
        self.socket.set_onerror(None);
        let _ = self.socket.close();
    }
}

pub(super) async fn connect(url: &str) -> ClientResult<WebSocketIo> {
    let socket = WebSocket::new(url).map_err(|_| ClientError::Disconnected)?;
    socket.set_binary_type(BinaryType::Arraybuffer);

    let (opened_tx, opened_rx) = oneshot::channel::<bool>();
    let opened_tx = Rc::new(RefCell::new(Some(opened_tx)));
    let on_open_tx = Rc::clone(&opened_tx);
    let on_open = Closure::wrap(Box::new(move |_event: Event| {
        if let Some(tx) = on_open_tx.borrow_mut().take() {
            let _ = tx.send(true);
        }
    }) as Box<dyn FnMut(Event)>);
    let on_error_tx = Rc::clone(&opened_tx);
    let on_open_error = Closure::wrap(Box::new(move |_event: Event| {
        if let Some(tx) = on_error_tx.borrow_mut().take() {
            let _ = tx.send(false);
        }
    }) as Box<dyn FnMut(Event)>);
    socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    socket.set_onerror(Some(on_open_error.as_ref().unchecked_ref()));

    let opened = runtime::timeout(Duration::from_secs(3), opened_rx).await;
    socket.set_onopen(None);
    socket.set_onerror(None);
    drop(on_open);
    drop(on_open_error);
    match opened {
        Ok(Ok(true)) => {}
        _ => {
            let _ = socket.close();
            return Err(ClientError::Disconnected);
        }
    }

    let (incoming_tx, incoming) = mpsc::unbounded_channel();
    let message_tx = incoming_tx.clone();
    let message = Closure::wrap(Box::new(move |event: MessageEvent| {
        let data = event.data();
        if !data.is_instance_of::<ArrayBuffer>() {
            let _ = message_tx.send(Err(ClientError::Unexpected("non-binary websocket frame")));
            return;
        }
        let bytes = Uint8Array::new(&data).to_vec();
        let _ = message_tx.send(Ok(Bytes::from(bytes)));
    }) as Box<dyn FnMut(MessageEvent)>);
    let close_tx = incoming_tx.clone();
    let close = Closure::wrap(Box::new(move |_event: Event| {
        let _ = close_tx.send(Err(ClientError::Disconnected));
    }) as Box<dyn FnMut(Event)>);
    let error = Closure::wrap(Box::new(move |_event: Event| {
        let _ = incoming_tx.send(Err(ClientError::Disconnected));
    }) as Box<dyn FnMut(Event)>);
    socket.set_onmessage(Some(message.as_ref().unchecked_ref()));
    socket.set_onclose(Some(close.as_ref().unchecked_ref()));
    socket.set_onerror(Some(error.as_ref().unchecked_ref()));

    Ok(WebSocketIo {
        socket,
        incoming,
        _message: message,
        _close: close,
        _error: error,
    })
}

pub(super) fn spawn(
    io: WebSocketIo,
    mut outgoing: mpsc::Receiver<OutboundFrame>,
    conn: Arc<Conn>,
    reconnect: Arc<Notify>,
) {
    let writer_socket = io.socket.clone();
    let writer_conn = Arc::clone(&conn);
    let writer_reconnect = Arc::clone(&reconnect);
    runtime::spawn(async move {
        let mut pending = BufferedSendQueue::default();
        let mut observed_buffered = 0u64;
        let mut last_progress = runtime::Clock::now();
        loop {
            if writer_socket.ready_state() != WebSocket::OPEN {
                break;
            }

            let buffered = u64::from(writer_socket.buffered_amount());
            if buffered < observed_buffered {
                last_progress = runtime::Clock::now();
            }
            observed_buffered = buffered;
            pending.acknowledge_drained(buffered);
            if pending.is_empty() {
                last_progress = runtime::Clock::now();
            } else if last_progress.elapsed_millis() >= SEND_STALL_TIMEOUT.as_millis() as u64 {
                break;
            }

            let can_send = writer_socket.buffered_amount() <= MAX_BUFFERED_BYTES;
            tokio::select! {
                biased;
                _ = writer_conn.writer_shutdown.notified() => return,
                frame = outgoing.recv(), if can_send => {
                    let Some(frame) = frame else { break };
                    let frame_len = frame.bytes.len() as u64;
                    writer_conn.counters.bytes_sent.fetch_add(
                        frame_len,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    writer_conn
                        .counters
                        .operations
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if writer_socket.send_with_u8_array(&frame.bytes).is_err() {
                        break;
                    }
                    pending.push(frame_len, frame.sent);
                    observed_buffered = u64::from(writer_socket.buffered_amount());
                    pending.acknowledge_drained(observed_buffered);
                }
                _ = sleep(BUFFER_POLL_INTERVAL) => {}
            }
        }
        writer_conn
            .dead
            .store(true, std::sync::atomic::Ordering::Release);
        writer_conn.reader_shutdown.notify_one();
        writer_reconnect.notify_waiters();
    });

    runtime::spawn(async move {
        let mut io = io;
        loop {
            let next = tokio::select! {
                biased;
                _ = conn.reader_shutdown.notified() => break,
                next = io.incoming.recv() => next,
            };
            let Some(Ok(frame)) = next else { break };
            conn.deliver(frame);
        }
        conn.connection_lost(&reconnect);
    });
}
