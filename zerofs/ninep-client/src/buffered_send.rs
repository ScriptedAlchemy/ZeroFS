use std::collections::VecDeque;
use tokio::sync::oneshot;

#[derive(Default)]
pub(crate) struct BufferedSendQueue {
    frames: VecDeque<(u64, oneshot::Sender<()>)>,
    pending_bytes: u64,
}

impl BufferedSendQueue {
    pub(crate) fn push(&mut self, bytes: u64, sent: oneshot::Sender<()>) {
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.frames.push_back((bytes, sent));
    }

    pub(crate) fn acknowledge_drained(&mut self, buffered_bytes: u64) {
        while let Some((front_bytes, _)) = self.frames.front() {
            let bytes_after_front = self.pending_bytes.saturating_sub(*front_bytes);
            if buffered_bytes > bytes_after_front {
                break;
            }
            let (_, sent) = self.frames.pop_front().expect("front frame exists");
            self.pending_bytes = bytes_after_front;
            let _ = sent.send(());
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedSendQueue;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::TryRecvError;

    #[test]
    fn acknowledgements_follow_drained_websocket_frame_boundaries() {
        let mut queue = BufferedSendQueue::default();
        let (first_tx, mut first_rx) = oneshot::channel();
        let (second_tx, mut second_rx) = oneshot::channel();
        queue.push(9, first_tx);
        queue.push(9, second_tx);

        queue.acknowledge_drained(10);
        assert_eq!(first_rx.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(second_rx.try_recv(), Err(TryRecvError::Empty));

        queue.acknowledge_drained(9);
        assert_eq!(first_rx.try_recv(), Ok(()));
        assert_eq!(second_rx.try_recv(), Err(TryRecvError::Empty));

        queue.acknowledge_drained(0);
        assert_eq!(second_rx.try_recv(), Ok(()));
        assert!(queue.is_empty());
    }
}
