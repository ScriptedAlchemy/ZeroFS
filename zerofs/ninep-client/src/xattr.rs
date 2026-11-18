use super::*;

const XATTR_VALUE_MAX: usize = 2_097_152;

struct XattrFid<'a> {
    client: &'a NinePClient,
    conn: Arc<Conn>,
    fid: u32,
    settled: bool,
    /// One admission slot covers the whole exchange. Each dispatched step's
    /// response slot also holds a clone until its reply or teardown.
    admission: AdmissionPermit,
}

impl Drop for XattrFid<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.client.force_reprobe(&self.conn);
        }
        self.client.free_fid(self.fid);
    }
}

impl NinePClient {
    async fn xattr_fid(&self, fid: u32) -> ClientResult<XattrFid<'_>> {
        self.validate_fids([fid])?;
        let admission = self.acquire_request_admission().await?;
        let conn = loop {
            self.wait_until_live().await?;
            self.check_admitted_request(&admission)?;
            let conn = self.conn.load_full();
            if !conn.dead.load(Ordering::Acquire) {
                break conn;
            }
            self.reconnect_notify.notify_waiters();
            runtime::yield_now().await;
        };
        self.validate_fids([fid])?;
        Ok(XattrFid {
            client: self,
            conn,
            fid: self.alloc_fid(),
            settled: false,
            admission,
        })
    }

    // An xattr fid exists only on this connection. A reconnect must abort the
    // whole exchange because staged bytes and the clunk result cannot replay.
    async fn xattr_rpc(attr: &XattrFid<'_>, body: Message) -> ClientResult<Message> {
        attr.client.check_admitted_request(&attr.admission)?;
        let conn = &attr.conn;
        let response = Self::send_raw_at_tag_ctx(
            conn,
            None,
            body,
            true,
            Some((attr.client, &attr.admission)),
            Self::wait_for_live_reply,
        )
        .await;
        attr.client.check_admitted_request(&attr.admission)?;
        match response {
            Ok(Message::Rlerror(e)) if matches!(e.ecode, P9_ENOTLEADER | P9_ENOTLEADER_CLEAN) => {
                conn.shutdown();
                Err(ClientError::Disconnected)
            }
            Ok(Message::Rlerror(e)) => Err(ClientError::Errno(e.ecode)),
            result => result,
        }
    }

    /// Read one attribute. Names remain byte-exact in every filename mode.
    pub async fn get_xattr(&self, fid: u32, name: &[u8]) -> ClientResult<Bytes> {
        if name.is_empty() {
            return Err(ClientError::Errno(22));
        }
        self.read_xattr(fid, name).await
    }

    /// Return visible names as a NUL-separated byte string.
    pub async fn list_xattrs(&self, fid: u32) -> ClientResult<Bytes> {
        self.read_xattr(fid, b"").await
    }

    async fn read_xattr(&self, fid: u32, name: &[u8]) -> ClientResult<Bytes> {
        let mut attr = self.xattr_fid(fid).await?;
        let result = async {
            let size = match Self::xattr_rpc(
                &attr,
                Message::Txattrwalk(Txattrwalk {
                    fid,
                    newfid: attr.fid,
                    name: P9String::new(name.to_vec()),
                }),
            )
            .await?
            {
                Message::Rxattrwalk(r) => r.size,
                _ => return Err(ClientError::Unexpected("xattrwalk")),
            };
            let size = usize::try_from(size).map_err(|_| ClientError::Unexpected("xattr size"))?;
            if !name.is_empty() && size > XATTR_VALUE_MAX {
                return Err(ClientError::Unexpected("xattr size"));
            }
            let mut value = BytesMut::new();
            while value.len() < size {
                let count = (size - value.len()).min(self.max_io().max(1) as usize) as u32;
                match Self::xattr_rpc(
                    &attr,
                    Message::Tread(Tread {
                        fid: attr.fid,
                        offset: value.len() as u64,
                        count,
                    }),
                )
                .await?
                {
                    Message::Rread(r) if !r.data.is_empty() && r.data.len() <= count as usize => {
                        value.extend_from_slice(&r.data)
                    }
                    _ => return Err(ClientError::Unexpected("xattr read")),
                }
            }
            Ok(value.freeze())
        }
        .await;
        if matches!(result, Err(ClientError::Disconnected)) {
            return result;
        }
        let clunk = Self::xattr_rpc(&attr, Message::Tclunk(Tclunk { fid: attr.fid })).await;
        attr.settled = matches!(&clunk, Ok(Message::Rclunk(_)) | Err(ClientError::Errno(_)));
        match result {
            Err(error) => Err(error),
            Ok(value) => match clunk {
                Ok(Message::Rclunk(_)) => Ok(value),
                Err(error) => Err(error),
                _ => Err(ClientError::Unexpected("xattr clunk")),
            },
        }
    }

    /// Set an attribute with Linux XATTR_CREATE or XATTR_REPLACE flags, or zero.
    /// A zero-length replacement preserves the attribute with an empty value.
    pub async fn set_xattr(
        &self,
        fid: u32,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> ClientResult<()> {
        if value.len() > XATTR_VALUE_MAX {
            return Err(ClientError::Errno(7));
        }
        if flags > 2 {
            return Err(ClientError::Errno(22));
        }
        self.write_xattr(fid, name, value, flags).await
    }

    async fn write_xattr(
        &self,
        fid: u32,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> ClientResult<()> {
        let mut attr = self.xattr_fid(fid).await?;
        let result = async {
            match Self::xattr_rpc(
                &attr,
                Message::Twalk(Twalk {
                    fid,
                    newfid: attr.fid,
                    nwname: 0,
                    wnames: vec![],
                }),
            )
            .await?
            {
                Message::Rwalk(r) if r.wqids.is_empty() => {}
                _ => return Err(ClientError::Unexpected("xattr clone")),
            }
            match Self::xattr_rpc(
                &attr,
                Message::Txattrcreate(Txattrcreate {
                    fid: attr.fid,
                    name: P9String::new(name.to_vec()),
                    size: value.len() as u64,
                    flags,
                }),
            )
            .await?
            {
                Message::Rxattrcreate(_) => {}
                _ => return Err(ClientError::Unexpected("xattrcreate")),
            }
            // Use standard Twrite frames on the fixed connection, with a zero
            // operation ID and without the ordinary write replay machinery.
            let max = self.max_write_payload().max(1) as usize;
            for (i, chunk) in value.chunks(max).enumerate() {
                match Self::xattr_rpc(
                    &attr,
                    Message::Twrite(Twrite {
                        fid: attr.fid,
                        offset: (i * max) as u64,
                        count: chunk.len() as u32,
                        data: Bytes::copy_from_slice(chunk).into(),
                    }),
                )
                .await?
                {
                    Message::Rwrite(r) if r.count == chunk.len() as u32 => {}
                    _ => return Err(ClientError::Unexpected("xattr write")),
                }
            }
            Ok(())
        }
        .await;
        match result {
            Err(error @ ClientError::Errno(_)) => {
                // An errno is a completed request. Clunk the temporary fid;
                // an incomplete write may itself return EINVAL at cleanup.
                let cleanup =
                    Self::xattr_rpc(&attr, Message::Tclunk(Tclunk { fid: attr.fid })).await;
                attr.settled =
                    matches!(cleanup, Ok(Message::Rclunk(_)) | Err(ClientError::Errno(_)));
                return Err(error);
            }
            // A lost response may hide a completed final write. Disconnect
            // rather than clunking and committing an ambiguous exchange.
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        let result = Self::xattr_rpc(&attr, Message::Tclunk(Tclunk { fid: attr.fid })).await;
        attr.settled = matches!(&result, Ok(Message::Rclunk(_)) | Err(ClientError::Errno(_)));
        match result {
            Ok(Message::Rclunk(_)) => {
                let registry = self.stateful_requests.lock().unwrap();
                self.validate_admission_generation(
                    &attr.admission,
                    registry.retirement_generation,
                )?;
                let token = attr.conn.lineage_token.load(Ordering::Relaxed);
                self.note_unsynced(fid, token);
                Ok(())
            }
            Err(error) => Err(error),
            _ => Err(ClientError::Unexpected("xattr clunk")),
        }
    }

    pub async fn remove_xattr(&self, fid: u32, name: &[u8]) -> ClientResult<()> {
        self.write_xattr(fid, name, b"", XATTR_REMOVE_ZEROFS).await
    }
}
