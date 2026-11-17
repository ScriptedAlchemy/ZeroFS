use super::*;
use crate::fs::{XATTR_VALUE_MAX, XattrName, XattrSetMode, XattrValue};

#[derive(Debug)]
pub(super) enum XattrFid {
    Closed,
    Read {
        data: Bytes,
        _memory: crate::ninep::server::P9XattrPermit,
    },
    Write {
        name: XattrName,
        mode: XattrSetMode,
        remove: bool,
        value: Vec<u8>,
        covered: Vec<u8>,
        received: usize,
        _memory: crate::ninep::server::P9XattrPermit,
    },
}

impl NinePHandler {
    pub(super) async fn xattrwalk(&self, tx: Txattrwalk) -> P9Result<Message> {
        if tx.fid == tx.newfid || self.session_state().fids.contains_key(&tx.newfid) {
            return Err(P9Error::FidInUse);
        }
        let fid = self.get_fid(tx.fid)?;
        let data = if tx.name.is_empty() {
            self.filesystem
                .list_xattrs(&fid.creds, fid.inode_id)
                .await?
        } else {
            self.filesystem
                .get_xattr(&fid.creds, fid.inode_id, &XattrName::new(tx.name.as_ref())?)
                .await?
        };
        let memory =
            crate::ninep::server::P9GlobalAdmission::shared().try_admit_xattr(data.len())?;
        let size = data.len() as u64;
        let mut state = self.connected_session()?;
        if state.fids.contains_key(&tx.newfid) {
            return Err(P9Error::FidInUse);
        }
        let mut slot = FidSlot::unopened(fid);
        slot.xattr = Some(XattrFid::Read {
            data,
            _memory: memory,
        });
        state.fids.insert(tx.newfid, slot);
        Ok(Message::Rxattrwalk(Rxattrwalk { size }))
    }

    pub(super) async fn xattrcreate(&self, tx: Txattrcreate) -> P9Result<Message> {
        let name = XattrName::new(tx.name.as_ref())?;
        let zerofs = self.zerofs_protocol_enabled();
        let (mode, remove) = if zerofs && tx.flags == XATTR_REMOVE_ZEROFS {
            if tx.size != 0 {
                return Err(P9Error::InvalidArgument);
            }
            (XattrSetMode::Replace, true)
        } else {
            let mode = XattrSetMode::from_flags(tx.flags)?;
            // Plain Linux v9fs uses an empty replacement to request removal.
            let remove = !zerofs && tx.size == 0 && matches!(mode, XattrSetMode::Replace);
            (mode, remove)
        };
        if tx.size > XATTR_VALUE_MAX as u64 {
            return Err(FsError::TooBig.into());
        }
        let memory = crate::ninep::server::P9GlobalAdmission::shared()
            .try_admit_xattr(tx.size as usize + (tx.size as usize).div_ceil(8))?;
        let fid = self.get_fid(tx.fid)?;
        if fid.opened {
            return Err(P9Error::FidAlreadyOpen);
        }
        let _guard = self.filesystem.lock_manager.acquire(fid.inode_id).await;
        let inode = self.filesystem.inode_store.get(fid.inode_id).await?;
        name.check(&inode, &fid.creds, true)?;
        let mut state = self.connected_session()?;
        let slot = state.fids.get_mut(&tx.fid).ok_or(P9Error::BadFid)?;
        if slot.xattr.is_some() || slot.fid.opened {
            return Err(P9Error::FidAlreadyOpen);
        }
        slot.handle = Some(self.filesystem.new_open_handle(fid.inode_id));
        slot.directory_pin = None;
        slot.fid = fid;
        slot.xattr = Some(XattrFid::Write {
            name,
            mode,
            remove,
            value: vec![0; tx.size as usize],
            covered: vec![0; (tx.size as usize).div_ceil(8)],
            received: 0,
            _memory: memory,
        });
        Ok(Message::Rxattrcreate(Rxattrcreate))
    }

    pub(super) fn read_xattr(&self, tr: &Tread) -> Option<P9Result<Message>> {
        let count = self.clamp_count(tr.count, P9_IOHDRSZ) as usize;
        let state = self.session_state();
        let xattr = state.fids.get(&tr.fid)?.xattr.as_ref()?;
        Some(match xattr {
            XattrFid::Read { data, .. } => {
                let start = usize::try_from(tr.offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                let end = start.saturating_add(count).min(data.len());
                let data = data.slice(start..end);
                Ok(Message::Rread(Rread {
                    count: data.len() as u32,
                    data: DekuBytes::from(data),
                }))
            }
            XattrFid::Write { .. } | XattrFid::Closed => Err(P9Error::BadFid),
        })
    }

    pub(super) fn write_xattr(&self, tw: &Twrite) -> Option<P9Result<Message>> {
        let mut state = self.session_state();
        let xattr = state.fids.get_mut(&tw.fid)?.xattr.as_mut()?;
        let XattrFid::Write {
            value,
            covered,
            received,
            ..
        } = xattr
        else {
            return Some(Err(P9Error::BadFid));
        };
        let Ok(start) = usize::try_from(tw.offset) else {
            return Some(Err(P9Error::InvalidArgument));
        };
        let Some(end) = start
            .checked_add(tw.data.len())
            .filter(|&end| end <= value.len())
        else {
            return Some(Err(P9Error::InvalidArgument));
        };
        if tw.count as usize != tw.data.len() {
            return Some(Err(P9Error::InvalidArgument));
        }
        value[start..end].copy_from_slice(&tw.data);
        for i in start..end {
            let mask = 1 << (i % 8);
            if covered[i / 8] & mask == 0 {
                covered[i / 8] |= mask;
                *received += 1;
            }
        }
        Some(Ok(Message::Rwrite(Rwrite { count: tw.count })))
    }

    pub(super) async fn commit_xattr(&self, mut slot: FidSlot) -> P9Result<()> {
        if let Some(XattrFid::Write {
            name,
            mode,
            remove,
            value,
            received,
            _memory,
            ..
        }) = slot.xattr.take()
        {
            if received != value.len() {
                return Err(P9Error::InvalidArgument);
            }
            // Keep the inode pin until the metadata transaction has committed.
            if remove {
                self.filesystem
                    .remove_xattr(&slot.fid.creds, slot.fid.inode_id, &name)
                    .await?;
            } else {
                self.filesystem
                    .set_xattr(
                        &slot.fid.creds,
                        slot.fid.inode_id,
                        &name,
                        XattrValue::new(Bytes::from(value))?,
                        mode,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
