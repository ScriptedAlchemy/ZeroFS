//! Map redb's file operations onto a claimed transactional region.

use redb::StorageBackend;
use std::{borrow::Borrow, fmt::Debug, io};
use zerofs_raw_device::journal_region::TransactionRegion;

#[derive(Debug)]
pub(crate) struct RawDatabase<T = TransactionRegion>(pub(crate) T);

impl<T> StorageBackend for RawDatabase<T>
where
    T: Borrow<TransactionRegion> + Debug + Send + Sync + 'static,
{
    fn len(&self) -> io::Result<u64> {
        self.0.borrow().len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.borrow().read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.borrow().set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.borrow().sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.borrow().write(offset, data)
    }
}
