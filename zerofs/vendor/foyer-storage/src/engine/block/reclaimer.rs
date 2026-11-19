// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{fmt::Debug, sync::Arc};

use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    error::Result,
    properties::Properties,
    spawn::Spawner,
};
use futures_core::future::BoxFuture;
use futures_util::{future::join_all, FutureExt};
use itertools::Itertools;

use crate::{
    engine::block::{
        flusher::{Flusher, Submission},
        indexer::Indexer,
        manager::{Block, ReclaimingBlock},
        scanner::BlockScanner,
        serde::Sequence,
    },
    io::{
        bytes::{IoSlice, IoSliceMut},
        PAGE,
    },
    Statistics, StorageFilter,
};

pub trait ReclaimerTrait: Send + Sync + 'static + Debug {
    fn reclaim(&self, block: ReclaimingBlock) -> BoxFuture<'static, ()>;
}

pub struct Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    indexer: Indexer,
    flushers: Vec<Flusher<K, V, P>>,
    reinsertion_filter: Arc<StorageFilter>,
    blob_index_size: usize,
    statistics: Arc<Statistics>,
    spawner: Spawner,
}

impl<K, V, P> Debug for Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reclaimer").finish()
    }
}

impl<K, V, P> Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        indexer: Indexer,
        flushers: Vec<Flusher<K, V, P>>,
        reinsertion_filter: Arc<StorageFilter>,
        blob_index_size: usize,
        statistics: Arc<Statistics>,
        spawner: Spawner,
    ) -> Self {
        Self {
            indexer,
            flushers,
            reinsertion_filter,
            blob_index_size,
            statistics,
            spawner,
        }
    }
}

impl<K, V, P> ReclaimerTrait for Reclaimer<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn reclaim(&self, block: ReclaimingBlock) -> BoxFuture<'static, ()> {
        let reinsertion_picker = self.reinsertion_filter.clone();
        let statistics = self.statistics.clone();
        let blob_index_size = self.blob_index_size;
        let flushers = self.flushers.clone();
        let spawner = self.spawner.clone();
        let indexer = self.indexer.clone();
        async move {
            let id = block.id();

            tracing::debug!(id, "[reclaimer]: Start reclaiming block.");

            let mut scanner = BlockScanner::new(block.clone(), blob_index_size);
            let mut picked_count = 0;
            let mut unpicked = vec![];
            // The loop will ends when:
            //
            // 1. no subsequent entries
            // 2. on error
            //
            // If the loop ends on error, the subsequent indices cannot be removed while reclaiming.
            // They will be removed when a query find a mismatch entry.
            'reinsert: loop {
                let infos = match scanner.next().await {
                    Ok(None) => break 'reinsert,
                    Err(e) => {
                        tracing::warn!(
                            "[reclaimer]: Error raised when reclaiming block {id}, skip the subsequent entries, err: {e}",
                            id = block.id()
                        );
                        break 'reinsert;
                    }
                    Ok(Some(infos)) => infos,
                };
                for info in infos {
                    if reinsertion_picker.filter(&statistics, info.hash, info.addr.len as _).is_admitted() {
                        let buf = IoSliceMut::new(bits::align_up(PAGE, info.addr.len as _));
                        let (buf, res) = block.read(Box::new(buf), info.addr.offset as _).await;
                        if let Err(e) = res {
                            tracing::warn!(
                                    "[reclaimer]: error raised when reclaiming block {id}, skip the subsequent entries, err: {e}",
                                    id = block.id()
                                );
                            break 'reinsert;
                        }
                        let buf = buf.try_into_io_slice_mut().unwrap().into_io_slice();
                        let slice = buf.slice(..bits::align_up(PAGE, info.addr.len as usize));
                        let flusher = flushers[picked_count % flushers.len()].clone();
                        flusher.submit(Submission::Reinsertion {
                            reinsertion: Reinsertion {
                                hash: info.hash,
                                len: info.addr.len as usize,
                                sequence: info.addr.sequence,
                                slice,
                            },
                        });
                        picked_count += 1;
                    } else {
                        unpicked.push((info.hash, info.addr.sequence));
                    }
                }
            }

            let unpicked_count = unpicked.len();

            let waits = flushers.iter().map(|flusher| flusher.wait()).collect_vec();
            spawner.spawn(async move {
                join_all(waits).await;
            });
            indexer.remove_batch(unpicked);

            if let Err(e) = BlockCleaner::clean(&block, &spawner).await {
                tracing::warn!("reclaimer]: mark block {id} clean error: {e}", id = block.id());
            }

            tracing::debug!(
                "[reclaimer]: Finish reclaiming block {id}, picked: {picked_count}, unpicked: {unpicked_count}."
            );

            block.statistics().reset();

            drop(block);
        }.boxed()
    }
}

#[derive(Debug)]
pub struct BlockCleaner;

impl BlockCleaner {
    pub(crate) fn is_fs_partition(block: &Block) -> bool {
        use std::any::Any;
        use crate::io::device::fs::FsPartition;
        (block.partition().as_ref() as &dyn Any).is::<FsPartition>()
    }

    /// Make a block read as zeros before it is written again.
    ///
    /// The next writer starts again at offset zero and rarely ends where the
    /// previous generation did, so everything past its last blob would still
    /// hold the previous generation's entries, padding and indices. Those
    /// bytes have no index left to say where their padding is, so no reader
    /// can check them. Punching the whole block leaves zeros there instead.
    /// Where the filesystem cannot punch holes, only the first index page is
    /// zeroed, as upstream does; recovery still stops there, but the bytes
    /// past it stay as they were.
    pub async fn clean(block: &Block, spawner: &Spawner) -> Result<()> {
        #[cfg(target_os = "linux")]
        if Self::is_fs_partition(block) {
            match Self::punch(block, spawner).await {
                Ok(()) => return Ok(()),
                Err(e) => tracing::warn!(
                    "[reclaimer]: cannot punch block {id}, zeroing its first page only, err: {e}",
                    id = block.id()
                ),
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = spawner;
        let mut page = IoSliceMut::new(PAGE);
        page.fill(0);
        let (_, res) = block.write(Box::new(page), 0).await;
        res?;
        Ok(())
    }

    /// Deallocate the block's whole range; the filesystem reads it back as zeros.
    #[cfg(target_os = "linux")]
    async fn punch(block: &Block, spawner: &Spawner) -> Result<()> {
        // The partition owns the file descriptor; keep it alive for the call.
        let partition = block.partition().clone();
        let len = block.size();
        spawner
            .spawn_blocking(move || {
                let (raw, offset) = partition.translate(0);
                // SAFETY: `partition` keeps the descriptor open for this call,
                // and the range lies inside the partition.
                let res = unsafe {
                    libc::fallocate(
                        raw.0,
                        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                        offset as libc::off_t,
                        len as libc::off_t,
                    )
                };
                if res == 0 {
                    Ok(())
                } else {
                    Err(foyer_common::error::Error::io_error(
                        std::io::Error::last_os_error(),
                    ))
                }
            })
            .await?
    }
}

#[derive(Debug)]
pub struct Reinsertion {
    pub hash: u64,
    pub len: usize,
    pub sequence: Sequence,
    pub slice: IoSlice,
}
