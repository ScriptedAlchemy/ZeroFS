//! Attribution of one file's indexed logical bytes. A segment may contain
//! frames from other files, so its entire physical size is never charged here.
use super::{Catalog, PoolObjectStore};
use crate::fs::{EXTENT_SIZE, ZeroFS, inode::Inode, key_codec::KeyCodec};
use crate::segment::{FrameLoc, Segid};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Default, Serialize)]
pub(crate) struct ExtentCount {
    pub(crate) extents: u64,
    pub(crate) logical_bytes: u64,
}

#[derive(Serialize)]
pub(crate) struct FilePlacement {
    pub(crate) inode: u64,
    pub(crate) file_bytes: u64,
    pub(crate) unindexed_bytes: u64,
    pub(crate) unattributed: ExtentCount,
    pub(crate) members: BTreeMap<String, ExtentCount>,
}

impl PoolObjectStore {
    pub(crate) async fn file_placement(&self, fs: &ZeroFS, inode: u64) -> Result<FilePlacement> {
        let codec = KeyCodec::new();
        let mut segments = BTreeMap::<Segid, ExtentCount>::new();
        let mut indexed = 0;
        let Inode::File(file) = fs.inode_store.get(inode).await? else {
            anyhow::bail!("file placement requires a regular file");
        };
        let size = file.size;
        let width = EXTENT_SIZE as u64;
        let end = codec.extent_key(inode, size.div_ceil(width));
        let inode_key = codec.inode_key(inode);
        {
            // Extent keys sort before the inode key. Seek over unrelated rows
            // and check the size in the same snapshot, including on a replica.
            let (mut rows, _snapshot) = fs
                .db
                .metadata_snapshot_range(codec.extent_key(inode, 0)..=inode_key.clone())
                .await?;
            loop {
                let row = rows
                    .next()
                    .await?
                    .context("file disappeared from placement snapshot")?;
                if row.key == inode_key {
                    let stored: Inode = bincode::deserialize(&row.value)?;
                    ensure!(
                        matches!(stored, Inode::File(f) if f.size == size),
                        "file size changed before the index snapshot"
                    );
                    break;
                }
                if row.key >= end {
                    rows.seek(&inode_key).await?;
                    continue;
                }
                let (owner, extent) = codec
                    .parse_extent_key_full(&row.key)
                    .context("invalid extent key")?;
                ensure!(
                    owner == inode && extent < size.div_ceil(width),
                    "extent outside file range"
                );
                let loc = FrameLoc::decode(&row.value).context("invalid extent location")?;
                let bytes = (size - extent * width).min(width);
                let count = segments.entry(loc.segid).or_default();
                count.extents += 1;
                count.logical_bytes += bytes;
                indexed += bytes;
                ensure!(
                    segments.len() <= 65_536,
                    "file exceeds placement report segment limit"
                );
            }
        }
        let prefix = self.inner.authority_prefix.to_string();
        let mut report = FilePlacement {
            inode,
            file_bytes: size,
            unindexed_bytes: size - indexed,
            unattributed: ExtentCount::default(),
            members: BTreeMap::new(),
        };
        self.catalog(move |catalog: &Catalog| {
            catalog.attribute_file_segments(&prefix, segments, &mut report)?;
            Ok(report)
        })
        .await
        .map_err(Into::into)
    }
}
