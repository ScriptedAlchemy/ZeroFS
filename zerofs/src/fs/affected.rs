use crate::db::Db;
use crate::fs::key_codec::{KeyCodec, KeyPrefix, ParsedKey};
use crate::fs::store::directory::decode_dir_scan_value;
use crate::segment::{FrameLoc, Segid};
use anyhow::{Context, Result, bail};
use futures::TryStreamExt;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Serialize)]
pub(crate) struct AffectedReference {
    pub inode: u64,
    pub extent: u64,
    pub segment: String,
    /// Lossless absolute pathname bytes; an empty list means unreachable/unlinked.
    pub paths: Vec<Vec<u8>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AffectedView {
    pub view: String,
    pub references: Vec<AffectedReference>,
    pub unresolved_segments: Vec<String>,
}

pub(crate) async fn scan(
    db: &Db,
    view: String,
    segments: &BTreeSet<Segid>,
) -> Result<AffectedView> {
    let codec = KeyCodec::new();
    let mut rows = db.scan_metadata_snapshot().await?;
    let mut parents = BTreeMap::<u64, Vec<(u64, Vec<u8>)>>::new();
    let mut references = Vec::new();
    let mut found = BTreeSet::new();
    let mut count = 0;
    while let Some((key, value)) = rows.try_next().await? {
        count += 1;
        if count > 1_000_000 {
            bail!("affected metadata scan exceeds one million rows; report incomplete");
        }
        if let Some((inode, extent)) = codec.parse_extent_key_full(&key) {
            let loc = FrameLoc::decode(&value)
                .context("invalid extent metadata; affected report incomplete")?;
            if segments.contains(&loc.segid) {
                found.insert(loc.segid);
                references.push(AffectedReference {
                    inode,
                    extent,
                    segment: loc.segid.object_key(),
                    paths: Vec::new(),
                });
            }
        } else if matches!(codec.parse_key(&key), ParsedKey::DirScan { .. }) {
            let offset = codec.id_offset(KeyPrefix::DirScan);
            let parent = u64::from_be_bytes(key[offset..offset + 8].try_into()?);
            let (name, child) = decode_dir_scan_value(&value)
                .map_err(|e| anyhow::anyhow!("invalid directory metadata: {e}"))?;
            let child = match child {
                crate::fs::store::directory::DirScanValue::WithInode { inode_id, .. }
                | crate::fs::store::directory::DirScanValue::Reference { inode_id } => inode_id,
            };
            parents.entry(child).or_default().push((parent, name));
        }
    }
    for reference in &mut references {
        reference.paths = paths(reference.inode, &parents)?;
    }
    Ok(AffectedView {
        view,
        references,
        unresolved_segments: segments
            .difference(&found)
            .map(|s| s.object_key())
            .collect(),
    })
}

fn paths(inode: u64, parents: &BTreeMap<u64, Vec<(u64, Vec<u8>)>>) -> Result<Vec<Vec<u8>>> {
    let mut pending = vec![(inode, Vec::<Vec<u8>>::new(), BTreeSet::new())];
    let mut result = Vec::new();
    let mut visited = 0;
    while let Some((current, components, mut ancestors)) = pending.pop() {
        visited += 1;
        if visited > 100_000 {
            bail!("affected path traversal limit exceeded; report incomplete");
        }
        if current == 0 {
            let mut path = Vec::new();
            for name in components.iter().rev() {
                path.push(b'/');
                path.extend_from_slice(name);
            }
            result.push(path);
            continue;
        }
        if !ancestors.insert(current) {
            bail!("directory cycle in affected path metadata");
        }
        if let Some(edges) = parents.get(&current) {
            for (parent, name) in edges {
                let mut components = components.clone();
                components.push(name.clone());
                pending.push((*parent, components, ancestors.clone()));
            }
        }
    }
    result.sort();
    result.dedup();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn affected_metadata_names_missing_segment_without_body_reads() {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        let raw = std::sync::Arc::new(
            slatedb::DbBuilder::new(object_store::path::Path::from("affected"), store)
                .with_segment_extractor(std::sync::Arc::new(
                    crate::segment_extractor::ZeroFsSegmentExtractor,
                ))
                .build()
                .await
                .unwrap(),
        );
        let codec = KeyCodec::new();
        let segment = Segid::new(5, 9);
        let loc = FrameLoc {
            segid: segment,
            frame_index: 0,
            byte_offset: 0,
            byte_len: 128,
        };
        raw.put(codec.extent_key(42, 3), loc.encode())
            .await
            .unwrap();
        for (cookie, name) in [(3, b"first".as_slice()), (4, b"second".as_slice())] {
            let mut value = (name.len() as u32).to_le_bytes().to_vec();
            value.extend_from_slice(name);
            value.extend_from_slice(
                &bincode::serialize(&crate::fs::store::directory::DirScanValue::Reference {
                    inode_id: 42,
                })
                .unwrap(),
            );
            raw.put(codec.dir_scan_key(0, cookie), value).await.unwrap();
        }
        let db = Db::new(raw.clone(), None);
        let report = scan(&db, "live".into(), &BTreeSet::from([segment]))
            .await
            .unwrap();
        assert_eq!(report.references.len(), 1);
        assert_eq!(report.references[0].extent, 3);
        assert_eq!(
            report.references[0].paths,
            vec![b"/first".to_vec(), b"/second".to_vec()]
        );
        assert!(report.unresolved_segments.is_empty());
        raw.close().await.unwrap();
    }

    #[test]
    fn affected_paths_include_hardlinks_and_non_utf8() {
        let parents = BTreeMap::from([
            (1, vec![(0, b"dir".to_vec())]),
            (2, vec![(1, b"a".to_vec()), (0, vec![255])]),
        ]);
        assert_eq!(
            paths(2, &parents).unwrap(),
            vec![b"/dir/a".to_vec(), vec![b'/', 255]]
        );
        assert!(paths(3, &parents).unwrap().is_empty());
    }
    #[test]
    fn affected_cycles_are_incomplete_errors() {
        assert!(paths(1, &BTreeMap::from([(1, vec![(1, b"cycle".to_vec())])])).is_err());
    }
}
