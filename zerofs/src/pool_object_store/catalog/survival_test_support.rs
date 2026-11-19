//! Test-only catalog observations and real allocation/deletion debts.

use super::*;
use crate::pool_object_store::PoolObjectStore;
use anyhow::ensure;
use bytes::Bytes;
use object_store::{ObjectStore, PutMode, PutOptions};
use redb::TableHandle;

pub(crate) type CatalogImage = BTreeMap<String, BTreeMap<String, Vec<u8>>>;

pub(crate) async fn capture(pool: &PoolObjectStore) -> Result<CatalogImage> {
    Ok(pool
        .catalog(|catalog| {
            let tx = catalog.database.begin_read()?;
            let mut image = BTreeMap::new();
            for definition in [META, MEMBERS, OBJECTS, UPLOADS, GARBAGE, HEALTH, FAULTS] {
                let table = tx.open_table(definition)?;
                let mut rows = BTreeMap::new();
                for row in table.iter()? {
                    let (key, value) = row?;
                    rows.insert(key.value().to_owned(), value.value().to_vec());
                }
                image.insert(definition.name().to_owned(), rows);
            }
            let table = tx.open_table(HIDDEN_LEGACY)?;
            let mut rows = BTreeMap::new();
            for row in table.iter()? {
                let (key, value) = row?;
                rows.insert(key.value().to_owned(), vec![value.value()]);
            }
            image.insert(HIDDEN_LEGACY.name().to_owned(), rows);
            Ok(image)
        })
        .await?)
}

pub(crate) async fn create_debts(pool: &PoolObjectStore) -> Result<()> {
    let location = Path::from(format!(
        "{}/{}",
        pool.inner.authority_prefix,
        crate::segment::Segid::new(900, 1).object_key()
    ));
    let mut options = PutOptions::from(PutMode::Create);
    options
        .extensions
        .insert(crate::segment_store::GeneratedSegmentCreate);
    pool.put_opts(
        &location,
        Bytes::from_static(b"retired physical payload").into(),
        options,
    )
    .await?;
    let key = location.to_string();
    pool.catalog(move |catalog| {
        let entry = catalog
            .lookup(&key)?
            .entry
            .context("published debt object")?;
        catalog.retire(&key, &entry)?;
        Ok(())
    })
    .await?;

    // Reserve and start an actual unpublished physical upload. Leave its row
    // and capacity charge for the recovery oracle instead of running cleanup.
    let upload = pool
        .reserve(&location, 23, Some("disk-b".to_owned()))
        .await?;
    let id = upload.id.clone();
    pool.catalog(move |catalog| catalog.mark_started_for_migration(&id, false, None))
        .await?;
    pool.ensure_member(&upload.member_id, true).await?;
    let mut options = PutOptions::from(PutMode::Create);
    options
        .extensions
        .insert(crate::local_object_store::pool::PoolObjectCreate);
    pool.member(&upload.member_id)?
        .store
        .put_opts(
            &Path::from(upload.physical_key.clone()),
            Bytes::from_static(b"pending physical upload").into(),
            options,
        )
        .await?;
    drop(upload);
    Ok(())
}

pub(crate) fn require_nonzero_debts(image: &CatalogImage) -> Result<()> {
    for name in ["pool_uploads", "pool_garbage"] {
        ensure!(
            !image.get(name).context("missing debt table")?.is_empty(),
            "{name} must be nonempty"
        );
    }
    let mut used = 0;
    let mut reserved = 0;
    for bytes in image["pool_members"].values() {
        let member: MemberRecord = bincode::deserialize(bytes)?;
        used += member.used;
        reserved += member.reserved;
    }
    ensure!(
        used > 0 && reserved > 0,
        "debts must retain real capacity charges"
    );
    for bytes in image["pool_objects"].values() {
        let entry: Entry = bincode::deserialize(bytes)?;
        ensure!(entry.sha256.is_some(), "every mapping needs a checksum");
        ensure!(
            matches!(entry.physical, Physical::Member { .. }),
            "every mapping needs a physical generation"
        );
    }
    Ok(())
}
