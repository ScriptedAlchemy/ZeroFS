// Single-range slices and a deliberately reversed range exercise the API.
#![allow(clippy::single_range_in_vec_init, clippy::reversed_empty_ranges)]

use super::*;
use futures::TryStreamExt;
use object_store::{ObjectStoreExt, UpdateVersion};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use zerofs_raw_device::Role;
use zerofs_raw_device::p3::P3Layout;

const SIZE: u64 = 4 << 20;
const BLOCK: u64 = BLOCK_SIZE;

/// A sparse regular file standing in for a claimed device. Removed on drop.
struct Scratch {
    path: PathBuf,
    size: u64,
}

impl Scratch {
    fn new(size: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zerofs-raw-member-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.set_len(size).unwrap();
        Self { path, size }
    }

    fn file(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .unwrap()
    }

    fn provision(&self) -> RawStore {
        RawStore::provision(self.file(), self.size, id()).unwrap()
    }

    fn reopen(&self) -> RawStore {
        RawStore::open(self.file(), self.size, id()).unwrap()
    }

    fn member(&self, store: RawStore, reserve: u64) -> RawMemberStore {
        RawMemberStore::new(store, (), reserve).unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn id() -> Identity {
    Identity {
        pool_uuid: *b"pool-uuid-000001",
        member_uuid: *b"member-uuid-0001",
        role: Role::Data,
    }
}

fn path(s: &str) -> Path {
    Path::from(s)
}

fn payload(seed: u8, len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect::<Vec<_>>(),
    )
}

async fn read(store: &RawMemberStore, location: &str) -> Bytes {
    store
        .get(&path(location))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
}

fn io_kind(err: &Error) -> Option<io::ErrorKind> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(io) = error.downcast_ref::<io::Error>() {
            return Some(io.kind());
        }
        current = error.source();
    }
    None
}

fn raw_error(err: &Error) -> Option<&RawMemberError> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(io) = error.downcast_ref::<io::Error>()
            && let Some(raw) = io
                .get_ref()
                .and_then(|e| e.downcast_ref::<RawMemberError>())
        {
            return Some(raw);
        }
        if let Some(raw) = error.downcast_ref::<RawMemberError>() {
            return Some(raw);
        }
        current = error.source();
    }
    None
}

fn is_no_space(err: &Error) -> bool {
    io_kind(err) == Some(io::ErrorKind::StorageFull)
        && matches!(raw_error(err), Some(RawMemberError::NoSpace { .. }))
}

#[tokio::test]
async fn put_get_head_and_ranges_round_trip_with_verified_bytes() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let data = payload(1, 3 * 4096 + 17);
    let before = Utc::now();
    let put = store
        .put(&path("a/b/obj"), data.clone().into())
        .await
        .unwrap();
    assert!(put.e_tag.is_some());
    assert_eq!(put.version, None);

    assert_eq!(read(&store, "a/b/obj").await, data);
    let meta = store.head(&path("a/b/obj")).await.unwrap();
    assert_eq!(meta.size, data.len() as u64);
    assert_eq!(meta.e_tag, put.e_tag);
    assert!(meta.last_modified >= before - chrono::Duration::seconds(1));
    assert!(meta.last_modified <= Utc::now() + chrono::Duration::seconds(1));

    let range = store.get_range(&path("a/b/obj"), 4090..4100).await.unwrap();
    assert_eq!(range, data.slice(4090..4100));
    let ranges = store
        .get_ranges(&path("a/b/obj"), &[0..1, 5000..9000, 12000..20000])
        .await
        .unwrap();
    assert_eq!(ranges[0], data.slice(0..1));
    assert_eq!(ranges[1], data.slice(5000..9000));
    // Bounded ranges past the end are clamped, as in object_store.
    assert_eq!(ranges[2], data.slice(12000..));

    let suffix = store
        .get_opts(
            &path("a/b/obj"),
            GetOptions {
                range: Some(GetRange::Suffix(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(suffix.range, (data.len() as u64 - 5)..data.len() as u64);
    assert_eq!(suffix.bytes().await.unwrap(), data.slice(data.len() - 5..));

    assert!(
        store
            .get_range(&path("a/b/obj"), data.len() as u64..data.len() as u64 + 1)
            .await
            .is_err()
    );
    assert!(matches!(
        store.get(&path("missing")).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        store.head(&path("missing")).await,
        Err(Error::NotFound { .. })
    ));

    store.put(&path("empty"), PutPayload::new()).await.unwrap();
    assert!(read(&store, "empty").await.is_empty());
    assert_eq!(store.head(&path("empty")).await.unwrap().size, 0);
}

#[tokio::test]
async fn get_preconditions_follow_the_e_tag() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let put = store.put(&path("k"), "v".into()).await.unwrap();
    let tag = put.e_tag.unwrap();
    let key = path("k");
    let get = |options: GetOptions| store.get_opts(&key, options);
    assert!(
        get(GetOptions {
            if_match: Some(tag.clone()),
            ..Default::default()
        })
        .await
        .is_ok()
    );
    assert!(matches!(
        get(GetOptions {
            if_match: Some("other".into()),
            ..Default::default()
        })
        .await,
        Err(Error::Precondition { .. })
    ));
    assert!(matches!(
        get(GetOptions {
            if_none_match: Some(tag),
            ..Default::default()
        })
        .await,
        Err(Error::NotModified { .. })
    ));
    assert!(matches!(
        get(GetOptions {
            version: Some("1".into()),
            ..Default::default()
        })
        .await,
        Err(Error::NotSupported { .. })
    ));
}

#[tokio::test]
async fn conditional_create_and_update() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let p = path("manifest");
    let first = store
        .put_opts(&p, "first".into(), PutMode::Create.into())
        .await
        .unwrap();
    assert!(matches!(
        store
            .put_opts(&p, "dup".into(), PutMode::Create.into())
            .await,
        Err(Error::AlreadyExists { .. })
    ));
    let updated = store
        .put_opts(
            &p,
            "second".into(),
            PutMode::Update(first.clone().into()).into(),
        )
        .await
        .unwrap();
    assert_ne!(updated.e_tag, first.e_tag);
    assert_eq!(read(&store, "manifest").await, "second");
    // Same content again still gets a new tag: the tag is per put.
    let third = store
        .put_opts(
            &p,
            "second".into(),
            PutMode::Update(updated.clone().into()).into(),
        )
        .await
        .unwrap();
    assert_ne!(third.e_tag, updated.e_tag);

    for stale in [
        UpdateVersion::from(first),
        UpdateVersion::from(updated),
        UpdateVersion {
            e_tag: None,
            version: None,
        },
        UpdateVersion {
            e_tag: third.e_tag.clone(),
            version: Some("v".into()),
        },
    ] {
        assert!(matches!(
            store
                .put_opts(&p, "stale".into(), PutMode::Update(stale).into())
                .await,
            Err(Error::Precondition { .. })
        ));
    }
    assert!(matches!(
        store
            .put_opts(
                &path("absent"),
                "x".into(),
                PutMode::Update(third.clone().into()).into()
            )
            .await,
        Err(Error::Precondition { .. })
    ));
    assert_eq!(read(&store, "manifest").await, "second");
    assert_eq!(store.head(&p).await.unwrap().e_tag, third.e_tag);
}

#[tokio::test]
async fn concurrent_creates_and_updates_have_exactly_one_winner() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let creates = (0..16).map(|i| {
        let store = store.clone();
        async move {
            store
                .put_opts(&path("race"), payload(i, 10).into(), PutMode::Create.into())
                .await
        }
    });
    let results = futures::future::join_all(creates).await;
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| matches!(e, Error::AlreadyExists { .. }))
    );

    let base = store.head(&path("race")).await.unwrap();
    let expected = UpdateVersion {
        e_tag: base.e_tag,
        version: None,
    };
    let updates = (0..16).map(|i| {
        let store = store.clone();
        let expected = expected.clone();
        async move {
            store
                .put_opts(
                    &path("race"),
                    payload(100 + i, 10).into(),
                    PutMode::Update(expected).into(),
                )
                .await
                .map(|put| (i, put))
        }
    });
    let results = futures::future::join_all(updates).await;
    let winners: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(winners.len(), 1);
    assert!(
        results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| matches!(e, Error::Precondition { .. }))
    );
    let (winner, put) = winners[0];
    assert_eq!(read(&store, "race").await, payload(100 + winner, 10));
    assert_eq!(store.head(&path("race")).await.unwrap().e_tag, put.e_tag);
}

#[tokio::test]
async fn every_acknowledged_mutation_is_durable_on_reopen() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    store.put(&path("keep"), "k".into()).await.unwrap();
    store.put(&path("gone"), "g".into()).await.unwrap();
    store.put(&path("over"), "1".into()).await.unwrap();
    let over = store.put(&path("over"), "2".into()).await.unwrap();
    store.delete(&path("gone")).await.unwrap();
    store.copy(&path("keep"), &path("copied")).await.unwrap();
    store.rename(&path("copied"), &path("moved")).await.unwrap();
    let modified = store.head(&path("keep")).await.unwrap().last_modified;
    drop(store);

    // No flush or shutdown call: every Ok above already flushed.
    let store = scratch.member(scratch.reopen(), 0);
    assert_eq!(read(&store, "keep").await, "k");
    assert_eq!(read(&store, "over").await, "2");
    assert_eq!(read(&store, "moved").await, "k");
    assert_eq!(store.head(&path("over")).await.unwrap().e_tag, over.e_tag);
    assert_eq!(
        store.head(&path("keep")).await.unwrap().last_modified,
        modified
    );
    for absent in ["gone", "copied"] {
        assert!(matches!(
            store.head(&path(absent)).await,
            Err(Error::NotFound { .. })
        ));
    }
}

#[tokio::test]
async fn delete_stream_preserves_input_order_and_reports_batch_failures() {
    for fail_deletes in [false, true] {
        let scratch = Scratch::new(SIZE);
        let store = scratch.member(scratch.provision(), 0);
        for name in ["first", "kept", "last"] {
            store.put(&path(name), name.into()).await.unwrap();
        }
        drop(store);
        let raw = if fail_deletes {
            RawStore::open(File::open(&scratch.path).unwrap(), SIZE, id()).unwrap()
        } else {
            scratch.reopen()
        };
        let store = scratch.member(raw, 0);
        let input = vec![
            Ok(path("first")),
            Err(Error::NotFound {
                path: "upstream-error".to_owned(),
                source: "upstream listing failed".into(),
            }),
            Ok(path("last")),
        ];
        let results: Vec<_> = store
            .delete_stream(stream::iter(input).boxed())
            .collect()
            .await;
        assert_eq!(results.len(), 3);
        assert!(matches!(
            &results[1],
            Err(Error::NotFound { path, .. }) if path == "upstream-error"
        ));
        for (index, expected) in [(0, "first"), (2, "last")] {
            if fail_deletes {
                assert!(matches!(&results[index], Err(Error::Generic { .. })));
            } else {
                assert_eq!(results[index].as_ref().unwrap(), &path(expected));
            }
        }
        drop(store);

        // Successful batch results are durable; upstream errors never turn
        // into deletion requests, and a failed batch reports no successes.
        let store = scratch.member(scratch.reopen(), 0);
        assert_eq!(read(&store, "kept").await, "kept");
        for name in ["first", "last"] {
            assert_eq!(store.head(&path(name)).await.is_ok(), fail_deletes);
        }
    }
}

#[tokio::test]
async fn costs_and_capacity_are_exact() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 1);
    let empty = store.capacity().unwrap();
    assert_eq!(empty.reserve_bytes, BLOCK, "reserve rounds up to a block");
    assert_eq!(empty.device_bytes, SIZE);
    assert!(empty.data_bytes < SIZE);
    assert_eq!(empty.free_bytes, empty.data_bytes);
    assert_eq!(empty.admissible_bytes, empty.free_bytes - BLOCK);
    assert_eq!((empty.allocated_bytes, empty.live_objects), (0, 0));

    for (len, cost) in [
        (0, BLOCK),
        (1, 2 * BLOCK),
        (4096, 2 * BLOCK),
        (4097, 3 * BLOCK),
    ] {
        assert_eq!(RawMemberStore::put_cost(len), Some(cost));
    }
    assert_eq!(RawMemberStore::put_cost(u64::MAX), None);

    store
        .put(&path("a"), payload(0, 5000).into())
        .await
        .unwrap();
    let one = store.capacity().unwrap();
    assert_eq!(one.allocated_bytes, 3 * BLOCK);
    assert_eq!(one.live_objects, 1);
    assert_eq!(one.admissible_bytes, empty.admissible_bytes - 3 * BLOCK);

    // The old allocation is released when the overwrite commits.
    store.put(&path("a"), payload(1, 10).into()).await.unwrap();
    let over = store.capacity().unwrap();
    assert_eq!(over.allocated_bytes, 2 * BLOCK);

    // Deleting the last key returns all of its space.
    store.delete(&path("a")).await.unwrap();
    let deleted = store.capacity().unwrap();
    assert_eq!(deleted, empty);

    // Deleting an absent key writes nothing.
    store.delete(&path("a")).await.unwrap();
    assert_eq!(store.capacity().unwrap(), deleted);
}

/// Fill to the last admissible byte: the exact maximum fits, one byte more
/// is refused without writing, and deletes always fit above the reserve.
#[tokio::test]
async fn exact_fill_never_crosses_the_reserve_and_allows_deletes() {
    let reserve = 64 * 1024;
    let scratch = Scratch::new(1 << 20);
    let store = scratch.member(scratch.provision(), reserve);
    let mut created = Vec::new();
    loop {
        let capacity = store.capacity().unwrap();
        let Some(max) = capacity.max_put_payload() else {
            break;
        };
        let len = max.min(100_000);
        let name = format!("obj/{}", created.len());
        store
            .put(
                &path(&name),
                payload(created.len() as u8, len as usize).into(),
            )
            .await
            .unwrap();
        created.push(name);
    }
    let full = store.capacity().unwrap();
    assert!(full.admissible_bytes < BLOCK);
    assert!(full.free_bytes >= reserve);

    // Smallest create no longer fits: honest ENOSPC, nothing written.
    let err = store
        .put(&path("one-more"), PutPayload::new())
        .await
        .unwrap_err();
    assert!(is_no_space(&err), "{err}");
    let Some(RawMemberError::NoSpace {
        needed,
        available,
        reserve: reported,
    }) = raw_error(&err)
    else {
        panic!("{err}");
    };
    assert_eq!(*reported, reserve);
    assert!(needed > available);
    assert_eq!(store.capacity().unwrap(), full);

    // Every live object can still be deleted, in one batch, and the reserve
    // is untouched afterwards.
    let deleted: Vec<_> = store
        .delete_stream(
            stream::iter(created.iter().map(|n| Ok(path(n))).collect::<Vec<_>>()).boxed(),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(deleted.len(), created.len());
    let empty = store.capacity().unwrap();
    assert_eq!(empty.live_objects, 0);
    assert!(empty.free_bytes >= reserve);
    drop(store);
    assert!(scratch.reopen().is_empty());
}

/// Concurrent puts under pressure: successes never overcommit, and every
/// refusal is true at the end, since admissible space only shrinks.
#[tokio::test]
async fn concurrent_admission_has_no_false_enospc() {
    let scratch = Scratch::new(1 << 20);
    let store = scratch.member(scratch.provision(), 8192);
    let sizes: Vec<u64> = (0..48).map(|i| 1 + (i * 7919) % 60_000).collect();
    let puts = sizes.iter().enumerate().map(|(i, &len)| {
        let store = store.clone();
        async move {
            store
                .put(
                    &path(&format!("c/{i}")),
                    payload(i as u8, len as usize).into(),
                )
                .await
        }
    });
    let results = futures::future::join_all(puts).await;
    let end = store.capacity().unwrap();
    let mut allocated = 0;
    for (len, result) in sizes.iter().zip(&results) {
        let cost = RawMemberStore::put_cost(*len).unwrap();
        match result {
            Ok(_) => allocated += cost,
            Err(err) => {
                assert!(is_no_space(err), "{err}");
                assert!(
                    end.admissible_bytes < cost,
                    "refused a {len}-byte create that still fits"
                );
            }
        }
    }
    let ok = results.iter().filter(|r| r.is_ok()).count() as u64;
    assert!(ok > 0 && ok < sizes.len() as u64);
    assert_eq!(end.allocated_bytes, allocated);
    assert_eq!(end.live_objects, ok);
    assert!(end.free_bytes >= end.reserve_bytes);

    drop(store);
    let store = scratch.member(scratch.reopen(), 8192);
    for (i, (len, result)) in sizes.iter().zip(&results).enumerate() {
        let location = format!("c/{i}");
        if result.is_ok() {
            assert_eq!(
                read(&store, &location).await,
                payload(i as u8, *len as usize)
            );
        } else {
            assert!(store.head(&path(&location)).await.is_err());
        }
    }
}

#[tokio::test]
async fn a_store_opened_below_the_floor_can_delete_to_recover() {
    let scratch = Scratch::new(1 << 20);
    let store = scratch.member(scratch.provision(), 0);
    let size = store.capacity().unwrap().max_put_payload().unwrap();
    let body = payload(1, size as usize);
    store
        .put(&path("filled"), body.clone().into())
        .await
        .unwrap();
    drop(store);

    let store = scratch.member(scratch.reopen(), 64 * 1024);
    assert_eq!(store.capacity().unwrap().admissible_bytes, 0);
    let err = store.put(&path("other"), "y".into()).await.unwrap_err();
    assert!(is_no_space(&err), "{err}");
    assert_eq!(read(&store, "filled").await, body);
    store.delete(&path("filled")).await.unwrap();
    assert!(store.capacity().unwrap().admissible_bytes > 0);
    store.put(&path("other"), "y".into()).await.unwrap();
    drop(store);
    let store = scratch.member(scratch.reopen(), 64 * 1024);
    assert_eq!(read(&store, "other").await, "y");
    assert!(store.head(&path("filled")).await.is_err());
}

#[tokio::test]
async fn hostile_sizes_are_refused_without_overflow() {
    assert_eq!(RawMemberStore::put_cost(u64::MAX - 1), None);
    let capacity = RawCapacity {
        device_bytes: 0,
        data_bytes: 0,
        allocated_bytes: 0,
        free_bytes: u64::MAX,
        reserve_bytes: 0,
        admissible_bytes: u64::MAX,
        live_objects: 0,
    };
    assert_eq!(capacity.max_put_payload(), Some(MAX_OBJECT_LEN));
    let capacity = RawCapacity {
        admissible_bytes: BLOCK - 1,
        ..capacity
    };
    assert_eq!(capacity.max_put_payload(), None);
}

#[tokio::test]
async fn copy_and_rename_honour_modes_and_commit_together() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    store.put(&path("src"), "data".into()).await.unwrap();
    store.put(&path("taken"), "old".into()).await.unwrap();

    assert!(matches!(
        store.copy_if_not_exists(&path("src"), &path("taken")).await,
        Err(Error::AlreadyExists { .. })
    ));
    assert!(matches!(
        store
            .rename_if_not_exists(&path("src"), &path("taken"))
            .await,
        Err(Error::AlreadyExists { .. })
    ));
    assert!(matches!(
        store.copy(&path("nope"), &path("x")).await,
        Err(Error::NotFound { .. })
    ));
    store.copy(&path("src"), &path("taken")).await.unwrap();
    assert_eq!(read(&store, "taken").await, "data");
    store.rename(&path("src"), &path("renamed")).await.unwrap();
    assert_eq!(read(&store, "renamed").await, "data");
    assert!(store.head(&path("src")).await.is_err());
    // Renaming onto itself changes nothing.
    let before = store.capacity().unwrap();
    store
        .rename(&path("renamed"), &path("renamed"))
        .await
        .unwrap();
    assert_eq!(store.capacity().unwrap(), before);
    assert_eq!(read(&store, "renamed").await, "data");
}

#[tokio::test]
async fn listings_use_segment_prefixes_and_sorted_offsets() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    for name in ["a/b/1", "a/b/2", "a/bc/3", "a/top", "z", "a/b/deep/4"] {
        store.put(&path(name), name.into()).await.unwrap();
    }
    let names = |metas: Vec<ObjectMeta>| -> Vec<String> {
        metas.into_iter().map(|m| m.location.to_string()).collect()
    };
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(
        names(all.clone()),
        ["a/b/1", "a/b/2", "a/b/deep/4", "a/bc/3", "a/top", "z"]
    );
    assert_eq!(all[0].size, 5);
    assert!(all[0].e_tag.is_some());

    let under: Vec<_> = store.list(Some(&path("a/b"))).try_collect().await.unwrap();
    assert_eq!(names(under), ["a/b/1", "a/b/2", "a/b/deep/4"]);
    // The prefix itself is not its own child.
    let exact: Vec<_> = store.list(Some(&path("z"))).try_collect().await.unwrap();
    assert!(exact.is_empty());

    let after: Vec<_> = store
        .list_with_offset(Some(&path("a")), &path("a/b/2"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(after), ["a/b/deep/4", "a/bc/3", "a/top"]);

    let level = store.list_with_delimiter(Some(&path("a"))).await.unwrap();
    assert_eq!(names(level.objects), ["a/top"]);
    let prefixes: Vec<_> = level
        .common_prefixes
        .iter()
        .map(|p| p.to_string())
        .collect();
    assert_eq!(prefixes, ["a/b", "a/bc"]);
    let root = store.list_with_delimiter(None).await.unwrap();
    assert_eq!(names(root.objects), ["z"]);
    assert_eq!(root.common_prefixes, vec![path("a")]);
}

#[tokio::test]
async fn damaged_payload_is_never_returned_even_for_a_distant_range() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let data = payload(9, 3 * 4096);
    store.put(&path("k"), data.into()).await.unwrap();
    // Damage the last payload block of the first object.
    scratch
        .file()
        .write_all_at(
            &[0xEE],
            SIZE - store.capacity().unwrap().data_bytes + BLOCK + 2 * BLOCK + 1,
        )
        .unwrap();
    for result in [
        store.get_range(&path("k"), 0..8).await,
        store.get(&path("k")).await.map(|_| Bytes::new()),
        store
            .get_ranges(&path("k"), &[0..1])
            .await
            .map(|_| Bytes::new()),
    ] {
        let err = result.unwrap_err();
        assert_eq!(io_kind(&err), Some(io::ErrorKind::InvalidData), "{err}");
    }
    // Metadata does not read the payload.
    assert_eq!(store.head(&path("k")).await.unwrap().size, 3 * 4096);
}

#[tokio::test]
async fn a_failed_write_poisons_every_later_call() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    store.put(&path("k"), "v".into()).await.unwrap();
    drop(store);
    // A read-only descriptor opens fine and fails on the first append.
    let read_only = RawStore::open(File::open(&scratch.path).unwrap(), SIZE, id()).unwrap();
    let store = scratch.member(read_only, 0);
    let err = store.put(&path("x"), "y".into()).await.unwrap_err();
    assert!(!is_no_space(&err));
    for err in [
        store.head(&path("k")).await.unwrap_err(),
        store.put(&path("z"), "1".into()).await.unwrap_err(),
        store.delete(&path("k")).await.unwrap_err(),
        store.list(None).try_collect::<Vec<_>>().await.unwrap_err(),
    ] {
        assert!(
            matches!(raw_error(&err), Some(RawMemberError::Poisoned)),
            "{err}"
        );
    }
    assert!(matches!(store.capacity(), Err(RawMemberError::Poisoned)));
    drop(store);
    let store = scratch.member(scratch.reopen(), 0);
    assert_eq!(read(&store, "k").await, "v");
    assert!(store.head(&path("x")).await.is_err());
}

#[tokio::test]
async fn unstored_features_and_invalid_keys_are_refused() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let mut attributes = object_store::Attributes::new();
    attributes.insert(object_store::Attribute::ContentType, "text/plain".into());
    let opts = PutOptions {
        attributes,
        ..Default::default()
    };
    assert!(matches!(
        store.put_opts(&path("k"), "v".into(), opts).await,
        Err(Error::NotSupported { .. })
    ));
    let mut tags = object_store::TagSet::default();
    tags.push("a", "b");
    let opts = PutOptions {
        tags,
        ..Default::default()
    };
    assert!(matches!(
        store.put_opts(&path("k"), "v".into(), opts).await,
        Err(Error::NotSupported { .. })
    ));
    let long = "x".repeat(zerofs_raw_device::MAX_KEY_LEN + 1);
    let err = store.put(&path(&long), "v".into()).await.unwrap_err();
    assert!(matches!(
        raw_error(&err),
        Some(RawMemberError::InvalidKey(_))
    ));
    assert_eq!(store.capacity().unwrap().allocated_bytes, 0);
}

#[tokio::test]
async fn multipart_buffers_until_complete_and_aborts_cleanly() {
    let scratch = Scratch::new(SIZE);
    let store = scratch.member(scratch.provision(), 0);
    let mut upload = store.put_multipart(&path("mp")).await.unwrap();
    upload.put_part(payload(1, 5000).into()).await.unwrap();
    upload.put_part(payload(2, 7).into()).await.unwrap();
    assert!(store.head(&path("mp")).await.is_err());
    let put = upload.complete().await.unwrap();
    let mut expected = payload(1, 5000).to_vec();
    expected.extend_from_slice(&payload(2, 7));
    assert_eq!(read(&store, "mp").await, expected);
    assert_eq!(store.head(&path("mp")).await.unwrap().e_tag, put.e_tag);
    assert!(upload.complete().await.is_err());

    let mut aborted = store.put_multipart(&path("ab")).await.unwrap();
    aborted.put_part("x".into()).await.unwrap();
    aborted.abort().await.unwrap();
    assert!(aborted.put_part("y".into()).await.is_err());
    assert!(store.head(&path("ab")).await.is_err());
}

#[tokio::test]
async fn the_claim_is_held_until_the_last_clone_drops() {
    struct Claim(Arc<AtomicBool>);
    impl Drop for Claim {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let released = Arc::new(AtomicBool::new(false));
    let scratch = Scratch::new(SIZE);
    let store = RawMemberStore::new(scratch.provision(), Claim(released.clone()), 0).unwrap();
    let clone = store.clone();
    drop(store);
    assert!(!released.load(Ordering::SeqCst));
    clone.put(&path("k"), "v".into()).await.unwrap();
    drop(clone);
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn works_over_a_p3_role_region() {
    let region = 1u64 << 20;
    let scratch = Scratch::new(6 * region);
    let layout = P3Layout::new(6 * region, [region; 6]).unwrap();
    let pool = *b"pool-uuid-000001";
    let device = *b"p3-device-uuid01";
    layout.provision(scratch.file(), pool, device).unwrap();
    let open_role = |role| {
        layout
            .open(scratch.file(), pool, device)
            .unwrap()
            .into_role(role)
            .unwrap()
    };
    let metadata = RawMemberStore::new(open_role(Role::Metadata), (), 0).unwrap();
    assert_eq!(metadata.identity().role, Role::Metadata);
    assert_eq!(metadata.capacity().unwrap().device_bytes, region);
    metadata
        .put_opts(&path("m"), "meta".into(), PutMode::Create.into())
        .await
        .unwrap();
    drop(metadata);
    let catalog = RawMemberStore::new(open_role(Role::Catalog), (), 0).unwrap();
    assert!(catalog.head(&path("m")).await.is_err());
    drop(catalog);
    let metadata = RawMemberStore::new(open_role(Role::Metadata), (), 0).unwrap();
    assert_eq!(read(&metadata, "m").await, "meta");
}
