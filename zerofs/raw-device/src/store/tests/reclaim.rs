use super::*;

#[test]
fn a_full_device_can_delete_and_refill_without_touching_survivors() {
    let scratch = Scratch::new(MIN_DEVICE_SIZE);
    let open = || RawStore::open(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    let mut store = RawStore::provision(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    let len = 60_000;
    let mut expected = BTreeMap::new();
    let mut count = 0u64;
    loop {
        let key = count.to_le_bytes();
        let body = value(count, len);
        match store.put(&key, &body) {
            Ok(()) => {
                expected.insert(key.to_vec(), body);
                count += 1;
            }
            Err(Error::NoSpace { .. }) => break,
            Err(error) => panic!("{error}"),
        }
    }
    store.flush().unwrap();
    assert!(count >= 8);
    for round in 1..=6 {
        for i in (0..count).step_by(2) {
            let key = ((round - 1) * count + i).to_le_bytes();
            assert!(store.delete(&key).unwrap());
            expected.remove(key.as_slice());
        }
        store.flush().unwrap();
        drop(store);
        store = open();
        assert_eq!(contents(&store), expected);

        for i in (0..count).step_by(2) {
            let name = (round * count + i).to_le_bytes();
            let body = value(round * count + i, len);
            store.put(&name, &body).unwrap();
            expected.insert(name.to_vec(), body);
        }
        store.flush().unwrap();
        drop(store);
        store = open();
        assert_eq!(contents(&store), expected);
    }
}

#[test]
fn a_large_object_uses_disjoint_holes_and_survives_reopen() {
    let size = 8 << 20;
    let scratch = Scratch::new(size);
    let mut store = RawStore::provision(scratch.file(), size, id()).unwrap();
    let len = 62_000;
    let mut count = 0u64;
    while store.geometry().free >= RawStore::put_cost(len).unwrap() {
        store
            .put(&count.to_le_bytes(), &value(count, len as usize))
            .unwrap();
        count += 1;
    }
    store.flush().unwrap();
    for i in (0..count).step_by(2) {
        assert!(store.delete(&i.to_le_bytes()).unwrap());
    }
    store.flush().unwrap();
    let body = value(321, 3 << 20);
    store.put(b"fragmented", &body).unwrap();
    let pending = store.handle(b"fragmented").unwrap().unwrap();
    let first_map = pending.entry.allocation.start();
    scratch.write_at(first_map + BLOCK_SIZE / 2, &[0x3d; 120]);
    drop(pending);
    drop(store);
    let mut store = RawStore::open(scratch.file(), size, id()).unwrap();
    assert_eq!(store.get(b"fragmented").unwrap(), None);
    for i in 0..count {
        let expected = (i % 2 == 1).then(|| value(i, len as usize));
        assert_eq!(store.get(&i.to_le_bytes()).unwrap(), expected);
    }

    store.put(b"fragmented", &body).unwrap();
    store.flush().unwrap();
    drop(store);

    let mut store = RawStore::open(scratch.file(), size, id()).unwrap();
    assert_eq!(
        store.get(b"fragmented").unwrap().as_deref(),
        Some(body.as_slice())
    );
    for i in 0..count {
        let value = (i % 2 == 1).then(|| value(i, len as usize));
        assert_eq!(store.get(&i.to_le_bytes()).unwrap(), value);
    }
    let free = store.geometry().free;
    assert!(store.delete(b"fragmented").unwrap());
    store.flush().unwrap();
    assert_eq!(
        store.geometry().free - free,
        RawStore::put_cost(body.len() as u64).unwrap()
    );
}

#[test]
fn repeated_overwrites_keep_steady_capacity_and_pin_a_reader() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    let initial = value(0, 250_000);
    store.put(b"k", &initial).unwrap();
    store.flush().unwrap();
    let steady = store.geometry().free;
    let handle = store.handle(b"k").unwrap().unwrap();
    let reader = store.reader();
    for i in 1..=140 {
        let body = value(i, initial.len());
        store.put(b"k", &body).unwrap();
        store.flush().unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(body));
        assert_eq!(
            store.geometry().free,
            steady - RawStore::put_cost(initial.len() as u64).unwrap()
        );
    }
    assert_eq!(
        reader
            .read_ranges(&handle, &[0..initial.len() as u64])
            .unwrap()[0],
        initial
    );
    drop(handle);
    store.flush().unwrap();
    assert_eq!(store.geometry().free, steady);
    drop(store);
    let store = reopen(&scratch);
    assert_eq!(store.get(b"k").unwrap().unwrap(), value(140, initial.len()));
    assert_eq!(store.geometry().free, steady);
}

#[test]
fn interrupted_bank_switch_never_replays_retired_keys() {
    let size = MIN_DEVICE_SIZE;
    let open = |scratch: &Scratch| RawStore::open(scratch.file(), size, id()).unwrap();
    // No checkpoint, torn slot 0, durable slot 0, torn slot 1 and complete.
    for cut in 0..5 {
        let scratch = Scratch::new(size);
        let mut store = RawStore::provision(scratch.file(), size, id()).unwrap();
        store.put(b"old", b"before").unwrap();
        store.put(b"keep", b"unrelated").unwrap();
        store.put(b"gone", b"never resurrect").unwrap();
        store.flush().unwrap();
        store.delete(b"gone").unwrap();
        store.flush().unwrap();
        while store.geometry().durable_tail < store.label.bank_start() + bank_bytes(size) {
            store.put(b"old", b"before").unwrap();
            store.flush().unwrap();
        }
        store.delete(b"old").unwrap();
        store.put(b"new", b"after").unwrap();
        let result = store.flush_with_checkpoint(|file, next| {
            for slot in 0..2 {
                let bytes = next.encode(slot);
                let count = if cut > 2 * slot + 1 {
                    BLOCK
                } else if cut == 2 * slot + 1 {
                    BLOCK / 2
                } else {
                    0
                };
                if count > 0 {
                    file.write_all_at(&bytes[..count], slot * BLOCK_SIZE)?;
                    file.sync_data()?;
                }
                if count < BLOCK {
                    return Err(io::Error::other("interrupted checkpoint"));
                }
            }
            Ok(())
        });
        assert_eq!(result.is_ok(), cut == 4);
        drop(store);
        let mut store = open(&scratch);
        assert_eq!(store.get(b"gone").unwrap(), None);
        assert_eq!(
            store.get(b"keep").unwrap().as_deref(),
            Some(&b"unrelated"[..])
        );
        let advanced = cut >= 2;
        assert_eq!(store.get(b"old").unwrap().is_none(), advanced);
        assert_eq!(store.get(b"new").unwrap().is_some(), advanced);

        // Reuse after a degraded open must repair the alternate label first.
        // This write consumes every free data block, including any freed by
        // the prior flush. The existing state must survive loss of slot 0.
        let room = store.geometry().free;
        let large = value(334, (room - BLOCK_SIZE) as usize);
        store.put(b"unflushed", &large).unwrap();
        drop(store);
        Damage::Torn.apply(&scratch, 0);
        let store = open(&scratch);
        assert_eq!(store.get(b"unflushed").unwrap(), None);
        assert_eq!(store.get(b"gone").unwrap(), None);
        assert_eq!(
            store.get(b"keep").unwrap().as_deref(),
            Some(&b"unrelated"[..])
        );
        assert_eq!(
            store.get(b"old").unwrap().as_deref(),
            (!advanced).then_some(&b"before"[..])
        );
        assert_eq!(
            store.get(b"new").unwrap().as_deref(),
            advanced.then_some(&b"after"[..])
        );
    }
}

#[test]
fn torn_reused_payload_and_unpublished_index_are_ignored() {
    let scratch = Scratch::new(MIN_DEVICE_SIZE);
    let open = || RawStore::open(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    let mut store = RawStore::provision(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    store.put(b"keep", b"committed").unwrap();
    store.put(b"delete", b"retired").unwrap();
    store.flush().unwrap();
    store.delete(b"delete").unwrap();
    store.flush().unwrap();
    let free = store.geometry().free;
    for n in 0..8u64 {
        store
            .put(b"new", &value(n, (free - BLOCK_SIZE) as usize))
            .unwrap();
        let entry = store.handle(b"new").unwrap().unwrap();
        let blocks = store.reader().blocks(&entry.entry).unwrap();
        let run = blocks.data_runs().next().unwrap();
        scratch.write_at(run.start + n * 513, &[0xE5; 173]);
        let next = store.next_checkpoint().unwrap();
        let end = store.write_index(&next).unwrap();
        scratch.write_at(end - BLOCK_SIZE + n * 512, &[0xE5; 151]);
        drop(store);
        store = open();
        assert_eq!(store.geometry().free, free);
        assert_eq!(store.get(b"new").unwrap(), None);
        assert_eq!(store.get(b"delete").unwrap(), None);
        assert_eq!(
            store.get(b"keep").unwrap().as_deref(),
            Some(&b"committed"[..])
        );
    }
    store.put(b"new", b"retry").unwrap();
    store.flush().unwrap();
    drop(store);
    assert_eq!(open().get(b"new").unwrap().as_deref(), Some(&b"retry"[..]));
}

#[test]
fn seeded_mutations_and_crashes_match_the_last_flush() {
    let size = MIN_DEVICE_SIZE;
    let scratch = Scratch::new(size);
    let mut store = RawStore::provision(scratch.file(), size, id()).unwrap();
    let mut staged = BTreeMap::new();
    let mut durable = BTreeMap::new();
    let mut rng = 0xEDC0_0386_2673_C7B1u64;
    for step in 0..2800 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = vec![((rng >> 10) % 37) as u8];
        match rng % 11 {
            0..=5 => {
                let body = value(rng, (rng >> 24) as usize % 65_000);
                match store.put(&key, &body) {
                    Ok(()) => {
                        staged.insert(key, body);
                    }
                    Err(Error::NoSpace { .. }) => {}
                    Err(error) => panic!("step {step}: {error}"),
                }
            }
            6..=7 => {
                assert_eq!(store.delete(&key).unwrap(), staged.remove(&key).is_some());
            }
            8..=9 => {
                store.flush().unwrap();
                durable.clone_from(&staged);
            }
            _ => {
                drop(store);
                store = RawStore::open(scratch.file(), size, id()).unwrap();
                assert_eq!(contents(&store), durable, "step {step}");
                staged.clone_from(&durable);
            }
        }
    }
    store.flush().unwrap();
    drop(store);
    store = RawStore::open(scratch.file(), size, id()).unwrap();
    assert_eq!(contents(&store), staged);
}

#[test]
fn a_v1_label_is_explicitly_refused_without_writes() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let label = durable_label(&scratch);
    for slot in 0..2 {
        let mut block = label.encode(slot);
        block[..8].copy_from_slice(b"ZFSRAWL1");
        block[8..12].copy_from_slice(&1u32.to_le_bytes());
        crate::format::seal(&mut block);
        scratch.write_at(slot * BLOCK_SIZE, &block[..]);
    }
    let before = scratch.all();
    assert!(matches!(open_err(&scratch), Error::UnsupportedVersion(1)));
    assert_eq!(scratch.all(), before);
}
