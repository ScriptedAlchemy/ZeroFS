// Single-range slices and a deliberately reversed range exercise the API.
#![allow(clippy::single_range_in_vec_init, clippy::reversed_empty_ranges)]

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::{MAX_DEVICE_SIZE, MIN_DEVICE_SIZE, Role, format_uuid};

const SIZE: u64 = 4 << 20;

/// A sparse regular file standing in for a claimed device. Removed on drop.
struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(size: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zerofs-raw-device-test-{}-{}",
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
        Self { path }
    }

    fn file(&self) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .unwrap()
    }

    fn read_only(&self) -> File {
        File::open(&self.path).unwrap()
    }

    fn write_at(&self, offset: u64, bytes: &[u8]) {
        self.file().write_all_at(bytes, offset).unwrap();
    }

    fn read_at(&self, offset: u64, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        self.file().read_exact_at(&mut buf, offset).unwrap();
        buf
    }

    fn all(&self) -> Vec<u8> {
        std::fs::read(&self.path).unwrap()
    }

    fn write_label(&self, slot: u64, label: &Label) {
        self.write_at(slot * BLOCK_SIZE, &label.encode(slot)[..]);
    }

    /// Simulate a torn label write: the first half landed, the rest is garbage.
    fn tear_slot(&self, slot: u64) {
        self.write_at(slot * BLOCK_SIZE + 2048, &[0xA5; 2048]);
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

fn provision(scratch: &Scratch) -> RawStore {
    RawStore::provision(scratch.file(), SIZE, id()).unwrap()
}

fn reopen(scratch: &Scratch) -> RawStore {
    RawStore::open(scratch.file(), SIZE, id()).unwrap()
}

fn open_err(scratch: &Scratch) -> Error {
    RawStore::open(scratch.file(), SIZE, id()).unwrap_err()
}

fn durable_label(scratch: &Scratch) -> Label {
    select_label(read_slots(&Region::new(scratch.file(), SIZE, 0, SIZE).unwrap()).unwrap())
        .unwrap()
        .0
}

fn value(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn contents(store: &RawStore) -> BTreeMap<Vec<u8>, Vec<u8>> {
    store
        .keys()
        .map(|key| (key.to_vec(), store.get(key).unwrap().unwrap()))
        .collect()
}

/// A device with one acknowledged flush ("old") followed by a second
/// acknowledged flush ("new"), at generation 3.
fn two_acknowledged_flushes() -> Scratch {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"old", b"first").unwrap();
    store.flush().unwrap();
    store.put(b"new", b"committed").unwrap();
    store.flush().unwrap();
    scratch
}

fn assert_latest(store: &RawStore) {
    assert_eq!(store.get(b"old").unwrap().unwrap(), b"first");
    assert_eq!(store.get(b"new").unwrap().unwrap(), b"committed");
    assert_eq!(store.geometry().generation, 3);
}

#[derive(Debug, Clone, Copy)]
enum Damage {
    Torn,
    Zeroed,
    BitFlip,
}

impl Damage {
    fn apply(self, scratch: &Scratch, slot: u64) {
        match self {
            Self::Torn => scratch.tear_slot(slot),
            Self::Zeroed => scratch.write_at(slot * BLOCK_SIZE, &[0u8; BLOCK]),
            Self::BitFlip => {
                let at = slot * BLOCK_SIZE + 90; // inside the durable tail field
                let byte = scratch.read_at(at, 1)[0];
                scratch.write_at(at, &[byte ^ 0x10]);
            }
        }
    }
}

// --- round trips -----------------------------------------------------------

#[test]
fn put_flush_reopen_get_round_trips_every_size_class() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    let sizes = [0, 1, 4095, 4096, 4097, 100_000, 300_000];
    for (i, len) in sizes.iter().enumerate() {
        store
            .put(format!("k{i}").as_bytes(), &value(i as u64, *len))
            .unwrap();
    }
    store.flush().unwrap();
    let before = contents(&store);
    drop(store);

    let store = reopen(&scratch);
    assert!(!store.label_degraded());
    assert_eq!(contents(&store), before);
    for (i, len) in sizes.iter().enumerate() {
        assert_eq!(
            store.get(format!("k{i}").as_bytes()).unwrap().unwrap(),
            value(i as u64, *len)
        );
    }
    assert_eq!(store.scrub().unwrap(), sizes.len());
    assert_eq!(store.get(b"missing").unwrap(), None);
}

#[test]
fn unflushed_appends_are_visible_then_lost_on_reopen() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"durable", b"one").unwrap();
    store.flush().unwrap();
    store.put(b"volatile", b"two").unwrap();
    store.put(b"durable", b"overwritten").unwrap();
    assert!(store.delete(b"volatile").unwrap());
    store.put(b"volatile", b"three").unwrap();
    assert_eq!(store.get(b"durable").unwrap().unwrap(), b"overwritten");
    assert_eq!(store.get(b"volatile").unwrap().unwrap(), b"three");
    drop(store);

    let mut store = reopen(&scratch);
    assert_eq!(store.get(b"durable").unwrap().unwrap(), b"one");
    assert_eq!(store.get(b"volatile").unwrap(), None);
    assert_eq!(store.geometry().tail, store.geometry().durable_tail);

    // The discarded tail is reused, and the reused region replays cleanly.
    store.put(b"after", b"crash").unwrap();
    store.flush().unwrap();
    drop(store);
    let store = reopen(&scratch);
    assert_eq!(store.get(b"after").unwrap().unwrap(), b"crash");
    assert_eq!(store.get(b"durable").unwrap().unwrap(), b"one");
}

#[test]
fn overwrite_and_delete_replay_in_order() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"a", b"1").unwrap();
    store.put(b"b", b"2").unwrap();
    store.put(b"a", b"3").unwrap();
    assert!(store.delete(b"b").unwrap());
    assert!(!store.delete(b"b").unwrap());
    store.put(b"c", b"4").unwrap();
    store.flush().unwrap();
    let generation = store.geometry().generation;
    // A flush with nothing new and healthy labels writes nothing.
    store.flush().unwrap();
    assert_eq!(store.geometry().generation, generation);
    drop(store);

    let store = reopen(&scratch);
    assert_eq!(store.get(b"a").unwrap().unwrap(), b"3");
    assert_eq!(store.get(b"b").unwrap(), None);
    assert_eq!(store.get(b"c").unwrap().unwrap(), b"4");
    assert_eq!(store.len(), 2);
}

#[test]
fn every_flush_leaves_both_slots_identical() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    for i in 0..60u64 {
        store
            .put(&i.to_le_bytes(), &value(i, (i % 7) as usize * 100))
            .unwrap();
        if i % 3 == 0 {
            store.flush().unwrap();
            match read_slots(&Region::new(scratch.file(), SIZE, 0, SIZE).unwrap()).unwrap() {
                [SlotState::Valid(a), SlotState::Valid(b)] => {
                    assert_eq!(a, b);
                    assert_eq!(a, store.label);
                }
                other => panic!("slots diverged after flush: {other:?}"),
            }
        }
    }
    store.flush().unwrap();
    let before = contents(&store);
    drop(store);
    let store = reopen(&scratch);
    assert_eq!(contents(&store), before);
    assert!(!store.label_degraded());
}

// --- binding and geometry -------------------------------------------------

#[test]
fn open_rejects_each_identity_field_mismatch_with_a_readable_error() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let mut pool = id();
    pool.pool_uuid[0] ^= 1;
    let mut member = id();
    member.member_uuid[15] ^= 1;
    let mut role = id();
    role.role = Role::Metadata;
    for (expected, field) in [(pool, "(pool)"), (member, "(member)"), (role, "(role)")] {
        let err = RawStore::open(scratch.file(), SIZE, expected).unwrap_err();
        let message = err.to_string();
        match err {
            Error::IdentityMismatch { expected: e, found } => {
                assert_eq!(e, expected);
                assert_eq!(found, id());
            }
            other => panic!("expected identity mismatch, got {other:?}"),
        }
        assert!(message.contains(field), "{message}");
        assert!(message.contains(&format_uuid(&id().pool_uuid)), "{message}");
        let io: io::Error = RawStore::open(scratch.file(), SIZE, expected)
            .unwrap_err()
            .into();
        assert_eq!(io.kind(), io::ErrorKind::InvalidData);
        assert_eq!(io.to_string(), message);
    }
}

#[test]
fn nil_uuids_are_refused_by_provision_and_open() {
    let scratch = Scratch::new(SIZE);
    for nil in [
        Identity {
            pool_uuid: [0; 16],
            ..id()
        },
        Identity {
            member_uuid: [0; 16],
            ..id()
        },
    ] {
        assert!(matches!(
            RawStore::provision(scratch.file(), SIZE, nil),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            RawStore::open(scratch.file(), SIZE, nil),
            Err(Error::InvalidArgument(_))
        ));
    }
    assert!(scratch.all().iter().all(|&b| b == 0));
}

#[test]
fn geometry_must_be_exact_aligned_and_in_bounds() {
    let scratch = Scratch::new(SIZE);
    let bad_sizes = [
        0,
        MIN_DEVICE_SIZE - BLOCK_SIZE,
        MAX_DEVICE_SIZE + BLOCK_SIZE,
        u64::MAX,
        SIZE - 512,
        SIZE + 512,
        SIZE - BLOCK_SIZE, // smaller than the device
        SIZE + BLOCK_SIZE, // larger than the device
    ];
    for size in bad_sizes {
        assert!(
            matches!(
                RawStore::provision(scratch.file(), size, id()),
                Err(Error::InvalidGeometry(_))
            ),
            "size {size} accepted"
        );
    }
    assert!(scratch.all().iter().all(|&b| b == 0));
}

#[test]
fn open_rejects_a_device_that_changed_size() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    scratch.file().set_len(SIZE * 2).unwrap();
    match RawStore::open(scratch.file(), SIZE * 2, id()) {
        Err(Error::DeviceSizeMismatch { recorded, supplied }) => {
            assert_eq!((recorded, supplied), (SIZE, SIZE * 2));
        }
        other => panic!("expected size mismatch, got {other:?}"),
    }
    assert!(matches!(
        RawStore::open(scratch.file(), SIZE, id()),
        Err(Error::InvalidGeometry(_))
    ));
}

#[test]
fn open_reports_blank_device_as_not_provisioned() {
    let scratch = Scratch::new(SIZE);
    assert!(matches!(open_err(&scratch), Error::NotProvisioned));
}

// --- provisioning refuses anything but a blank device ---------------------

#[test]
fn provision_refuses_an_existing_label_without_writing() {
    let scratch = two_acknowledged_flushes();
    let before = scratch.all();
    match RawStore::provision(scratch.file(), SIZE, id()) {
        Err(Error::AlreadyProvisioned { found }) => assert_eq!(found, id()),
        other => panic!("expected AlreadyProvisioned, got {other:?}"),
    }
    assert_eq!(scratch.all(), before);
}

#[test]
fn provision_refuses_nonzero_bytes_anywhere_and_leaves_them_unchanged() {
    let sector = value(7, 512);
    let cases = [
        (0, b"\x55\xaa".to_vec()),                // boot signature bytes
        (512, b"EFI PART".to_vec()),              // GPT header
        (1024, b"ext4-like superblock".to_vec()), // inside a label slot
        (SIZE / 2 + 1536, sector.clone()),        // a middle sector
        (SIZE - 512, sector.clone()),             // final sector
        (SIZE - 1, vec![1]),                      // final byte
    ];
    for (offset, bytes) in cases {
        let scratch = Scratch::new(SIZE);
        scratch.write_at(offset, &bytes);
        let before = scratch.all();
        let first_nonzero = offset + bytes.iter().position(|&b| b != 0).unwrap() as u64;
        match RawStore::provision(scratch.file(), SIZE, id()) {
            Err(Error::NotBlank { offset }) => assert_eq!(offset, first_nonzero),
            other => panic!("offset {offset}: expected NotBlank, got {other:?}"),
        }
        assert_eq!(scratch.all(), before, "provision modified the device");
        assert_eq!(scratch.read_at(offset, bytes.len()), bytes);
    }
}

#[test]
fn a_device_with_only_its_label_erased_is_still_refused() {
    let scratch = two_acknowledged_flushes();
    scratch.write_at(0, &[0u8; 2 * BLOCK]);
    assert!(matches!(
        RawStore::provision(scratch.file(), SIZE, id()),
        Err(Error::NotBlank { offset: LOG_START })
    ));
}

#[test]
fn interrupted_provision_reopens_with_a_degraded_label() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    // Provision wrote slot 0 and crashed before slot 1.
    scratch.write_at(BLOCK_SIZE, &[0u8; BLOCK]);
    let mut store = reopen(&scratch);
    assert!(store.label_degraded());
    assert!(store.is_empty());
    store.flush().unwrap();
    assert!(!store.label_degraded());
    drop(store);
    assert!(!reopen(&scratch).label_degraded());
    assert!(matches!(
        RawStore::provision(scratch.file(), SIZE, id()),
        Err(Error::AlreadyProvisioned { .. })
    ));
}

// --- an acknowledged checkpoint survives damage to one slot ---------------

#[test]
fn acknowledged_flush_survives_damage_to_slot_0_alone() {
    for damage in [Damage::Torn, Damage::Zeroed, Damage::BitFlip] {
        let scratch = two_acknowledged_flushes();
        damage.apply(&scratch, 0);
        let mut store = reopen(&scratch);
        assert_latest(&store);
        assert!(store.label_degraded(), "{damage:?}");
        // A flush with no new data repairs both slots.
        store.flush().unwrap();
        assert!(!store.label_degraded());
        drop(store);
        let store = reopen(&scratch);
        assert!(!store.label_degraded());
        assert_eq!(store.get(b"new").unwrap().unwrap(), b"committed");
    }
}

#[test]
fn acknowledged_flush_survives_damage_to_slot_1_alone() {
    for damage in [Damage::Torn, Damage::Zeroed, Damage::BitFlip] {
        let scratch = two_acknowledged_flushes();
        damage.apply(&scratch, 1);
        let mut store = reopen(&scratch);
        assert_latest(&store);
        assert!(store.label_degraded(), "{damage:?}");
        store.put(b"later", b"x").unwrap();
        store.flush().unwrap();
        drop(store);
        let store = reopen(&scratch);
        assert!(!store.label_degraded());
        assert_eq!(store.get(b"later").unwrap().unwrap(), b"x");
    }
}

#[test]
fn both_slots_damaged_is_an_error_not_a_rollback() {
    for (first, second) in [
        (Damage::Torn, Damage::Torn),
        (Damage::BitFlip, Damage::Zeroed),
        (Damage::Zeroed, Damage::Torn),
    ] {
        let scratch = two_acknowledged_flushes();
        first.apply(&scratch, 0);
        second.apply(&scratch, 1);
        match open_err(&scratch) {
            Error::Corrupt { offset: 0, reason } => {
                assert!(
                    reason.contains("slot 0") && reason.contains("slot 1"),
                    "{reason}"
                )
            }
            other => panic!("{first:?}/{second:?}: expected Corrupt, got {other:?}"),
        }
    }
    // Both zeroed reads as unlabelled, but the log is not blank, so
    // provisioning over it is still refused.
    let scratch = two_acknowledged_flushes();
    Damage::Zeroed.apply(&scratch, 0);
    Damage::Zeroed.apply(&scratch, 1);
    assert!(matches!(open_err(&scratch), Error::NotProvisioned));
    assert!(matches!(
        RawStore::provision(scratch.file(), SIZE, id()),
        Err(Error::NotBlank { .. })
    ));
}

// --- interruption inside flush, before it returns -------------------------

/// A device whose acknowledged state is {old} with {pending} appended and its
/// data synced, as a flush leaves it just before the label writes. Returns the
/// durable checkpoint and the one the flush was about to write.
fn interrupted_flush() -> (Scratch, Label, Label) {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"old", b"acked").unwrap();
    store.flush().unwrap();
    store.put(b"pending", b"unacked").unwrap();
    let durable = store.label;
    let next = store.next_checkpoint().unwrap();
    store.file.sync_data().unwrap();
    drop(store);
    (scratch, durable, next)
}

fn assert_old_state(store: &RawStore) {
    assert_eq!(store.get(b"old").unwrap().unwrap(), b"acked");
    assert_eq!(store.get(b"pending").unwrap(), None);
}

fn assert_new_state(store: &RawStore) {
    assert_eq!(store.get(b"old").unwrap().unwrap(), b"acked");
    assert_eq!(store.get(b"pending").unwrap().unwrap(), b"unacked");
}

#[test]
fn crash_before_any_label_write_keeps_the_acknowledged_state() {
    let (scratch, durable, _) = interrupted_flush();
    let store = reopen(&scratch);
    assert_old_state(&store);
    assert_eq!(store.label, durable);
    assert!(!store.label_degraded());
}

#[test]
fn crash_during_slot_0_write_keeps_the_acknowledged_state() {
    let (scratch, durable, next) = interrupted_flush();
    scratch.write_label(0, &next);
    scratch.tear_slot(0);
    let store = reopen(&scratch);
    assert_old_state(&store);
    assert_eq!(store.label, durable);
    assert!(store.label_degraded());
}

#[test]
fn crash_between_slot_writes_takes_the_completed_checkpoint() {
    let (scratch, _, next) = interrupted_flush();
    scratch.write_label(0, &next);
    let mut store = reopen(&scratch);
    assert_new_state(&store);
    assert_eq!(store.label, next);
    assert!(store.label_degraded());
    store.flush().unwrap();
    drop(store);
    let store = reopen(&scratch);
    assert!(!store.label_degraded());
    assert_new_state(&store);
}

#[test]
fn crash_during_slot_1_write_takes_the_completed_checkpoint() {
    let (scratch, _, next) = interrupted_flush();
    scratch.write_label(0, &next);
    scratch.write_label(1, &next);
    scratch.tear_slot(1);
    let store = reopen(&scratch);
    assert_new_state(&store);
    assert!(store.label_degraded());
}

#[test]
fn consecutive_interrupted_checkpoints_preserve_recoverability() {
    let (scratch, _, first) = interrupted_flush();
    scratch.write_label(0, &first);
    let mut store = reopen(&scratch);
    assert_new_state(&store);
    store.put(b"later", b"second interrupted flush").unwrap();

    let result = store.flush_with_checkpoint(|file, next| {
        file.write_all_at(&next.encode(0)[..], 0)?;
        file.sync_data()?;
        Err(io::Error::other("crash after checkpoint slot 0"))
    });
    assert!(matches!(result, Err(Error::Io(_))));
    drop(store);

    let store = reopen(&scratch);
    assert_new_state(&store);
    assert_eq!(
        store.get(b"later").unwrap().unwrap(),
        b"second interrupted flush"
    );
    assert!(store.label_degraded());
}

#[test]
fn torn_checkpoint_after_single_slot_recovery_preserves_acknowledged_data() {
    let scratch = two_acknowledged_flushes();
    Damage::Torn.apply(&scratch, 1);
    let mut store = reopen(&scratch);
    assert_latest(&store);
    store.put(b"later", b"not acknowledged").unwrap();

    let result = store.flush_with_checkpoint(|file, next| {
        file.write_all_at(&next.encode(0)[..2048], 0)?;
        file.write_all_at(&[0xa5; 2048], 2048)?;
        file.sync_data()?;
        Err(io::Error::other("crash during checkpoint slot 0"))
    });
    assert!(matches!(result, Err(Error::Io(_))));
    drop(store);

    let store = reopen(&scratch);
    assert_latest(&store);
    assert_eq!(store.get(b"later").unwrap(), None);
    assert!(store.label_degraded());
}

#[test]
fn slot_states_no_checkpoint_sequence_produces_are_corrupt() {
    // Slot 1 ahead of slot 0.
    let (scratch, _, next) = interrupted_flush();
    scratch.write_label(1, &next);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt { offset: 0, .. }
    ));

    // Generations two apart.
    let (scratch, durable, next) = interrupted_flush();
    let skipped = Label {
        generation: durable.generation + 2,
        ..next
    };
    scratch.write_label(0, &skipped);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt { offset: 0, .. }
    ));

    // Same generation, different checkpoints.
    let (scratch, durable, _) = interrupted_flush();
    let forked = Label {
        record_count: 0,
        durable_tail: LOG_START,
        ..durable
    };
    scratch.write_label(1, &forked);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt { offset: 0, .. }
    ));

    // Different provisionings.
    let (scratch, durable, _) = interrupted_flush();
    let other = Label {
        nonce: [0x77; 16],
        ..durable
    };
    scratch.write_label(1, &other);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt { offset: 0, .. }
    ));
}

#[test]
fn label_copied_into_the_wrong_slot_is_not_trusted() {
    let scratch = two_acknowledged_flushes();
    let slot0 = scratch.read_at(0, BLOCK);
    scratch.write_at(BLOCK_SIZE, &slot0);
    let store = reopen(&scratch);
    assert_latest(&store);
    assert!(store.label_degraded());
}

/// After an acknowledged flush, arbitrary damage past the durable tail plus
/// damage to at most one label slot never changes the reopened state.
#[test]
fn arbitrary_damage_past_the_tail_and_to_one_slot_never_changes_state() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    for i in 0..20u64 {
        store
            .put(&i.to_le_bytes(), &value(i, 3000 + i as usize * 50))
            .unwrap();
    }
    store.flush().unwrap();
    let durable = contents(&store);
    let geometry = store.geometry();
    for i in 20..40u64 {
        store.put(&(i % 25).to_le_bytes(), &value(i, 7000)).unwrap();
    }
    let appended_tail = store.geometry().tail;
    drop(store);

    let mut rng = 0x1234_5678_9abc_def1u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let labels = [
        scratch.read_at(0, BLOCK),
        scratch.read_at(BLOCK_SIZE, BLOCK),
    ];
    for _ in 0..64 {
        let blocks = (appended_tail + 8 * BLOCK_SIZE - geometry.durable_tail) / BLOCK_SIZE;
        for _ in 0..(next() % 6) {
            let block = geometry.durable_tail + (next() % blocks) * BLOCK_SIZE;
            let at = block + next() % BLOCK_SIZE;
            let len = (next() % 512 + 1).min(block + BLOCK_SIZE - at) as usize;
            scratch.write_at(at, &value(next(), len));
        }
        scratch.write_at(0, &labels[0]);
        scratch.write_at(BLOCK_SIZE, &labels[1]);
        if next() % 3 != 0 {
            let slot = next() % 2;
            let at = slot * BLOCK_SIZE + next() % BLOCK_SIZE;
            let len = (next() % 64 + 1).min((slot + 1) * BLOCK_SIZE - at) as usize;
            scratch.write_at(at, &value(next(), len));
        }
        let store = reopen(&scratch);
        assert_eq!(store.geometry().durable_tail, geometry.durable_tail);
        assert_eq!(contents(&store), durable);
    }
}

// --- damage inside the durable range ---------------------------------------

#[test]
fn damaged_header_inside_durable_range_is_reported() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"a", &value(1, 5000)).unwrap();
    store.put(b"b", b"2").unwrap();
    store.flush().unwrap();
    drop(store);
    let second_header = LOG_START + 3 * BLOCK_SIZE;
    scratch.write_at(second_header + 40, &[0xEE]);
    match open_err(&scratch) {
        Error::Corrupt { offset, .. } => assert_eq!(offset, second_header),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn damaged_payload_fails_get_and_scrub_only_for_that_object() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"bad", &value(1, 5000)).unwrap();
    store.put(b"good", &value(2, 5000)).unwrap();
    store.flush().unwrap();
    drop(store);
    scratch.write_at(LOG_START + BLOCK_SIZE + 4321, &[0x00, 0x01]);
    let store = reopen(&scratch);
    match store.get(b"bad") {
        Err(Error::Corrupt { offset, .. }) => assert_eq!(offset, LOG_START + BLOCK_SIZE),
        other => panic!("expected Corrupt, got {other:?}"),
    }
    assert_eq!(store.get(b"good").unwrap().unwrap(), value(2, 5000));
    assert!(matches!(store.scrub(), Err(Error::Corrupt { .. })));
}

#[test]
fn records_from_another_provisioning_are_rejected() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let label = durable_label(&scratch);
    let foreign = RecordHeader {
        seq: 1,
        kind: Kind::Put,
        key: b"k".to_vec(),
        value_len: 0,
        value_sha256: sha256(&[]),
        modified_unix_nanos: 0,
    };
    scratch.write_at(LOG_START, &foreign.encode(&[0x42; 16])[..]);
    let forged = Label {
        generation: label.generation + 1,
        durable_tail: LOG_START + BLOCK_SIZE,
        record_count: 1,
        ..label
    };
    scratch.write_label(0, &forged);
    scratch.write_label(1, &forged);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt {
            offset: LOG_START,
            ..
        }
    ));
}

// --- overflow and bad input -------------------------------------------------

#[test]
fn exhausted_generation_errors_instead_of_wrapping() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let last = Label {
        generation: u64::MAX - 1,
        ..durable_label(&scratch)
    };
    scratch.write_label(0, &last);
    scratch.write_label(1, &last);
    let mut store = reopen(&scratch);
    store.put(b"k", b"v").unwrap();
    assert!(matches!(store.flush(), Err(Error::Corrupt { .. })));
    assert!(!store.is_poisoned());
    drop(store);
    assert_eq!(reopen(&scratch).geometry().generation, u64::MAX - 1);
}

#[test]
fn sealed_label_with_impossible_fields_is_corrupt() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let label = durable_label(&scratch);
    for bad in [
        Label {
            record_count: u64::MAX,
            ..label
        },
        Label {
            durable_tail: u64::MAX - BLOCK_SIZE + 1,
            ..label
        },
        Label {
            generation: u64::MAX,
            ..label
        },
        Label {
            device_size: u64::MAX,
            ..label
        },
    ] {
        scratch.write_label(0, &bad);
        scratch.write_label(1, &bad);
        assert!(matches!(open_err(&scratch), Error::Corrupt { .. }));
    }
}

#[test]
fn record_count_disagreeing_with_the_log_is_corrupt() {
    let scratch = two_acknowledged_flushes();
    let label = durable_label(&scratch);
    let bad = Label {
        record_count: label.record_count - 1,
        ..label
    };
    scratch.write_label(0, &bad);
    scratch.write_label(1, &bad);
    assert!(matches!(open_err(&scratch), Error::Corrupt { .. }));
}

#[test]
fn oversized_record_length_in_a_header_is_corrupt() {
    let scratch = two_acknowledged_flushes();
    let label = durable_label(&scratch);
    let huge = RecordHeader {
        seq: 1,
        kind: Kind::Put,
        key: b"old".to_vec(),
        value_len: u64::MAX,
        value_sha256: sha256(&[]),
        modified_unix_nanos: 0,
    };
    scratch.write_at(LOG_START, &huge.encode(&label.nonce)[..]);
    assert!(matches!(
        open_err(&scratch),
        Error::Corrupt {
            offset: LOG_START,
            ..
        }
    ));
}

// --- space, poisoning, arguments --------------------------------------------

#[test]
fn fills_to_no_space_without_damage() {
    let scratch = Scratch::new(MIN_DEVICE_SIZE);
    let mut store = RawStore::provision(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    let payload = value(1, 60_000); // 15 payload blocks + header = 16 blocks
    let mut stored = 0u64;
    let err = loop {
        match store.put(&stored.to_le_bytes(), &payload) {
            Ok(()) => stored += 1,
            Err(err) => break err,
        }
    };
    let free = store.geometry().free;
    match err {
        Error::NoSpace { needed, available } => {
            assert_eq!(needed, 16 * BLOCK_SIZE);
            assert_eq!(available, free);
            assert!(available < needed);
        }
        other => panic!("expected NoSpace, got {other:?}"),
    }
    assert_eq!(stored, (MIN_DEVICE_SIZE - LOG_START) / (16 * BLOCK_SIZE));
    assert!(!store.is_poisoned());
    store.put(b"tiny", b"").unwrap();
    store.flush().unwrap();
    let before = contents(&store);
    drop(store);
    let store = RawStore::open(scratch.file(), MIN_DEVICE_SIZE, id()).unwrap();
    assert_eq!(contents(&store), before);
}

#[test]
fn write_failure_poisons_the_store() {
    let scratch = Scratch::new(SIZE);
    drop(provision(&scratch));
    let mut store = RawStore::open(scratch.read_only(), SIZE, id()).unwrap();
    assert!(matches!(store.put(b"k", b"v"), Err(Error::Io(_))));
    assert!(store.is_poisoned());
    assert!(matches!(store.put(b"k", b"v"), Err(Error::Poisoned)));
    assert!(matches!(store.flush(), Err(Error::Poisoned)));
    assert!(matches!(store.get(b"k"), Err(Error::Poisoned)));
    assert!(reopen(&scratch).is_empty());
}

#[test]
fn label_repair_failure_poisons_the_store() {
    let scratch = two_acknowledged_flushes();
    Damage::Torn.apply(&scratch, 1);
    let mut store = RawStore::open(scratch.read_only(), SIZE, id()).unwrap();
    assert!(store.label_degraded());
    assert!(store.flush().is_err());
    assert!(store.is_poisoned());
    assert_latest(&reopen(&scratch));
}

#[test]
fn arguments_are_bounded() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    assert!(matches!(
        store.put(b"", b"v"),
        Err(Error::InvalidArgument(_))
    ));
    let long = vec![b'k'; MAX_KEY_LEN + 1];
    assert!(matches!(
        store.put(&long, b"v"),
        Err(Error::InvalidArgument(_))
    ));
    let longest = vec![b'k'; MAX_KEY_LEN];
    store.put(&longest, b"v").unwrap();
    store.flush().unwrap();
    drop(store);
    assert_eq!(reopen(&scratch).get(&longest).unwrap().unwrap(), b"v");
}

#[test]
fn store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RawStore>();
}

#[test]
fn stat_and_handles_track_overwrites_and_survive_reopen() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    assert_eq!(store.stat(b"k"), None);
    let first = value(1, 9000);
    store.put(b"k", &first).unwrap();
    let a = store.stat(b"k").unwrap();
    assert_eq!((a.len, a.sha256, a.seq), (9000, sha256(&first), 1));
    assert!(a.modified_unix_nanos > 1_600_000_000_000_000_000);
    let old = store.handle(b"k").unwrap().unwrap();
    let second = value(2, 10);
    store.put(b"k", &second).unwrap();
    let b = store.stat(b"k").unwrap();
    assert_eq!((b.len, b.seq), (10, 2));
    assert!(b.modified_unix_nanos >= a.modified_unix_nanos);
    // The old handle still reads the old, immutable record.
    let reader = store.reader();
    assert_eq!(reader.read_ranges(&old, &[0..9000]).unwrap()[0], first);
    store.flush().unwrap();
    drop(store);
    let store = reopen(&scratch);
    assert_eq!(store.stat(b"k"), Some(b));
    let listed: Vec<_> = store.objects().map(|(k, i)| (k.to_vec(), i)).collect();
    assert_eq!(listed, vec![(b"k".to_vec(), b)]);
}

#[test]
fn verified_range_reads_return_only_the_requested_bytes() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    // Spans more than one verify chunk.
    let payload = value(7, (VERIFY_CHUNK as usize) + 5000);
    store.put(b"big", &payload).unwrap();
    store.put(b"empty", &[]).unwrap();
    let handle = store.handle(b"big").unwrap().unwrap();
    let len = payload.len() as u64;
    let ranges = [
        0..0,
        0..10,
        VERIFY_CHUNK - 3..VERIFY_CHUNK + 3,
        len - 1..len,
        5..len,
    ];
    let got = store.reader().read_ranges(&handle, &ranges).unwrap();
    for (range, bytes) in ranges.iter().zip(&got) {
        assert_eq!(bytes[..], payload[range.start as usize..range.end as usize]);
    }
    let empty = store.handle(b"empty").unwrap().unwrap();
    assert_eq!(
        store.reader().read_ranges(&empty, &[0..0]).unwrap(),
        vec![Vec::<u8>::new()]
    );
    for bad in [len..len + 1, 4..3] {
        assert!(matches!(
            store.reader().read_ranges(&handle, &[bad]),
            Err(Error::InvalidArgument(_))
        ));
    }
    assert!(store.handle(b"missing").unwrap().is_none());
}

#[test]
fn range_reads_verify_the_whole_payload_even_outside_the_range() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    let payload = value(3, 3 * 4096);
    store.put(b"k", &payload).unwrap();
    store.flush().unwrap();
    let handle = store.handle(b"k").unwrap().unwrap();
    // Damage the last payload block; ask only for the first bytes.
    scratch.write_at(LOG_START + BLOCK_SIZE + 2 * 4096 + 7, &[0xFF]);
    assert!(matches!(
        store.reader().read_ranges(&handle, &[0..16]),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn costs_match_the_tail_advance_exactly() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    for len in [0usize, 1, 4095, 4096, 4097, 300_000] {
        let before = store.geometry().tail;
        store.put(b"k", &value(len as u64, len)).unwrap();
        assert_eq!(
            Some(store.geometry().tail - before),
            RawStore::put_cost(len as u64),
            "len {len}"
        );
    }
    assert_eq!(RawStore::put_cost(u64::MAX), None);
    assert_eq!(RawStore::put_cost(u64::MAX - BLOCK_SIZE), None);
    assert_eq!(
        RawStore::put_cost(u64::MAX - 2 * BLOCK_SIZE + 1),
        Some(u64::MAX - BLOCK_SIZE + 1)
    );
    let before = store.geometry().tail;
    assert!(store.delete(b"k").unwrap());
    assert_eq!(store.geometry().tail - before, RawStore::delete_cost());
}

#[test]
fn handles_are_refused_once_poisoned() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    store.put(b"k", b"v").unwrap();
    store.flush().unwrap();
    drop(store);
    let mut store = RawStore::open(scratch.read_only(), SIZE, id()).unwrap();
    assert!(store.handle(b"k").unwrap().is_some());
    assert!(store.put(b"x", b"v").is_err());
    assert!(matches!(store.handle(b"k"), Err(Error::Poisoned)));
}

#[test]
fn live_index_refusal_leaves_tail_and_checkpoint_unchanged() {
    let scratch = Scratch::new(SIZE);
    let mut store = provision(&scratch);
    let existing = 0u64.to_le_bytes();
    store.put(&existing, b"first").unwrap();
    store.flush().unwrap();
    let entry = store.index[existing.as_slice()];
    for i in 1..MAX_INDEX_KEYS as u64 {
        store.index.insert(i.to_le_bytes().to_vec(), entry);
    }
    let before = store.geometry();
    for _ in 0..2 {
        assert!(matches!(
            store.put(b"new key", b"never appended"),
            Err(Error::IndexFull {
                limit: MAX_INDEX_KEYS
            })
        ));
        assert_eq!(store.geometry(), before);
        assert!(!store.is_poisoned());
    }
    store.put(&existing, b"replacement").unwrap();
    assert!(store.delete(&existing).unwrap());
    store.put(b"new key", b"now fits").unwrap();
    store.flush().unwrap();
    let reopened = reopen(&scratch);
    assert_eq!(reopened.get(b"new key").unwrap().unwrap(), b"now fits");
    assert!(reopened.get(&existing).unwrap().is_none());
}

#[test]
fn subregion_io_preserves_offsets_above_four_gibibytes() {
    let start = 12 << 30;
    let device_size = start + SIZE;
    let scratch = Scratch::new(device_size);
    let region = Region::new(scratch.file(), device_size, start, SIZE).unwrap();
    let mut store = RawStore::provision_region(region, id()).unwrap();
    store.put(b"key", b"high-offset value").unwrap();
    store.flush().unwrap();
    drop(store);
    let region = Region::new(scratch.file(), device_size, start, SIZE).unwrap();
    let reopened = RawStore::open_region(region, id()).unwrap();
    assert_eq!(reopened.get(b"key").unwrap().unwrap(), b"high-offset value");
    assert_eq!(scratch.read_at(0, BLOCK), vec![0; BLOCK]);
}
