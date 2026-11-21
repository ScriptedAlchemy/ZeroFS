use super::*;

#[test]
fn updating_an_unreadable_shard_cannot_make_partial_totals_known_after_restart() {
    let stats = FileSystemGlobalStats::new(Arc::new(KeyCodec::new()));
    stats.load_shard_record(3, b"bad");
    assert_eq!(stats.known_totals(), None);

    let staged = stats.stage_delta(3, 23, 1);
    stats.publish(&staged);
    let restarted = FileSystemGlobalStats::new(Arc::new(KeyCodec::new()));
    restarted.load_shard_record(3, &staged.value);
    assert_eq!(
        restarted.known_totals(),
        None,
        "a delta cannot reconstruct the missing original shard counters"
    );
    assert_eq!(
        staged.value.as_ref(),
        b"bad",
        "retain the damaged record for repair"
    );
}

#[test]
fn healthy_shards_still_persist_their_counters_while_another_is_unreadable() {
    let stats = FileSystemGlobalStats::new(Arc::new(KeyCodec::new()));
    stats.load_shard_record(3, b"bad");
    stats.load_shard(
        1,
        &StatsShardData {
            used_bytes: 100,
            used_inodes: 2,
        },
    );
    let staged = stats.stage_delta(1, 24, 1);
    stats.publish(&staged);
    let persisted: StatsShardData = bincode::deserialize(&staged.value).unwrap();
    assert_eq!((persisted.used_bytes, persisted.used_inodes), (124, 3));
    assert_eq!(stats.known_totals(), None);
}
