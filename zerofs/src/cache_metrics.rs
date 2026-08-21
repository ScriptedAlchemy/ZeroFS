use foyer::{HybridCache, StorageKey, StorageValue};
use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MetricKey {
    name: Cow<'static, str>,
    labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

/// Atomic bridge for the Foyer counters and gauges ZeroFS exports.
///
/// Foyer 0.22 does not expose current disk-queue depth. Histograms are not
/// retained here; ZeroFS only consumes the bounded, low-cardinality counters
/// and gauges represented in [`CacheMetricsSnapshot`].
#[derive(Clone, Debug, Default)]
pub(crate) struct FoyerMetricsRegistry {
    values: Arc<Mutex<HashMap<MetricKey, Arc<AtomicU64>>>>,
}

impl FoyerMetricsRegistry {
    fn value(&self, key: MetricKey) -> Arc<AtomicU64> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key)
            .or_default()
            .clone()
    }

    fn counter_value(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let key = MetricKey {
            name: Cow::Owned(name.to_owned()),
            labels: labels
                .iter()
                .map(|(name, value)| {
                    (
                        Cow::Owned((*name).to_owned()),
                        Cow::Owned((*value).to_owned()),
                    )
                })
                .collect(),
        };
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .map_or(0, |value| value.load(Ordering::Relaxed))
    }
}

#[derive(Debug)]
struct Counter(Arc<AtomicU64>);

impl CounterOps for Counter {
    fn increase(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct Gauge(Arc<AtomicU64>);

impl GaugeOps for Gauge {
    fn increase(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn decrease(&self, value: u64) {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(value))
            })
            .ok();
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct CounterVec {
    registry: FoyerMetricsRegistry,
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl CounterVecOps for CounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        let key = MetricKey {
            name: self.name.clone(),
            labels: self
                .label_names
                .iter()
                .zip(labels)
                .map(|(name, value)| (Cow::Borrowed(*name), value.clone()))
                .collect(),
        };
        Box::new(Counter(self.registry.value(key)))
    }
}

#[derive(Debug)]
struct GaugeVec {
    registry: FoyerMetricsRegistry,
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl GaugeVecOps for GaugeVec {
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        let key = MetricKey {
            name: self.name.clone(),
            labels: self
                .label_names
                .iter()
                .zip(labels)
                .map(|(name, value)| (Cow::Borrowed(*name), value.clone()))
                .collect(),
        };
        Box::new(Gauge(self.registry.value(key)))
    }
}

#[derive(Debug)]
struct IgnoreHistogram;

impl HistogramOps for IgnoreHistogram {
    fn record(&self, _value: f64) {}
}

#[derive(Debug)]
struct IgnoreHistogramVec;

impl HistogramVecOps for IgnoreHistogramVec {
    fn histogram(&self, _labels: &[Cow<'static, str>]) -> BoxedHistogram {
        Box::new(IgnoreHistogram)
    }
}

impl RegistryOps for FoyerMetricsRegistry {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        _description: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        Box::new(CounterVec {
            registry: self.clone(),
            name,
            label_names,
        })
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        _description: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        Box::new(GaugeVec {
            registry: self.clone(),
            name,
            label_names,
        })
    }

    fn register_histogram_vec(
        &self,
        _name: Cow<'static, str>,
        _description: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        Box::new(IgnoreHistogramVec)
    }

    fn register_histogram_vec_with_buckets(
        &self,
        _name: Cow<'static, str>,
        _description: Cow<'static, str>,
        _label_names: &'static [&'static str],
        _buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        Box::new(IgnoreHistogramVec)
    }
}

/// Foyer's own logical occupancy and disk-device counters for one cache.
///
/// Occupancy is the sum of the configured entry weights. It deliberately does
/// not claim to measure allocator RSS or memory retained by callers and the
/// disk-cache write pipeline.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CacheTierSnapshot {
    pub logical_usage_bytes: usize,
    pub logical_capacity_bytes: usize,
    pub entries: usize,
    pub disk_read_bytes: usize,
    pub disk_write_bytes: usize,
    pub disk_read_ios: usize,
    pub disk_write_ios: usize,
    pub queue_buffer_overflow_total: u64,
    pub queue_channel_overflow_total: u64,
}

fn snapshot_hybrid<K, V>(cache: &HybridCache<K, V>) -> CacheTierSnapshot
where
    K: StorageKey,
    V: StorageValue,
{
    let memory = cache.memory();
    let disk = cache.statistics();
    CacheTierSnapshot {
        logical_usage_bytes: memory.usage(),
        logical_capacity_bytes: memory.capacity(),
        entries: memory.entries(),
        disk_read_bytes: disk.disk_read_bytes(),
        disk_write_bytes: disk.disk_write_bytes(),
        disk_read_ios: disk.disk_read_ios(),
        disk_write_ios: disk.disk_write_ios(),
        queue_buffer_overflow_total: 0,
        queue_channel_overflow_total: 0,
    }
}

trait CacheStatsSource: Send + Sync {
    fn snapshot(&self) -> CacheTierSnapshot;
}

struct HybridCacheStatsSource<K, V>(HybridCache<K, V>)
where
    K: StorageKey,
    V: StorageValue;

impl<K, V> CacheStatsSource for HybridCacheStatsSource<K, V>
where
    K: StorageKey,
    V: StorageValue,
{
    fn snapshot(&self) -> CacheTierSnapshot {
        snapshot_hybrid(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CacheMetricsSnapshot {
    pub raw_parts: CacheTierSnapshot,
    pub decoded_blocks: CacheTierSnapshot,
}

#[derive(Clone)]
pub(crate) struct CacheMetrics {
    raw_parts: Arc<dyn CacheStatsSource>,
    decoded_blocks: Arc<dyn CacheStatsSource>,
    registry: FoyerMetricsRegistry,
}

impl CacheMetrics {
    pub(crate) fn new<PK, PV, BK, BV>(
        raw_parts: HybridCache<PK, PV>,
        decoded_blocks: HybridCache<BK, BV>,
        registry: FoyerMetricsRegistry,
    ) -> Self
    where
        PK: StorageKey,
        PV: StorageValue,
        BK: StorageKey,
        BV: StorageValue,
    {
        Self {
            raw_parts: Arc::new(HybridCacheStatsSource(raw_parts)),
            decoded_blocks: Arc::new(HybridCacheStatsSource(decoded_blocks)),
            registry,
        }
    }

    pub(crate) fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            raw_parts: self.snapshot_tier(self.raw_parts.as_ref(), "zerofs-object-prefetch-parts"),
            decoded_blocks: self
                .snapshot_tier(self.decoded_blocks.as_ref(), "zerofs-slatedb-hybrid"),
        }
    }

    fn snapshot_tier(
        &self,
        source: &dyn CacheStatsSource,
        foyer_name: &'static str,
    ) -> CacheTierSnapshot {
        let mut snapshot = source.snapshot();
        snapshot.queue_buffer_overflow_total = self.registry.counter_value(
            "foyer_storage_inner_op_total",
            &[("name", foyer_name), ("op", "buffer_overflow")],
        );
        snapshot.queue_channel_overflow_total = self.registry.counter_value(
            "foyer_storage_inner_op_total",
            &[("name", foyer_name), ("op", "channel_overflow")],
        );
        snapshot
    }
}

#[cfg(test)]
pub(crate) async fn build_test_cache(
    root: &std::path::Path,
    name: &'static str,
    capacity: usize,
    submit_queue_size: usize,
    registry: FoyerMetricsRegistry,
) -> HybridCache<u64, Vec<u8>> {
    use foyer::{
        BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    };

    const KIB: usize = 1024;

    HybridCacheBuilder::new()
        .with_name(name)
        .with_metrics_registry(Box::new(registry))
        .memory(capacity)
        .with_shards(1)
        .with_weighter(|_: &u64, value: &Vec<u8>| value.len())
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(root)
                    .with_capacity(16 * 1024 * KIB)
                    .build()
                    .unwrap(),
            )
            .with_block_size(1024 * KIB)
            .with_submit_queue_size_threshold(submit_queue_size),
        )
        .build()
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::{CacheMetrics, FoyerMetricsRegistry, build_test_cache, snapshot_hybrid};

    const KIB: usize = 1024;

    #[tokio::test]
    async fn logical_occupancy_stays_within_capacity_during_eviction_churn() {
        let dir = tempfile::tempdir().unwrap();
        let cache = build_test_cache(
            dir.path(),
            "bounded-test",
            64 * KIB,
            16 * KIB,
            FoyerMetricsRegistry::default(),
        )
        .await;

        for key in 0..256 {
            cache.insert(key, vec![key as u8; 4 * KIB]);
        }

        let snapshot = snapshot_hybrid(&cache);
        assert_eq!(snapshot.logical_capacity_bytes, 64 * KIB);
        assert!(
            snapshot.logical_usage_bytes <= snapshot.logical_capacity_bytes,
            "usage {} exceeded capacity {}",
            snapshot.logical_usage_bytes,
            snapshot.logical_capacity_bytes
        );
        assert!(snapshot.entries <= 16);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn foyer_registry_counts_real_disk_channel_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let registry = FoyerMetricsRegistry::default();
        let cache =
            build_test_cache(dir.path(), "overflow-test", 4 * KIB, 0, registry.clone()).await;

        // This loop does not yield to the flusher. The first eviction fills the
        // zero-threshold queue; later evictions exercise Foyer's real overflow
        // counter rather than a synthetic estimate.
        for key in 0..256 {
            cache.insert(key, vec![key as u8; KIB]);
        }

        assert!(
            registry.counter_value(
                "foyer_storage_inner_op_total",
                &[("name", "overflow-test"), ("op", "channel_overflow")],
            ) > 0
        );
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn foyer_registry_counts_real_disk_buffer_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let registry = FoyerMetricsRegistry::default();
        let cache = build_test_cache(
            dir.path(),
            "buffer-overflow-test",
            64 * KIB,
            usize::MAX,
            registry.clone(),
        )
        .await;

        // Values larger than the block engine's 1 MiB block cannot fit its
        // serialization buffer. The test cache has one shard, so replacing
        // its over-capacity entry deterministically reaches the real flusher.
        for key in 0..2 {
            cache.insert(key, vec![key as u8; 2 * 1024 * KIB]);
        }
        cache.storage().wait().await;

        assert!(
            registry.counter_value(
                "foyer_storage_inner_op_total",
                &[("name", "buffer-overflow-test"), ("op", "buffer_overflow")],
            ) > 0
        );
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn aggregate_snapshot_keeps_cache_tiers_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let registry = FoyerMetricsRegistry::default();
        let parts = build_test_cache(
            &dir.path().join("parts"),
            "zerofs-object-prefetch-parts",
            32 * KIB,
            16 * KIB,
            registry.clone(),
        )
        .await;
        let blocks = build_test_cache(
            &dir.path().join("blocks"),
            "zerofs-slatedb-hybrid",
            16 * KIB,
            16 * KIB,
            registry.clone(),
        )
        .await;
        parts.insert(1, vec![1; 4 * KIB]);
        blocks.insert(1, vec![2; 2 * KIB]);

        let snapshot = CacheMetrics::new(parts.clone(), blocks.clone(), registry).snapshot();
        assert_eq!(snapshot.raw_parts.logical_capacity_bytes, 32 * KIB);
        assert_eq!(snapshot.raw_parts.logical_usage_bytes, 4 * KIB);
        assert_eq!(snapshot.decoded_blocks.logical_capacity_bytes, 16 * KIB);
        assert_eq!(snapshot.decoded_blocks.logical_usage_bytes, 2 * KIB);

        parts.close().await.unwrap();
        blocks.close().await.unwrap();
    }
}
