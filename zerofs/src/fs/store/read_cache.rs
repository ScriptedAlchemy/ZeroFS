use crate::db::Db;
use crate::fs::errors::FsError;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use foyer::{Cache, CacheBuilder};
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

struct KeyState<V> {
    loads: Vec<u64>,
    mutation_count: usize,
    /// Most recently completed successful mutation while invalidations overlap.
    /// The outer option distinguishes "no successful mutation" from a committed
    /// deletion represented by the inner `None`.
    pending_update: Option<Option<V>>,
}

impl<V> Default for KeyState<V> {
    fn default() -> Self {
        Self {
            loads: Vec::new(),
            mutation_count: 0,
            pending_update: None,
        }
    }
}

/// Positive read-through cache that rejects fills overtaken by a mutation.
///
/// `states` contains only active loads and mutations; it does not retain a
/// generation record for every key ever read.
pub(crate) struct ReadCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    entries: Cache<K, V>,
    states: DashMap<K, KeyState<V>>,
    next_load: AtomicU64,
    #[cfg(test)]
    load_count: AtomicU64,
}

/// Optional coherent metadata cache with one serving-authority policy.
///
/// Read-only database handles bypass the in-memory cache because their
/// manifests can advance outside the local write coordinator. Every cached
/// return is gated both before lookup and immediately before return.
pub(crate) struct MetadataCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    db: Arc<Db>,
    cache: Option<Arc<ReadCache<K, V>>>,
}

impl<K, V> Clone for MetadataCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            cache: self.cache.clone(),
        }
    }
}

#[must_use = "hold the guard until the database mutation finishes"]
pub(crate) struct InvalidationGuard<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    cache: Option<Arc<ReadCache<K, V>>>,
    keys: Vec<K>,
}

impl<K, V> Drop for InvalidationGuard<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(cache) = &self.cache {
            cache.end_invalidation(&self.keys);
        }
    }
}

impl<K, V> InvalidationGuard<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn disabled() -> Self {
        Self {
            cache: None,
            keys: Vec::new(),
        }
    }

    /// Publish values from a successful database mutation while ending its
    /// invalidation window. `None` records a committed deletion.
    pub(crate) fn publish(mut self, updates: HashMap<K, Option<V>>) {
        let Some(cache) = self.cache.take() else {
            return;
        };
        let keys = std::mem::take(&mut self.keys);
        assert_eq!(
            keys.len(),
            updates.len(),
            "cache publication must cover every invalidated key"
        );
        assert!(
            keys.iter().all(|key| updates.contains_key(key)),
            "cache publication key does not match an invalidated key"
        );
        cache.end_invalidation_with(&keys, &updates);
    }
}

struct LoadGuard<'a, K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    cache: &'a ReadCache<K, V>,
    key: K,
    token: Option<u64>,
}

impl<K, V> LoadGuard<'_, K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn publish(mut self, value: &V) {
        let token = self
            .token
            .take()
            .expect("active load guard without a token");
        self.cache.finish_load(&self.key, token, Some(value));
    }
}

impl<K, V> Drop for LoadGuard<'_, K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.cache.finish_load(&self.key, token, None);
        }
    }
}

impl<K, V> ReadCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn new(
        capacity: usize,
        name: &'static str,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Self {
        let entries = CacheBuilder::new(capacity)
            .with_name(name)
            .with_weighter(weighter)
            .build();
        let states = DashMap::new();
        Self {
            entries,
            states,
            next_load: AtomicU64::new(1),
            #[cfg(test)]
            load_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    pub(crate) async fn get_or_load<F, Fut, E>(&self, key: K, load: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        if let Some(value) = self.get(&key) {
            return Ok(value);
        }

        let load_guard = match self.states.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                if let Some(value) = self.get(&key) {
                    return Ok(value);
                }
                let state = entry.get_mut();
                if state.mutation_count != 0 {
                    None
                } else {
                    let token = self.next_load.fetch_add(1, Ordering::Relaxed);
                    state.loads.push(token);
                    Some(LoadGuard {
                        cache: self,
                        key: key.clone(),
                        token: Some(token),
                    })
                }
            }
            Entry::Vacant(entry) => {
                // A fill can land between the optimistic lookup and acquiring
                // the state shard. Recheck before creating persistent state.
                if let Some(value) = self.get(&key) {
                    return Ok(value);
                }
                let token = self.next_load.fetch_add(1, Ordering::Relaxed);
                entry.insert(KeyState {
                    loads: vec![token],
                    mutation_count: 0,
                    pending_update: None,
                });
                Some(LoadGuard {
                    cache: self,
                    key: key.clone(),
                    token: Some(token),
                })
            }
        };

        #[cfg(test)]
        self.load_count.fetch_add(1, Ordering::Relaxed);
        match load().await {
            Ok(value) => {
                if let Some(guard) = load_guard {
                    guard.publish(&value);
                }
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolve an ordered key set from one shared load. A partially warm set is
    /// deliberately reloaded as a whole so range readers observe one database
    /// scan rather than a mixture of independently loaded snapshots.
    async fn get_all_or_load<F, Fut, E>(&self, keys: Vec<K>, load: F) -> Result<Vec<V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<V>, E>>,
    {
        let mut cached = Vec::with_capacity(keys.len());
        let mut load_guards = Vec::with_capacity(keys.len());
        let mut all_cached = true;

        for key in &keys {
            if let Some(value) = self.get(key) {
                cached.push(Some(value));
                load_guards.push(None);
                continue;
            }
            all_cached = false;
            cached.push(None);

            let token = self.next_load.fetch_add(1, Ordering::Relaxed);
            let registered = match self.states.entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    let state = entry.get_mut();
                    if state.mutation_count != 0 {
                        false
                    } else {
                        state.loads.push(token);
                        true
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(KeyState {
                        loads: vec![token],
                        mutation_count: 0,
                        pending_update: None,
                    });
                    true
                }
            };
            load_guards.push(registered.then(|| LoadGuard {
                cache: self,
                key: key.clone(),
                token: Some(token),
            }));
        }

        if all_cached {
            return Ok(cached
                .into_iter()
                .map(|value| value.expect("all batch keys were cached"))
                .collect());
        }

        #[cfg(test)]
        self.load_count.fetch_add(1, Ordering::Relaxed);
        let loaded = load().await?;
        assert_eq!(
            loaded.len(),
            keys.len(),
            "batch cache load must return one value per requested key"
        );
        for (guard, value) in load_guards.into_iter().zip(&loaded) {
            if let Some(guard) = guard {
                guard.publish(value);
            }
        }
        Ok(loaded)
    }

    /// Evict `keys` and block fills until the returned guard is dropped.
    pub(crate) fn invalidate(
        self: &Arc<Self>,
        keys: impl IntoIterator<Item = K>,
    ) -> InvalidationGuard<K, V> {
        let keys: Vec<_> = keys.into_iter().collect();
        for key in &keys {
            let mut state = self.states.entry(key.clone()).or_default();
            state.mutation_count = state
                .mutation_count
                .checked_add(1)
                .expect("cache invalidation count overflow");
            state.loads.clear();
            self.entries.remove(key);
        }
        InvalidationGuard {
            cache: Some(self.clone()),
            keys,
        }
    }

    fn end_invalidation(&self, keys: &[K]) {
        self.finish_invalidation(keys, None);
    }

    fn end_invalidation_with(&self, keys: &[K], updates: &HashMap<K, Option<V>>) {
        self.finish_invalidation(keys, Some(updates));
    }

    fn finish_invalidation(&self, keys: &[K], updates: Option<&HashMap<K, Option<V>>>) {
        for key in keys {
            let Entry::Occupied(mut entry) = self.states.entry(key.clone()) else {
                panic!("ending a cache invalidation that was not active");
            };
            let state = entry.get_mut();
            assert!(
                state.mutation_count != 0,
                "ending a cache invalidation that was not active"
            );
            if let Some(updates) = updates {
                state.pending_update = Some(
                    updates
                        .get(key)
                        .expect("cache publication missing an invalidated key")
                        .clone(),
                );
            }
            state.mutation_count -= 1;
            if state.mutation_count == 0 {
                if let Some(update) = state.pending_update.take() {
                    match update {
                        Some(value) => {
                            self.entries.insert(key.clone(), value);
                        }
                        None => {
                            self.entries.remove(key);
                        }
                    }
                }
                if state.loads.is_empty() {
                    entry.remove();
                }
            }
        }
    }

    fn finish_load(&self, key: &K, token: u64, value: Option<&V>) {
        let Entry::Occupied(mut entry) = self.states.entry(key.clone()) else {
            return;
        };
        let state = entry.get_mut();
        let Some(index) = state.loads.iter().position(|current| *current == token) else {
            return;
        };
        state.loads.swap_remove(index);

        if state.mutation_count == 0
            && let Some(value) = value
        {
            self.entries.insert(key.clone(), value.clone());
        }
        if state.mutation_count == 0 && state.loads.is_empty() {
            entry.remove();
        }
    }

    #[cfg(test)]
    fn active_state_count(&self) -> usize {
        self.states.len()
    }

    #[cfg(test)]
    fn load_count(&self) -> u64 {
        self.load_count.load(Ordering::Relaxed)
    }
}

impl<K, V> MetadataCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        db: Arc<Db>,
        capacity: usize,
        name: &'static str,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Self {
        let cache =
            (!db.is_read_only()).then(|| Arc::new(ReadCache::new(capacity, name, weighter)));
        Self { db, cache }
    }

    pub(crate) fn get(&self, key: &K) -> Result<Option<V>, FsError> {
        self.db.check_serving_authority()?;
        let value = self.cache.as_ref().and_then(|cache| cache.get(key));
        self.db.check_serving_authority()?;
        Ok(value)
    }

    pub(crate) async fn get_or_load<F, Fut>(&self, key: K, load: F) -> Result<V, FsError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, FsError>>,
    {
        self.db.check_serving_authority()?;
        let value = match &self.cache {
            Some(cache) => cache.get_or_load(key, load).await?,
            None => load().await?,
        };
        self.db.check_serving_authority()?;
        Ok(value)
    }

    pub(crate) async fn get_all_or_load<F, Fut>(
        &self,
        keys: Vec<K>,
        load: F,
    ) -> Result<Vec<V>, FsError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<V>, FsError>>,
    {
        self.db.check_serving_authority()?;
        let value = match &self.cache {
            Some(cache) => cache.get_all_or_load(keys, load).await?,
            None => load().await?,
        };
        self.db.check_serving_authority()?;
        Ok(value)
    }

    pub(crate) fn invalidate(&self, keys: impl IntoIterator<Item = K>) -> InvalidationGuard<K, V> {
        match &self.cache {
            Some(cache) => cache.invalidate(keys),
            None => InvalidationGuard::disabled(),
        }
    }

    #[cfg(test)]
    pub(crate) fn peek(&self, key: &K) -> Option<V> {
        self.cache.as_ref()?.get(key)
    }

    #[cfg(test)]
    pub(crate) fn is_enabled(&self) -> bool {
        self.cache.is_some()
    }

    #[cfg(test)]
    pub(crate) fn load_count(&self) -> u64 {
        self.cache.as_ref().map_or(0, |cache| cache.load_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn invalidation_overtakes_a_cold_fill_without_waiting_for_it() {
        let cache = Arc::new(ReadCache::new(16, "overtaken-fill-test", |_, _| 1));
        let key = 42;
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (finish_load_tx, finish_load_rx) = oneshot::channel();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move {
            reader_cache
                .get_or_load(key, || async move {
                    load_started_tx.send(()).unwrap();
                    finish_load_rx.await.unwrap();
                    Ok::<_, ()>(10)
                })
                .await
        });
        load_started_rx.await.unwrap();

        let invalidation = cache.invalidate([key]);
        assert_eq!(
            cache.get_or_load(key, || async { Ok::<_, ()>(11) }).await,
            Ok(11)
        );
        assert_eq!(
            cache.get(&key),
            None,
            "a load started inside the mutation window must not fill"
        );

        finish_load_tx.send(()).unwrap();
        assert_eq!(reader.await.unwrap(), Ok(10));
        assert_eq!(cache.get(&key), None);

        drop(invalidation);
        assert_eq!(
            cache.get_or_load(key, || async { Ok::<_, ()>(20) }).await,
            Ok(20)
        );
        assert_eq!(cache.get(&key), Some(20));
    }

    #[tokio::test]
    async fn mutation_overtaking_a_batch_fill_keeps_the_committed_value() {
        let cache = Arc::new(ReadCache::new(16, "overtaken-batch-fill-test", |_, _| 1));
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (finish_load_tx, finish_load_rx) = oneshot::channel();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move {
            reader_cache
                .get_all_or_load(vec![1, 2], || async move {
                    load_started_tx.send(()).unwrap();
                    finish_load_rx.await.unwrap();
                    Ok::<_, ()>(vec![1, 2])
                })
                .await
        });
        load_started_rx.await.unwrap();

        let mutation = cache.invalidate([1]);
        mutation.publish(HashMap::from([(1, Some(10))]));
        finish_load_tx.send(()).unwrap();

        assert_eq!(reader.await.unwrap(), Ok(vec![1, 2]));
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.get(&2), Some(2));
    }

    #[tokio::test]
    async fn same_key_cold_loads_do_not_serialize() {
        let cache = Arc::new(ReadCache::new(16, "parallel-fill-test", |_, _| 1));
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (second_started_tx, second_started_rx) = oneshot::channel();
        let (finish_first_tx, finish_first_rx) = oneshot::channel();
        let (finish_second_tx, finish_second_rx) = oneshot::channel();

        let first_cache = cache.clone();
        let first = tokio::spawn(async move {
            first_cache
                .get_or_load(1, || async move {
                    first_started_tx.send(()).unwrap();
                    finish_first_rx.await.unwrap();
                    Ok::<_, ()>(1)
                })
                .await
        });
        first_started_rx.await.unwrap();

        let second_cache = cache.clone();
        let second = tokio::spawn(async move {
            second_cache
                .get_or_load(1, || async move {
                    second_started_tx.send(()).unwrap();
                    finish_second_rx.await.unwrap();
                    Ok::<_, ()>(2)
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), second_started_rx)
            .await
            .expect("second load serialized behind the first")
            .unwrap();

        finish_first_tx.send(()).unwrap();
        finish_second_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap(), Ok(1));
        assert_eq!(second.await.unwrap(), Ok(2));
        assert_eq!(cache.active_state_count(), 0);
    }

    #[tokio::test]
    async fn cancelling_a_cold_load_removes_its_state() {
        let cache = Arc::new(ReadCache::new(16, "cancelled-fill-test", |_, _| 1));
        let (load_started_tx, load_started_rx) = oneshot::channel();

        let reader_cache = cache.clone();
        let reader = tokio::spawn(async move {
            reader_cache
                .get_or_load(1, || async move {
                    load_started_tx.send(()).unwrap();
                    std::future::pending::<Result<i32, ()>>().await
                })
                .await
        });
        load_started_rx.await.unwrap();
        assert_eq!(cache.active_state_count(), 1);

        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        assert_eq!(cache.active_state_count(), 0);
    }

    #[tokio::test]
    async fn failed_parallel_load_does_not_discard_a_successful_fill() {
        let cache = Arc::new(ReadCache::new(16, "independent-fill-test", |_, _| 1));
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (finish_first_tx, finish_first_rx) = oneshot::channel();

        let first_cache = cache.clone();
        let first = tokio::spawn(async move {
            first_cache
                .get_or_load(1, || async move {
                    first_started_tx.send(()).unwrap();
                    finish_first_rx.await.unwrap();
                    Ok::<_, &'static str>(10)
                })
                .await
        });
        first_started_rx.await.unwrap();

        assert_eq!(
            cache
                .get_or_load(1, || async { Err::<i32, _>("load failed") })
                .await,
            Err("load failed")
        );
        finish_first_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap(), Ok(10));
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.active_state_count(), 0);
    }

    #[tokio::test]
    async fn overlapping_invalidations_keep_fills_blocked_until_both_end() {
        let cache = Arc::new(ReadCache::new(16, "nested-invalidation-test", |_, _| 1));
        assert_eq!(
            cache.get_or_load(1, || async { Ok::<_, ()>(1) }).await,
            Ok(1)
        );

        let first = cache.invalidate([1]);
        let second = cache.invalidate([1]);
        drop(first);

        assert_eq!(
            cache.get_or_load(1, || async { Ok::<_, ()>(2) }).await,
            Ok(2)
        );
        assert_eq!(cache.get(&1), None);

        drop(second);
        assert_eq!(
            cache.get_or_load(1, || async { Ok::<_, ()>(3) }).await,
            Ok(3)
        );
        assert_eq!(cache.get(&1), Some(3));
        assert_eq!(cache.active_state_count(), 0);
    }

    #[tokio::test]
    async fn failed_overlapping_invalidation_keeps_the_successful_publication() {
        let cache = Arc::new(ReadCache::new(16, "overlapping-publish-test", |_, _| 1));
        assert_eq!(
            cache.get_or_load(1, || async { Ok::<_, ()>(1) }).await,
            Ok(1)
        );

        let successful = cache.invalidate([1]);
        let failed = cache.invalidate([1]);
        successful.publish(HashMap::from([(1, Some(2))]));

        assert_eq!(
            cache.get(&1),
            None,
            "a committed value must remain hidden during an overlapping mutation"
        );
        drop(failed);

        assert_eq!(cache.get(&1), Some(2));
        assert_eq!(cache.active_state_count(), 0);
    }
}
