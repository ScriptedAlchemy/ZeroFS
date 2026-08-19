//! Sole owner of physical-space generations for the writeback directory.
//!
//! Admission, journal transition, remote cleanup, and refresher tasks consume
//! [`PhysicalSpaceSample`] values from [`PhysicalSpaceSampler::sample`]. They
//! do not allocate or construct generations themselves.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// One successful probe of the writeback filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhysicalSpaceSample {
    pub(crate) generation: u64,
    pub(crate) available_bytes: u64,
}

/// The only type that allocates [`PhysicalSpaceSample::generation`].
#[derive(Debug)]
pub(crate) struct PhysicalSpaceSampler {
    writeback_dir: PathBuf,
    next_generation: AtomicU64,
    latest_generation: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SpaceSampleError {
    #[error("writeback space probe failed for {}: {source}", path.display())]
    Probe {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("writeback space sample generation {sample} is stale; latest is {latest}")]
    Stale { sample: u64, latest: u64 },
}

impl PhysicalSpaceSampler {
    pub(crate) fn new(writeback_dir: impl Into<PathBuf>) -> Self {
        Self {
            writeback_dir: writeback_dir.into(),
            next_generation: AtomicU64::new(1),
            latest_generation: AtomicU64::new(0),
        }
    }

    pub(crate) fn writeback_dir(&self) -> &Path {
        &self.writeback_dir
    }

    pub(crate) fn latest_generation(&self) -> u64 {
        self.latest_generation.load(Ordering::Acquire)
    }

    /// Probe the canonical writeback directory and publish the next generation.
    ///
    /// The generation is allocated only after `fs4::available_space` succeeds.
    pub(crate) async fn sample(&self) -> Result<PhysicalSpaceSample, SpaceSampleError> {
        let path = self.writeback_dir.clone();
        let available_bytes = tokio::task::spawn_blocking(move || fs4::available_space(&path))
            .await
            .map_err(|error| SpaceSampleError::Probe {
                path: self.writeback_dir.clone(),
                source: io::Error::other(format!("space probe task failed: {error}")),
            })?
            .map_err(|source| SpaceSampleError::Probe {
                path: self.writeback_dir.clone(),
                source,
            })?;
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        publish_latest(&self.latest_generation, generation);
        Ok(PhysicalSpaceSample {
            generation,
            available_bytes,
        })
    }

    pub(crate) fn reject_stale(
        &self,
        sample: PhysicalSpaceSample,
    ) -> Result<PhysicalSpaceSample, SpaceSampleError> {
        let latest = self.latest_generation();
        if sample.generation < latest {
            Err(SpaceSampleError::Stale {
                sample: sample.generation,
                latest,
            })
        } else {
            Ok(sample)
        }
    }
}

fn publish_latest(latest: &AtomicU64, generation: u64) {
    let mut current = latest.load(Ordering::Acquire);
    while generation > current {
        match latest.compare_exchange_weak(current, generation, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalSpaceSampler, SpaceSampleError};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[tokio::test]
    async fn sample_generation_is_strictly_monotonic() {
        let temp = tempfile::tempdir().unwrap();
        let sampler = PhysicalSpaceSampler::new(temp.path());

        let first = sampler.sample().await.unwrap();
        let second = sampler.sample().await.unwrap();
        let third = sampler.sample().await.unwrap();

        assert_eq!(first.generation, 1);
        assert_eq!(second.generation, 2);
        assert_eq!(third.generation, 3);
        assert_eq!(sampler.latest_generation(), 3);
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_generation_source() {
        let temp = tempfile::tempdir().unwrap();
        let sampler = Arc::new(PhysicalSpaceSampler::new(temp.path()));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let sampler = sampler.clone();
            tasks.push(tokio::spawn(async move { sampler.sample().await }));
        }

        let mut generations = BTreeSet::new();
        for task in tasks {
            generations.insert(task.await.unwrap().unwrap().generation);
        }

        assert_eq!(generations.len(), 32);
        assert_eq!(*generations.first().unwrap(), 1);
        assert_eq!(*generations.last().unwrap(), 32);
        assert_eq!(sampler.latest_generation(), 32);
    }

    #[tokio::test]
    async fn sample_uses_canonical_writeback_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let writeback_dir = temp.path().join("dirty").join("bucket_space");
        std::fs::create_dir_all(&writeback_dir).unwrap();
        let sampler = PhysicalSpaceSampler::new(&writeback_dir);

        let sample = sampler.sample().await.unwrap();

        assert_eq!(sampler.writeback_dir(), writeback_dir.as_path());
        assert_eq!(
            sample.available_bytes,
            fs4::available_space(&writeback_dir).unwrap()
        );
    }

    #[tokio::test]
    async fn failed_probe_does_not_publish_generation() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing-writeback");
        let sampler = PhysicalSpaceSampler::new(&missing);

        let error = sampler.sample().await.unwrap_err();
        assert!(matches!(error, SpaceSampleError::Probe { .. }));
        assert_eq!(sampler.latest_generation(), 0);

        std::fs::create_dir_all(&missing).unwrap();
        let sample = sampler.sample().await.unwrap();
        assert_eq!(sample.generation, 1);
        assert_eq!(sampler.latest_generation(), 1);
    }

    #[tokio::test]
    async fn stale_sample_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let sampler = PhysicalSpaceSampler::new(temp.path());
        let first = sampler.sample().await.unwrap();
        let second = sampler.sample().await.unwrap();

        assert_eq!(sampler.reject_stale(second).unwrap(), second);
        let error = sampler.reject_stale(first).unwrap_err();
        assert!(matches!(
            error,
            SpaceSampleError::Stale {
                sample: 1,
                latest: 2
            }
        ));
    }
}
