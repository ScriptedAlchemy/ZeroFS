//! Sole owner of physical-space generations for the writeback directory.
//!
//! Admission, journal transition, remote cleanup, and refresher tasks consume
//! [`PhysicalSpaceSample`] values from [`PhysicalSpaceSampler::sample`]. They
//! do not allocate or construct generations themselves.

use crate::writeback::anchored_dir::AnchoredDir;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
enum SpaceSource {
    Path(PathBuf),
    Anchored(AnchoredDir),
}

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
    source: SpaceSource,
    next_generation: AtomicU64,
    latest_generation: AtomicU64,
    latest_available: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SpaceSampleError {
    #[error("writeback space probe failed for {}: {source}", path.display())]
    Probe {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    // Landed-but-not-wired: constructed once stale-sample rejection is wired.
    #[allow(dead_code)]
    #[error("writeback space sample generation {sample} is stale; latest is {latest}")]
    Stale { sample: u64, latest: u64 },
}

impl PhysicalSpaceSampler {
    pub(crate) fn new(writeback_dir: impl Into<PathBuf>) -> Self {
        let writeback_dir = writeback_dir.into();
        Self {
            source: SpaceSource::Path(writeback_dir.clone()),
            writeback_dir,
            next_generation: AtomicU64::new(1),
            latest_generation: AtomicU64::new(0),
            latest_available: AtomicU64::new(0),
        }
    }

    pub(crate) fn new_anchored(writeback_dir: AnchoredDir) -> Self {
        Self {
            writeback_dir: writeback_dir.display().to_path_buf(),
            source: SpaceSource::Anchored(writeback_dir),
            next_generation: AtomicU64::new(1),
            latest_generation: AtomicU64::new(0),
            latest_available: AtomicU64::new(0),
        }
    }

    // Landed-but-not-wired accessor.
    #[allow(dead_code)]
    pub(crate) fn writeback_dir(&self) -> &Path {
        &self.writeback_dir
    }

    pub(crate) fn latest_generation(&self) -> u64 {
        self.latest_generation.load(Ordering::Acquire)
    }

    /// Last successful probe, if any. Never invents a generation.
    pub(crate) fn latest_sample(&self) -> Option<PhysicalSpaceSample> {
        let generation = self.latest_generation();
        if generation == 0 {
            None
        } else {
            Some(PhysicalSpaceSample {
                generation,
                available_bytes: self.latest_available.load(Ordering::Acquire),
            })
        }
    }

    /// Probe the canonical writeback directory and publish the next generation.
    ///
    /// The generation is allocated only after `fs4::available_space` succeeds.
    pub(crate) async fn sample(&self) -> Result<PhysicalSpaceSample, SpaceSampleError> {
        let source = self.source.clone();
        let available_bytes = tokio::task::spawn_blocking(move || match source {
            SpaceSource::Path(path) => fs4::available_space(path),
            SpaceSource::Anchored(directory) => directory
                .available_space()
                .map_err(|error| io::Error::other(error.to_string())),
        })
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
        self.latest_available
            .store(available_bytes, Ordering::Release);
        Ok(PhysicalSpaceSample {
            generation,
            available_bytes,
        })
    }

    // Landed-but-not-wired: called once stale-sample rejection is wired.
    #[allow(dead_code)]
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
    use crate::writeback::anchored_dir::AnchoredDir;
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
        // The sampler and the direct probe read the live filesystem at
        // different instants; parallel tests writing to the same filesystem
        // legitimately move free space between the two reads. A coarse band
        // still catches a sampler probing the wrong thing (zero, wrong unit).
        let probe = fs4::available_space(&writeback_dir).unwrap();
        let drift = sample.available_bytes.abs_diff(probe);
        assert!(
            drift <= 64 * 1024 * 1024,
            "sample {} and probe {probe} disagree by {drift} bytes",
            sample.available_bytes
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
    async fn anchored_sample_survives_pathname_replacement() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let physical_root = temp.path().canonicalize().unwrap();
        let original = physical_root.join("original");
        let retained = physical_root.join("retained");
        std::fs::create_dir(&original).unwrap();
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o700)).unwrap();
        let anchored = AnchoredDir::open_or_create_absolute(&original, 0o700).unwrap();
        let sampler = PhysicalSpaceSampler::new_anchored(anchored);
        std::fs::rename(&original, &retained).unwrap();

        let sample = sampler.sample().await.unwrap();

        assert!(sample.available_bytes > 0);
        assert_eq!(sample.generation, 1);
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
