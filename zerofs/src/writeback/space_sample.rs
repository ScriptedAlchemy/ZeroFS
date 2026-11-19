//! Sole owner of physical-space generations for the writeback directory.
//!
//! Admission, journal transition, remote cleanup, and refresher tasks consume
//! [`PhysicalSpaceSample`] values from [`PhysicalSpaceSampler::sample`]. They
//! do not allocate or construct generations themselves.

use crate::writeback::anchored_dir::AnchoredDir;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    probe_gate: Arc<tokio::sync::Mutex<()>>,
    next_generation: Arc<AtomicU64>,
    latest: Arc<Mutex<Option<PhysicalSpaceSample>>>,
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
            probe_gate: Arc::new(tokio::sync::Mutex::new(())),
            next_generation: Arc::new(AtomicU64::new(1)),
            latest: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn new_anchored(writeback_dir: AnchoredDir) -> Self {
        Self {
            writeback_dir: writeback_dir.display().to_path_buf(),
            source: SpaceSource::Anchored(writeback_dir),
            probe_gate: Arc::new(tokio::sync::Mutex::new(())),
            next_generation: Arc::new(AtomicU64::new(1)),
            latest: Arc::new(Mutex::new(None)),
        }
    }

    // Landed-but-not-wired accessor.
    #[allow(dead_code)]
    pub(crate) fn writeback_dir(&self) -> &Path {
        &self.writeback_dir
    }

    pub(crate) fn latest_generation(&self) -> u64 {
        self.latest_sample().map_or(0, |sample| sample.generation)
    }

    /// Last successful probe, if any. Never invents a generation.
    pub(crate) fn latest_sample(&self) -> Option<PhysicalSpaceSample> {
        *self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Probe the canonical writeback directory and publish the next generation.
    ///
    /// Probe and publication are serialized on the blocking worker. Allocating
    /// a generation after awaiting an independent probe can relabel an older
    /// free-space observation as newer when async continuations resume out of
    /// order. Allocating it before dispatch has the same problem when workers
    /// start out of order. Keep the observation and its generation together.
    pub(crate) async fn sample(&self) -> Result<PhysicalSpaceSample, SpaceSampleError> {
        let source = self.source.clone();
        self.sample_with(move || match source {
            SpaceSource::Path(path) => fs4::available_space(path),
            SpaceSource::Anchored(directory) => directory
                .available_space()
                .map_err(|error| io::Error::other(error.to_string())),
        })
        .await
    }

    async fn sample_with(
        &self,
        probe: impl FnOnce() -> io::Result<u64> + Send + 'static,
    ) -> Result<PhysicalSpaceSample, SpaceSampleError> {
        let guard = Arc::clone(&self.probe_gate).lock_owned().await;
        let next_generation = Arc::clone(&self.next_generation);
        let latest = Arc::clone(&self.latest);
        tokio::task::spawn_blocking(move || {
            // Cancellation must not admit another probe while this native
            // observation can still publish. Snapshot reads use a separate
            // lock, so a slow filesystem does not block latest_sample().
            let _guard = guard;
            let available_bytes = probe()?;
            let sample = PhysicalSpaceSample {
                generation: next_generation.fetch_add(1, Ordering::AcqRel),
                available_bytes,
            };
            Self::publish_sample(&latest, sample);
            Ok(sample)
        })
        .await
        .map_err(|error| SpaceSampleError::Probe {
            path: self.writeback_dir.clone(),
            source: io::Error::other(format!("space probe task failed: {error}")),
        })?
        .map_err(|source| SpaceSampleError::Probe {
            path: self.writeback_dir.clone(),
            source,
        })
    }

    fn publish_sample(latest: &Mutex<Option<PhysicalSpaceSample>>, sample: PhysicalSpaceSample) {
        // Keep the generation and its byte count together: separate atomics
        // could pair a fresh generation with an older, larger free-space value
        // when latest_sample() reads the published snapshot.
        let mut latest = latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if latest.is_none_or(|current| sample.generation > current.generation) {
            *latest = Some(sample);
        }
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

#[cfg(test)]
mod tests {
    use super::{PhysicalSpaceSample, PhysicalSpaceSampler, SpaceSampleError};
    use crate::writeback::anchored_dir::AnchoredDir;
    use crate::writeback::reservation::{ReservationError, SsdAdmission, SsdReservationRequest};
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

    #[test]
    fn late_sample_cannot_relabel_stale_free_space_as_the_latest_generation() {
        let temp = tempfile::tempdir().unwrap();
        let sampler = PhysicalSpaceSampler::new(temp.path());
        // The older probe acquired generation 1, then its thread paused before
        // publication while the next caller observed the disk filling up.
        PhysicalSpaceSampler::publish_sample(
            &sampler.latest,
            PhysicalSpaceSample {
                generation: 2,
                available_bytes: 50,
            },
        );
        PhysicalSpaceSampler::publish_sample(
            &sampler.latest,
            PhysicalSpaceSample {
                generation: 1,
                available_bytes: 1_000,
            },
        );
        // A stale admission retries with latest_sample(). Returning generation
        // 2 with the older free-byte count would bypass its freshness check and
        // allow a claim that the latest observation cannot accommodate.
        let latest = sampler.latest_sample().unwrap();
        let admission = SsdAdmission::new(1_000, 8, 90, 70, 0).unwrap();
        let result = admission.try_reserve(
            SsdReservationRequest {
                ssd_reservation_bytes: 200,
                physical_reservation_bytes: 200,
                operations: 1,
            },
            latest,
        );
        assert!(
            matches!(
                result,
                Err(ReservationError::TooLargePhysical { available: 50, .. })
            ),
            "the newest sample must not admit more than its observed free space: {latest:?}"
        );
        assert_eq!(latest.generation, 2);
        assert_eq!(latest.available_bytes, 50);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_earlier_probe_cannot_replace_newer_physical_space() {
        assert_probe_ordering(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_keeps_its_native_probe_serialized() {
        assert_probe_ordering(true).await;
    }

    async fn assert_probe_ordering(cancel_first: bool) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;

        let temp = tempfile::tempdir().unwrap();
        let sampler = Arc::new(PhysicalSpaceSampler::new(temp.path()));
        let disk_free = Arc::new(AtomicU64::new(1_000));
        let (observed, first_observation) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let first = tokio::spawn({
            let sampler = Arc::clone(&sampler);
            let disk_free = Arc::clone(&disk_free);
            async move {
                sampler
                    .sample_with(move || {
                        let available = disk_free.load(Ordering::SeqCst);
                        observed.send(()).unwrap();
                        paused.recv().unwrap();
                        Ok(available)
                    })
                    .await
                    .unwrap()
            }
        });
        first_observation.await.unwrap();
        let first = if cancel_first {
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            None
        } else {
            Some(first)
        };
        disk_free.store(50, Ordering::SeqCst);
        let mut second = tokio::spawn({
            let sampler = Arc::clone(&sampler);
            let disk_free = Arc::clone(&disk_free);
            async move {
                sampler
                    .sample_with(move || Ok(disk_free.load(Ordering::SeqCst)))
                    .await
                    .unwrap()
            }
        });
        // Give the second probe an opportunity to finish first. It must wait
        // for the older observation's publication; with independent probes it
        // publishes 50 first and the stalled probe then relabels 1,000 as new.
        let early_second = tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .ok();
        resume.send(()).unwrap();
        let first = match first {
            Some(first) => Some(first.await.unwrap()),
            None => None,
        };
        let second = match early_second {
            Some(result) => result.unwrap(),
            None => second.await.unwrap(),
        };
        if let Some(first) = first {
            assert!(first.generation < second.generation);
        }
        assert_eq!(sampler.latest_sample(), Some(second));
        assert_eq!(second.available_bytes, 50);

        let admission = SsdAdmission::new(1_000, 8, 90, 70, 0).unwrap();
        let request = SsdReservationRequest {
            ssd_reservation_bytes: 200,
            physical_reservation_bytes: 200,
            operations: 1,
        };
        assert!(matches!(
            admission.try_reserve(request, sampler.latest_sample().unwrap()),
            Err(ReservationError::TooLargePhysical { available: 50, .. })
        ));
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
