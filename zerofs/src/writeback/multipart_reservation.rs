//! Exact ownership for multipart payload staging and final mutation promotion.

use crate::coordination::admission::{AcceptedAdmission, Admission, AdmissionError};
use crate::writeback::reservation::{ReservationError, SsdAdmission, SsdReservationToken};
use crate::writeback::space_sample::PhysicalSpaceSample;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct RamMultipartPartReservation {
    final_ram_share: AcceptedAdmission,
    admission: Admission,
}

impl RamMultipartPartReservation {
    pub(crate) async fn reserve(admission: &Admission, bytes: u64) -> Result<Self, AdmissionError> {
        Ok(Self {
            final_ram_share: admission.reserve(bytes).await?.accept(),
            admission: admission.clone(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct SsdStagingToken {
    token: Option<SsdReservationToken>,
    admission: SsdAdmission,
}

impl SsdStagingToken {
    fn release_after_cleanup(mut self) {
        drop(self.token.take());
    }

    fn retain_claim(mut self) {
        if let Some(token) = self.token.take() {
            std::mem::forget(token);
        }
    }
}

impl Drop for SsdStagingToken {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            // Keep the accounting claim if a staging token is ever lost
            // without proving that its file has been removed.
            self.admission
                .poison("multipart staging ownership dropped before durable cleanup");
            std::mem::forget(token);
        }
    }
}

#[derive(Debug)]
pub(crate) struct SsdJournalShareToken {
    token: SsdReservationToken,
    admission: SsdAdmission,
}

#[derive(Debug)]
pub(crate) struct SsdMultipartPartReservation {
    staging: SsdStagingToken,
    final_journal_share: SsdJournalShareToken,
}

impl SsdMultipartPartReservation {
    pub(crate) async fn reserve(
        admission: &SsdAdmission,
        staging_bytes: u64,
        final_journal_bytes: u64,
        final_journal_operations: u64,
        sample: PhysicalSpaceSample,
    ) -> Result<Self, ReservationError> {
        let tokens = admission
            .reserve_multipart_part(
                staging_bytes,
                final_journal_bytes,
                final_journal_operations,
                sample,
            )
            .await?;
        Ok(Self {
            staging: SsdStagingToken {
                token: Some(tokens.staging),
                admission: admission.clone(),
            },
            final_journal_share: SsdJournalShareToken {
                token: tokens.final_journal,
                admission: admission.clone(),
            },
        })
    }
}

#[derive(Debug)]
pub(crate) enum MultipartReservationSet {
    Ram(Vec<RamMultipartPartReservation>),
    Ssd(Vec<SsdMultipartPartReservation>),
}

#[derive(Debug)]
pub(crate) struct RamMutationReservation {
    pub(crate) final_ram: AcceptedAdmission,
}

#[derive(Debug)]
pub(crate) struct SsdMutationReservation {
    final_journal: SsdReservationToken,
    staging_cleanup: Vec<SsdStagingToken>,
    admission: SsdAdmission,
}

impl SsdMutationReservation {
    pub(crate) fn into_owned(
        self,
        staging: PathBuf,
    ) -> (SsdReservationToken, MultipartStagingCleanup) {
        (
            self.final_journal,
            MultipartStagingCleanup::new(staging, self.staging_cleanup, self.admission),
        )
    }
}

/// Owns both the private multipart staging directory and every physical claim
/// that accounts for it. Claims are released only after the payload file and
/// directory are removed and the parent directory is durably synced.
#[derive(Debug)]
pub(crate) struct MultipartStagingCleanup {
    staging: Option<PathBuf>,
    tokens: Vec<SsdStagingToken>,
    admission: SsdAdmission,
    settled: bool,
}

pub(crate) struct CleanedMultipartStaging {
    tokens: Vec<SsdStagingToken>,
    admission: SsdAdmission,
}

impl Drop for CleanedMultipartStaging {
    fn drop(&mut self) {
        for token in std::mem::take(&mut self.tokens) {
            token.release_after_cleanup();
        }
    }
}

impl CleanedMultipartStaging {
    pub(crate) fn admission(&self) -> &SsdAdmission {
        &self.admission
    }

    pub(crate) fn retain_claims(mut self) {
        for token in std::mem::take(&mut self.tokens) {
            token.retain_claim();
        }
    }

    pub(crate) fn poison_and_retain(self, message: impl Into<String>) {
        self.admission.poison(message);
        self.retain_claims();
    }
}

impl MultipartStagingCleanup {
    fn new(staging: PathBuf, tokens: Vec<SsdStagingToken>, admission: SsdAdmission) -> Self {
        Self {
            staging: Some(staging),
            tokens,
            admission,
            settled: false,
        }
    }

    pub(crate) fn remove(mut self) -> std::io::Result<CleanedMultipartStaging> {
        self.remove_owned()
    }

    fn remove_owned(&mut self) -> std::io::Result<CleanedMultipartStaging> {
        let Some(staging) = self.staging.take() else {
            self.settled = true;
            return Ok(CleanedMultipartStaging {
                tokens: std::mem::take(&mut self.tokens),
                admission: self.admission.clone(),
            });
        };
        match remove_staged_payload(&staging) {
            Ok(()) => {
                self.settled = true;
                Ok(CleanedMultipartStaging {
                    tokens: std::mem::take(&mut self.tokens),
                    admission: self.admission.clone(),
                })
            }
            Err(error) => {
                self.settled = true;
                self.admission.poison(format!(
                    "multipart staging cleanup failed for {}: {error}",
                    staging.display()
                ));
                for token in std::mem::take(&mut self.tokens) {
                    token.retain_claim();
                }
                Err(error)
            }
        }
    }
}

impl Drop for MultipartStagingCleanup {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        match self.remove_owned() {
            Ok(cleaned) => {
                cleaned.poison_and_retain(
                    "multipart staging cleanup dropped without a fresh physical-space sample",
                );
            }
            Err(error) => {
                tracing::error!(%error, "multipart staging cleanup failed; SSD claims retained");
            }
        }
    }
}

fn remove_staged_payload(staging: &Path) -> std::io::Result<()> {
    let payload = staging.join("payload.staged");
    match fs::remove_file(&payload) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match fs::remove_dir(staging) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = staging
        .parent()
        .ok_or_else(|| std::io::Error::other("multipart staging directory has no parent"))?;
    fs::File::open(parent)?.sync_all()
}

#[derive(Debug)]
pub(crate) enum MutationReservation {
    Ram(RamMutationReservation),
    Ssd(SsdMutationReservation),
}

#[derive(Debug)]
pub(crate) struct PromotionResult {
    pub(crate) mutation: MutationReservation,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MultipartReservationError {
    #[error("multipart reservation set is empty")]
    Empty,
    #[error(transparent)]
    Ram(#[from] AdmissionError),
    #[error(transparent)]
    Ssd(#[from] ReservationError),
}

pub(crate) fn promote_multipart(
    parts: MultipartReservationSet,
) -> Result<PromotionResult, MultipartReservationError> {
    let mutation = match parts {
        MultipartReservationSet::Ram(parts) => {
            let admission = parts
                .first()
                .map(|part| part.admission.clone())
                .ok_or(MultipartReservationError::Empty)?;
            let shares = parts.into_iter().map(|part| part.final_ram_share).collect();
            MutationReservation::Ram(RamMutationReservation {
                final_ram: admission.merge_accepted(shares)?,
            })
        }
        MultipartReservationSet::Ssd(parts) => {
            let admission = parts
                .first()
                .map(|part| part.final_journal_share.admission.clone())
                .ok_or(MultipartReservationError::Empty)?;
            let mut staging_cleanup = Vec::with_capacity(parts.len());
            let mut journal_shares = Vec::with_capacity(parts.len());
            for part in parts {
                staging_cleanup.push(part.staging);
                journal_shares.push(part.final_journal_share);
            }
            MutationReservation::Ssd(SsdMutationReservation {
                final_journal: admission.merge_multipart_tokens(
                    journal_shares
                        .into_iter()
                        .map(|share| share.token)
                        .collect(),
                )?,
                staging_cleanup,
                admission,
            })
        }
    };
    Ok(PromotionResult { mutation })
}

pub(crate) fn remove_aborted_ssd_multipart(
    staging: PathBuf,
    parts: Vec<SsdMultipartPartReservation>,
) -> std::io::Result<Option<CleanedMultipartStaging>> {
    let Some(admission) = parts
        .first()
        .map(|part| part.final_journal_share.admission.clone())
    else {
        remove_staged_payload(&staging)?;
        return Ok(None);
    };
    let mut staging_tokens = Vec::with_capacity(parts.len());
    for part in parts {
        staging_tokens.push(part.staging);
        drop(part.final_journal_share);
    }
    MultipartStagingCleanup::new(staging, staging_tokens, admission)
        .remove()
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writeback::space_sample::PhysicalSpaceSample;
    use std::sync::Arc;
    use std::time::Duration;

    fn sample(available_bytes: u64) -> PhysicalSpaceSample {
        PhysicalSpaceSample {
            generation: 1,
            available_bytes,
        }
    }

    fn ssd() -> SsdAdmission {
        SsdAdmission::new(180, 100, 100, 80, 10).unwrap()
    }

    async fn ram_part(admission: &Admission, bytes: u64) -> RamMultipartPartReservation {
        RamMultipartPartReservation::reserve(admission, bytes)
            .await
            .unwrap()
    }

    async fn ssd_part(
        admission: &SsdAdmission,
        staging_bytes: u64,
        journal_bytes: u64,
        journal_operations: u64,
    ) -> SsdMultipartPartReservation {
        SsdMultipartPartReservation::reserve(
            admission,
            staging_bytes,
            journal_bytes,
            journal_operations,
            sample(1_000),
        )
        .await
        .unwrap()
    }

    fn release_cleaned(cleaned: CleanedMultipartStaging) {
        cleaned.admission().observe_sample(sample(1_000)).unwrap();
        drop(cleaned);
    }

    fn cleanup_aborted_ssd_multipart(staging: PathBuf, parts: Vec<SsdMultipartPartReservation>) {
        if let Some(cleaned) = remove_aborted_ssd_multipart(staging, parts).unwrap() {
            release_cleaned(cleaned);
        }
    }

    #[tokio::test]
    async fn parallel_memory_parts_share_global_cap() {
        let admission = Admission::new(10);
        let held = ram_part(&admission, 7).await;
        let blocked = tokio::spawn({
            let admission = admission.clone();
            async move { ram_part(&admission, 4).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        drop(held);
        assert_eq!(blocked.await.unwrap().final_ram_share.bytes(), 4);
    }

    #[tokio::test]
    async fn ram_promotion_succeeds_with_zero_spare_headroom() {
        let admission = Admission::new(10);
        let parts = vec![ram_part(&admission, 6).await, ram_part(&admission, 4).await];
        let promoted = promote_multipart(MultipartReservationSet::Ram(parts)).unwrap();
        assert_eq!(admission.used_bytes(), 10);
        assert_eq!(admission.used_operations(), 1);
        let MutationReservation::Ram(mutation) = promoted.mutation else {
            panic!("expected RAM promotion")
        };
        assert_eq!(mutation.final_ram.bytes(), 10);
        drop(mutation);
        assert_eq!(admission.used_bytes(), 0);
    }

    #[tokio::test]
    async fn multipart_promotion_is_atomic_under_capacity_pressure() {
        let admission = Admission::new(10);
        let parts = vec![ram_part(&admission, 5).await, ram_part(&admission, 5).await];
        let competing = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(1).await.unwrap().accept() }
        });
        tokio::task::yield_now().await;
        assert!(!competing.is_finished());
        let promoted = promote_multipart(MultipartReservationSet::Ram(parts)).unwrap();
        assert_eq!(admission.used_bytes(), 10);
        assert_eq!(admission.used_operations(), 1);
        assert!(
            !competing.is_finished(),
            "promotion released capacity to a competing mutation"
        );
        drop(promoted);
        drop(competing.await.unwrap());
        assert_eq!(admission.used_bytes(), 0);
    }

    #[tokio::test]
    async fn ram_promotion_rejects_mixed_owners_without_mutating_either_gate() {
        let first = Admission::new(10);
        let second = Admission::new(10);
        let result = promote_multipart(MultipartReservationSet::Ram(vec![
            ram_part(&first, 4).await,
            ram_part(&second, 6).await,
        ]));
        assert!(matches!(
            result,
            Err(MultipartReservationError::Ram(
                AdmissionError::InvalidConfiguration(_)
            ))
        ));
        assert_eq!(first.used_bytes(), 0);
        assert_eq!(second.used_bytes(), 0);
    }

    #[tokio::test]
    async fn parallel_ssd_parts_share_physical_free_space_reserve_atomically() {
        let admission = Arc::new(SsdAdmission::new(200, 100, 95, 80, 10).unwrap());
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let (granted, mut grants) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let admission = Arc::clone(&admission);
            let start = Arc::clone(&start);
            let granted = granted.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let reservation =
                    SsdMultipartPartReservation::reserve(&admission, 41, 9, 1, sample(100)).await;
                granted.send(reservation).unwrap();
            }));
        }
        drop(granted);
        start.wait().await;

        let first = tokio::time::timeout(Duration::from_secs(1), grants.recv())
            .await
            .expect("neither physical reservation was admitted")
            .expect("physical reservation workers exited")
            .unwrap();
        let unexpected_second = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if admission.has_min_free_waiters() {
                    return None;
                }
                match grants.try_recv() {
                    Ok(reservation) => return Some(reservation),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        tokio::task::yield_now().await;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        panic!("physical reservation workers exited")
                    }
                }
            }
        })
        .await
        .expect("the competing physical reservation neither queued nor completed");
        assert!(
            unexpected_second.is_none(),
            "parallel parts both crossed one-part physical headroom"
        );
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.used_ssd_bytes, 50);
        assert_eq!(snapshot.used_operations, 2);
        assert_eq!(snapshot.outstanding_physical_claims, 50);
        assert_eq!(snapshot.available_bytes, 100);

        let root = tempfile::tempdir().unwrap();
        let first_staging = root.path().join("first");
        fs::create_dir(&first_staging).unwrap();
        fs::write(first_staging.join("payload.staged"), b"first").unwrap();
        cleanup_aborted_ssd_multipart(first_staging, vec![first]);

        let second = tokio::time::timeout(Duration::from_secs(1), grants.recv())
            .await
            .expect("cleanup did not release the competing physical reservation")
            .expect("physical reservation workers exited")
            .unwrap();
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.used_ssd_bytes, 50);
        assert_eq!(snapshot.used_operations, 2);
        assert_eq!(snapshot.outstanding_physical_claims, 50);
        let second_staging = root.path().join("second");
        fs::create_dir(&second_staging).unwrap();
        fs::write(second_staging.join("payload.staged"), b"second").unwrap();
        cleanup_aborted_ssd_multipart(second_staging, vec![second]);
        for task in tasks {
            task.await.unwrap();
        }
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.used_ssd_bytes, 0);
        assert_eq!(snapshot.used_operations, 0);
        assert_eq!(snapshot.outstanding_physical_claims, 0);
    }

    #[tokio::test]
    async fn ssd_promotion_succeeds_with_zero_spare_headroom() {
        let admission = ssd();
        let parts = vec![
            ssd_part(&admission, 40, 60, 1).await,
            ssd_part(&admission, 40, 40, 0).await,
        ];
        let promoted = promote_multipart(MultipartReservationSet::Ssd(parts)).unwrap();
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.used_ssd_bytes, 180);
        assert_eq!(snapshot.outstanding_physical_claims, 180);
        assert_eq!(snapshot.used_operations, 3);
        let MutationReservation::Ssd(mutation) = promoted.mutation else {
            panic!("expected SSD promotion")
        };
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("multipart");
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("payload.staged"), b"staged").unwrap();
        let (final_journal, cleanup) = mutation.into_owned(staging.clone());
        assert!(staging.exists());
        release_cleaned(cleanup.remove().unwrap());
        assert!(!staging.exists());
        let after_cleanup = admission.snapshot();
        assert_eq!(after_cleanup.used_ssd_bytes, 100);
        assert_eq!(after_cleanup.outstanding_physical_claims, 100);
        assert_eq!(after_cleanup.used_operations, 1);
        drop(final_journal);
        assert_eq!(admission.used_bytes(), 0);
        assert_eq!(admission.outstanding_physical_claims(), 0);
    }

    #[tokio::test]
    async fn ssd_promotion_rejects_mixed_owners_without_recharging() {
        let first = ssd();
        let second = ssd();
        let first_part = ssd_part(&first, 4, 6, 1).await;
        let second_part = ssd_part(&second, 4, 6, 1).await;
        let SsdMultipartPartReservation {
            staging: first_staging,
            final_journal_share: first_journal,
        } = first_part;
        let SsdMultipartPartReservation {
            staging: second_staging,
            final_journal_share: second_journal,
        } = second_part;
        let result = first.merge_multipart_tokens(vec![first_journal.token, second_journal.token]);
        assert!(matches!(
            result,
            Err(ReservationError::InvalidConfiguration(_))
        ));

        let root = tempfile::tempdir().unwrap();
        for (name, staging_token, admission) in [
            ("first", first_staging, first.clone()),
            ("second", second_staging, second.clone()),
        ] {
            let staging = root.path().join(name);
            fs::create_dir(&staging).unwrap();
            fs::write(staging.join("payload.staged"), b"part").unwrap();
            release_cleaned(
                MultipartStagingCleanup::new(staging, vec![staging_token], admission)
                    .remove()
                    .unwrap(),
            );
        }
        assert_eq!(first.used_bytes(), 0);
        assert_eq!(second.used_bytes(), 0);
    }

    #[tokio::test]
    async fn multipart_abort_releases_all_part_reservations() {
        let ram = Admission::new(10);
        let ram_parts = MultipartReservationSet::Ram(vec![ram_part(&ram, 4).await]);
        drop(ram_parts);
        assert_eq!(ram.used_bytes(), 0);

        let ssd = ssd();
        let ssd_part = ssd_part(&ssd, 4, 6, 1).await;
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("aborted");
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("payload.staged"), b"part").unwrap();
        cleanup_aborted_ssd_multipart(staging, vec![ssd_part]);
        assert_eq!(ssd.used_bytes(), 0);
        assert_eq!(ssd.outstanding_physical_claims(), 0);
    }

    #[tokio::test]
    async fn multipart_cleanup_failure_poisons_and_retains_staging_claim() {
        let admission = ssd();
        let promoted = promote_multipart(MultipartReservationSet::Ssd(vec![
            ssd_part(&admission, 4, 6, 1).await,
        ]))
        .unwrap();
        let MutationReservation::Ssd(mutation) = promoted.mutation else {
            panic!("expected SSD promotion")
        };
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("uncleanable");
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("payload.staged"), b"part").unwrap();
        fs::write(staging.join("unexpected"), b"keep").unwrap();
        let (final_journal, cleanup) = mutation.into_owned(staging.clone());

        assert!(cleanup.remove().is_err());
        drop(final_journal);
        assert_eq!(admission.used_bytes(), 4);
        assert_eq!(admission.outstanding_physical_claims(), 4);
        assert!(matches!(
            admission
                .reserve(
                    crate::writeback::reservation::SsdReservationRequest {
                        ssd_reservation_bytes: 1,
                        physical_reservation_bytes: 1,
                        operations: 1,
                    },
                    sample(1_000),
                )
                .await,
            Err(ReservationError::Poisoned(_))
        ));
        assert!(staging.join("unexpected").exists());
    }
}
