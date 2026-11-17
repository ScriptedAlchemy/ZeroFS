//! Scenarios added after the independent review. Actions bind to production
//! entry points; assertions observe files, filesystem calls and recovery.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum Action {
    Enable(Row),
    RestartWithoutTable,
    RestartWithDifferentSet,
    Restart,
    DisableOffline,
    ReenableFreshEpoch,
    InstallStaleReplica,
    InstallAuthenticatedFork,
    PauseRemotePastDeadline,
    ResumeRemote,
    CloseAdmission,
    ReopenAdmission,
    LatchReadOnly,
    InjectReadFault,
    EvictReadCaches,
    CommitIfChanged,
    PromoteOffline,
    ReconcileCheckpointIntents,
    SweepRetainedUnion,
}

#[derive(Default)]
pub(super) struct ReviewObservation {
    receipts: Vec<SurvivalReceipt>,
    errors: Vec<SurvivalFailure>,
    statuses: Vec<SurvivalStatus>,
    api: Vec<Result<(), FsError>>,
    before_files: BTreeMap<PathBuf, [u8; 32]>,
    after_files: BTreeMap<PathBuf, [u8; 32]>,
    before_catalog: Option<CatalogImage>,
    after_catalog: Option<CatalogImage>,
    selected: Option<[u8; 32]>,
    elapsed: Duration,
    owned_checkpoints: Vec<slatedb::Checkpoint>,
    retained: Vec<(PathBuf, [u8; 32])>,
    dead: Option<PathBuf>,
    counts: Vec<usize>,
    lock_wait: Option<Duration>,
    damaged_record: Option<(PathBuf, [u8; 32])>,
    byte_counts: Vec<u64>,
    terminal_dirs: Vec<PathBuf>,
    prior_roots: Vec<PathBuf>,
    active_roots: Vec<PathBuf>,
}

async fn action_error(
    driver: &dyn Survival,
    live: &mut Live,
    action: Action,
) -> Result<SurvivalFailure> {
    let error = tokio::time::timeout(Duration::from_secs(5), driver.review_action(live, action))
        .await?
        .expect_err("an unsafe startup or transition succeeded");
    error
        .downcast_ref::<SurvivalFailure>()
        .copied()
        .context("the operation returned an unrelated error")
}

async fn recovery_error(driver: &dyn Survival, live: &Live) -> Result<SurvivalFailure> {
    let error = match driver.recover(&RecoverySources::from_live(live)).await {
        Err(error) => error,
        Ok(_) => anyhow::bail!("recovery accepted unsafe source evidence"),
    };
    error
        .downcast_ref::<SurvivalFailure>()
        .copied()
        .context("recovery returned an unrelated error")
}

fn survival_tree(live: &Live) -> Result<BTreeMap<PathBuf, [u8; 32]>> {
    let mut files = BTreeMap::new();
    for (index, root) in live.replica_roots().iter().enumerate() {
        for (path, digest) in tree_hashes(&root.join(".zerofs-survival/v1"))? {
            files.insert(PathBuf::from(index.to_string()).join(path), digest);
        }
    }
    Ok(files)
}

fn flip_byte(path: &Path) -> Result<()> {
    let mut bytes = std::fs::read(path)?;
    ensure!(!bytes.is_empty(), "the corruption target must be nonempty");
    bytes[0] ^= 1;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn membership_epoch(receipt: &SurvivalReceipt) -> Result<&std::ffi::OsStr> {
    let mut components = receipt.relative_record.components();
    ensure!(
        matches!(components.next(), Some(Component::Normal(root)) if root == "commits"),
        "receipt must name a commit"
    );
    let Some(Component::Normal(epoch)) = components.next() else {
        anyhow::bail!("receipt must name a membership epoch");
    };
    ensure!(
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none(),
        "receipt must name commits/<epoch>/<sequence>.json"
    );
    Ok(epoch)
}

fn verify_fresh_activation(
    prior: &SurvivalReceipt,
    fresh: &SurvivalReceipt,
    prior_roots: &[PathBuf],
    active_roots: &[PathBuf],
) -> Result<()> {
    ensure!(
        membership_epoch(prior)? != membership_epoch(fresh)?,
        "activation reused the old membership epoch"
    );
    ensure!(
        prior_roots.len() == 2 && active_roots.len() == 2,
        "activation must identify both old and new replica roots"
    );
    let prior_roots = prior_roots
        .iter()
        .map(|root| match root.canonicalize() {
            Ok(path) => Ok(path),
            // Authority loss intentionally removes one of the old roots.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(root.clone()),
            Err(error) => Err(error),
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let active_roots = active_roots
        .iter()
        .map(|root| root.canonicalize())
        .collect::<std::io::Result<Vec<_>>>()?;
    for (index, active) in active_roots.iter().enumerate() {
        for previous in prior_roots.iter().chain(&active_roots[..index]) {
            ensure!(
                !active.starts_with(previous) && !previous.starts_with(active),
                "activation reused or overlapped a replica root"
            );
        }
    }
    Ok(())
}

fn verify_restarted_replica_roots(
    promoted_roots: &[PathBuf],
    restarted_roots: &[PathBuf],
) -> Result<()> {
    let restarted_roots = restarted_roots
        .iter()
        .map(|root| root.canonicalize())
        .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(
        promoted_roots.len() == 2 && promoted_roots == restarted_roots,
        "restart changed the promoted replica roots"
    );
    Ok(())
}

async fn probe_read(live: &Live, range: &ReadRange) -> Result<Result<(), FsError>> {
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        live.fs().read_file(
            &AuthContext::default(),
            range.inode,
            range.offset,
            range.length.max(1),
        ),
    )
    .await
    .context("a read blocked on a deferred catalog observation")?;
    match read {
        Ok((bytes, _)) => {
            ensure!(
                <[u8; 32]>::from(Sha256::digest(bytes)) == range.sha256,
                "a successful read returned corrupt bytes"
            );
            Ok(Ok(()))
        }
        Err(error) => Ok(Err(error)),
    }
}

pub(super) async fn exercise(
    row: Row,
    live: &mut Live,
    driver: &dyn Survival,
) -> Result<Observation> {
    // A no-op action alone cannot satisfy any row. Every scenario below then
    // examines persisted evidence or calls the corresponding public operation.
    driver.review_action(live, Action::Enable(row)).await?;
    let prior = driver.commit(live).await?;
    verify_replica_record(live, &prior, RecordCoverage::AllRequired)?;
    let mut result = ReviewObservation {
        receipts: vec![prior.clone()],
        prior_roots: live
            .replica_roots()
            .into_iter()
            .map(|root| root.canonicalize())
            .collect::<std::io::Result<_>>()?,
        ..Default::default()
    };
    let mut observed = Observation::default();
    match row {
        Row::DurableEnablement => {
            result
                .errors
                .push(action_error(driver, live, Action::RestartWithoutTable).await?);
            result
                .errors
                .push(action_error(driver, live, Action::RestartWithDifferentSet).await?);
            // A failed startup must not have cleaned a retained generation.
            verify_replica_record(live, &prior, RecordCoverage::AllRequired)?;
            let retention = overwrite_and_reclaim(live).await?;
            payload_retention_oracle(live, &retention)?;
            result
                .retained
                .push((retention.old_object, retention.old_object_digest));
        }
        Row::TerminalDisable => {
            driver.review_action(live, Action::DisableOffline).await?;
            live.close_writer().await;
            result.errors.push(recovery_error(driver, live).await?);
            for root in live.replica_roots() {
                let terminal = root.join(".zerofs-survival/v1/terminal");
                result.terminal_dirs.push(terminal.clone());
                ensure!(
                    !tree_hashes(&terminal)?.is_empty(),
                    "offline disable omitted a terminal record"
                );
            }
            driver
                .review_action(live, Action::ReenableFreshEpoch)
                .await?;
            let fresh = driver.commit(live).await?;
            result.active_roots = live
                .replica_roots()
                .into_iter()
                .map(|root| root.canonicalize())
                .collect::<std::io::Result<_>>()?;
            result.receipts.push(fresh);
        }
        Row::StaleRestart => {
            overwrite_first_file(live, 45).await?;
            result.receipts.push(driver.commit(live).await?);
            driver
                .review_action(live, Action::InstallStaleReplica)
                .await?;
            result
                .errors
                .push(action_error(driver, live, Action::Restart).await?);
        }
        Row::CorruptDependency | Row::CorruptRecord => {
            live.close_writer().await;
            lose_device(live, Device::Authority)?;
            let root = live.replica_roots()[1].join(".zerofs-survival/v1");
            let target = if row == Row::CorruptRecord {
                root.join(&prior.relative_record)
            } else {
                let objects = tree_hashes(&root.join("objects"))?;
                root.join("objects").join(
                    objects
                        .keys()
                        .next()
                        .context("no copied dependency to corrupt")?,
                )
            };
            flip_byte(&target)?;
            if row == Row::CorruptRecord {
                result.damaged_record = Some((target, prior.digest));
            }
            result.before_files = survival_tree(live)?;
            result.errors.push(recovery_error(driver, live).await?);
            result.after_files = survival_tree(live)?;
        }
        Row::HighestGeneration => {
            overwrite_first_file(live, 46).await?;
            let latest = driver.commit(live).await?;
            verify_replica_record(live, &latest, RecordCoverage::AllRequired)?;
            verify_replica_record(live, &prior, RecordCoverage::AllRequired)?;
            live.expected =
                State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
            live.close_writer().await;
            lose_device(live, Device::Authority)?;
            remove_active_originals(live, &mut observed)?;
            collect_recovery(live, driver, &[(&latest, &live.reads)], &mut observed).await?;
            result.selected = observed.recovered_digest;
            result.receipts.push(latest);
        }
        Row::ConflictingForks => {
            driver
                .review_action(live, Action::InstallAuthenticatedFork)
                .await?;
            live.close_writer().await;
            result.before_files = survival_tree(live)?;
            result.errors.push(recovery_error(driver, live).await?);
            result.after_files = survival_tree(live)?;
        }
        Row::PartialFanoutRestart => {
            overwrite_first_file(live, 47).await?;
            let cut = Cut {
                point: CommitPoint::CommitDirectorySynced,
                side: Side::After,
                replica: "authority",
                lost: Device::Journal,
            };
            let pause = driver
                .pause_commit(live, cut)
                .await?
                .ok_or_else(|| Missing("the partial-fanout commit hook is absent".to_owned()))?;
            pause
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("commit start closed"))?;
            let event = tokio::time::timeout(Duration::from_secs(5), pause.entered).await??;
            ensure!(
                event.cut == cut && !pause.task.is_finished(),
                "no actual partial fanout was reached"
            );
            let candidate = pause
                .candidate
                .lock()
                .unwrap()
                .clone()
                .context("partial fanout has no record")?;
            let record = |root: &Path| {
                root.join(".zerofs-survival/v1")
                    .join(&candidate.relative_record)
            };
            ensure!(
                record(&live.replica_roots()[0]).is_file()
                    && !record(&live.replica_roots()[1]).exists(),
                "fanout was not partial"
            );
            pause.task.abort();
            let _ = pause.task.await;
            drop(pause.release);
            driver.crash_writer(live).await?;
            driver.review_action(live, Action::Restart).await?;
            verify_replica_record(live, &candidate, RecordCoverage::AllRequired)?;
            let first = survival_tree(live)?;
            driver.review_action(live, Action::Restart).await?;
            result.before_files = first;
            result.after_files = survival_tree(live)?;
            result.statuses.push(driver.status(live).await?);
            result.receipts.push(candidate);
            result.api.push(probe_create(live, b"after-restart").await?);
        }
        Row::BacklogRetry => {
            live.attach_journal().await?;
            driver
                .review_action(live, Action::PauseRemotePastDeadline)
                .await?;
            let file = live.reads[0].inode;
            live.fs()
                .write(&AuthContext::default(), file, 0, &payload(48))
                .await?;
            let local_receipt = live
                .fs()
                .flush_coordinator
                .durable_through(
                    live.fs().capture_mutation_cutoff(),
                    DurabilityTarget::LocalSsd,
                )
                .await?;
            ensure!(
                local_receipt.target == DurabilityTarget::LocalSsd
                    && matches!(
                        local_receipt.object_coverage,
                        ObjectCoverage::Writeback { .. }
                    ),
                "the backlog has no typed SSD durability receipt"
            );
            let backlog = live
                .writeback
                .as_ref()
                .context("no real backlog journal")?
                .status()?;
            ensure!(
                backlog.local_seq > backlog.remote_seq,
                "the backlog must be real"
            );
            result.statuses.push(driver.status(live).await?);
            let started = std::time::Instant::now();
            let mut committing = Box::pin(driver.commit(live));
            // Poll the real attempt into its blocked pre-drain. Preserve the
            // future when this short observation window ends, then try a create
            // while it is still pending. An immediate skipped attempt is legal.
            let early = tokio::time::timeout(Duration::from_millis(25), &mut committing).await;
            result
                .api
                .push(probe_create(live, b"while-backlog-pending").await?);
            let commit = match early {
                Ok(result) => result,
                Err(_) => tokio::time::timeout(Duration::from_secs(185), &mut committing).await?,
            };
            drop(committing);
            result.elapsed = started.elapsed();
            let error = commit.expect_err("a blocked backend was incorrectly committed");
            result.errors.push(
                *error
                    .downcast_ref::<SurvivalFailure>()
                    .context("unrelated backlog error")?,
            );
            result.statuses.push(driver.status(live).await?);
            result
                .api
                .push(probe_create(live, b"backlog-admission").await?);
            driver.review_action(live, Action::ResumeRemote).await?;
            let retry = driver.commit(live).await?;
            verify_replica_record(live, &retry, RecordCoverage::AllRequired)?;
            result.receipts.push(retry);
        }
        Row::ReadFaultClosed | Row::ReadFaultReadOnly => {
            driver
                .review_action(
                    live,
                    if row == Row::ReadFaultClosed {
                        Action::CloseAdmission
                    } else {
                        Action::LatchReadOnly
                    },
                )
                .await?;
            result.before_catalog = Some(survival_test_support::capture(live.pool()).await?);
            driver.review_action(live, Action::InjectReadFault).await?;
            driver.review_action(live, Action::EvictReadCaches).await?;
            let bad = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-a"))
                .context("faulted range")?
                .clone();
            let safe = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-b"))
                .context("safe range")?
                .clone();
            result.api.push(probe_read(live, &bad).await?);
            result.api.push(probe_read(live, &safe).await?);
            result.api.push(probe_create(live, b"read-gate").await?);
            result.after_catalog = Some(survival_test_support::capture(live.pool()).await?);
            if row == Row::ReadFaultClosed {
                driver.review_action(live, Action::ReopenAdmission).await?;
                let flushed = survival_test_support::capture(live.pool()).await?;
                ensure!(
                    Some(flushed) != result.before_catalog,
                    "the deferred observation was never flushed"
                );
            }
        }
        Row::IdleAndBounds => {
            let root = live.replica_roots()[1].join(".zerofs-survival/v1");
            let idle_before = tree_hashes(&root)?;
            for _ in 0..8 {
                driver.review_action(live, Action::CommitIfChanged).await?;
            }
            ensure!(
                idle_before == tree_hashes(&root)?,
                "idle scheduling wrote new generation bytes"
            );
            for seed in 50..82 {
                overwrite_first_file(live, seed).await?;
                let receipt = driver.commit(live).await?;
                verify_replica_record(live, &receipt, RecordCoverage::AllRequired)?;
                result.receipts.push(receipt);
                result.statuses.push(driver.status(live).await?);
                result
                    .owned_checkpoints
                    .extend(driver.owned_checkpoints(live).await?);
                driver
                    .review_action(live, Action::SweepRetainedUnion)
                    .await?;
                // Count records and catalog objects after settlement. Churn
                // below changes namespace data, not the live catalog fixture.
                let files = tree_hashes(&root)?;
                let count = files
                    .keys()
                    .filter(|p| {
                        ["commits", "retirements", "intents", "catalog"]
                            .iter()
                            .any(|part| p.starts_with(part))
                    })
                    .count();
                let bytes = files
                    .keys()
                    .filter(|p| {
                        ["commits", "retirements", "intents", "catalog"]
                            .iter()
                            .any(|part| p.starts_with(part))
                    })
                    .map(|p| std::fs::metadata(root.join(p)).map(|m| m.len()))
                    .collect::<std::io::Result<Vec<_>>>()?
                    .into_iter()
                    .sum();
                result.byte_counts.push(bytes);
                result.counts.push(count);
            }
            result.after_files = tree_hashes(&root)?;
        }
        Row::StatusLag => {
            live.attach_journal().await?;
            result.statuses.push(driver.status(live).await?);
            overwrite_first_file(live, 85).await?;
            driver
                .review_action(live, Action::PauseRemotePastDeadline)
                .await?;
            // Exercise an actual failed attempt before sampling growing lag.
            let error = driver
                .commit(live)
                .await
                .expect_err("blocked attempt unexpectedly succeeded");
            result.errors.push(
                *error
                    .downcast_ref::<SurvivalFailure>()
                    .context("unrelated status error")?,
            );
            result.statuses.push(driver.status(live).await?);
            tokio::time::sleep(Duration::from_millis(30)).await;
            result.statuses.push(driver.status(live).await?);
            driver.review_action(live, Action::ResumeRemote).await?;
        }
        Row::PromoteAuthority => {
            live.close_writer().await;
            lose_device(live, Device::Authority)?;
            result.before_files = survival_tree(live)?;
            let original_roots = live.replica_roots();
            // Delete the config too. Recovery inputs must come from the replica.
            for config in ["fixture.toml", "proposed.toml"] {
                let path = live.fixture.root.path().join(config);
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
            }
            // The binding takes only surviving identity plus PASSWORD, builds
            // new authority roots and updates this owned fixture's handles.
            driver.review_action(live, Action::PromoteOffline).await?;
            result.active_roots = live
                .replica_roots()
                .into_iter()
                .map(|root| root.canonicalize())
                .collect::<std::io::Result<_>>()?;
            let promoted = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
            live.expected.verify(&promoted, false)?;
            result.api.push(probe_create(live, b"promoted").await?);
            let safe = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-b"))
                .context("promoted safe range")?
                .clone();
            live.fs()
                .write(
                    &AuthContext::default(),
                    safe.inode,
                    0,
                    &payload(83).slice(..safe.length as usize),
                )
                .await?;
            let _ = live.fs().administrative_remote_durability().await?;
            live.close_writer().await;
            driver.review_action(live, Action::Restart).await?;
            let (bytes, _) = live
                .fs()
                .read_file(&AuthContext::default(), safe.inode, 0, safe.length)
                .await?;
            ensure!(
                bytes == payload(83).slice(..safe.length as usize),
                "promoted writes failed cold reopen"
            );
            result.receipts.push(driver.commit(live).await?);
            result.statuses.push(driver.status(live).await?);
            // Source identities remain immutable; the new set uses fresh roots.
            for (path, digest) in &result.before_files {
                let index: usize = path
                    .components()
                    .next()
                    .unwrap()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .parse()?;
                let relative: PathBuf = path.components().skip(1).collect();
                ensure!(
                    Sha256::digest(std::fs::read(
                        original_roots[index]
                            .join(".zerofs-survival/v1")
                            .join(relative)
                    )?)
                    .as_slice()
                        == digest,
                    "promotion changed old surviving evidence"
                );
            }
        }
        Row::OrphanCheckpoint => {
            overwrite_first_file(live, 86).await?;
            let cut = Cut {
                point: CommitPoint::CheckpointPublished,
                side: Side::After,
                replica: "authority",
                lost: Device::Journal,
            };
            let pause = driver
                .pause_commit(live, cut)
                .await?
                .ok_or_else(|| Missing("the orphan checkpoint crash hook is absent".to_owned()))?;
            pause
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("checkpoint start closed"))?;
            let event = tokio::time::timeout(Duration::from_secs(5), pause.entered).await??;
            ensure!(
                event.cut == cut && !pause.task.is_finished(),
                "checkpoint creation did not block at its crash boundary"
            );
            let checkpoints = driver.owned_checkpoints(live).await?;
            ensure!(
                checkpoints.len() == 1,
                "the fixture needs exactly one owned orphan checkpoint"
            );
            let checkpoint = &checkpoints[0];
            let name = checkpoint
                .name
                .as_ref()
                .context("orphan checkpoint has no deterministic name")?;
            ensure!(
                name.starts_with("zerofs-survival/"),
                "checkpoint name has no ownership prefix"
            );
            let expiry = checkpoint
                .expire_time
                .context("orphan checkpoint does not expire")?;
            ensure!(
                expiry - checkpoint.create_time > chrono::Duration::seconds(185),
                "checkpoint lifetime does not cover the commit and settlement deadline"
            );
            let intents_root = live.replica_roots()[0].join(".zerofs-survival/v1/intents");
            let intents = tree_hashes(&intents_root)?;
            let mut matching_intent = false;
            for path in intents.keys() {
                let intent: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(intents_root.join(path))?)?;
                matching_intent |= intent
                    .get("checkpoint_name")
                    .and_then(|value| value.as_str())
                    == Some(name.as_str());
            }
            ensure!(
                matching_intent,
                "no durable intent identified the checkpoint before the crash"
            );
            pause.task.abort();
            let _ = pause.task.await;
            drop(pause.release);
            driver.crash_writer(live).await?;
            driver.review_action(live, Action::Restart).await?;
            driver
                .review_action(live, Action::ReconcileCheckpointIntents)
                .await?;
            result.owned_checkpoints = driver.owned_checkpoints(live).await?;
            result.statuses.push(driver.status(live).await?);
        }
        Row::AdmissionLockOrder => {
            let inode = live.reads[0].inode;
            let guard = live.fs().lock_manager.acquire(inode).await;
            let write = driver
                .pause_write(live)
                .await?
                .ok_or_else(|| Missing("the admission-before-inode hook is absent".to_owned()))?;
            write
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("write start closed"))?;
            tokio::time::timeout(Duration::from_secs(2), write.admitted).await??;
            driver.review_action(live, Action::CloseAdmission).await?;
            let fs = Arc::clone(live.fs());
            let late = tokio::spawn(async move {
                fs.write(
                    &AuthContext::default(),
                    inode,
                    0,
                    &Bytes::from_static(b"late"),
                )
                .await
            });
            let started = std::time::Instant::now();
            let late_result = tokio::time::timeout(FINAL_PAUSE_MAX + Duration::from_secs(1), late)
                .await??
                .map(|_| ());
            // Late admission must return while the inode remains held. It
            // therefore cannot hold that inode while waiting at the gate.
            result.lock_wait = Some(started.elapsed());
            result.api.push(late_result);
            drop(guard);
            let _ = write.release.send(());
            tokio::time::timeout(Duration::from_secs(2), write.task).await???;
            driver.review_action(live, Action::ReopenAdmission).await?;
            result.api.push(probe_create(live, b"lock-progress").await?);
        }
        Row::DataOnlyLoss => {
            // disk-b holds no metadata replica and owns current payload.
            lose_device(live, Device::DiskB)?;
            driver.review_action(live, Action::EvictReadCaches).await?;
            let bad = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-b"))
                .context("lost data range")?
                .clone();
            let safe = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-a"))
                .context("safe data range")?
                .clone();
            result.api.push(probe_read(live, &bad).await?);
            result.api.push(probe_read(live, &safe).await?);
            result
                .api
                .push(probe_create(live, b"after-data-loss").await?);
            // A successful create alone would not prove surviving write placement.
            live.fs()
                .write(
                    &AuthContext::default(),
                    safe.inode,
                    0,
                    &payload(84).slice(..4096),
                )
                .await?;
            let _ = live.fs().administrative_remote_durability().await?;
            let placements = live.pool().placements().await?;
            let loc = live
                .fs()
                .db
                .get_bytes(&KeyCodec::new().extent_key(safe.inode, 0))
                .await?
                .context("new healthy extent")?;
            let loc = FrameLoc::decode(&loc).context("new healthy frame")?;
            ensure!(
                placements.iter().any(|p| p.member_id == "disk-a"
                    && p.location
                        .as_ref()
                        .ends_with(loc.segid.object_key().as_str())),
                "new data was assigned to the lost member"
            );
            result.statuses.push(driver.status(live).await?);
        }
        Row::RetentionCrash => {
            let retention = overwrite_and_reclaim(live).await?;
            payload_retention_oracle(live, &retention)?;
            result
                .retained
                .push((retention.old_object, retention.old_object_digest));
            result.dead = retention.unretained_dead_object;
            // Exercise repeatable mark/sweep after restart. The cut matrix
            // supplies before/after cuts for retirement and deletion itself.
            driver.crash_writer(live).await?;
            driver.review_action(live, Action::Restart).await?;
            for _ in 0..2 {
                driver
                    .review_action(live, Action::SweepRetainedUnion)
                    .await?;
            }
            result.statuses.push(driver.status(live).await?);
        }
        Row::MetadataReplicaLoss => {
            // disk-a holds the second metadata copy and current payload. The
            // writer stays open, so the share is writable when the disk dies.
            result.statuses.push(driver.status(live).await?);
            lose_device(live, Device::DiskA)?;
            result.before_files = survival_tree(live)?;
            let Err(error) = driver.commit(live).await else {
                anyhow::bail!("a commit succeeded without its metadata replica");
            };
            result.errors.push(
                *error
                    .downcast_ref::<SurvivalFailure>()
                    .context("unrelated replica-loss commit error")?,
            );
            result.statuses.push(driver.status(live).await?);
            driver.review_action(live, Action::EvictReadCaches).await?;
            let lost = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-a"))
                .context("sole-copy range on the lost disk")?
                .clone();
            let safe = live
                .reads
                .iter()
                .find(|r| r.member.as_deref() == Some("disk-b"))
                .context("range on the surviving disk")?
                .clone();
            result.api.push(probe_read(live, &lost).await?);
            result.api.push(probe_read(live, &safe).await?);
            result
                .api
                .push(probe_create(live, b"after-replica-loss").await?);
            result.after_files = survival_tree(live)?;
            // Writable startup needs offline promotion or verified replacement.
            live.close_writer().await;
            result
                .errors
                .push(action_error(driver, live, Action::Restart).await?);
        }
        _ => anyhow::bail!("not an independent-review row"),
    }
    observed.review = Some(result);
    Ok(observed)
}

pub(super) fn oracle(row: Row, live: &Live, observed: &Observation) -> Result<()> {
    let evidence = observed
        .review
        .as_ref()
        .context("no independently exercised review scenario")?;
    let prior = evidence
        .receipts
        .first()
        .context("the scenario has no prior durable generation")?;
    // Corruption and stale-membership cases intentionally invalidate a replica.
    if !matches!(
        row,
        Row::CorruptRecord | Row::StaleRestart | Row::TerminalDisable | Row::PromoteAuthority
    ) {
        let record = if row == Row::IdleAndBounds {
            evidence.receipts.last().unwrap()
        } else {
            prior
        };
        verify_replica_record(live, record, RecordCoverage::AllSurviving)?;
    }
    if matches!(row, Row::TerminalDisable | Row::PromoteAuthority) {
        verify_fresh_activation(
            prior,
            evidence.receipts.last().context("no fresh epoch record")?,
            &evidence.prior_roots,
            &evidence.active_roots,
        )?;
        if row == Row::PromoteAuthority {
            verify_restarted_replica_roots(&evidence.active_roots, &live.replica_roots())?;
        }
        verify_replica_record(
            live,
            evidence.receipts.last().context("no fresh epoch record")?,
            RecordCoverage::AllRequired,
        )?;
    }
    if row == Row::TerminalDisable {
        ensure!(
            evidence.terminal_dirs.len() == 2,
            "not every old replica has a terminal record"
        );
        for terminal in &evidence.terminal_dirs {
            ensure!(
                !tree_hashes(terminal)?.is_empty(),
                "the terminal record is absent"
            );
        }
    }
    if row == Row::StaleRestart {
        let latest = evidence
            .receipts
            .last()
            .context("no generation beyond the stale replica")?;
        let roots = live.replica_roots();
        let stale = roots[1].join(".zerofs-survival/v1");
        ensure!(
            stale.join(&prior.relative_record).is_file()
                && !stale.join(&latest.relative_record).exists(),
            "the restart fixture did not contain a stale replica"
        );
        let record = roots[0]
            .join(".zerofs-survival/v1")
            .join(&latest.relative_record);
        ensure!(
            <[u8; 32]>::from(Sha256::digest(std::fs::read(record)?)) == latest.digest,
            "the authority lacks the newer durable generation"
        );
    }
    if row == Row::CorruptRecord {
        let (path, expected) = evidence
            .damaged_record
            .as_ref()
            .context("no actual corrupted record")?;
        ensure!(
            <[u8; 32]>::from(Sha256::digest(std::fs::read(path)?)) != *expected,
            "the corruption row never damaged record bytes"
        );
    }
    match row {
        Row::DurableEnablement => {
            ensure!(
                evidence.errors
                    == [
                        SurvivalFailure::Configuration,
                        SurvivalFailure::Configuration
                    ],
                "startup ignored durable enablement"
            );
            ensure!(
                !evidence.retained.is_empty(),
                "missing configuration bypassed retention"
            );
        }
        Row::TerminalDisable => ensure!(
            evidence.errors == [SurvivalFailure::Disabled]
                && evidence.receipts.len() == 2
                && evidence.receipts[0].relative_record != evidence.receipts[1].relative_record,
            "terminal records did not prevent stale recovery or fresh activation"
        ),
        Row::StaleRestart => ensure!(
            evidence.errors == [SurvivalFailure::Stale] && evidence.receipts.len() == 2,
            "stale restart was accepted"
        ),
        Row::CorruptDependency | Row::CorruptRecord | Row::ConflictingForks => {
            let expected = if row == Row::ConflictingForks {
                SurvivalFailure::Fork
            } else {
                SurvivalFailure::Corrupt
            };
            ensure!(
                evidence.errors == [expected]
                    && !evidence.before_files.is_empty()
                    && evidence.before_files == evidence.after_files,
                "unsafe recovery succeeded or modified evidence"
            );
        }
        Row::HighestGeneration => {
            let latest = evidence
                .receipts
                .get(1)
                .context("selection needs two valid generations")?;
            ensure!(
                latest.sequence > prior.sequence
                    && latest.digest != prior.digest
                    && evidence.selected == Some(latest.digest),
                "recovery did not choose the highest valid chained generation without a hint"
            );
            live.expected.verify(
                observed
                    .recovered
                    .as_ref()
                    .context("no cold selected namespace")?,
                false,
            )?;
            read_oracle(row, live, observed)?;
        }
        Row::PartialFanoutRestart => {
            ensure!(
                evidence.receipts.len() == 2
                    && evidence.before_files == evidence.after_files
                    && !evidence.after_files.is_empty()
                    && evidence.api == [Ok(())]
                    && evidence.statuses.last().is_some_and(|s| s.admission_open
                        && s.last_sequence == Some(evidence.receipts[1].sequence)),
                "restart did not converge on the same partial record"
            )
        }
        Row::BacklogRetry => ensure!(
            evidence.errors == [SurvivalFailure::Timeout]
                && evidence.elapsed <= Duration::from_secs(185)
                && evidence.api == [Ok(()), Ok(())]
                && evidence.statuses.len() == 2
                && evidence.statuses[1].admission_open
                && evidence.statuses[1].last_sequence == evidence.statuses[0].last_sequence
                && evidence
                    .receipts
                    .last()
                    .is_some_and(|r| r.sequence > prior.sequence),
            "backlog latched read-only or failed online retry"
        ),
        Row::ReadFaultClosed | Row::ReadFaultReadOnly => ensure!(
            evidence.api
                == [
                    Err(FsError::IoError),
                    Ok(()),
                    // A commit's closed phase is transient. A latch is not.
                    Err(if row == Row::ReadFaultClosed {
                        FsError::RetryLater
                    } else {
                        FsError::ReadOnlyFilesystem
                    })
                ]
                && evidence.before_catalog.is_some()
                && evidence.before_catalog == evidence.after_catalog,
            "read faults wrote the closed catalog, blocked reads or changed EIO"
        ),
        Row::IdleAndBounds => {
            ensure!(
                evidence.counts.len() == 32
                    && evidence.owned_checkpoints.is_empty()
                    && evidence.statuses.iter().all(|s| s.owned_checkpoints == 0),
                "commits leaked owned checkpoints"
            );
            let settled = &evidence.counts[16..];
            ensure!(
                settled.iter().max() == settled.iter().min() && settled[0] > 0,
                "record counts did not stabilize after retirement"
            );
            ensure!(
                evidence.byte_counts.len() == 32,
                "record byte counts were not measured"
            );
            let sizes = &evidence.byte_counts[16..];
            let min = *sizes.iter().min().unwrap();
            let max = *sizes.iter().max().unwrap();
            ensure!(
                min > 0 && max <= min + 4096,
                "record bytes grow despite stable generation sizes"
            );
        }
        Row::StatusLag => {
            ensure!(
                evidence.errors == [SurvivalFailure::Timeout] && evidence.statuses.len() == 3,
                "no real failed status attempt"
            );
            let [before, after, later] = evidence.statuses.as_slice() else {
                unreachable!()
            };
            ensure!(
                before.last_sequence == after.last_sequence
                    && after.last_sequence == later.last_sequence
                    && before.completed_at == after.completed_at
                    && after.completed_at == later.completed_at
                    && before.last_digest == after.last_digest
                    && after.last_digest == later.last_digest
                    && later.lag > after.lag
                    && !after.failed_replicas.is_empty(),
                "status fabricated freshness or hid lag"
            );
        }
        Row::PromoteAuthority => ensure!(
            evidence.api == [Ok(())]
                && evidence.receipts.len() == 2
                && evidence.receipts[0].relative_record != evidence.receipts[1].relative_record
                && evidence.statuses.last().is_some_and(|s| s.admission_open),
            "promotion did not return a writable new epoch"
        ),
        Row::OrphanCheckpoint => ensure!(
            evidence.owned_checkpoints.is_empty()
                && evidence
                    .statuses
                    .last()
                    .is_some_and(|s| s.owned_checkpoints == 0 && s.admission_open),
            "startup left an owned orphan checkpoint"
        ),
        Row::AdmissionLockOrder => ensure!(
            evidence.api == [Err(FsError::RetryLater), Ok(())]
                && evidence
                    .lock_wait
                    .is_some_and(|wait| wait <= FINAL_PAUSE_MAX + Duration::from_secs(1)),
            "admission waited while holding an inode lock"
        ),
        Row::DataOnlyLoss => ensure!(
            evidence.api == [Err(FsError::IoError), Ok(()), Ok(())]
                && evidence.statuses.last().is_some_and(|s| s.admission_open),
            "payload-only loss disabled unaffected writes"
        ),
        Row::RetentionCrash => ensure!(
            !evidence.retained.is_empty()
                && evidence.dead.as_ref().is_some_and(|p| !p.exists())
                && evidence.statuses.last().is_some_and(|s| s.admission_open),
            "crashed sweep lost retention or disabled all reclaim"
        ),
        Row::MetadataReplicaLoss => {
            ensure!(
                evidence.errors == [SurvivalFailure::Lost, SurvivalFailure::Lost],
                "a commit or a writable restart proceeded without the lost metadata replica"
            );
            let [before, after] = evidence.statuses.as_slice() else {
                anyhow::bail!("replica loss needs one status before and one after the loss");
            };
            ensure!(
                before.admission_open
                    && before.failed_replicas.is_empty()
                    && before.last_sequence == Some(prior.sequence),
                "the share was not writable at a current generation before the loss"
            );
            ensure!(
                !after.admission_open
                    && after.failed_replicas.contains_key("disk-a-metadata")
                    && after.last_sequence == before.last_sequence
                    && after.last_digest == before.last_digest
                    && after.completed_at == before.completed_at,
                "replica loss left admission open, hid the replica or moved the recovery point"
            );
            ensure!(
                evidence.api
                    == [
                        Err(FsError::IoError),
                        Ok(()),
                        Err(FsError::ReadOnlyFilesystem)
                    ],
                "replica loss must return EIO for lost payload, keep other reads and refuse writes with EROFS"
            );
            ensure!(
                !evidence.before_files.is_empty() && evidence.before_files == evidence.after_files,
                "a failed commit changed the surviving metadata copy"
            );
        }
        _ => anyhow::bail!("not an independent-review row"),
    }
    for (path, digest) in &evidence.retained {
        ensure!(
            <[u8; 32]>::from(Sha256::digest(std::fs::read(path)?)) == *digest,
            "a retained object changed after restart"
        );
    }
    Ok(())
}

pub(super) fn optimistic(row: Row, live: &Live, receipt: &SurvivalReceipt) -> ReviewObservation {
    let status = SurvivalStatus {
        last_sequence: Some(receipt.sequence),
        last_digest: Some(receipt.digest),
        completed_at: Some(1),
        lag: Duration::from_secs(1),
        failed_replicas: BTreeMap::new(),
        admission_open: true,
        owned_checkpoints: 0,
    };
    let mut latest = receipt.clone();
    latest.sequence += 1;
    latest.digest[0] ^= 1;
    latest.relative_record = "commits/2/2.json".into();
    let mut result = ReviewObservation {
        receipts: vec![receipt.clone(), latest.clone()],
        statuses: vec![status.clone(); 3],
        selected: Some(latest.digest),
        api: vec![Ok(())],
        counts: vec![4; 32],
        byte_counts: vec![4096; 32],
        lock_wait: Some(Duration::ZERO),
        before_catalog: Some(live.expected.catalog.clone()),
        after_catalog: Some(live.expected.catalog.clone()),
        before_files: BTreeMap::from([("commits/stub".into(), receipt.digest)]),
        after_files: BTreeMap::from([("commits/stub".into(), receipt.digest)]),
        ..Default::default()
    };
    result.errors = match row {
        Row::DurableEnablement => vec![SurvivalFailure::Configuration; 2],
        Row::TerminalDisable => vec![SurvivalFailure::Disabled],
        Row::StaleRestart => vec![SurvivalFailure::Stale],
        Row::CorruptDependency | Row::CorruptRecord => vec![SurvivalFailure::Corrupt],
        Row::ConflictingForks => vec![SurvivalFailure::Fork],
        Row::BacklogRetry | Row::StatusLag => vec![SurvivalFailure::Timeout],
        _ => vec![],
    };
    if matches!(row, Row::ReadFaultClosed | Row::ReadFaultReadOnly) {
        result.api = vec![
            Err(FsError::IoError),
            Ok(()),
            Err(if row == Row::ReadFaultClosed {
                FsError::RetryLater
            } else {
                FsError::ReadOnlyFilesystem
            }),
        ];
    }
    if row == Row::DataOnlyLoss {
        result.api = vec![Err(FsError::IoError), Ok(()), Ok(())];
    }
    if row == Row::AdmissionLockOrder {
        result.api = vec![Err(FsError::RetryLater), Ok(())];
    }
    if row == Row::MetadataReplicaLoss {
        let mut closed = status.clone();
        closed.admission_open = false;
        closed
            .failed_replicas
            .insert("disk-a-metadata".to_owned(), "device lost".to_owned());
        result.statuses = vec![status.clone(), closed];
        result.errors = vec![SurvivalFailure::Lost; 2];
        result.api = vec![
            Err(FsError::IoError),
            Ok(()),
            Err(FsError::ReadOnlyFilesystem),
        ];
        result.before_files = BTreeMap::from([("0/commits/stub".into(), receipt.digest)]);
        result.after_files = result.before_files.clone();
    }
    result
}

#[test]
fn fresh_activation_requires_a_new_epoch_and_disjoint_roots() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let old_roots = [fixture.path().join("old-a"), fixture.path().join("old-b")];
    let new_roots = [fixture.path().join("new-a"), fixture.path().join("new-b")];
    for root in old_roots.iter().chain(&new_roots) {
        std::fs::create_dir(root)?;
    }
    let prior = SurvivalReceipt {
        relative_record: "commits/old-epoch/1.json".into(),
        digest: [1; 32],
        mutation_cutoff: 1,
        envelope_epoch: 1,
        sequence: 1,
    };
    let mut fresh = prior.clone();
    fresh.sequence = 2;
    fresh.relative_record = "commits/old-epoch/2.json".into();
    // A different sequence filename alone previously passed the epoch check.
    assert_ne!(prior.relative_record, fresh.relative_record);
    ensure!(verify_fresh_activation(&prior, &fresh, &old_roots, &new_roots).is_err());

    fresh.relative_record = "commits/new-epoch/1.json".into();
    fresh.sequence = 1;
    verify_fresh_activation(&prior, &fresh, &old_roots, &new_roots)?;
    ensure!(verify_fresh_activation(&prior, &fresh, &old_roots, &old_roots).is_err());
    ensure!(
        verify_fresh_activation(
            &prior,
            &fresh,
            &old_roots,
            &[new_roots[0].clone(), new_roots[0].clone()],
        )
        .is_err()
    );
    let nested = old_roots[0].join("nested");
    std::fs::create_dir(&nested)?;
    ensure!(
        verify_fresh_activation(&prior, &fresh, &old_roots, &[nested, new_roots[1].clone()])
            .is_err()
    );
    #[cfg(unix)]
    {
        let alias = fixture.path().join("old-alias");
        std::os::unix::fs::symlink(&old_roots[0], &alias)?;
        ensure!(
            verify_fresh_activation(&prior, &fresh, &old_roots, &[alias, new_roots[1].clone()])
                .is_err()
        );
    }
    std::fs::remove_dir_all(&old_roots[0])?;
    verify_fresh_activation(&prior, &fresh, &old_roots, &new_roots)?;
    Ok(())
}

#[test]
fn membership_epoch_rejects_paths_without_one_commit_generation() {
    let mut receipt = SurvivalReceipt {
        relative_record: "commits/epoch/1.json".into(),
        digest: [1; 32],
        mutation_cutoff: 1,
        envelope_epoch: 1,
        sequence: 1,
    };
    for path in [
        "commits/1.json",
        "commits/epoch/subdir/1.json",
        "objects/epoch/1.json",
        "/commits/epoch/1.json",
        "commits/../1.json",
    ] {
        receipt.relative_record = path.into();
        assert!(membership_epoch(&receipt).is_err(), "{path}");
    }
}

#[test]
fn promotion_cannot_restart_using_the_previous_replica_roots() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let old_roots = [fixture.path().join("old-a"), fixture.path().join("old-b")];
    let new_roots = [fixture.path().join("new-a"), fixture.path().join("new-b")];
    for root in old_roots.iter().chain(&new_roots) {
        std::fs::create_dir(root)?;
    }
    let promoted_roots = new_roots
        .iter()
        .map(|root| root.canonicalize())
        .collect::<std::io::Result<Vec<_>>>()?;
    verify_restarted_replica_roots(&promoted_roots, &new_roots)?;
    ensure!(verify_restarted_replica_roots(&promoted_roots, &old_roots).is_err());
    ensure!(
        verify_restarted_replica_roots(
            &promoted_roots,
            &[new_roots[0].clone(), old_roots[1].clone()],
        )
        .is_err()
    );
    Ok(())
}
