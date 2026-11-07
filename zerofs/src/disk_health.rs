use crate::pool_object_store::{PoolObjectStore, SmartSample};
use anyhow::{Context, Result, bail};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

struct Binding {
    member: String,
    directory: PathBuf,
    device: PathBuf,
    filesystem: u64,
    node: u64,
    kernel_names: Vec<String>,
}

impl Binding {
    fn validate(&self) -> Result<()> {
        let device = std::fs::metadata(&self.device)?;
        let directory = std::fs::metadata(&self.directory)?;
        if !device.file_type().is_block_device()
            || device.rdev() != self.node
            || directory.dev() != self.filesystem
            || self.filesystem != self.node
        {
            bail!("mounted member no longer matches its configured block device");
        }
        Ok(())
    }
}

pub(crate) struct DiskMonitor {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

pub(crate) struct PreparedDiskMonitor {
    pool: PoolObjectStore,
    bindings: Vec<Binding>,
    kernel: KernelWatcher,
    prepared_at: std::time::Instant,
}

impl PreparedDiskMonitor {
    pub(crate) async fn prepare(
        pool: PoolObjectStore,
        settings: &crate::config::Settings,
    ) -> Result<Self> {
        let mut bindings = Vec::new();
        let mut devices = std::collections::BTreeSet::new();
        if let Some(config) = &settings.storage.pool {
            for member in &config.members {
                let Some(device) = &member.health_device else {
                    continue;
                };
                if !cfg!(target_os = "linux") {
                    bail!("health_device monitoring requires Linux");
                }
                let directory = url::Url::parse(&member.url)?
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("health_device requires a local file member"))?;
                let (filesystem, node) = match std::fs::metadata(&directory).and_then(|directory| {
                    std::fs::metadata(device).map(|device| (directory.dev(), device.rdev()))
                }) {
                    Ok(identity) => identity,
                    Err(issue) => {
                        unavailable_binding(&pool, &member.id, issue.to_string()).await?;
                        continue;
                    }
                };
                let sysfs = match std::fs::canonicalize(format!(
                    "/sys/dev/block/{}:{}",
                    libc::major(node as libc::dev_t),
                    libc::minor(node as libc::dev_t)
                )) {
                    Ok(sysfs) => sysfs,
                    Err(issue) => {
                        unavailable_binding(
                            &pool,
                            &member.id,
                            format!("physical device unavailable: {issue}"),
                        )
                        .await?;
                        continue;
                    }
                };
                let physical = if sysfs.join("partition").exists() {
                    sysfs
                        .parent()
                        .context("partition has no parent disk")?
                        .to_path_buf()
                } else {
                    sysfs.clone()
                };
                if !physical.exists() {
                    unavailable_binding(
                        &pool,
                        &member.id,
                        "physical device disappeared during validation".into(),
                    )
                    .await?;
                    continue;
                }
                if !physical.join("device").exists() {
                    bail!("health_device is not a directly attached physical disk/partition");
                }
                let metadata = url::Url::parse(&settings.storage.url)?
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("metadata must be local"))?;
                for authority in [&metadata, &config.catalog_dir] {
                    let authority_device = std::fs::metadata(authority)?.dev();
                    if authority_device == filesystem {
                        bail!(
                            "metadata and catalog must be on separate dependable storage from monitored data disks"
                        );
                    }
                    if let Ok(authority_sysfs) = std::fs::canonicalize(format!(
                        "/sys/dev/block/{}:{}",
                        libc::major(authority_device as libc::dev_t),
                        libc::minor(authority_device as libc::dev_t)
                    )) {
                        let authority_disk = if authority_sysfs.join("partition").exists() {
                            authority_sysfs.parent().context("authority disk parent")?
                        } else {
                            &authority_sysfs
                        };
                        if authority_disk == physical {
                            bail!("metadata/catalog and data partitions share a physical disk");
                        }
                    }
                }
                let kernel_names = vec![
                    sysfs
                        .file_name()
                        .context("device name")?
                        .to_string_lossy()
                        .into_owned(),
                    physical
                        .file_name()
                        .context("disk name")?
                        .to_string_lossy()
                        .into_owned(),
                ];
                let binding = Binding {
                    member: member.id.clone(),
                    directory,
                    device: device.clone(),
                    filesystem,
                    node,
                    kernel_names,
                };
                if let Err(issue) = binding.validate() {
                    unavailable_binding(&pool, &member.id, issue.to_string()).await?;
                    continue;
                }
                if !devices.insert(physical) {
                    bail!("multiple monitored members on one block device");
                }
                pool.monitoring_error(&member.id, Some("initial probe pending".into()))
                    .await?;
                bindings.push(binding);
            }
        }
        let mut kernel = if bindings.is_empty() {
            KernelWatcher {
                file: Err(std::io::Error::other("no monitored members")),
                boot: String::new(),
                coverage: "unmonitored".into(),
            }
        } else {
            KernelWatcher::open()
        };
        sample_kernel(&pool, &bindings, &mut kernel).await?;
        sample_all(&pool, &bindings).await?;
        #[cfg(feature = "disk-health-test-hooks")]
        test_observations(&pool).await?;
        Ok(Self {
            pool,
            bindings,
            kernel,
            prepared_at: std::time::Instant::now(),
        })
    }

    pub(crate) async fn start(self, failure: CancellationToken) -> Result<DiskMonitor> {
        let Self {
            pool,
            bindings,
            mut kernel,
            prepared_at,
        } = self;
        if prepared_at.elapsed() >= poll_interval() {
            sample_kernel(&pool, &bindings, &mut kernel).await?;
            sample_all(&pool, &bindings).await?;
        }
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let task = tokio::spawn(async move {
            let _failure = failure.drop_guard();
            loop {
                tokio::select! {
                    _ = cancelled.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(poll_interval()) => {}
                }
                sample_kernel(&pool, &bindings, &mut kernel).await?;
                sample_all(&pool, &bindings).await?;
                #[cfg(feature = "disk-health-test-hooks")]
                test_observations(&pool).await?;
            }
        });
        Ok(DiskMonitor { stop, task })
    }
}

impl DiskMonitor {
    pub(crate) async fn shutdown(self) -> Result<()> {
        self.stop.cancel();
        self.task.await??;
        Ok(())
    }
}

async fn unavailable_binding(pool: &PoolObjectStore, member: &str, issue: String) -> Result<()> {
    pool.quarantine(member, "monitored device binding unavailable")
        .await?;
    pool.monitoring_error(member, Some(issue)).await?;
    Ok(())
}

async fn sample_all(pool: &PoolObjectStore, bindings: &[Binding]) -> Result<()> {
    for binding in bindings {
        if let Err(error) = binding.validate() {
            pool.quarantine(&binding.member, &format!("device binding lost: {error}"))
                .await?;
            continue;
        }
        match smartctl(&binding.device).await {
            Ok((sample, coverage)) => {
                pool.observe_health(&binding.member, sample, coverage)
                    .await?;
            }
            Err(error) => {
                pool.monitoring_error(&binding.member, Some(error.to_string()))
                    .await?
            }
        }
    }
    Ok(())
}

async fn smartctl(device: &Path) -> Result<(SmartSample, Option<String>)> {
    let mut command = Command::new("smartctl");
    command
        .args(["--json", "--all"])
        .arg(device)
        .stdin(Stdio::null());
    let (status, output) = bounded_command(command, Duration::from_secs(10)).await?;
    parse_smart(&output, status)
}

#[derive(Debug, thiserror::Error)]
#[error("disk probe timed out (reaped child {pid})")]
struct ProbeTimeout {
    pid: u32,
}

async fn bounded_command(mut command: Command, timeout: Duration) -> Result<(i32, Vec<u8>)> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start read-only disk probe")?;
    let pid = child.id().context("probe child id missing")?;
    let mut stdout = child
        .stdout
        .take()
        .context("probe stdout missing")?
        .take(1024 * 1024 + 1);
    let mut bytes = Vec::new();
    let result = tokio::time::timeout(timeout, async {
        stdout.read_to_end(&mut bytes).await?;
        if bytes.len() > 1024 * 1024 {
            return Err(std::io::Error::other("disk probe output exceeded limit"));
        }
        let status = child.wait().await?;
        Ok::<_, std::io::Error>(status)
    })
    .await;
    match result {
        Ok(Ok(status)) => {
            if bytes.len() > 1024 * 1024 {
                bail!("disk probe output exceeded limit");
            }
            Ok((status.code().context("probe terminated by signal")?, bytes))
        }
        other => {
            let _ = child.kill().await;
            child.wait().await.context("reap disk probe")?;
            match other {
                Err(_) => Err(ProbeTimeout { pid }.into()),
                Ok(Err(error)) => Err(error.into()),
                _ => unreachable!(),
            }
        }
    }
}

fn parse_smart(bytes: &[u8], exit: i32) -> Result<(SmartSample, Option<String>)> {
    if exit & 3 != 0 || !(0..=255).contains(&exit) {
        bail!("SMART command or device access failed ({exit})");
    }
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    let serial = json["serial_number"]
        .as_str()
        .context("SMART device serial unavailable")?;
    let attributes = json["ata_smart_attributes"]["table"].as_array();
    let counter = |id| {
        attributes?
            .iter()
            .find(|v| v["id"].as_u64() == Some(id))
            .and_then(|v| v["raw"]["value"].as_u64())
    };
    let sample = SmartSample {
        device: serial.into(),
        reallocated: counter(5),
        pending: counter(197),
        uncorrectable: counter(198),
        crc: counter(199),
        // Bit 3 means the overall SMART health check failed; bit 4 means a
        // current pre-failure attribute has reached its failure threshold.
        failed: exit & (8 | 16) != 0 || json["smart_status"]["passed"].as_bool() == Some(false),
    };
    let coverage = if exit & 4 != 0 {
        Some(format!(
            "partial SMART command failure ({exit}); known failure evidence retained"
        ))
    } else {
        smart_coverage(&sample)
    };
    Ok((sample, coverage))
}

fn smart_coverage(sample: &SmartSample) -> Option<String> {
    (sample.reallocated.is_none()
        || sample.pending.is_none()
        || sample.uncorrectable.is_none()
        || sample.crc.is_none())
    .then(|| "required SMART counters unavailable; known failure evidence retained".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disk_health_native_exit_bitmask_and_counters() {
        let fixture = br#"{"serial_number":"disk","smart_status":{"passed":false},"ata_smart_attributes":{"table":[{"id":5,"raw":{"value":0}},{"id":197,"raw":{"value":3}},{"id":198,"raw":{"value":0}},{"id":199,"raw":{"value":9}}]}}"#;
        let (sample, _) = parse_smart(fixture, 8).unwrap();
        assert!(sample.failed);
        assert_eq!(sample.pending, Some(3));
        assert!(parse_smart(fixture, 2).is_err());
        assert!(parse_smart(b"{}", 0).is_err());
    }
    #[test]
    fn disk_health_partial_smart_keeps_overall_failure_and_pending_without_crc() {
        let (failed, _) = parse_smart(
            br#"{"serial_number":"disk","smart_status":{"passed":false}}"#,
            8,
        )
        .unwrap();
        assert!(failed.failed);
        assert!(smart_coverage(&failed).is_some());
        let (pending, _) = parse_smart(br#"{"serial_number":"disk","ata_smart_attributes":{"table":[{"id":197,"raw":{"value":2}}]}}"#, 0).unwrap();
        assert_eq!(pending.pending, Some(2));
        assert_eq!(pending.crc, None);
        assert!(smart_coverage(&pending).is_some());
    }

    #[test]
    fn disk_health_prefail_threshold_exit_quarantines_despite_overall_pass() {
        let fixture = br#"{"serial_number":"disk","smart_status":{"passed":true}}"#;
        let (sample, coverage) = parse_smart(fixture, 16).unwrap();
        assert!(sample.failed, "SMART exit bit 4 is an active prefail threshold breach");
        assert!(coverage.is_some(), "missing counters must retain coverage warning");
        assert!(!parse_smart(fixture, 4).unwrap().0.failed);
    }

    #[tokio::test]
    async fn disk_health_combined_smart_bits_preserve_failure_and_evacuate() {
        let (sample, coverage) = parse_smart(
            br#"{"serial_number":"disk","smart_status":{"passed":false}}"#,
            12,
        )
        .unwrap();
        assert!(sample.failed);
        assert!(
            coverage
                .as_ref()
                .unwrap()
                .contains("partial SMART command failure")
        );
        let root = tempfile::tempdir().unwrap();
        let members = ["source", "target"]
            .into_iter()
            .map(|id| crate::pool_object_store::PoolMember {
                id: id.into(),
                identity: format!("memory://{id}"),
                store: std::sync::Arc::new(object_store::memory::InMemory::new()),
                capacity_bytes: 1024,
                local_path: None,
                min_free_bytes: 0,
                health_device: None,
            })
            .collect();
        let pool = PoolObjectStore::initialize(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            object_store::path::Path::from("db"),
            root.path().join("catalog"),
            members,
        )
        .await
        .unwrap();
        use object_store::{ObjectStore, ObjectStoreExt};
        let location = object_store::path::Path::from(format!(
            "db/{}",
            crate::segment::Segid::new(41, 1202).object_key()
        ));
        let mut options = object_store::PutOptions::from(object_store::PutMode::Create);
        options
            .extensions
            .insert(crate::segment_store::GeneratedSegmentCreate);
        pool.put_opts(
            &location,
            bytes::Bytes::from_static(b"healthy").into(),
            options,
        )
        .await
        .unwrap();
        pool.observe_health("source", sample, coverage)
            .await
            .unwrap();
        let status = pool.status().await.unwrap();
        assert!(status.members[0].health.quarantine.is_some());
        assert!(status.members[0].health.monitoring_error.is_some());
        pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
        assert_eq!(pool.placements().await.unwrap()[0].member_id, "target");
        assert_eq!(
            pool.get(&location).await.unwrap().bytes().await.unwrap(),
            bytes::Bytes::from_static(b"healthy")
        );
        assert!(pool.status().await.unwrap().members[0].safe_to_remove);
    }

    #[tokio::test]
    async fn disk_health_probe_output_is_bounded() {
        let mut command = Command::new("head");
        command.args(["-c", "2097152", "/dev/zero"]);
        let failure = bounded_command(command, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(failure.to_string().contains("output exceeded limit"));
    }

    #[tokio::test]
    async fn disk_health_probe_timeout_reaps_child() {
        let mut command = Command::new("sleep");
        command.arg("60");
        let result = bounded_command(command, Duration::from_millis(20)).await;
        let failure = result.unwrap_err();
        let pid = failure.downcast_ref::<ProbeTimeout>().unwrap().pid;
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}

struct KernelWatcher {
    file: std::io::Result<std::fs::File>,
    boot: String,
    coverage: String,
}

impl KernelWatcher {
    fn open() -> Self {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open("/dev/kmsg");
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap_or_default();
        Self {
            file,
            boot,
            coverage:
                "current boot kernel ring; unrecognized device/error forms are not attributed"
                    .into(),
        }
    }
}

fn kernel_fault(record: &str, names: &[String]) -> Option<u64> {
    let (header, message) = record.split_once(';')?;
    let sequence = header.split(',').nth(1)?.parse().ok()?;
    if !message.contains("I/O error") && !message.contains("Buffer I/O error") {
        return None;
    }
    let tokens: Vec<_> = message.split_whitespace().collect();
    let matched = tokens.windows(2).any(|pair| {
        pair[0] == "dev"
            && names
                .iter()
                .any(|name| pair[1].trim_end_matches([',', ':']) == name)
    });
    matched.then_some(sequence)
}

async fn sample_kernel(
    pool: &PoolObjectStore,
    bindings: &[Binding],
    watcher: &mut KernelWatcher,
) -> Result<()> {
    use std::io::Read;
    let mut events = std::collections::BTreeMap::new();
    match &mut watcher.file {
        Err(error) => watcher.coverage = format!("kernel watcher unavailable: {error}"),
        Ok(file) => {
            let mut buffer = [0u8; 8192];
            for i in 0..1024 {
                match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(size) => {
                        let record = String::from_utf8_lossy(&buffer[..size]);
                        for binding in bindings {
                            if let Some(sequence) = kernel_fault(&record, &binding.kernel_names) {
                                events.insert(binding.member.clone(), sequence);
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        watcher.coverage = format!("kernel coverage gap: {error}");
                        break;
                    }
                }
                if i == 1023 {
                    watcher.coverage =
                        "kernel backlog exceeds bounded pass; coverage incomplete".into();
                }
            }
        }
    }
    for binding in bindings {
        let cursor = events
            .get(&binding.member)
            .map(|seq| (watcher.boot.clone(), *seq));
        pool.kernel_observation(&binding.member, watcher.coverage.clone(), cursor)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod kernel_tests {
    use super::*;
    #[test]
    fn disk_health_kernel_exact_device_attribution() {
        let names = vec!["sda".into(), "sda1".into()];
        assert_eq!(
            kernel_fault("3,42,100,-;I/O error, dev sda, sector 5", &names),
            Some(42)
        );
        assert_eq!(
            kernel_fault("3,43,100,-;I/O error, dev sdaa, sector 5", &names),
            None
        );
        assert_eq!(
            kernel_fault(
                "3,44,100,-;Buffer I/O error on dev sda1, logical block 5",
                &names
            ),
            Some(44)
        );
        assert_eq!(kernel_fault("3,45,100,-;generic I/O error", &names), None);
    }
}

fn poll_interval() -> Duration {
    #[cfg(feature = "disk-health-test-hooks")]
    if std::env::var_os("ZEROFS_TEST_DISK_HEALTH").is_some() {
        return Duration::from_millis(100);
    }
    Duration::from_secs(60)
}

#[cfg(feature = "disk-health-test-hooks")]
async fn test_observations(pool: &PoolObjectStore) -> Result<()> {
    let Some(path) = std::env::var_os("ZEROFS_TEST_DISK_HEALTH") else {
        return Ok(());
    };
    let bytes = tokio::fs::read(path).await?;
    let samples: std::collections::BTreeMap<String, SmartSample> = serde_json::from_slice(&bytes)?;
    for (member, sample) in samples {
        pool.observe_health(&member, sample, None).await?;
    }
    Ok(())
}
