use crate::nbd::{
    NBD_PROVISION_STAGING_PREFIX as PROVISION_PREFIX, NBD_STRIPE_MANIFEST_MAX_BYTES,
    NBD_STRIPE_MARKER as STRIPE_MARKER, StripeManifest, is_nbd_provision_staging_name,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use zerofs_client::{Client, OpenOptions, ZeroFsError};

const PROVISION_MARKER: &str = ".zerofs-nbd-provision-v1";
const STRIPE_TEMP_PREFIX: &str = ".zerofs-nbd-stripe-v1-";

/// A validated striped NBD export geometry.
#[derive(Clone, Debug)]
pub struct StripedLayout {
    export_name: String,
    logical_size: u64,
    member_size: u64,
    stripe_size: u64,
    members: Vec<String>,
}

impl StripedLayout {
    /// Validate and derive a striped export layout accepted by the NBD server.
    pub fn new(
        export_name: impl Into<String>,
        logical_size: u64,
        lanes: u8,
        stripe_size: u64,
    ) -> Result<Self> {
        let export_name = export_name.into();
        if export_name.is_empty()
            || export_name == "."
            || export_name == ".."
            || export_name.as_bytes().contains(&b'/')
            || export_name.len() > 255
            || is_nbd_provision_staging_name(export_name.as_bytes())
        {
            bail!(
                "export name must be a non-empty, non-reserved direct child name of at most 255 bytes"
            );
        }
        let members: Vec<String> = (0..lanes).map(|lane| format!("lane-{lane}")).collect();
        StripeManifest {
            version: 1,
            stripe_bytes: stripe_size,
            members: members.clone(),
        }
        .validate()
        .map_err(anyhow::Error::msg)?;
        let layout_unit = stripe_size
            .checked_mul(u64::from(lanes))
            .context("lane count times stripe size overflows u64")?;
        if logical_size == 0 || !logical_size.is_multiple_of(layout_unit) {
            bail!(
                "logical size must be non-zero and divisible by lanes times stripe size ({layout_unit} bytes)"
            );
        }
        let member_size = logical_size / u64::from(lanes);
        Ok(Self {
            export_name,
            logical_size,
            member_size,
            stripe_size,
            members,
        })
    }

    /// Export name presented by the NBD server.
    pub fn export_name(&self) -> &str {
        &self.export_name
    }

    /// Logical size presented to the NBD client.
    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    /// Sparse size assigned to every lane file.
    pub fn member_size(&self) -> u64 {
        self.member_size
    }

    /// Number of logical bytes assigned to one lane before rotating.
    pub fn stripe_size(&self) -> u64 {
        self.stripe_size
    }

    /// Lane filenames in stripe order.
    pub fn members(&self) -> &[String] {
        &self.members
    }

    fn export_path(&self) -> String {
        format!("/.nbd/{}", self.export_name)
    }

    fn manifest(&self) -> StripeManifest {
        StripeManifest {
            version: 1,
            stripe_bytes: self.stripe_size,
            members: self.members.clone(),
        }
    }
}

/// Result of a safe provisioning attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvisionOutcome {
    /// A new striped export was published.
    Created,
    /// The requested geometry was already present and was left unchanged.
    AlreadyProvisioned,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct ProvisionManifest {
    version: u32,
    export_name: String,
    layout: StripeManifest,
}

#[derive(Default)]
struct ProvisionHooks {
    #[cfg(test)]
    before_first_missing_lane_create: Option<std::sync::Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    before_missing_stripe_manifest_create: Option<std::sync::Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    before_final_staging_validation: Option<std::sync::Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    resume_final_staging_validation: Option<std::sync::Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    before_final_publish: Option<std::sync::Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    resume_final_publish: Option<std::sync::Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    missing_lane_hook_used: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    missing_stripe_manifest_hook_used: std::sync::atomic::AtomicBool,
}

impl ProvisionHooks {
    async fn before_missing_lane_create(&self) {
        #[cfg(test)]
        if !self
            .missing_lane_hook_used
            .swap(true, std::sync::atomic::Ordering::AcqRel)
            && let Some(barrier) = &self.before_first_missing_lane_create
        {
            barrier.wait().await;
        }
    }

    async fn before_missing_stripe_manifest_create(&self) {
        #[cfg(test)]
        if !self
            .missing_stripe_manifest_hook_used
            .swap(true, std::sync::atomic::Ordering::AcqRel)
            && let Some(barrier) = &self.before_missing_stripe_manifest_create
        {
            barrier.wait().await;
        }
    }

    async fn before_final_staging_validation(&self) {
        #[cfg(test)]
        if let Some(barrier) = &self.before_final_staging_validation {
            barrier.wait().await;
            if let Some(resume) = &self.resume_final_staging_validation {
                resume.notified().await;
            }
        }
    }

    async fn before_final_publish(&self) {
        #[cfg(test)]
        if let Some(barrier) = &self.before_final_publish {
            barrier.wait().await;
            if let Some(resume) = &self.resume_final_publish {
                resume.notified().await;
            }
        }
    }
}

impl ProvisionManifest {
    fn new(layout: &StripedLayout) -> Self {
        Self {
            version: 1,
            export_name: layout.export_name.clone(),
            layout: layout.manifest(),
        }
    }
}

/// Parse an integer byte size with an optional binary suffix.
///
/// Supported suffixes are `B`, `KiB`, `MiB`, `GiB`, and `TiB`.
pub fn parse_byte_size(input: &str) -> std::result::Result<u64, String> {
    let input = input.trim();
    let (number, multiplier) = [
        ("TiB", 1_u64 << 40),
        ("GiB", 1_u64 << 30),
        ("MiB", 1_u64 << 20),
        ("KiB", 1_u64 << 10),
        ("B", 1_u64),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        input
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .unwrap_or((input, 1));
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "invalid byte size {input:?}; use an integer with B, KiB, MiB, GiB, or TiB"
        ));
    }
    number
        .parse::<u64>()
        .map_err(|_| format!("byte size {input:?} is out of range"))?
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte size {input:?} is out of range"))
}

fn conflict(layout: &StripedLayout, detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!(
        "existing NBD export '{}' conflicts with the requested layout: {detail}",
        layout.export_name
    )
}

fn decode_manifest(bytes: &[u8], layout: &StripedLayout) -> Result<StripeManifest> {
    serde_json::from_slice(bytes)
        .map_err(|error| conflict(layout, format!("invalid marker: {error}")))
}

async fn read_marker_bounded(
    client: &Client,
    path: &str,
) -> std::result::Result<bytes::Bytes, ZeroFsError> {
    let limit = u32::try_from(NBD_STRIPE_MANIFEST_MAX_BYTES)
        .expect("NBD stripe manifest limit fits in u32");
    let bytes = client.read_range(path, 0, limit + 1).await?;
    if bytes.len() > limit as usize {
        return Err(ZeroFsError::InvalidArgument {
            message: format!(
                "marker {path} is too large (maximum {NBD_STRIPE_MANIFEST_MAX_BYTES} bytes)"
            ),
        });
    }
    Ok(bytes)
}

async fn verify_lane(client: &Client, path: &str, expected_size: u64) -> Result<()> {
    let metadata = client
        .stat(path)
        .await
        .with_context(|| format!("inspect striped NBD lane {path}"))?;
    if !metadata.is_file() || metadata.size != expected_size {
        bail!(
            "striped NBD lane {path} must be a regular file of {expected_size} bytes (found {} bytes)",
            metadata.size
        );
    }
    Ok(())
}

async fn verify_complete(client: &Client, layout: &StripedLayout) -> Result<()> {
    let export_path = layout.export_path();
    let metadata = client
        .stat(&export_path)
        .await
        .with_context(|| format!("inspect existing NBD export {export_path}"))?;
    if !metadata.is_dir() {
        return Err(conflict(layout, "the export path is not a directory"));
    }

    let marker_path = format!("{export_path}/{STRIPE_MARKER}");
    let marker = read_marker_bounded(client, &marker_path)
        .await
        .map_err(|error| conflict(layout, format!("cannot read {STRIPE_MARKER}: {error}")))?;
    if decode_manifest(&marker, layout)? != layout.manifest() {
        return Err(conflict(
            layout,
            "the stripe manifest has different geometry",
        ));
    }

    let expected = layout
        .members
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(STRIPE_MARKER))
        .chain(std::iter::once(PROVISION_MARKER))
        .collect::<HashSet<_>>();
    let entries = client
        .read_dir(&export_path)
        .await
        .with_context(|| format!("list existing NBD export {export_path}"))?;
    if !(entries.len() == expected.len() || entries.len() + 1 == expected.len())
        || entries
            .iter()
            .any(|entry| !entry.name_is_utf8 || !expected.contains(entry.name.as_str()))
    {
        return Err(conflict(
            layout,
            "the export directory has unexpected entries",
        ));
    }
    let provision_path = format!("{export_path}/{PROVISION_MARKER}");
    if client
        .exists(&provision_path)
        .await
        .with_context(|| format!("inspect {provision_path}"))?
    {
        let bytes = read_marker_bounded(client, &provision_path)
            .await
            .map_err(|error| {
                conflict(layout, format!("cannot read {PROVISION_MARKER}: {error}"))
            })?;
        let provision: ProvisionManifest = serde_json::from_slice(&bytes)
            .map_err(|error| conflict(layout, format!("invalid provision marker: {error}")))?;
        if provision != ProvisionManifest::new(layout) {
            return Err(conflict(
                layout,
                "the provision marker has different geometry",
            ));
        }
    }
    for member in &layout.members {
        verify_lane(
            client,
            &format!("{export_path}/{member}"),
            layout.member_size,
        )
        .await
        .map_err(|error| conflict(layout, error))?;
    }
    Ok(())
}

async fn create_manifest_file(client: &Client, path: &str, bytes: &[u8]) -> Result<()> {
    let file = client
        .open(path, OpenOptions::write_only().create_new(true))
        .await
        .with_context(|| format!("create {path}"))?;
    file.write_at(0, bytes)
        .await
        .with_context(|| format!("write {path}"))?;
    file.close().await;
    Ok(())
}

async fn validate_staging(
    client: &Client,
    layout: &StripedLayout,
    staging_path: &str,
) -> Result<()> {
    let allowed = layout
        .members
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(PROVISION_MARKER))
        .chain(std::iter::once(STRIPE_MARKER))
        .collect::<HashSet<_>>();
    let entries = client
        .read_dir(staging_path)
        .await
        .with_context(|| format!("list incomplete NBD staging directory {staging_path}"))?;
    if entries
        .iter()
        .any(|entry| !entry.name_is_utf8 || !allowed.contains(entry.name.as_str()))
    {
        return Err(conflict(
            layout,
            format!("staging directory {staging_path} has unexpected entries"),
        ));
    }
    Ok(())
}

async fn create_unique_staging(client: &Client, _layout: &StripedLayout) -> Result<String> {
    for _ in 0..8 {
        let path = format!("/.nbd/{PROVISION_PREFIX}{}", uuid::Uuid::new_v4());
        match client.create_dir(&path, 0o755).await {
            Ok(_) => return Ok(path),
            Err(ZeroFsError::AlreadyExists { .. }) => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create NBD staging directory {path}"));
            }
        }
    }
    bail!("could not allocate a unique NBD staging directory after 8 attempts")
}

async fn prepare_lanes(
    client: &Client,
    layout: &StripedLayout,
    root_path: &str,
    hooks: &ProvisionHooks,
) -> Result<()> {
    for member in &layout.members {
        let path = format!("{root_path}/{member}");
        loop {
            match client.stat(&path).await {
                Ok(metadata) if metadata.is_file() && metadata.size == layout.member_size => break,
                Ok(metadata) if metadata.is_file() && metadata.size == 0 => {
                    client
                        .truncate(&path, layout.member_size)
                        .await
                        .with_context(|| format!("size striped NBD lane {path}"))?;
                    break;
                }
                Ok(metadata) => {
                    return Err(conflict(
                        layout,
                        format!(
                            "lane {member} has unexpected size {} or type",
                            metadata.size
                        ),
                    ));
                }
                Err(ZeroFsError::NotFound { .. }) => {
                    hooks.before_missing_lane_create().await;
                    match client
                        .open(&path, OpenOptions::write_only().create_new(true))
                        .await
                    {
                        Ok(file) => {
                            let size_result = file.set_len(layout.member_size).await;
                            file.close().await;
                            size_result.with_context(|| format!("size striped NBD lane {path}"))?;
                            break;
                        }
                        Err(ZeroFsError::AlreadyExists { .. }) => continue,
                        Err(error) => {
                            return Err(error)
                                .with_context(|| format!("create striped NBD lane {path}"));
                        }
                    }
                }
                Err(error) => return Err(error).with_context(|| format!("inspect {path}")),
            }
        }
    }
    Ok(())
}

async fn prepare_stripe_manifest(
    client: &Client,
    layout: &StripedLayout,
    staging_path: &str,
    hooks: &ProvisionHooks,
) -> Result<()> {
    let manifest_path = format!("{staging_path}/{STRIPE_MARKER}");
    let manifest = serde_json::to_vec(&layout.manifest()).context("encode stripe manifest")?;
    loop {
        match read_marker_bounded(client, &manifest_path).await {
            Ok(existing) => match serde_json::from_slice::<StripeManifest>(&existing) {
                Ok(existing) if existing == layout.manifest() => return Ok(()),
                Ok(_) => {
                    return Err(conflict(
                        layout,
                        "the staged stripe manifest has different geometry",
                    ));
                }
                Err(error) => {
                    return Err(conflict(
                        layout,
                        format!("the staged stripe manifest is incomplete: {error}"),
                    ));
                }
            },
            Err(ZeroFsError::NotFound { .. }) => {
                hooks.before_missing_stripe_manifest_create().await;
                let temp_path = format!(
                    "{staging_path}/{STRIPE_TEMP_PREFIX}{}",
                    uuid::Uuid::new_v4()
                );
                let file = client
                    .open(&temp_path, OpenOptions::write_only().create_new(true))
                    .await
                    .with_context(|| format!("create temporary stripe manifest {temp_path}"))?;
                let write_result = file.write_at(0, &manifest).await;
                file.close().await;
                write_result
                    .with_context(|| format!("write temporary stripe manifest {temp_path}"))?;
                if let Err(rename_error) =
                    client.rename_no_replace(&temp_path, &manifest_path).await
                {
                    let final_is_exact = read_marker_bounded(client, &manifest_path)
                        .await
                        .ok()
                        .and_then(|bytes| serde_json::from_slice::<StripeManifest>(&bytes).ok())
                        .is_some_and(|existing| existing == layout.manifest());
                    if !final_is_exact {
                        return Err(rename_error)
                            .with_context(|| format!("publish stripe manifest {manifest_path}"));
                    }
                    match client.remove_file(&temp_path).await {
                        Ok(()) | Err(ZeroFsError::NotFound { .. }) => {}
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("remove superseded temporary stripe manifest {temp_path}")
                            });
                        }
                    }
                }
            }
            Err(error) => {
                return Err(error).with_context(|| format!("inspect {manifest_path}"));
            }
        }
    }
}

async fn prepare_staging(
    client: &Client,
    layout: &StripedLayout,
    hooks: &ProvisionHooks,
) -> Result<String> {
    // The successful exclusive mkdir is the ownership boundary for this
    // attempt. Interrupted siblings are never shared with a live caller.
    let staging_path = create_unique_staging(client, layout).await?;
    let provision = serde_json::to_vec(&ProvisionManifest::new(layout))
        .context("encode NBD provision marker")?;
    create_manifest_file(
        client,
        &format!("{staging_path}/{PROVISION_MARKER}"),
        &provision,
    )
    .await?;
    validate_staging(client, layout, &staging_path).await?;
    prepare_lanes(client, layout, &staging_path, hooks).await?;
    prepare_stripe_manifest(client, layout, &staging_path, hooks).await?;
    hooks.before_final_staging_validation().await;
    validate_staging(client, layout, &staging_path).await?;
    client
        .sync()
        .await
        .context("flush completed NBD staging directory")?;
    Ok(staging_path)
}

async fn cleanup_staging(client: &Client, staging_path: &str) -> Result<()> {
    match client.remove_dir_all(staging_path).await {
        Ok(()) | Err(ZeroFsError::NotFound { .. }) => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove superseded NBD draft {staging_path}"))
        }
    }
}

/// Provision a striped export through ZeroFS itself without formatting or attaching it.
pub async fn provision_striped(
    client: &Client,
    layout: &StripedLayout,
) -> Result<ProvisionOutcome> {
    provision_striped_inner(client, layout, &ProvisionHooks::default()).await
}

async fn provision_striped_inner(
    client: &Client,
    layout: &StripedLayout,
    hooks: &ProvisionHooks,
) -> Result<ProvisionOutcome> {
    client
        .create_dir_all("/.nbd", 0o755)
        .await
        .context("create ZeroFS NBD export directory")?;

    if client
        .exists(layout.export_path())
        .await
        .context("check for an existing NBD export")?
    {
        let metadata = client
            .stat(layout.export_path())
            .await
            .context("inspect the existing NBD export")?;
        if !metadata.is_dir() {
            return Err(conflict(layout, "the export path is not a directory"));
        }
        let marker_path = format!("{}/{STRIPE_MARKER}", layout.export_path());
        if client
            .exists(&marker_path)
            .await
            .context("check the existing NBD export marker")?
        {
            verify_complete(client, layout).await?;
            client.sync().await.context("flush existing NBD export")?;
            return Ok(ProvisionOutcome::AlreadyProvisioned);
        }
        return Err(conflict(
            layout,
            "the existing directory is not a completed export; legacy in-place drafts are unsupported",
        ));
    }

    let staging_path = match prepare_staging(client, layout, hooks).await {
        Ok(staging_path) => staging_path,
        Err(prepare_error) => {
            let final_matches = client.exists(layout.export_path()).await.unwrap_or(false)
                && verify_complete(client, layout).await.is_ok();
            if final_matches {
                client
                    .sync()
                    .await
                    .context("flush converged NBD provisioning")?;
                return Ok(ProvisionOutcome::AlreadyProvisioned);
            }
            return Err(prepare_error);
        }
    };
    if client
        .exists(layout.export_path())
        .await
        .context("recheck the final NBD export before publication")?
    {
        verify_complete(client, layout).await?;
        cleanup_staging(client, &staging_path).await?;
        client
            .sync()
            .await
            .context("flush converged NBD provisioning")?;
        return Ok(ProvisionOutcome::AlreadyProvisioned);
    }
    hooks.before_final_publish().await;
    if let Err(rename_error) = client
        .rename_no_replace(&staging_path, layout.export_path())
        .await
    {
        if client
            .exists(layout.export_path())
            .await
            .context("check for a concurrently published NBD export")?
        {
            verify_complete(client, layout).await?;
            cleanup_staging(client, &staging_path).await?;
            client
                .sync()
                .await
                .context("flush converged NBD provisioning")?;
            return Ok(ProvisionOutcome::AlreadyProvisioned);
        }
        return Err(rename_error).context("atomically publish striped NBD export");
    }
    verify_complete(client, layout).await?;
    client
        .sync()
        .await
        .context("flush provisioned NBD export")?;
    Ok(ProvisionOutcome::Created)
}

/// Connect to a 9P endpoint and provision one striped NBD export.
pub async fn run_provision_striped(
    target: &str,
    export_name: &str,
    logical_size: u64,
    lanes: u8,
    stripe_size: u64,
) -> Result<()> {
    let layout = StripedLayout::new(export_name, logical_size, lanes, stripe_size)?;
    let client = Client::connect(target)
        .await
        .with_context(|| format!("connect to ZeroFS 9P endpoint {target}"))?;
    let result = provision_striped(&client, &layout).await;
    client.close().await;
    match result? {
        ProvisionOutcome::Created => println!(
            "Created striped NBD export '{}' ({} bytes, {} lanes of {} bytes, {}-byte stripes)",
            layout.export_name(),
            layout.logical_size(),
            layout.members().len(),
            layout.member_size(),
            layout.stripe_size()
        ),
        ProvisionOutcome::AlreadyProvisioned => println!(
            "Striped NBD export '{}' already matches the requested layout",
            layout.export_name()
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        PROVISION_MARKER, ProvisionHooks, ProvisionOutcome, StripedLayout, create_unique_staging,
        parse_byte_size, prepare_staging, provision_striped, provision_striped_inner,
    };
    use crate::fs::ZeroFS;
    use crate::nbd::NbdExportGates;
    use crate::nbd::handler::NBDHandler;
    use crate::ninep::NinePServer;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Barrier, Notify};
    use tokio_util::sync::CancellationToken;
    use zerofs_client::Client;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    async fn setup() -> (
        Arc<Client>,
        Arc<ZeroFS>,
        CancellationToken,
        tempfile::TempDir,
    ) {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.expect("in-memory filesystem"));
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let socket = directory.path().join("provision.9p.sock");
        let server = NinePServer::new_unix(Arc::clone(&filesystem), socket.clone());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            server.start(server_shutdown).await.expect("test 9P server");
        });

        for _ in 0..100 {
            if socket.exists() {
                let client = Client::connect(&format!("unix:{}", socket.display()))
                    .await
                    .expect("connect provisioning client");
                return (client, filesystem, shutdown, directory);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("test 9P socket was not created");
    }

    #[test]
    fn byte_sizes_are_binary_and_overflow_checked() {
        assert_eq!(parse_byte_size("1048576").unwrap(), MIB);
        assert_eq!(parse_byte_size("1MiB").unwrap(), MIB);
        assert_eq!(parse_byte_size("64GiB").unwrap(), 64 * GIB);
        assert_eq!(parse_byte_size("2TiB").unwrap(), 2 * 1024 * GIB);
        assert!(parse_byte_size("1.5GiB").is_err());
        assert!(parse_byte_size("1GB").is_err());
        assert!(parse_byte_size("18446744073709551615TiB").is_err());
    }

    #[test]
    fn striped_layout_enforces_the_server_geometry() {
        let layout = StripedLayout::new("vm100", 64 * GIB, 4, MIB).unwrap();
        assert_eq!(layout.export_name(), "vm100");
        assert_eq!(layout.logical_size(), 64 * GIB);
        assert_eq!(layout.member_size(), 16 * GIB);
        assert_eq!(layout.stripe_size(), MIB);
        assert_eq!(layout.members(), ["lane-0", "lane-1", "lane-2", "lane-3"]);

        for name in ["", ".", "..", "nested/export"] {
            assert!(StripedLayout::new(name, 64 * GIB, 4, MIB).is_err());
        }
        assert!(StripedLayout::new("vm100", 64 * GIB, 1, MIB).is_err());
        assert!(StripedLayout::new("vm100", 64 * GIB, 33, MIB).is_err());
        assert!(StripedLayout::new("vm100", 64 * GIB, 4, 1000).is_err());
        assert!(StripedLayout::new("vm100", 64 * GIB, 4, 128 * MIB).is_err());
        assert!(StripedLayout::new("vm100", 0, 4, MIB).is_err());
        assert!(StripedLayout::new("vm100", 64 * GIB + 1, 4, MIB).is_err());
    }

    #[test]
    fn striped_layout_rejects_only_the_reserved_provisioning_namespace() {
        assert!(
            StripedLayout::new(
                ".zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000000",
                64 * GIB,
                4,
                MIB,
            )
            .is_err()
        );
        for legitimate in [
            ".zerofs-nbd-provision-v1-archive",
            ".zerofs-nbd-provision-v1-000000000000000000000000000000000000",
            ".zerofs-nbd-provision-v1-00000000-0000-0000-0000-00000000000g",
            ".zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000000-extra",
        ] {
            assert!(
                StripedLayout::new(legitimate, 64 * GIB, 4, MIB).is_ok(),
                "{legitimate} is outside the exact reserved UUID namespace"
            );
        }
    }

    #[tokio::test]
    async fn provision_creates_an_nbd_discoverable_sparse_layout_and_is_idempotent() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        let first = provision_striped(&client, &layout).await.unwrap();
        assert_eq!(first, ProvisionOutcome::Created);

        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        let device = handler
            .get_device(b"vm100")
            .await
            .expect("NBD discovers provisioned export");
        assert_eq!(device.size, 64 * MIB);

        let marker = client
            .read("/.nbd/vm100/.zerofs-nbd-stripe-v1")
            .await
            .unwrap();
        assert_eq!(
            marker.as_ref(),
            br#"{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}"#
        );
        for lane in layout.members() {
            let metadata = client.stat(format!("/.nbd/vm100/{lane}")).await.unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.size, 16 * MIB);
        }

        let second = provision_striped(&client, &layout).await.unwrap();
        assert_eq!(second, ProvisionOutcome::AlreadyProvisioned);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_refuses_a_conflicting_export_without_modifying_it() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd", 0o755).await.unwrap();
        client.write("/.nbd/vm100", b"keep-me").await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        let error = provision_striped(&client, &layout).await.unwrap_err();
        assert!(error.to_string().contains("conflicts"), "{error:#}");
        assert_eq!(client.read("/.nbd/vm100").await.unwrap(), b"keep-me"[..]);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_rejects_a_legacy_final_draft_without_modifying_it() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd/vm100", 0o755).await.unwrap();
        let legacy_marker =
            br#"{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}"#;
        client
            .write("/.nbd/vm100/.zerofs-nbd-provision-v1", legacy_marker)
            .await
            .unwrap();
        client.write("/.nbd/vm100/lane-0", b"").await.unwrap();
        client
            .truncate("/.nbd/vm100/lane-0", 16 * MIB)
            .await
            .unwrap();
        client.write("/.nbd/vm100/lane-1", b"").await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        let error = provision_striped(&client, &layout).await.unwrap_err();
        assert!(
            error.to_string().contains("not a completed export"),
            "{error:#}"
        );
        assert_eq!(
            client
                .read("/.nbd/vm100/.zerofs-nbd-provision-v1")
                .await
                .unwrap(),
            legacy_marker[..]
        );
        assert_eq!(
            client.stat("/.nbd/vm100/lane-0").await.unwrap().size,
            16 * MIB
        );
        assert_eq!(client.stat("/.nbd/vm100/lane-1").await.unwrap().size, 0);
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert!(handler.get_device(b"vm100").await.is_err());
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_never_replaces_an_export_with_different_geometry() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        let original = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        provision_striped(&client, &original).await.unwrap();
        let original_marker = client
            .read("/.nbd/vm100/.zerofs-nbd-stripe-v1")
            .await
            .unwrap();
        let conflicting = StripedLayout::new("vm100", 64 * MIB, 4, 2 * MIB).unwrap();

        let error = provision_striped(&client, &conflicting).await.unwrap_err();
        assert!(
            error.to_string().contains("different geometry"),
            "{error:#}"
        );
        assert_eq!(
            client
                .read("/.nbd/vm100/.zerofs-nbd-stripe-v1")
                .await
                .unwrap(),
            original_marker
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn interrupted_staging_creation_never_exposes_the_final_export_name() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd", 0o755).await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        let staging_path = create_unique_staging(&client, &layout).await.unwrap();

        assert_ne!(staging_path, "/.nbd/vm100");
        assert!(staging_path.starts_with("/.nbd/.zerofs-nbd-provision-v1-"));
        assert!(client.exists(&staging_path).await.unwrap());
        assert!(!client.exists("/.nbd/vm100").await.unwrap());
        shutdown.cancel();
    }

    #[tokio::test]
    async fn interrupted_marker_write_does_not_block_a_fresh_staging_attempt() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd", 0o755).await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let abandoned = create_unique_staging(&client, &layout).await.unwrap();
        client
            .write(format!("{abandoned}/{PROVISION_MARKER}"), b"")
            .await
            .unwrap();

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        assert!(client.exists(&abandoned).await.unwrap());
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_leaves_an_interrupted_sibling_staging_directory_untouched() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let staging = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000001";
        client.create_dir_all(staging, 0o755).await.unwrap();
        client
            .write(
                format!("{staging}/.zerofs-nbd-provision-v1"),
                br#"{"version":1,"export_name":"vm100","layout":{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}}"#,
            )
            .await
            .unwrap();
        client
            .write(format!("{staging}/lane-0"), b"")
            .await
            .unwrap();
        client
            .truncate(format!("{staging}/lane-0"), 16 * MIB)
            .await
            .unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        assert!(client.exists(staging).await.unwrap());
        assert_eq!(
            client.stat(format!("{staging}/lane-0")).await.unwrap().size,
            16 * MIB
        );
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_leaves_a_partial_stripe_draft_and_uses_a_fresh_one() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let staging = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000002";
        client.create_dir_all(staging, 0o755).await.unwrap();
        client
            .write(
                format!("{staging}/{PROVISION_MARKER}"),
                br#"{"version":1,"export_name":"vm100","layout":{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}}"#,
            )
            .await
            .unwrap();
        client
            .write(
                format!("{staging}/.zerofs-nbd-stripe-v1"),
                br#"{"version":1,"stripe_bytes":1048576,"members":["lane-0""#,
            )
            .await
            .unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        assert!(client.exists(staging).await.unwrap());
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn concurrent_identical_provisioning_converges_without_staging_drafts() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        const CALLERS: usize = 8;
        let barrier = Arc::new(Barrier::new(CALLERS));
        let mut tasks = Vec::new();
        for _ in 0..CALLERS {
            let client = Arc::clone(&client);
            let layout = layout.clone();
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                provision_striped(&client, &layout).await
            }));
        }

        let mut created = 0;
        for task in tasks {
            match task
                .await
                .expect("provisioning task did not panic")
                .expect("identical provisioning caller converged")
            {
                ProvisionOutcome::Created => created += 1,
                ProvisionOutcome::AlreadyProvisioned => {}
            }
        }
        assert!(created >= 1);

        let entries = client.read_dir("/.nbd").await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["vm100"]
        );
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provisioning_leaves_all_unowned_drafts_untouched() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd", 0o755).await.unwrap();
        let provision = br#"{"version":1,"export_name":"vm100","layout":{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}}"#;
        let first = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000003";
        let second = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000004";
        for draft in [first, second] {
            client.create_dir(draft, 0o755).await.unwrap();
            client
                .write(format!("{draft}/{PROVISION_MARKER}"), provision)
                .await
                .unwrap();
        }
        let unsafe_partial = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000005";
        client.create_dir(unsafe_partial, 0o755).await.unwrap();
        client
            .write(format!("{unsafe_partial}/{PROVISION_MARKER}"), b"{")
            .await
            .unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        assert!(client.exists(first).await.unwrap());
        assert!(client.exists(second).await.unwrap());
        assert!(client.exists(unsafe_partial).await.unwrap());
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn concurrent_preparation_uses_separate_owned_staging_directories() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd", 0o755).await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let client = Arc::clone(&client);
            let layout = layout.clone();
            tasks.push(tokio::spawn(async move {
                prepare_staging(&client, &layout, &ProvisionHooks::default()).await
            }));
        }

        let mut staging_paths = Vec::new();
        for task in tasks {
            staging_paths.push(
                task.await
                    .expect("staging task did not panic")
                    .expect("owned draft preparation succeeded"),
            );
        }
        assert_ne!(staging_paths[0], staging_paths[1]);
        for staging in staging_paths {
            for lane in layout.members() {
                assert_eq!(
                    client.stat(format!("{staging}/{lane}")).await.unwrap().size,
                    16 * MIB
                );
            }
        }
        shutdown.cancel();
    }

    #[tokio::test]
    async fn callers_forced_to_create_separate_drafts_converge_and_clean_their_own() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let unsafe_partial = "/.nbd/.zerofs-nbd-provision-v1-00000000-0000-0000-0000-000000000007";
        client.create_dir_all(unsafe_partial, 0o755).await.unwrap();
        client
            .write(format!("{unsafe_partial}/{PROVISION_MARKER}"), b"{")
            .await
            .unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let client = Arc::clone(&client);
            let layout = layout.clone();
            tasks.push(tokio::spawn(async move {
                provision_striped_inner(&client, &layout, &ProvisionHooks::default()).await
            }));
        }

        let outcomes = futures::future::try_join_all(tasks)
            .await
            .expect("provisioning tasks did not panic")
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("separate draft callers converged");
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == ProvisionOutcome::Created)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == ProvisionOutcome::AlreadyProvisioned)
                .count(),
            1
        );
        let entries = client.read_dir("/.nbd").await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec![unsafe_partial.rsplit('/').next().unwrap(), "vm100"]
        );
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn final_validation_rejects_a_temporary_stripe_marker_before_publication() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let validation_reached = Arc::new(Barrier::new(2));
        let resume_validation = Arc::new(Notify::new());
        let task_client = Arc::clone(&client);
        let task_layout = layout.clone();
        let hooks = ProvisionHooks {
            before_final_staging_validation: Some(Arc::clone(&validation_reached)),
            resume_final_staging_validation: Some(Arc::clone(&resume_validation)),
            ..Default::default()
        };
        let task = tokio::spawn(async move {
            provision_striped_inner(&task_client, &task_layout, &hooks).await
        });
        validation_reached.wait().await;

        let staging = client
            .read_dir("/.nbd")
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.name.starts_with(".zerofs-nbd-provision-v1-"))
            .expect("provisioning created a staging directory")
            .name;
        let staging = format!("/.nbd/{staging}");
        client
            .write(
                format!("{staging}/.zerofs-nbd-stripe-v1-injected"),
                b"not publishable",
            )
            .await
            .unwrap();
        resume_validation.notify_one();

        let error = task
            .await
            .expect("provisioning task did not panic")
            .unwrap_err();
        assert!(
            error.to_string().contains("unexpected entries"),
            "{error:#}"
        );
        assert!(!client.exists("/.nbd/vm100").await.unwrap());
        assert!(client.exists(&staging).await.unwrap());
        shutdown.cancel();
    }

    #[tokio::test]
    async fn final_publish_never_replaces_a_racing_destination() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let publish_reached = Arc::new(Barrier::new(2));
        let resume_publish = Arc::new(Notify::new());
        let task_client = Arc::clone(&client);
        let task_layout = layout.clone();
        let hooks = ProvisionHooks {
            before_final_publish: Some(Arc::clone(&publish_reached)),
            resume_final_publish: Some(Arc::clone(&resume_publish)),
            ..Default::default()
        };
        let task = tokio::spawn(async move {
            provision_striped_inner(&task_client, &task_layout, &hooks).await
        });
        publish_reached.wait().await;

        client.create_dir("/.nbd/vm100", 0o700).await.unwrap();
        let racing_inode = client.stat("/.nbd/vm100").await.unwrap().ino;
        resume_publish.notify_one();

        let error = task
            .await
            .expect("provisioning task did not panic")
            .unwrap_err();
        assert!(error.to_string().contains("conflicts"), "{error:#}");
        let final_metadata = client.stat("/.nbd/vm100").await.unwrap();
        assert_eq!(final_metadata.ino, racing_inode);
        assert_eq!(final_metadata.permissions(), 0o700);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn publish_loser_converges_only_on_an_exact_concurrent_winner() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        let publish_reached = Arc::new(Barrier::new(2));
        let resume_publish = Arc::new(Notify::new());
        let task_client = Arc::clone(&client);
        let task_layout = layout.clone();
        let hooks = ProvisionHooks {
            before_final_publish: Some(Arc::clone(&publish_reached)),
            resume_final_publish: Some(Arc::clone(&resume_publish)),
            ..Default::default()
        };
        let task = tokio::spawn(async move {
            provision_striped_inner(&task_client, &task_layout, &hooks).await
        });
        publish_reached.wait().await;

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        let winner_inode = client.stat("/.nbd/vm100").await.unwrap().ino;
        resume_publish.notify_one();

        assert_eq!(
            task.await
                .expect("provisioning task did not panic")
                .expect("loser verified the exact winner"),
            ProvisionOutcome::AlreadyProvisioned
        );
        assert_eq!(client.stat("/.nbd/vm100").await.unwrap().ino, winner_inode);
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provisioning_rejects_an_oversized_stripe_marker() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        provision_striped(&client, &layout).await.unwrap();
        let marker_path = "/.nbd/vm100/.zerofs-nbd-stripe-v1";
        let mut oversized = client.read(marker_path).await.unwrap().to_vec();
        oversized.resize(4097, b' ');
        client.write(marker_path, &oversized).await.unwrap();

        let error = provision_striped(&client, &layout).await.unwrap_err();
        assert!(error.to_string().contains("too large"), "{error:#}");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provisioning_rejects_an_oversized_provision_marker() {
        let (client, _filesystem, shutdown, _directory) = setup().await;
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();
        provision_striped(&client, &layout).await.unwrap();
        let marker_path = "/.nbd/vm100/.zerofs-nbd-provision-v1";
        let mut oversized = client.read(marker_path).await.unwrap().to_vec();
        oversized.resize(4097, b' ');
        client.write(marker_path, &oversized).await.unwrap();

        let error = provision_striped(&client, &layout).await.unwrap_err();
        assert!(error.to_string().contains("too large"), "{error:#}");
        shutdown.cancel();
    }
}
