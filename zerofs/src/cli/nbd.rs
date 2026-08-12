use crate::nbd::{
    NBD_STRIPE_MARKER as STRIPE_MARKER, NBD_STRIPE_MAX_BYTES as MAX_STRIPE_BYTES,
    NBD_STRIPE_MAX_MEMBERS, NBD_STRIPE_MIN_BYTES as MIN_STRIPE_BYTES, StripeManifest,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use zerofs_client::{Client, OpenOptions, ZeroFsError};

const PROVISION_MARKER: &str = ".zerofs-nbd-provision-v1";
const PROVISION_PREFIX: &str = ".zerofs-nbd-provision-v1-";

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
        {
            bail!("export name must be a non-empty direct child name of at most 255 bytes");
        }
        if usize::from(lanes) < 2 || usize::from(lanes) > NBD_STRIPE_MAX_MEMBERS {
            bail!("lane count must be in 2..={NBD_STRIPE_MAX_MEMBERS}");
        }
        if !stripe_size.is_power_of_two()
            || !(MIN_STRIPE_BYTES..=MAX_STRIPE_BYTES).contains(&stripe_size)
        {
            bail!(
                "stripe size must be a power of two between {MIN_STRIPE_BYTES} and {MAX_STRIPE_BYTES} bytes"
            );
        }
        let layout_unit = stripe_size
            .checked_mul(u64::from(lanes))
            .context("lane count times stripe size overflows u64")?;
        if logical_size == 0 || !logical_size.is_multiple_of(layout_unit) {
            bail!(
                "logical size must be non-zero and divisible by lanes times stripe size ({layout_unit} bytes)"
            );
        }
        let member_size = logical_size / u64::from(lanes);
        let members = (0..lanes).map(|lane| format!("lane-{lane}")).collect();
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
    let marker = client
        .read(&marker_path)
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
        let bytes = client
            .read(&provision_path)
            .await
            .with_context(|| format!("read {provision_path}"))?;
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

async fn read_provision_manifest(
    client: &Client,
    staging_path: &str,
) -> Result<Option<ProvisionManifest>> {
    let marker_path = format!("{staging_path}/{PROVISION_MARKER}");
    match client.read(&marker_path).await {
        // A crash can leave a marker inode before its short payload reaches
        // ZeroFS. It cannot identify a resumable draft, so leave that unique
        // staging directory untouched and let this attempt use a fresh one.
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(ZeroFsError::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read staging marker {marker_path}")),
    }
}

async fn find_matching_staging(client: &Client, layout: &StripedLayout) -> Result<Option<String>> {
    let mut matching = None;
    for entry in client
        .read_dir("/.nbd")
        .await
        .context("list ZeroFS NBD directory for interrupted provisioning")?
    {
        if !entry.metadata.is_dir()
            || !entry.name_is_utf8
            || !entry.name.starts_with(PROVISION_PREFIX)
        {
            continue;
        }
        let path = format!("/.nbd/{}", entry.name);
        let Some(provision) = read_provision_manifest(client, &path).await? else {
            continue;
        };
        if provision.export_name != layout.export_name {
            continue;
        }
        if provision.version != 1 {
            return Err(conflict(
                layout,
                format!(
                    "staging directory {path} uses unsupported provision version {}",
                    provision.version
                ),
            ));
        }
        if provision.layout != layout.manifest() {
            return Err(conflict(
                layout,
                format!("staging directory {path} has different geometry"),
            ));
        }
        validate_staging(client, layout, &path).await?;
        if matching.replace(path).is_some() {
            return Err(conflict(
                layout,
                "multiple matching staging directories require manual inspection",
            ));
        }
    }
    Ok(matching)
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

async fn prepare_lanes(client: &Client, layout: &StripedLayout, root_path: &str) -> Result<()> {
    for member in &layout.members {
        let path = format!("{root_path}/{member}");
        match client.stat(&path).await {
            Ok(metadata) if metadata.is_file() && metadata.size == layout.member_size => {}
            Ok(metadata) if metadata.is_file() && metadata.size == 0 => {
                client
                    .truncate(&path, layout.member_size)
                    .await
                    .with_context(|| format!("size striped NBD lane {path}"))?;
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
                let file = client
                    .open(&path, OpenOptions::write_only().create_new(true))
                    .await
                    .with_context(|| format!("create striped NBD lane {path}"))?;
                file.set_len(layout.member_size)
                    .await
                    .with_context(|| format!("size striped NBD lane {path}"))?;
                file.close().await;
            }
            Err(error) => return Err(error).with_context(|| format!("inspect {path}")),
        }
    }
    Ok(())
}

async fn prepare_staging(client: &Client, layout: &StripedLayout) -> Result<String> {
    let staging_path = match find_matching_staging(client, layout).await? {
        Some(path) => path,
        None => {
            let path = create_unique_staging(client, layout).await?;
            let provision = serde_json::to_vec(&ProvisionManifest::new(layout))
                .context("encode NBD provision marker")?;
            create_manifest_file(client, &format!("{path}/{PROVISION_MARKER}"), &provision).await?;
            path
        }
    };
    validate_staging(client, layout, &staging_path).await?;
    prepare_lanes(client, layout, &staging_path).await?;

    let manifest_path = format!("{staging_path}/{STRIPE_MARKER}");
    let manifest = serde_json::to_vec(&layout.manifest()).context("encode stripe manifest")?;
    match client.read(&manifest_path).await {
        Ok(existing) => match serde_json::from_slice::<StripeManifest>(&existing) {
            Ok(existing) if existing == layout.manifest() => {}
            Ok(_) => {
                return Err(conflict(
                    layout,
                    "the staged stripe manifest has different geometry",
                ));
            }
            Err(_) => {
                // `create_manifest_file` creates the inode before writing its
                // payload. A crash can therefore leave a recognizable, valid
                // provision draft with a truncated stripe marker. That marker
                // was never publishable, so recreate it from the durable
                // provision geometry before the atomic directory rename.
                client
                    .remove_file(&manifest_path)
                    .await
                    .with_context(|| format!("remove partial stripe manifest {manifest_path}"))?;
                create_manifest_file(client, &manifest_path, &manifest).await?;
            }
        },
        Err(ZeroFsError::NotFound { .. }) => {
            create_manifest_file(client, &manifest_path, &manifest).await?
        }
        Err(error) => return Err(error).with_context(|| format!("inspect {manifest_path}")),
    }
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

async fn resume_legacy_final_draft(client: &Client, layout: &StripedLayout) -> Result<()> {
    let export_path = layout.export_path();
    let draft_path = format!("{export_path}/{PROVISION_MARKER}");
    let draft = client.read(&draft_path).await.map_err(|error| {
        conflict(
            layout,
            format!("existing directory is not a recognized export or legacy draft: {error}"),
        )
    })?;
    if decode_manifest(&draft, layout)? != layout.manifest() {
        return Err(conflict(
            layout,
            "the legacy provision marker has different geometry",
        ));
    }
    let allowed = layout
        .members
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(PROVISION_MARKER))
        .collect::<HashSet<_>>();
    let entries = client
        .read_dir(&export_path)
        .await
        .with_context(|| format!("list legacy draft {export_path}"))?;
    if entries
        .iter()
        .any(|entry| !entry.name_is_utf8 || !allowed.contains(entry.name.as_str()))
    {
        return Err(conflict(layout, "the legacy draft has unexpected entries"));
    }
    prepare_lanes(client, layout, &export_path).await?;
    client
        .sync()
        .await
        .context("flush completed legacy NBD draft")?;
    client
        .rename(&draft_path, format!("{export_path}/{STRIPE_MARKER}"))
        .await
        .context("publish legacy striped NBD manifest")?;
    Ok(())
}

/// Provision a striped export through ZeroFS itself without formatting or attaching it.
pub async fn provision_striped(
    client: &Client,
    layout: &StripedLayout,
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
        resume_legacy_final_draft(client, layout).await?;
        verify_complete(client, layout).await?;
        client
            .sync()
            .await
            .context("flush migrated legacy NBD export")?;
        return Ok(ProvisionOutcome::Created);
    }

    let staging_path = prepare_staging(client, layout).await?;
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
    if let Err(rename_error) = client.rename(&staging_path, layout.export_path()).await {
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
        PROVISION_MARKER, ProvisionOutcome, StripedLayout, create_unique_staging, parse_byte_size,
        provision_striped,
    };
    use crate::fs::ZeroFS;
    use crate::nbd::NbdExportGates;
    use crate::nbd::handler::NBDHandler;
    use crate::ninep::NinePServer;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;
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
    async fn provision_resumes_only_its_matching_draft_layout() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        client.create_dir_all("/.nbd/vm100", 0o755).await.unwrap();
        client
            .write(
                "/.nbd/vm100/.zerofs-nbd-provision-v1",
                br#"{"version":1,"stripe_bytes":1048576,"members":["lane-0","lane-1","lane-2","lane-3"]}"#,
            )
            .await
            .unwrap();
        client.write("/.nbd/vm100/lane-0", b"").await.unwrap();
        client
            .truncate("/.nbd/vm100/lane-0", 16 * MIB)
            .await
            .unwrap();
        client.write("/.nbd/vm100/lane-1", b"").await.unwrap();
        let layout = StripedLayout::new("vm100", 64 * MIB, 4, MIB).unwrap();

        assert_eq!(
            provision_striped(&client, &layout).await.unwrap(),
            ProvisionOutcome::Created
        );
        assert!(
            !client
                .exists("/.nbd/vm100/.zerofs-nbd-provision-v1")
                .await
                .unwrap()
        );
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
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
    async fn provision_recovers_a_matching_sibling_staging_directory() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let staging = "/.nbd/.zerofs-nbd-provision-v1-interrupted";
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
        assert!(!client.exists(staging).await.unwrap());
        let handler = NBDHandler::new(filesystem, Arc::new(NbdExportGates::default()));
        assert_eq!(handler.get_device(b"vm100").await.unwrap().size, 64 * MIB);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn provision_recovers_a_valid_draft_with_a_partial_stripe_marker() {
        let (client, filesystem, shutdown, _directory) = setup().await;
        let staging = "/.nbd/.zerofs-nbd-provision-v1-partial-manifest";
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
        assert!(!client.exists(staging).await.unwrap());
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
}
