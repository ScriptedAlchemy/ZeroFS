//! Offline administration holds the same catalog lock as the server.

use super::{PoolCommands, finish_with_sftp_cleanup};
use crate::config::Settings;
use crate::storage_backend::open_pool_storage;
use anyhow::{Context, Result};
use object_store::path::Path;

pub async fn run(command: PoolCommands) -> Result<()> {
    let endpoint = match &command {
        PoolCommands::Affected { endpoint, .. }
        | PoolCommands::FilePlacement { endpoint, .. }
        | PoolCommands::Status {
            endpoint: Some(endpoint),
            ..
        } => Some(endpoint),
        _ => None,
    };
    if let Some(endpoint) = endpoint {
        let mut client = if let Some(path) = endpoint.strip_prefix("unix:") {
            crate::rpc::client::RpcClient::connect_unix(path.into()).await?
        } else {
            crate::rpc::client::RpcClient::connect_tcp(
                endpoint
                    .parse()
                    .context("endpoint must be unix:/path or IP:port")?,
            )
            .await?
        };
        let report = match command {
            PoolCommands::Affected { member, .. } => client.pool_affected(member).await?,
            PoolCommands::FilePlacement { path, .. } => {
                use std::os::unix::ffi::OsStringExt;
                client
                    .file_placement(path.into_os_string().into_vec())
                    .await?
            }
            PoolCommands::Status { .. } => client.pool_status().await?,
            _ => unreachable!(),
        };
        println!("{report}");
        return Ok(());
    }
    let config = match &command {
        PoolCommands::Affected { .. } | PoolCommands::FilePlacement { .. } => unreachable!(),
        PoolCommands::VerifyRaw { config }
        | PoolCommands::Init { config }
        | PoolCommands::List { config }
        | PoolCommands::Migrate { config, .. }
        | PoolCommands::Verify { config, .. } => config,
        PoolCommands::Status { config, .. } => {
            config.as_ref().context("--config required offline")?
        }
    };
    let settings = Settings::from_runtime_file(config)?;
    let verify = matches!(command, PoolCommands::VerifyRaw { .. });
    anyhow::ensure!(
        !verify || settings.raw_device_p3_nas_fit(),
        "pool verify-raw requires the NAS-fit raw-device profile"
    );
    let admission = if settings.raw_device_p3_nas_fit() {
        Some(
            super::memory_budget::validate_server_startup(&settings)?
                .context("raw pool maintenance requires finite cgroup admission")?,
        )
    } else {
        None
    };
    let initialize = matches!(command, PoolCommands::Init { .. });
    let (mut opened, pool) = open_pool_storage(&settings, initialize)
        .await
        .context("cannot open storage pool; stop the ZeroFS server before maintenance")?;
    let sessions = opened.sftp_pool.clone();
    let result: Result<()> = async {
        let journal = if admission.is_some() && (initialize || verify) {
            let region = opened.raw_journal.take().context("missing held raw journal region")?;
            Some(super::init::raw_pool_journal(&settings, &pool, &opened.path, region, initialize).await?)
        } else { None };
        match command {
            PoolCommands::VerifyRaw { .. } => {
                let journal = journal.context("raw journal was not verified")?;
                let progress = journal.progress()?;
                let allocation = tokio::task::spawn_blocking(move || journal.raw_allocation()).await??;
                let status = pool.status().await?;
                println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                    "schema_version": 1, "profile": "raw-device-p3-nas-fit-v1",
                    "pool_id": status.pool_id, "members": status.members,
                    "authority_verified": true, "catalog_verified": true, "key_envelope_verified": true,
                    "journal_verified": true, "local_seq": progress.local_seq, "remote_seq": progress.remote_seq,
                    "journal_allocation": allocation, "memory": admission
                }))?);
            }
            PoolCommands::Affected { .. } | PoolCommands::FilePlacement { .. } => unreachable!(),
            PoolCommands::Init { .. } | PoolCommands::Status { .. } => {
                println!("{}", serde_json::to_string_pretty(&pool.status().await?)?);
            }
            PoolCommands::List { .. } => {
                println!("{}", serde_json::to_string_pretty(&pool.placements().await?)?);
            }
            PoolCommands::Verify { member, .. } => {
                let report = pool.verify_member(&member).await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                if !report.failures.is_empty() {
                    anyhow::bail!("member {member} failed verification for {} segment(s)", report.failures.len());
                }
            }
            PoolCommands::Migrate { to, from, segment, limit, .. } => {
                let locations = if let Some(segment) = segment {
                    vec![Path::parse(segment).context("invalid segment object path")?]
                } else {
                    let from = from.context("--from or --segment is required")?;
                    if from == to {
                        anyhow::bail!("source and destination members must differ");
                    }
                    pool.placements().await?.into_iter()
                        .filter(|placement| placement.member_id == from)
                        .take(limit)
                        .map(|placement| placement.location)
                        .collect()
                };
                let mut migrated = 0usize;
                for location in locations {
                    pool.migrate(&location, &to).await
                        .with_context(|| format!("migration failed after {migrated} completed segments; source retained unless placement was committed"))?;
                    migrated += 1;
                    eprintln!("Moved {location} to {to}");
                }
                println!("{}", serde_json::json!({ "migrated_segments": migrated, "target_member": to }));
            }
        }
        Ok(())
    }.await;
    drop(pool);
    drop(opened);
    finish_with_sftp_cleanup(
        sessions.as_ref(),
        "Failed to shut down storage pool transports",
        result,
    )
    .await
}
