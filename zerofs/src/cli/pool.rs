//! Offline administration holds the same catalog lock as the server.

use super::{PoolCommands, finish_with_sftp_cleanup};
use crate::config::Settings;
use crate::storage_backend::open_pool_storage;
use anyhow::{Context, Result};
use object_store::path::Path;

pub async fn run(command: PoolCommands) -> Result<()> {
    if let PoolCommands::Affected { endpoint, member } = &command {
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
        println!("{}", client.pool_affected(member.clone()).await?);
        return Ok(());
    }
    if let PoolCommands::Status {
        endpoint: Some(endpoint),
        ..
    } = &command
    {
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
        println!("{}", client.pool_status().await?);
        return Ok(());
    }
    let config = match &command {
        PoolCommands::Affected { .. } => unreachable!(),
        PoolCommands::Init { config }
        | PoolCommands::List { config }
        | PoolCommands::Migrate { config, .. } => config,
        PoolCommands::Status { config, .. } => {
            config.as_ref().context("--config required offline")?
        }
    };
    let settings = Settings::from_file(config)?;
    let initialize = matches!(command, PoolCommands::Init { .. });
    let (opened, pool) = open_pool_storage(&settings, initialize)
        .await
        .context("cannot open storage pool; stop the ZeroFS server before maintenance")?;
    let sessions = opened.sftp_pool.clone();
    let result: Result<()> = async {
        match command {
            PoolCommands::Affected { .. } => unreachable!(),
            PoolCommands::Init { .. } | PoolCommands::Status { .. } => {
                println!("{}", serde_json::to_string_pretty(&pool.status().await?)?);
            }
            PoolCommands::List { .. } => {
                println!("{}", serde_json::to_string_pretty(&pool.placements().await?)?);
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
