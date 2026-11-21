//! `zerofs survival status` and `zerofs survival recover`.

use super::SurvivalCommands;
use crate::config::Settings;
use crate::metadata_survival::{self, recover};
use anyhow::{Context, Result};
use std::io::BufRead;

pub async fn run(command: SurvivalCommands) -> Result<()> {
    match command {
        SurvivalCommands::Status { config } => {
            let settings = Settings::from_file(&config)?;
            let status = metadata_survival::offline_status(&settings).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            Ok(())
        }
        SurvivalCommands::Recover {
            config: Some(config),
            allow_missing,
            ..
        } => {
            let settings = Settings::from_file(&config)?;
            let report = recover::rebuild_raw_p3(
                &settings,
                &settings.storage.encryption_password,
                &allow_missing,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        SurvivalCommands::Recover {
            config: None,
            replicas,
            scratch,
            keep,
            ..
        } => {
            let scratch =
                scratch.context("recovery needs --config, or --replica with --scratch")?;
            eprintln!("Reading the encryption password from stdin...");
            let mut password = String::new();
            std::io::stdin()
                .lock()
                .read_line(&mut password)
                .context("read the encryption password from stdin")?;
            let password = password.trim_end_matches(['\r', '\n']).to_owned();
            let roots = replicas
                .into_iter()
                .map(|dir| dir.join(metadata_survival::format::ROOT))
                .collect();
            let recovered = recover::recover(recover::RecoverySources {
                replicas: roots,
                password,
                scratch_parent: scratch,
            })
            .await?;
            let recover::Recovered {
                fs,
                pool,
                scratch,
                report,
                ..
            } = recovered;
            println!("{}", serde_json::to_string_pretty(&report)?);
            fs.stop_new_mutation_admission();
            drop(fs);
            pool.settle().await;
            if keep {
                let kept = scratch.keep();
                eprintln!("Recovered generation kept read-only at {}", kept.display());
            } else {
                scratch.close().context("remove recovery scratch")?;
            }
            Ok(())
        }
    }
}
