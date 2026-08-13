use crate::config::Settings;
use crate::rpc::client::RpcClient;
use crate::sftp_transport::SftpSessionPool;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

pub mod checkpoint;
pub mod debug;
pub mod fatrace;
pub mod flush;
mod init;
pub mod monitor;
pub mod nbd;
pub mod otrace;
pub mod password;
pub mod server;

#[derive(Parser)]
#[command(name = "zerofs")]
#[command(author, version, about = "The Filesystem That Makes S3 your Primary Storage", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Generate a default configuration file
    Init {
        /// Output path for the config file, or "-" to write to stdout
        #[arg(default_value = "zerofs.toml")]
        path: PathBuf,
    },
    /// Run the filesystem server
    Run {
        #[arg(short, long)]
        config: PathBuf,
        /// Open the filesystem in read-only mode
        #[arg(long, conflicts_with = "checkpoint")]
        read_only: bool,
        /// Open from a specific checkpoint by name (read-only mode)
        #[arg(long, conflicts_with = "read_only")]
        checkpoint: Option<String>,
    },
    /// Change the encryption password
    ///
    /// Reads new password from stdin. Examples:
    ///
    /// echo "newpassword" | zerofs change-password -c config.toml
    ///
    /// zerofs change-password -c config.toml < password.txt
    ChangePassword {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Debug commands for inspecting the database
    Debug {
        #[command(subcommand)]
        subcommand: DebugCommands,
    },
    /// Checkpoint management commands
    Checkpoint {
        #[command(subcommand)]
        subcommand: CheckpointCommands,
    },
    /// Trace file system operations in real-time
    Fatrace {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Trace object store requests in real-time
    Otrace {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Flush pending writes to storage
    Flush {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Monitor filesystem activity in real-time
    Monitor {
        #[arg(short, long)]
        config: PathBuf,
        /// Stats refresh interval in milliseconds
        #[arg(long, default_value = "250")]
        interval: u32,
    },
    /// Manage Network Block Device exports
    Nbd {
        #[command(subcommand)]
        subcommand: NbdCommands,
    },
    /// Mount a ZeroFS 9P export as a local filesystem (FUSE client)
    ///
    /// Connects to a running ZeroFS 9P server and exposes it at a local mount
    /// point. The server may be local or remote. Examples:
    ///
    /// zerofs mount 127.0.0.1:5564 /mnt/zerofs
    ///
    /// zerofs mount unix:/tmp/zerofs.9p.sock /mnt/zerofs
    #[cfg(target_os = "linux")]
    Mount {
        /// 9P server address: host[:port], tcp://host:port, or unix:/path/to.sock
        target: String,
        /// Local directory to mount at
        mountpoint: PathBuf,
        /// Mount read-only
        #[arg(long)]
        read_only: bool,
        /// Who may access the mount: `owner` (only the mounting user), `root`
        /// (owner + root), or `all` (any user). `root`/`all` need
        /// `user_allow_other` in /etc/fuse.conf unless mounting as root.
        #[arg(long, value_enum, default_value_t = crate::mount::MountAccess::Owner)]
        access: crate::mount::MountAccess,
        /// Maximum 9P message size in bytes
        #[arg(long, default_value_t = 10 * 1024 * 1024)]
        msize: u32,
        /// Use a writeback page cache: writes are buffered and flushed
        /// asynchronously (higher throughput, looser cross-client coherence).
        /// Pass `--writeback false` to write through synchronously instead.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        writeback: bool,
        /// Allow consistency-relaxing client/kernel caches for speed: the
        /// open+read prefetch fold, cached symlink targets, a 1s attribute cache,
        /// and page-cached reads. Pass `--relaxed-consistency false` for strict
        /// consistency, where every read and lookup hits the server (direct I/O,
        /// no attribute cache) and writes are synchronous (implies write-through).
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        relaxed_consistency: bool,
        /// Root the mount at this server-side directory (a path from the
        /// filesystem root, e.g. /volumes/pvc-1) instead of the whole
        /// filesystem. The directory must already exist.
        #[arg(long)]
        aname: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum NbdCommands {
    /// Create a striped sparse NBD export through a ZeroFS 9P endpoint
    ///
    /// This only provisions the export. It does not format a filesystem,
    /// attach an NBD client, or replace a conflicting existing export.
    ProvisionStriped {
        /// 9P server address: host[:port], tcp://host:port, or unix:/path/to.sock
        target: String,
        /// Export name presented to NBD clients
        export: String,
        /// Logical device size (integer bytes or B/KiB/MiB/GiB/TiB)
        #[arg(long, value_parser = nbd::parse_byte_size)]
        size: u64,
        /// Number of sparse backing lanes (2-32)
        #[arg(long, default_value_t = 4)]
        lanes: u8,
        /// Bytes per lane before rotating (power of two, 4KiB-64MiB)
        #[arg(long, default_value = "256KiB", value_parser = nbd::parse_byte_size)]
        stripe_size: u64,
    },
}

#[derive(Subcommand)]
pub enum DebugCommands {
    /// List all keys in the database
    ListKeys {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Explicitly repair one pruned writeback predecessor from remote HEAD.
    ///
    /// The ZeroFS server must be stopped. This command is intended only for
    /// upgrading journals created before remote predecessor ETags were retained.
    ReseedWritebackPredecessor {
        #[arg(short, long)]
        config: PathBuf,
        /// Exact writeback journal namespace containing journal.redb.
        #[arg(long)]
        journal: PathBuf,
        /// Full object-store path reported by the terminal replay error.
        #[arg(long)]
        path: String,
        /// Pruned local sequence reported by the terminal replay error.
        #[arg(long)]
        sequence: u64,
    },
}

#[derive(Subcommand)]
pub enum CheckpointCommands {
    /// Create a new checkpoint
    Create {
        #[arg(short, long)]
        config: PathBuf,
        /// Name for the checkpoint (must be unique)
        name: String,
    },
    /// List all checkpoints
    List {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Delete a checkpoint by name
    Delete {
        #[arg(short, long)]
        config: PathBuf,
        /// Checkpoint name to delete
        name: String,
    },
    /// Get checkpoint information
    Info {
        #[arg(short, long)]
        config: PathBuf,
        /// Checkpoint name to query
        name: String,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

pub async fn connect_rpc_client(config_path: &Path) -> Result<RpcClient> {
    let settings = Settings::from_file(config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let rpc_config = settings
        .servers
        .rpc
        .as_ref()
        .context("RPC server not configured in config file")?;

    RpcClient::connect_from_config(rpc_config)
        .await
        .context("Failed to connect to RPC server. Is the server running?")
}

/// An operation's own failure, carrying the cleanup failures that followed it.
/// The primary error stays the source so `{:#}` reports what actually went
/// wrong first and treats the cleanup failures as trailing detail.
#[derive(Debug)]
struct PrimaryErrorWithCleanup {
    primary: anyhow::Error,
    cleanup: Vec<anyhow::Error>,
}

impl std::fmt::Display for PrimaryErrorWithCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.primary)?;
        for error in &self.cleanup {
            write!(formatter, "; cleanup also failed: {error:#}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PrimaryErrorWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

pub(crate) fn attach_cleanup_errors(
    primary: anyhow::Error,
    cleanup: Vec<anyhow::Error>,
) -> anyhow::Error {
    if cleanup.is_empty() {
        primary
    } else {
        anyhow::Error::new(PrimaryErrorWithCleanup { primary, cleanup })
    }
}

/// Shut down an SFTP session pool (when one exists) and fold its cleanup failure
/// into `result`: the primary error always wins and the cleanup failure rides
/// along as attached detail, a cleanup-only failure becomes the error, and when
/// both succeed the value is returned.
///
/// `cleanup_context` labels the cleanup failure with the caller's own wording.
/// Callers that must only clean up on failure keep that shape by calling this
/// with an already-`Err` result.
pub(crate) async fn finish_with_sftp_cleanup<T>(
    pool: Option<&SftpSessionPool>,
    cleanup_context: &'static str,
    result: Result<T>,
) -> Result<T> {
    let cleanup = match pool {
        Some(pool) => pool.shutdown().await.context(cleanup_context).err(),
        None => None,
    };

    match (result, cleanup) {
        (Ok(value), None) => Ok(value),
        (Ok(_), Some(cleanup)) => Err(cleanup),
        (Err(primary), None) => Err(primary),
        (Err(primary), Some(cleanup)) => Err(attach_cleanup_errors(primary, vec![cleanup])),
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, Commands, NbdCommands};
    use clap::Parser;

    #[test]
    fn striped_nbd_provision_command_has_operational_defaults() {
        let cli = Cli::try_parse_from([
            "zerofs",
            "nbd",
            "provision-striped",
            "unix:/run/zerofs/9p.sock",
            "vm100",
            "--size",
            "64GiB",
        ])
        .unwrap();

        let Commands::Nbd {
            subcommand:
                NbdCommands::ProvisionStriped {
                    target,
                    export,
                    size,
                    lanes,
                    stripe_size,
                },
        } = cli.command
        else {
            panic!("expected nbd provision-striped command");
        };
        assert_eq!(target, "unix:/run/zerofs/9p.sock");
        assert_eq!(export, "vm100");
        assert_eq!(size, 64 * 1024 * 1024 * 1024);
        assert_eq!(lanes, 4);
        assert_eq!(stripe_size, 256 * 1024);
    }
}
