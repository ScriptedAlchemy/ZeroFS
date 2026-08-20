use crate::sftp_protocol::{SftpProtocolSession, SshConnectionOwner, handshake_sftp};
use crate::sftp_transport::{SessionFactory, TransportError, TransportSession};
use async_trait::async_trait;
use russh::client;
use russh::keys::{Algorithm, EcdsaCurve, HashAlg, PrivateKeyWithHashAlg, load_secret_key};
use std::borrow::Cow;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub use crate::sftp_protocol::RUSSH_SFTP_MAX_CONCURRENT_WRITES;
#[cfg(test)]
pub use crate::sftp_protocol::russh_sftp_config;

/// Large static SSH channel window for high-bandwidth, high-latency links.
pub const RUSSH_WINDOW_SIZE: u32 = 16 * 1024 * 1024;
/// russh channel_buffer_size is an mpsc *message* depth, not bytes.
/// 1024 slots covers a 16 MiB window of SSH packets plus control messages.
pub const RUSSH_CHANNEL_BUFFER_SIZE: usize = 1024;
/// russh requires the SSH transport packet size to fit in a TCP packet.
/// Larger SFTP frames are streamed independently across these packets.
pub const RUSSH_MAXIMUM_PACKET_SIZE: u32 = u16::MAX as u32;

pub fn russh_client_config() -> client::Config {
    client::Config {
        window_size: RUSSH_WINDOW_SIZE,
        maximum_packet_size: RUSSH_MAXIMUM_PACKET_SIZE,
        channel_buffer_size: RUSSH_CHANNEL_BUFFER_SIZE,
        preferred: russh::Preferred {
            key: Cow::Borrowed(&[
                Algorithm::Ed25519,
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP256,
                },
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP384,
                },
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP521,
                },
                Algorithm::Rsa {
                    hash: Some(HashAlg::Sha512),
                },
                Algorithm::Rsa {
                    hash: Some(HashAlg::Sha256),
                },
            ]),
            cipher: Cow::Borrowed(&[
                russh::cipher::AES_256_GCM,
                russh::cipher::AES_128_GCM,
                russh::cipher::AES_256_CTR,
                russh::cipher::AES_192_CTR,
                russh::cipher::AES_128_CTR,
            ]),
            ..russh::Preferred::DEFAULT
        },
        inactivity_timeout: None, // pool owns idle lifetime
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 3,
        nodelay: true,
        ..client::Config::default()
    }
}

struct StrictHostKey {
    host: String,
    port: u16,
    known_hosts: PathBuf,
}

impl client::Handler for StrictHostKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        match russh::keys::check_known_hosts_path(
            &self.host,
            self.port,
            server_public_key,
            &self.known_hosts,
        ) {
            Ok(true) => Ok(true),
            Ok(false) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    "SSH host key is missing from or does not match known_hosts"
                );
                Ok(false)
            }
            Err(error) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    %error,
                    "failed to consult known_hosts"
                );
                Ok(false)
            }
        }
    }
}

pub struct RusshSessionFactory {
    endpoint: crate::config::SftpEndpoint,
    identity_file: PathBuf,
    identity_key: Arc<russh::keys::PrivateKey>,
    known_hosts: PathBuf,
}

impl RusshSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        identity_file: PathBuf,
        known_hosts: PathBuf,
    ) -> Result<Self, TransportError> {
        let identity_metadata = fs::metadata(&identity_file).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] identity_file {} is unavailable: {error}",
                identity_file.display()
            ))
        })?;
        if !identity_metadata.is_file() {
            return Err(TransportError::Open(format!(
                "[sftp] identity_file {} is not a regular file",
                identity_file.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = identity_metadata.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(TransportError::Open(format!(
                    "[sftp] identity_file {} permissions are too open: {:04o}; expected 0600 or stricter",
                    identity_file.display(),
                    mode & 0o7777
                )));
            }
        }
        let identity_key = load_secret_key(&identity_file, None).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] identity_file {} is not an unencrypted OpenSSH private key: {error}",
                identity_file.display()
            ))
        })?;
        let known_hosts_metadata = fs::metadata(&known_hosts).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] known_hosts {} is unavailable: {error}",
                known_hosts.display()
            ))
        })?;
        if !known_hosts_metadata.is_file() {
            return Err(TransportError::Open(format!(
                "[sftp] known_hosts {} is not a regular file",
                known_hosts.display()
            )));
        }
        Ok(Self {
            endpoint,
            identity_file,
            identity_key: Arc::new(identity_key),
            known_hosts,
        })
    }
}

impl fmt::Debug for RusshSessionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RusshSessionFactory")
            .field("host", &self.endpoint.host)
            .field("port", &self.endpoint.port)
            .field("known_hosts", &self.known_hosts)
            .field("window_size", &RUSSH_WINDOW_SIZE)
            .field("maximum_packet_size", &RUSSH_MAXIMUM_PACKET_SIZE)
            .finish_non_exhaustive()
    }
}

async fn authenticate_identity(
    handle: &mut client::Handle<StrictHostKey>,
    username: &str,
    identity_file: &Path,
    identity_key: Arc<russh::keys::PrivateKey>,
    hash: Option<HashAlg>,
) -> Result<(), TransportError> {
    let authenticated = handle
        .authenticate_publickey(username, PrivateKeyWithHashAlg::new(identity_key, hash))
        .await
        .map_err(|error| {
            TransportError::Open(format!("public-key authentication failed: {error}"))
        })?;
    if authenticated.success() {
        Ok(())
    } else {
        Err(TransportError::Open(format!(
            "public-key authentication rejected for {}",
            identity_file.display()
        )))
    }
}

fn select_rsa_auth_hash(
    server_support: Option<Option<HashAlg>>,
) -> Result<HashAlg, TransportError> {
    match server_support {
        Some(Some(hash)) => Ok(hash),
        // RFC 8308 extension info is optional. Optimistically try the strongest
        // SHA-2 signature when it is absent; authentication will fail closed if
        // the server does not support it.
        None => Ok(HashAlg::Sha512),
        Some(None) => Err(TransportError::Open(
            "server advertises only legacy SHA-1 ssh-rsa user authentication".to_owned(),
        )),
    }
}

async fn close_ssh_handle(
    mut handle: client::Handle<StrictHostKey>,
    force: &CancellationToken,
) -> Result<(), TransportError> {
    let disconnect = handle.disconnect(russh::Disconnect::ByApplication, "", "");
    tokio::select! {
        biased;
        _ = force.cancelled() => {
            return Err(TransportError::Close(
                "russh disconnect was cancelled before it could be queued".to_owned()
            ));
        }
        result = disconnect => {
            if let Err(error) = result {
                tracing::warn!(%error, "russh disconnect failed after SFTP close");
            }
        }
    }
    tokio::select! {
        biased;
        _ = force.cancelled() => {
            Err(TransportError::Close(
                "russh connection did not terminate before forced cleanup".to_owned()
            ))
        }
        result = &mut handle => {
            if let Err(error) = result {
                tracing::warn!(%error, "russh connection task failed during close");
            }
            Ok(())
        }
    }
}

async fn connect_ssh_handle(
    endpoint: &crate::config::SftpEndpoint,
    known_hosts: &Path,
    force: &CancellationToken,
) -> Result<client::Handle<StrictHostKey>, TransportError> {
    let socket = tokio::select! {
        biased;
        _ = force.cancelled() => return Err(TransportError::PoolClosed),
        result = tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port)) => {
            result.map_err(|error| {
                TransportError::Open(format!(
                    "TCP connect to {}:{} failed: {error}",
                    endpoint.host, endpoint.port
                ))
            })?
        }
    };
    let config = Arc::new(russh_client_config());
    if config.nodelay
        && let Err(error) = socket.set_nodelay(true)
    {
        tracing::warn!(%error, "failed to enable TCP_NODELAY for russh");
    }
    let std_socket = socket.into_std().map_err(|error| {
        TransportError::Open(format!("failed to retain ownership of SSH socket: {error}"))
    })?;
    let abort_socket = std_socket.try_clone().map_err(|error| {
        TransportError::Open(format!(
            "failed to duplicate SSH socket for cancellation: {error}"
        ))
    })?;
    let socket = tokio::net::TcpStream::from_std(std_socket).map_err(|error| {
        TransportError::Open(format!(
            "failed to restore asynchronous SSH socket: {error}"
        ))
    })?;
    let handler = StrictHostKey {
        host: endpoint.host.clone(),
        port: endpoint.port,
        known_hosts: known_hosts.to_path_buf(),
    };
    let connecting = client::connect_stream(config, socket, handler);
    tokio::pin!(connecting);
    tokio::select! {
        result = &mut connecting => result.map_err(|error| {
            TransportError::Open(format!(
                "russh connect to {}:{} failed: {error}",
                endpoint.host, endpoint.port
            ))
        }),
        _ = force.cancelled() => {
            if let Err(error) = abort_socket.shutdown(std::net::Shutdown::Both) {
                tracing::warn!(%error, "failed to shut down cancelled SSH socket");
            }
            match connecting.await {
                Ok(handle) => {
                    let cleanup_force = CancellationToken::new();
                    close_ssh_handle(handle, &cleanup_force).await?;
                }
                Err(error) => {
                    tracing::debug!(%error, "cancelled SSH connection terminated during handshake");
                }
            }
            Err(TransportError::PoolClosed)
        }
    }
}

#[async_trait]
impl SessionFactory for RusshSessionFactory {
    async fn open(
        &self,
        force: CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError> {
        if force.is_cancelled() {
            return Err(TransportError::PoolClosed);
        }

        let mut handle = connect_ssh_handle(&self.endpoint, &self.known_hosts, &force).await?;
        let session = {
            let session = async {
                let hash = if self.identity_key.algorithm().is_rsa() {
                    Some(select_rsa_auth_hash(
                        handle.best_supported_rsa_hash().await.map_err(|error| {
                            TransportError::Open(format!("RSA hash probe failed: {error}"))
                        })?,
                    )?)
                } else {
                    None
                };
                authenticate_identity(
                    &mut handle,
                    self.endpoint.username.as_str(),
                    &self.identity_file,
                    self.identity_key.clone(),
                    hash,
                )
                .await?;

                let channel = handle.channel_open_session().await.map_err(|error| {
                    TransportError::Open(format!("failed to open SSH session channel: {error}"))
                })?;
                channel
                    .request_subsystem(true, "sftp")
                    .await
                    .map_err(|error| {
                        TransportError::Open(format!("failed to start SFTP subsystem: {error}"))
                    })?;
                handshake_sftp(channel.into_stream()).await
            };
            tokio::pin!(session);
            tokio::select! {
                result = &mut session => result,
                _ = force.cancelled() => Err(TransportError::PoolClosed),
            }
        };
        let (sftp, capabilities, limits) = match session {
            Ok(session) => session,
            Err(open_error) => {
                // The pool owns the open deadline. Once a Handle exists, its
                // cleanup must outlive that deadline so account capacity is not
                // released while a detached SSH connection can still exist.
                let cleanup_force = CancellationToken::new();
                return match close_ssh_handle(handle, &cleanup_force).await {
                    Ok(()) => Err(open_error),
                    Err(close_error) => Err(close_error),
                };
            }
        };
        tracing::info!(
            host = %self.endpoint.host,
            port = self.endpoint.port,
            window_size = RUSSH_WINDOW_SIZE,
            maximum_packet_size = RUSSH_MAXIMUM_PACKET_SIZE,
            max_concurrent_writes = RUSSH_SFTP_MAX_CONCURRENT_WRITES,
            fsync = capabilities.fsync,
            hardlink = capabilities.hardlink,
            posix_rename = capabilities.posix_rename,
            "opened russh SFTP session"
        );
        Ok(Box::new(SftpProtocolSession::new(
            sftp,
            capabilities,
            limits,
            Box::new(RusshConnectionOwner { handle }),
        )) as Box<dyn TransportSession>)
    }
}

struct RusshConnectionOwner {
    handle: client::Handle<StrictHostKey>,
}

impl fmt::Debug for RusshConnectionOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RusshConnectionOwner")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SshConnectionOwner for RusshConnectionOwner {
    async fn close(self: Box<Self>, force: CancellationToken) -> Result<(), TransportError> {
        close_ssh_handle(self.handle, &force).await
    }
}

/// Historical test-only name retained while existing integration tests move to russh.
#[cfg(test)]
#[allow(dead_code)]
pub type OpenSshSessionFactory = RusshSessionFactory;

/// Historical test-only name retained while existing integration tests move to russh-sftp.
#[cfg(test)]
pub type OpenSshTransportSession = SftpProtocolSession;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sftp_protocol::{
        CLIENT_WRITE_IN_FLIGHT, CLIENT_WRITE_PEAK, CLIENT_WRITE_TRACKING, FSYNC, HARDLINK,
        POSIX_RENAME, SFTP_READ_PACKET_SIZE, SFTP_WRITE_PACKET_SIZE,
    };
    use bytes::Bytes;
    use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
    use std::sync::atomic::Ordering;

    const CLIENT_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymAAAAJgFEUUfBRFF
HwAAAAtzc2gtZWQyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymA
AAAEDC+zNpo65+8VYQLiNUGNNxWqEww8yfkOaqMHdwmfuTzhCKCm5OLslts14nPEyRyftp
MaHALDvYi2xPw/SMSDKYAAAAEnplcm9mcy1jbGllbnQtdGVzdAECAw==
-----END OPENSSH PRIVATE KEY-----
"#;
    const SERVER_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACDC6IEYT3UM1UuN2KP2eb4ToFDSgq120q/tQOu7O1c+egAAAJg0qMDxNKjA
8QAAAAtzc2gtZWQyNTUxOQAAACDC6IEYT3UM1UuN2KP2eb4ToFDSgq120q/tQOu7O1c+eg
AAAEDU8+PlxIN0Fyv3xBh4UgtcDTbPt7CQ3KIkmRRR8dsOaMLogRhPdQzVS43Yo/Z5vhOg
UNKCrXbSr+1A67s7Vz56AAAAEnplcm9mcy1zZXJ2ZXItdGVzdAECAw==
-----END OPENSSH PRIVATE KEY-----
"#;
    const OTHER_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCRi+8XRj8Tq1Or1mR02iOEWRKCh7xRszil1JfMIs9RjwAAAJhxG/RDcRv0
QwAAAAtzc2gtZWQyNTUxOQAAACCRi+8XRj8Tq1Or1mR02iOEWRKCh7xRszil1JfMIs9Rjw
AAAEAB9KRIf/0YJRj8Atj1eLnGRkrHutd6JDXafNflsuUDDJGL7xdGPxOrU6vWZHTaI4RZ
EoKHvFGzOKXUl8wiz1GPAAAAEXplcm9mcy1vdGhlci10ZXN0AQIDBA==
-----END OPENSSH PRIVATE KEY-----
"#;
    const ENCRYPTED_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAACmFlczI1Ni1jdHIAAAAGYmNyeXB0AAAAGAAAABBA6hrXpS
LPoPaV7M7G9DtaAAAAGAAAAAEAAAAzAAAAC3NzaC1lZDI1NTE5AAAAIC3aUS9vD1G77q+1
0WpfmDRbafRtG5Ry+YTGXiQynPOsAAAAoG12ZqxPoDsGoR9DwynbDlcIfoCqrOE13219I1
bcxvi0nQxL+AqlsH6Ws1XeGzQJI43NujzRhKle1UUAIW5yIX9Hu5Nzjmc/L58QMyNVejCl
qCqH6hiJAS8PQ2MVjb9TYJ9ujM5FIr0Goy2X3x+HX0oExG56xthfBD0pkUwXWdiD451TLR
SiHvLIjvZnsP6UHEZvepD9dSLx72qVi3Qb2/E=
-----END OPENSSH PRIVATE KEY-----
"#;

    #[test]
    fn russh_client_config_caps_ssh_packets_at_the_tcp_packet_limit() {
        let config = russh_client_config();
        assert_eq!(config.window_size, 16 * 1024 * 1024);
        assert_eq!(config.maximum_packet_size, u16::MAX as u32);
        assert_eq!(config.channel_buffer_size, 1024);
        assert_eq!(config.inactivity_timeout, None);
        assert_eq!(
            config.keepalive_interval,
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(config.keepalive_max, 3);
        assert!(config.nodelay);
        assert!(
            config.preferred.cipher.iter().all(|cipher| matches!(
                cipher.as_ref(),
                "aes256-gcm@openssh.com"
                    | "aes128-gcm@openssh.com"
                    | "aes256-ctr"
                    | "aes192-ctr"
                    | "aes128-ctr"
            )),
            "ciphers must be AES-GCM or AES-CTR: {:?}",
            config
                .preferred
                .cipher
                .iter()
                .map(|cipher| cipher.as_ref())
                .collect::<Vec<_>>()
        );
        assert!(
            !config
                .preferred
                .cipher
                .iter()
                .any(|cipher| cipher.as_ref().contains("cbc")
                    || cipher.as_ref() == "none"
                    || cipher.as_ref() == "clear")
        );
        assert!(
            !config.preferred.key.iter().any(|algorithm| matches!(
                algorithm,
                russh::keys::Algorithm::Dsa | russh::keys::Algorithm::Rsa { hash: None }
            )),
            "host-key negotiation must exclude DSA and SHA-1 ssh-rsa: {:?}",
            config.preferred.key
        );
    }

    #[test]
    fn russh_sftp_config_matches_the_proven_raw_sftp_request_window() {
        let config = russh_sftp_config();
        assert_eq!(config.max_packet_len, 256 * 1024);
        assert_eq!(config.max_concurrent_writes, 128);
        assert_ne!(
            config.max_concurrent_writes,
            russh_sftp::client::Config::default().max_concurrent_writes
        );
    }

    #[test]
    fn rsa_user_auth_never_selects_legacy_sha1() {
        assert_eq!(
            select_rsa_auth_hash(Some(Some(HashAlg::Sha512))).unwrap(),
            HashAlg::Sha512
        );
        assert_eq!(
            select_rsa_auth_hash(Some(Some(HashAlg::Sha256))).unwrap(),
            HashAlg::Sha256
        );
        assert_eq!(
            select_rsa_auth_hash(None).unwrap(),
            HashAlg::Sha512,
            "servers without EXT_INFO should get a safe optimistic SHA-512 attempt"
        );
        let error = select_rsa_auth_hash(Some(None))
            .expect_err("a server advertising only legacy ssh-rsa must fail closed");
        assert!(error.to_string().contains("SHA-1"));
    }

    #[test]
    fn factory_debug_redacts_the_username() {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        std::fs::write(&known_hosts, "").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let factory = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "storage.example.test".to_owned(),
                port: 2222,
                username: "account-secret-name".to_owned(),
            },
            identity,
            known_hosts,
        )
        .unwrap();
        let debug = format!("{factory:?}");
        assert!(debug.contains("storage.example.test"));
        assert!(debug.contains("2222"));
        assert!(debug.contains("16777216"));
        assert!(!debug.contains("account-secret-name"));
    }

    #[test]
    fn factory_rejects_a_missing_configured_identity_before_network_dial() {
        let root = tempfile::tempdir().unwrap();
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&known_hosts, "").unwrap();
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            root.path().join("missing-identity"),
            known_hosts,
        )
        .expect_err("a missing configured identity must fail before network startup");

        assert!(error.to_string().contains("identity_file"));
    }

    #[test]
    fn factory_rejects_missing_known_hosts_before_network_dial() {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            identity,
            root.path().join("missing-known-hosts"),
        )
        .expect_err("missing known_hosts must fail before network startup");

        assert!(error.to_string().contains("known_hosts"));
    }

    #[cfg(unix)]
    #[test]
    fn factory_rejects_a_group_readable_private_key() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        std::fs::write(&known_hosts, "").unwrap();
        std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o640)).unwrap();
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            identity,
            known_hosts,
        )
        .expect_err("group-readable private keys must fail closed");

        assert!(error.to_string().contains("permissions"));
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_unknown_host_key() {
        let env = Loopback::start().await;
        std::fs::write(&env.known_hosts, "").unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("unknown host keys must fail closed");
        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_cancelled_stalled_kex_closes_the_pre_handle_tcp_runner() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let banner_sent = Arc::new(tokio::sync::Notify::new());
        let server_active = active.clone();
        let server_banner_sent = banner_sent.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            server_active.fetch_add(1, Ordering::SeqCst);
            socket.write_all(b"SSH-2.0-stalled-kex\r\n").await.unwrap();
            server_banner_sent.notify_one();
            let mut buffer = [0; 1024];
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            server_active.fetch_sub(1, Ordering::SeqCst);
        });

        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        std::fs::write(&known_hosts, "").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let factory = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: address.port(),
                username: "zerofs".to_owned(),
            },
            identity,
            known_hosts,
        )
        .unwrap();
        let force = CancellationToken::new();
        let cancel_force = force.clone();
        let cancel = async {
            banner_sent.notified().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_force.cancel();
        };

        let (result, ()) = tokio::join!(factory.open(force), cancel);

        assert!(matches!(result, Err(TransportError::PoolClosed)));
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled KEX must not detach a live TCP/session runner");
    }

    #[tokio::test]
    async fn russh_factory_rejects_a_changed_host_key() {
        let env = Loopback::start().await;
        let other = russh::keys::PrivateKey::from_openssh(OTHER_KEY).unwrap();
        russh::keys::known_hosts::learn_known_hosts_path(
            &env.endpoint.host,
            env.endpoint.port,
            other.public_key(),
            &env.known_hosts,
        )
        .unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("changed host keys must fail closed");
        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_unconfigured_client_key() {
        let env = Loopback::start().await;
        std::fs::write(&env.identity, OTHER_KEY.as_bytes()).unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();

        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("the server must reject a client key it did not configure");
        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_loopback_pipelines_writes_and_reads_with_large_windows() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory
            .open(CancellationToken::new())
            .await
            .expect("native russh client must complete a loopback handshake");
        assert_eq!(
            session.capabilities(),
            crate::sftp_object_store::SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        );

        let packets = 32;
        let payload = Bytes::from(vec![0x5a; packets * SFTP_WRITE_PACKET_SIZE]);
        CLIENT_WRITE_TRACKING.store(false, Ordering::SeqCst);
        CLIENT_WRITE_IN_FLIGHT.store(0, Ordering::SeqCst);
        CLIENT_WRITE_PEAK.store(0, Ordering::SeqCst);
        CLIENT_WRITE_TRACKING.store(true, Ordering::SeqCst);
        let started = std::time::Instant::now();
        session
            .write_file_durable(std::path::Path::new("bulk.bin"), vec![payload.clone()])
            .await
            .unwrap();
        CLIENT_WRITE_TRACKING.store(false, Ordering::SeqCst);
        assert_eq!(
            env.exec_requests.load(Ordering::SeqCst),
            0,
            "production writes must remain entirely within the SFTP namespace"
        );
        let elapsed = started.elapsed();
        let peak = CLIENT_WRITE_PEAK.load(Ordering::SeqCst);
        assert!(
            peak >= 16,
            "client must pipeline WRITE requests; peak in-flight={peak} elapsed={elapsed:?}"
        );
        let read = session
            .read_exact(std::path::Path::new("bulk.bin"), 0, payload.len())
            .await
            .unwrap();
        assert_eq!(read, payload);

        session
            .ensure_directory_component(std::path::Path::new("nested"))
            .await
            .unwrap();
        session
            .hard_link(
                std::path::Path::new("bulk.bin"),
                std::path::Path::new("nested/link.bin"),
            )
            .await
            .unwrap();
        session
            .posix_rename(
                std::path::Path::new("nested/link.bin"),
                std::path::Path::new("nested/renamed.bin"),
            )
            .await
            .unwrap();
        let entries = session
            .list_directory(std::path::Path::new("nested"))
            .await
            .unwrap();
        assert!(entries.iter().any(|entry| {
            entry.filename == std::path::Path::new("renamed.bin")
                && entry.kind == crate::sftp_transport::RemoteEntryKind::File
        }));
        session.close(CancellationToken::new()).await.unwrap();
    }

    #[tokio::test]
    async fn russh_loopback_reassembles_short_sftp_read_replies() {
        let env = Loopback::start_with_read_cap(Some(32 * 1024)).await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory.open(CancellationToken::new()).await.unwrap();
        let payload = Bytes::from(vec![0xa5; SFTP_READ_PACKET_SIZE + 17]);

        session
            .write_file_durable(
                std::path::Path::new("short-read.bin"),
                vec![payload.clone()],
            )
            .await
            .unwrap();
        let read = session
            .read_exact(std::path::Path::new("short-read.bin"), 0, payload.len())
            .await
            .unwrap();

        assert_eq!(read, payload);
        session.close(CancellationToken::new()).await.unwrap();
    }

    #[tokio::test]
    async fn russh_close_waits_for_the_ssh_connection_to_terminate() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory.open(CancellationToken::new()).await.unwrap();
        assert_eq!(env.active_connections.load(Ordering::SeqCst), 1);

        session.close(CancellationToken::new()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while env.active_connections.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the remote SSH handler must terminate after close returns");
    }

    #[tokio::test]
    async fn russh_close_awaits_connection_after_pool_shutdown_token_is_cancelled() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let pool_shutdown = CancellationToken::new();
        let session = factory.open(pool_shutdown.clone()).await.unwrap();
        assert_eq!(env.active_connections.load(Ordering::SeqCst), 1);
        pool_shutdown.cancel();

        session.close(CancellationToken::new()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while env.active_connections.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pool shutdown must terminate the SSH connection");
    }

    #[tokio::test]
    async fn russh_forced_close_never_reports_success() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory.open(CancellationToken::new()).await.unwrap();
        let force = CancellationToken::new();
        force.cancel();

        let error = session
            .close(force)
            .await
            .expect_err("forced cleanup must fail closed instead of releasing capacity");

        assert!(matches!(error, TransportError::Close(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_encrypted_identity_without_a_passphrase() {
        let env = Loopback::start().await;
        std::fs::write(&env.identity, ENCRYPTED_KEY.as_bytes()).unwrap();
        let error = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .expect_err("encrypted identities must fail before network startup");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("identity_file"),
                    "failure must name the invalid configured identity: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_advertised_limits_query_failure() {
        let env = Loopback::start_with_broken_limits().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();

        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("a failed advertised limits query must retire the session");

        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
        assert!(error.to_string().contains("limits"), "{error:?}");
        tokio::time::timeout(Duration::from_secs(1), async {
            while env.active_connections.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed session establishment must await SSH connection cleanup");
    }

    #[tokio::test]
    async fn russh_read_object_closes_handle_after_metadata_error() {
        let env = Loopback::start_without_mtime().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory.open(CancellationToken::new()).await.unwrap();
        session
            .write_file_durable(
                Path::new("missing-mtime.bin"),
                vec![Bytes::from_static(b"not-an-object")],
            )
            .await
            .unwrap();
        assert_eq!(env.open_handles.load(Ordering::SeqCst), 0);

        let error = session
            .read_object(Path::new("missing-mtime.bin"), None, false)
            .await
            .expect_err("missing object metadata must fail closed");

        assert!(matches!(error, TransportError::CorruptObject(_)));
        assert_eq!(
            env.open_handles.load(Ordering::SeqCst),
            0,
            "every read_object error path must close its raw SFTP handle"
        );
        session.close(CancellationToken::new()).await.unwrap();
    }

    struct Loopback {
        _root: tempfile::TempDir,
        endpoint: crate::config::SftpEndpoint,
        identity: PathBuf,
        known_hosts: PathBuf,
        _inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        _peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        exec_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        active_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Loopback {
        async fn start() -> Self {
            Self::start_with(None, false, false).await
        }

        async fn start_with_read_cap(read_cap: Option<usize>) -> Self {
            Self::start_with(read_cap, false, false).await
        }

        async fn start_without_mtime() -> Self {
            Self::start_with(None, true, false).await
        }

        async fn start_with_broken_limits() -> Self {
            Self::start_with(None, false, true).await
        }

        async fn start_with(
            read_cap: Option<usize>,
            omit_mtime: bool,
            advertise_broken_limits: bool,
        ) -> Self {
            use russh::server::{Auth, Msg, Server, Session};
            use russh::{Channel, ChannelId};
            use std::net::SocketAddr;
            use tokio::net::TcpListener;

            let root = tempfile::tempdir().unwrap();
            let identity = root.path().join("id_ed25519");
            let known_hosts = root.path().join("known_hosts");
            let fs_root = root.path().join("fs");
            std::fs::create_dir(&fs_root).unwrap();

            let client_key = russh::keys::PrivateKey::from_openssh(CLIENT_KEY).unwrap();
            std::fs::write(&identity, CLIENT_KEY.as_bytes()).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
            let client_public = client_key.public_key().clone();

            let server_key = russh::keys::PrivateKey::from_openssh(SERVER_KEY).unwrap();
            let server_public = server_key.public_key().clone();

            let inflight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let exec_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let active_connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let open_handles = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            russh::keys::known_hosts::learn_known_hosts_path(
                "127.0.0.1",
                addr.port(),
                &server_public,
                &known_hosts,
            )
            .unwrap();

            let server_config = russh::server::Config {
                window_size: RUSSH_WINDOW_SIZE,
                maximum_packet_size: RUSSH_MAXIMUM_PACKET_SIZE,
                channel_buffer_size: RUSSH_CHANNEL_BUFFER_SIZE,
                inactivity_timeout: None,
                nodelay: true,
                auth_rejection_time: Duration::from_secs(0),
                auth_rejection_time_initial: Some(Duration::from_secs(0)),
                keys: vec![server_key],
                preferred: russh::Preferred {
                    cipher: Cow::Borrowed(&[
                        russh::cipher::AES_256_GCM,
                        russh::cipher::AES_128_GCM,
                        russh::cipher::AES_256_CTR,
                        russh::cipher::AES_192_CTR,
                        russh::cipher::AES_128_CTR,
                    ]),
                    ..russh::Preferred::DEFAULT
                },
                ..russh::server::Config::default()
            };
            let server_config = std::sync::Arc::new(server_config);

            #[derive(Clone)]
            struct ServerState {
                fs_root: PathBuf,
                client_public: russh::keys::PublicKey,
                read_cap: Option<usize>,
                omit_mtime: bool,
                advertise_broken_limits: bool,
                inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                exec_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                active_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            }

            struct SshSession {
                state: ServerState,
                channels: std::sync::Arc<
                    tokio::sync::Mutex<std::collections::HashMap<ChannelId, Channel<Msg>>>,
                >,
            }

            impl russh::server::Server for ServerState {
                type Handler = SshSession;
                fn new_client(&mut self, _: Option<SocketAddr>) -> Self::Handler {
                    self.active_connections.fetch_add(1, Ordering::SeqCst);
                    SshSession {
                        state: self.clone(),
                        channels: std::sync::Arc::new(tokio::sync::Mutex::new(
                            std::collections::HashMap::new(),
                        )),
                    }
                }
            }

            impl Drop for SshSession {
                fn drop(&mut self) {
                    self.state.active_connections.fetch_sub(1, Ordering::SeqCst);
                }
            }

            impl russh::server::Handler for SshSession {
                type Error = anyhow::Error;

                async fn auth_publickey(
                    &mut self,
                    _user: &str,
                    public_key: &russh::keys::PublicKey,
                ) -> Result<Auth, Self::Error> {
                    // Compare key material only; comments and encoding wrappers differ.
                    if public_key.key_data() == self.state.client_public.key_data() {
                        Ok(Auth::Accept)
                    } else {
                        Ok(Auth::Reject {
                            proceed_with_methods: None,
                            partial_success: false,
                        })
                    }
                }

                async fn channel_open_session(
                    &mut self,
                    channel: Channel<Msg>,
                    reply: russh::server::ChannelOpenHandle,
                    _session: &mut Session,
                ) -> Result<(), Self::Error> {
                    self.channels.lock().await.insert(channel.id(), channel);
                    reply.accept().await;
                    Ok(())
                }

                async fn subsystem_request(
                    &mut self,
                    channel_id: ChannelId,
                    name: &str,
                    session: &mut Session,
                ) -> Result<(), Self::Error> {
                    if name != "sftp" {
                        session.channel_failure(channel_id)?;
                        return Ok(());
                    }
                    let channel = self
                        .channels
                        .lock()
                        .await
                        .remove(&channel_id)
                        .ok_or_else(|| anyhow::anyhow!("missing sftp channel"))?;
                    session.channel_success(channel_id)?;
                    let sftp = FsSftp {
                        root: self.state.fs_root.clone(),
                        read_cap: self.state.read_cap,
                        omit_mtime: self.state.omit_mtime,
                        advertise_broken_limits: self.state.advertise_broken_limits,
                        files: std::collections::HashMap::new(),
                        dirs: std::collections::HashMap::new(),
                        next: 1,
                        inflight: self.state.inflight.clone(),
                        peak: self.state.peak.clone(),
                        open_handles: self.state.open_handles.clone(),
                    };
                    // Drive SFTP off the russh session task so SSH packets
                    // keep flowing into ChannelStream.
                    tokio::spawn(russh_sftp::server::run(channel.into_stream(), sftp));
                    Ok(())
                }

                async fn exec_request(
                    &mut self,
                    channel_id: ChannelId,
                    _data: &[u8],
                    session: &mut Session,
                ) -> Result<(), Self::Error> {
                    self.state.exec_requests.fetch_add(1, Ordering::SeqCst);
                    session.channel_failure(channel_id)?;
                    Ok(())
                }
            }

            let mut server = ServerState {
                fs_root: fs_root.clone(),
                client_public,
                read_cap,
                omit_mtime,
                advertise_broken_limits,
                inflight: inflight.clone(),
                peak: peak.clone(),
                exec_requests: exec_requests.clone(),
                active_connections: active_connections.clone(),
                open_handles: open_handles.clone(),
            };
            tokio::spawn(async move {
                loop {
                    let (socket, _) = match listener.accept().await {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let handler = server.new_client(socket.peer_addr().ok());
                    let config = server_config.clone();
                    tokio::spawn(async move {
                        if let Ok(running) =
                            russh::server::run_stream(config, socket, handler).await
                        {
                            let _ = running.await;
                        }
                    });
                }
            });

            Self {
                _root: root,
                endpoint: crate::config::SftpEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: addr.port(),
                    username: "zerofs".to_owned(),
                },
                identity,
                known_hosts,
                _inflight: inflight,
                _peak: peak,
                exec_requests,
                active_connections,
                open_handles,
            }
        }
    }

    struct Opened {
        path: PathBuf,
    }

    struct FsSftp {
        root: PathBuf,
        read_cap: Option<usize>,
        omit_mtime: bool,
        advertise_broken_limits: bool,
        files: std::collections::HashMap<String, Opened>,
        dirs: std::collections::HashMap<String, std::vec::IntoIter<std::fs::DirEntry>>,
        next: u64,
        inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl FsSftp {
        fn resolve(&self, path: &str) -> PathBuf {
            let trimmed = path.trim_start_matches('/');
            if trimmed.is_empty() {
                self.root.clone()
            } else {
                self.root.join(trimmed)
            }
        }

        fn attrs(
            &self,
            path: &std::path::Path,
        ) -> Result<FileAttributes, russh_sftp::protocol::StatusCode> {
            let meta = std::fs::symlink_metadata(path)
                .map_err(|_| russh_sftp::protocol::StatusCode::NoSuchFile)?;
            let mut attrs = FileAttributes {
                size: Some(meta.len()),
                ..FileAttributes::default()
            };
            if meta.is_dir() {
                attrs.set_dir(true);
            } else if meta.is_file() {
                attrs.set_regular(true);
            } else if meta.file_type().is_symlink() {
                attrs.set_symlink(true);
            }
            if !self.omit_mtime
                && let Ok(modified) = meta.modified()
                && let Ok(secs) = modified.duration_since(std::time::UNIX_EPOCH)
            {
                attrs.mtime = Some(secs.as_secs() as u32);
            }
            Ok(attrs)
        }

        fn ok(id: u32) -> russh_sftp::protocol::Status {
            russh_sftp::protocol::Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Ok".to_owned(),
                language_tag: "en-US".to_owned(),
            }
        }

        fn alloc(&mut self, path: PathBuf) -> String {
            let handle = format!("h{}", self.next);
            self.next += 1;
            self.files.insert(handle.clone(), Opened { path });
            self.open_handles.fetch_add(1, Ordering::SeqCst);
            handle
        }
    }

    impl russh_sftp::server::Handler for FsSftp {
        type Error = StatusCode;

        fn unimplemented(&self) -> Self::Error {
            StatusCode::OpUnsupported
        }

        async fn init(
            &mut self,
            _version: u32,
            _extensions: std::collections::HashMap<String, String>,
        ) -> Result<russh_sftp::protocol::Version, Self::Error> {
            let mut version = russh_sftp::protocol::Version::new();
            version.extensions.insert(FSYNC.to_owned(), "1".to_owned());
            version
                .extensions
                .insert(HARDLINK.to_owned(), "1".to_owned());
            version
                .extensions
                .insert(POSIX_RENAME.to_owned(), "1".to_owned());
            if self.advertise_broken_limits {
                version
                    .extensions
                    .insert(russh_sftp::extensions::LIMITS.to_owned(), "1".to_owned());
            }
            Ok(version)
        }

        async fn open(
            &mut self,
            id: u32,
            filename: String,
            pflags: OpenFlags,
            _attrs: FileAttributes,
        ) -> Result<russh_sftp::protocol::Handle, Self::Error> {
            let path = self.resolve(&filename);
            let mut options = std::fs::OpenOptions::new();
            options.read(pflags.contains(OpenFlags::READ) || !pflags.contains(OpenFlags::WRITE));
            options.write(pflags.contains(OpenFlags::WRITE));
            options.create(pflags.contains(OpenFlags::CREATE));
            options.truncate(pflags.contains(OpenFlags::TRUNCATE));
            options.open(&path).map_err(|_| StatusCode::NoSuchFile)?;
            let handle = self.alloc(path);
            Ok(russh_sftp::protocol::Handle { id, handle })
        }

        async fn close(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            if self.files.remove(&handle).is_some() {
                self.open_handles.fetch_sub(1, Ordering::SeqCst);
            }
            self.dirs.remove(&handle);
            Ok(Self::ok(id))
        }

        async fn read(
            &mut self,
            id: u32,
            handle: String,
            offset: u64,
            len: u32,
        ) -> Result<russh_sftp::protocol::Data, Self::Error> {
            use std::io::{Read, Seek, SeekFrom};
            let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
            let mut file = std::fs::File::open(path).map_err(|_| StatusCode::Failure)?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|_| StatusCode::Failure)?;
            let requested = usize::try_from(len).map_err(|_| StatusCode::Failure)?;
            let mut buf = vec![0; self.read_cap.map_or(requested, |cap| requested.min(cap))];
            let n = file.read(&mut buf).map_err(|_| StatusCode::Failure)?;
            buf.truncate(n);
            Ok(russh_sftp::protocol::Data { id, data: buf })
        }

        async fn write(
            &mut self,
            id: u32,
            handle: String,
            offset: u64,
            data: Vec<u8>,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            use std::io::{Seek, SeekFrom, Write};
            let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            let result = (|| {
                let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(|_| StatusCode::Failure)?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|_| StatusCode::Failure)?;
                file.write_all(&data).map_err(|_| StatusCode::Failure)?;
                Ok(Self::ok(id))
            })();
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn lstat(
            &mut self,
            id: u32,
            path: String,
        ) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
            Ok(russh_sftp::protocol::Attrs {
                id,
                attrs: self.attrs(&self.resolve(&path))?,
            })
        }

        async fn fstat(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
            let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
            Ok(russh_sftp::protocol::Attrs {
                id,
                attrs: self.attrs(path)?,
            })
        }

        async fn opendir(
            &mut self,
            id: u32,
            path: String,
        ) -> Result<russh_sftp::protocol::Handle, Self::Error> {
            let resolved = self.resolve(&path);
            let entries = std::fs::read_dir(&resolved)
                .map_err(|_| StatusCode::NoSuchFile)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| StatusCode::Failure)?;
            let handle = format!("d{}", self.next);
            self.next += 1;
            self.dirs.insert(handle.clone(), entries.into_iter());
            Ok(russh_sftp::protocol::Handle { id, handle })
        }

        async fn readdir(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Name, Self::Error> {
            let entries = self.dirs.get_mut(&handle).ok_or(StatusCode::Failure)?;
            let batch: Vec<_> = entries.by_ref().take(64).collect();
            if batch.is_empty() {
                return Err(StatusCode::Eof);
            }
            let mut files = Vec::new();
            for entry in batch {
                let attrs = self.attrs(&entry.path()).unwrap_or_default();
                files.push(russh_sftp::protocol::File::new(
                    entry.file_name().to_string_lossy().into_owned(),
                    attrs,
                ));
            }
            Ok(russh_sftp::protocol::Name { id, files })
        }

        async fn remove(
            &mut self,
            id: u32,
            filename: String,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            std::fs::remove_file(self.resolve(&filename)).map_err(|_| StatusCode::NoSuchFile)?;
            Ok(Self::ok(id))
        }

        async fn mkdir(
            &mut self,
            id: u32,
            path: String,
            _attrs: FileAttributes,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            match std::fs::create_dir(self.resolve(&path)) {
                Ok(()) => Ok(Self::ok(id)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Self::ok(id)),
                Err(_) => Err(StatusCode::Failure),
            }
        }

        async fn extended(
            &mut self,
            id: u32,
            request: String,
            data: Vec<u8>,
        ) -> Result<russh_sftp::protocol::Packet, Self::Error> {
            let mut bytes = Bytes::from(data);
            match request.as_str() {
                FSYNC => {
                    let _ext: russh_sftp::extensions::FsyncExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                HARDLINK => {
                    let ext: russh_sftp::extensions::HardlinkExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    std::fs::hard_link(self.resolve(&ext.oldpath), self.resolve(&ext.newpath))
                        .map_err(|_| StatusCode::Failure)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                POSIX_RENAME => {
                    let ext: russh_sftp::extensions::HardlinkExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    std::fs::rename(self.resolve(&ext.oldpath), self.resolve(&ext.newpath))
                        .map_err(|_| StatusCode::Failure)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                _ => Err(StatusCode::OpUnsupported),
            }
        }
    }
}
