use crate::config::validate_pinned_hpn_program;
use crate::sftp_protocol::{Duplex, SftpProtocolSession, SshConnectionOwner, handshake_sftp};
use crate::sftp_transport::{SessionFactory, TransportError, TransportSession};
use async_trait::async_trait;
use russh::keys::{Algorithm, load_secret_key};
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const STDERR_RING_BYTES: usize = 8 * 1024;
const HOST_KEY_ALGORITHMS: &str = concat!(
    "ssh-ed25519,ecdsa-sha2-nistp256,ecdsa-sha2-nistp384,ecdsa-sha2-nistp521,",
    "rsa-sha2-512,rsa-sha2-256"
);

pub struct HpnSessionFactory {
    endpoint: crate::config::SftpEndpoint,
    identity_file: PathBuf,
    known_hosts: PathBuf,
    program: PathBuf,
    program_sha256: String,
}

impl HpnSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        identity_file: PathBuf,
        known_hosts: PathBuf,
        program: PathBuf,
        program_sha256: String,
    ) -> Result<Self, TransportError> {
        validate_pinned_hpn_program(&program, &program_sha256)
            .map_err(|error| TransportError::Open(error.to_string()))?;
        verify_identity_file(&identity_file)?;
        verify_known_hosts(&known_hosts)?;
        Ok(Self {
            endpoint,
            identity_file,
            known_hosts,
            program,
            program_sha256,
        })
    }

    pub fn from_config(
        endpoint: crate::config::SftpEndpoint,
        config: &crate::config::SftpConfig,
    ) -> Result<Self, TransportError> {
        let program = config.hpn_program.clone().ok_or_else(|| {
            TransportError::Open(
                "[sftp] hpn_program is required when transport = \"hpn_openssh\"".to_owned(),
            )
        })?;
        let program_sha256 = config.hpn_sha256.clone().ok_or_else(|| {
            TransportError::Open(
                "[sftp] hpn_sha256 is required when transport = \"hpn_openssh\"".to_owned(),
            )
        })?;
        Self::new(
            endpoint,
            config.identity_file.clone(),
            config.known_hosts.clone(),
            program,
            program_sha256,
        )
    }
}

impl fmt::Debug for HpnSessionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HpnSessionFactory")
            .field("host", &self.endpoint.host)
            .field("port", &self.endpoint.port)
            .field("program", &self.program)
            .field("known_hosts", &self.known_hosts)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SessionFactory for HpnSessionFactory {
    async fn open(
        &self,
        force: CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError> {
        if force.is_cancelled() {
            return Err(TransportError::PoolClosed);
        }
        let program = self.program.clone();
        let program_sha256 = self.program_sha256.clone();
        tokio::task::spawn_blocking(move || validate_pinned_hpn_program(&program, &program_sha256))
            .await
            .map_err(|error| TransportError::Open(format!("HPN pin check failed: {error}")))?
            .map_err(|error| TransportError::Open(error.to_string()))?;
        if force.is_cancelled() {
            return Err(TransportError::PoolClosed);
        }

        let mut command = hpn_command(
            &self.program,
            &self.endpoint,
            &self.identity_file,
            &self.known_hosts,
        );
        let mut child = command.spawn().map_err(|error| {
            TransportError::Open(format!(
                "failed to spawn pinned HPN-SSH {}: {error}",
                self.program.display()
            ))
        })?;
        let (stdin, stdout, stderr) =
            match (child.stdin.take(), child.stdout.take(), child.stderr.take()) {
                (Some(stdin), Some(stdout), Some(stderr)) => (stdin, stdout, stderr),
                _ => {
                    let _ = kill_and_reap(child, tokio::spawn(async {})).await;
                    return Err(TransportError::Open(
                        "HPN-SSH child stdio was not piped".to_owned(),
                    ));
                }
            };
        let stderr_ring = StderrRing::new();
        let stderr_task = tokio::spawn(stderr_ring.clone().capture(stderr));

        let handshake = handshake_sftp(Duplex {
            reader: stdout,
            writer: stdin,
        });
        tokio::pin!(handshake);
        let session = tokio::select! {
            biased;
            _ = force.cancelled() => Err(TransportError::PoolClosed),
            result = &mut handshake => result,
        };

        let (sftp, capabilities, limits) = match session {
            Ok(session) => session,
            Err(open_error) => {
                let stderr = stderr_ring.snapshot();
                let close_error = kill_and_reap(child, stderr_task).await.err();
                return Err(match close_error {
                    Some(close_error) => close_error,
                    None if stderr.is_empty() => open_error,
                    None => TransportError::Open(format!("{open_error}; stderr: {stderr}")),
                });
            }
        };

        tracing::info!(
            host = %self.endpoint.host,
            port = self.endpoint.port,
            program = %self.program.display(),
            fsync = capabilities.fsync,
            hardlink = capabilities.hardlink,
            posix_rename = capabilities.posix_rename,
            "opened HPN-OpenSSH SFTP session"
        );
        Ok(Box::new(SftpProtocolSession::new(
            sftp,
            capabilities,
            limits,
            Box::new(HpnConnectionOwner {
                child: Some(child),
                stderr_task: Some(stderr_task),
            }),
        )) as Box<dyn TransportSession>)
    }
}

struct HpnConnectionOwner {
    child: Option<Child>,
    stderr_task: Option<JoinHandle<()>>,
}

impl fmt::Debug for HpnConnectionOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HpnConnectionOwner")
            .field("pid", &self.child.as_ref().and_then(Child::id))
            .finish_non_exhaustive()
    }
}

impl Drop for HpnConnectionOwner {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            kill_process_group(child);
            let _ = child.start_kill();
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl SshConnectionOwner for HpnConnectionOwner {
    async fn close(mut self: Box<Self>, _force: CancellationToken) -> Result<(), TransportError> {
        let child = self.child.take().expect("HPN child already taken");
        let stderr_task = self
            .stderr_task
            .take()
            .expect("HPN stderr task already taken");
        kill_and_reap(child, stderr_task).await
    }
}

fn hpn_command(
    program: &Path,
    endpoint: &crate::config::SftpEndpoint,
    identity_file: &Path,
    known_hosts: &Path,
) -> Command {
    let mut command = Command::new(program);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env_remove("SSH_AUTH_SOCK")
        .env_remove("SSH_AGENT_PID")
        .env_remove("SSH_ASKPASS")
        .env_remove("SSH_ASKPASS_REQUIRE")
        .env_remove("DISPLAY")
        .env_remove("SSH_SK_HELPER");
    for arg in hpn_args(endpoint, identity_file, known_hosts) {
        command.arg(arg);
    }
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    command
}

fn hpn_args(
    endpoint: &crate::config::SftpEndpoint,
    identity_file: &Path,
    known_hosts: &Path,
) -> Vec<String> {
    let identity = identity_file.display().to_string();
    let known_hosts = openssh_config_path(known_hosts);
    vec![
        "-F".to_owned(),
        "/dev/null".to_owned(),
        "-T".to_owned(),
        "-p".to_owned(),
        endpoint.port.to_string(),
        "-l".to_owned(),
        endpoint.username.clone(),
        "-o".to_owned(),
        "BatchMode=yes".to_owned(),
        "-o".to_owned(),
        "PasswordAuthentication=no".to_owned(),
        "-o".to_owned(),
        "KbdInteractiveAuthentication=no".to_owned(),
        "-o".to_owned(),
        "ChallengeResponseAuthentication=no".to_owned(),
        "-o".to_owned(),
        "PreferredAuthentications=publickey".to_owned(),
        "-o".to_owned(),
        "NumberOfPasswordPrompts=0".to_owned(),
        "-o".to_owned(),
        "IdentitiesOnly=yes".to_owned(),
        "-i".to_owned(),
        identity,
        "-o".to_owned(),
        "IdentityAgent=none".to_owned(),
        "-o".to_owned(),
        "PKCS11Provider=none".to_owned(),
        "-o".to_owned(),
        "AddKeysToAgent=no".to_owned(),
        "-o".to_owned(),
        "ForwardAgent=no".to_owned(),
        "-o".to_owned(),
        "ForwardX11=no".to_owned(),
        "-o".to_owned(),
        "RequestTTY=no".to_owned(),
        "-o".to_owned(),
        "ClearAllForwardings=yes".to_owned(),
        "-o".to_owned(),
        "PermitLocalCommand=no".to_owned(),
        "-o".to_owned(),
        "Tunnel=no".to_owned(),
        "-o".to_owned(),
        "ProxyCommand=none".to_owned(),
        "-o".to_owned(),
        "ProxyJump=none".to_owned(),
        "-o".to_owned(),
        "ControlMaster=no".to_owned(),
        "-o".to_owned(),
        "ControlPersist=no".to_owned(),
        "-o".to_owned(),
        "ControlPath=none".to_owned(),
        "-o".to_owned(),
        "StrictHostKeyChecking=yes".to_owned(),
        "-o".to_owned(),
        format!("UserKnownHostsFile={known_hosts}"),
        "-o".to_owned(),
        "GlobalKnownHostsFile=/dev/null".to_owned(),
        "-o".to_owned(),
        "UpdateHostKeys=no".to_owned(),
        "-o".to_owned(),
        format!("HostKeyAlgorithms={HOST_KEY_ALGORITHMS}"),
        "-o".to_owned(),
        format!("PubkeyAcceptedAlgorithms={HOST_KEY_ALGORITHMS}"),
        "-s".to_owned(),
        "--".to_owned(),
        endpoint.host.clone(),
        "sftp".to_owned(),
    ]
}

fn openssh_config_path(path: &Path) -> String {
    let escaped = path
        .display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    format!("\"{escaped}\"")
}

fn verify_identity_file(identity_file: &Path) -> Result<(), TransportError> {
    let identity_metadata = fs::metadata(identity_file).map_err(|error| {
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
    let key = load_secret_key(identity_file, None).map_err(|error| {
        TransportError::Open(format!(
            "[sftp] identity_file {} is not an unencrypted OpenSSH private key: {error}",
            identity_file.display()
        ))
    })?;
    match key.algorithm() {
        Algorithm::Ed25519 | Algorithm::Rsa { .. } => Ok(()),
        algorithm => Err(TransportError::Open(format!(
            "[sftp] identity_file {} uses unsupported algorithm {algorithm}",
            identity_file.display()
        ))),
    }
}

fn verify_known_hosts(known_hosts: &Path) -> Result<(), TransportError> {
    let metadata = fs::metadata(known_hosts).map_err(|error| {
        TransportError::Open(format!(
            "[sftp] known_hosts {} is unavailable: {error}",
            known_hosts.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(TransportError::Open(format!(
            "[sftp] known_hosts {} is not a regular file",
            known_hosts.display()
        )));
    }
    Ok(())
}

async fn kill_and_reap(
    mut child: Child,
    stderr_task: JoinHandle<()>,
) -> Result<(), TransportError> {
    kill_process_group(&child);
    if let Err(error) = child.start_kill() {
        if error.kind() != io::ErrorKind::InvalidInput {
            tracing::warn!(%error, "failed to signal HPN-SSH child");
        }
    }
    match child.wait().await {
        Ok(_) => {
            let _ = stderr_task.await;
            Ok(())
        }
        Err(error) => {
            stderr_task.abort();
            Err(TransportError::Close(format!(
                "failed to reap HPN-SSH child: {error}"
            )))
        }
    }
}

fn kill_process_group(child: &Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let pgid = pid as libc::pid_t;
        if pgid > 1 {
            let ours = unsafe { libc::getpgrp() };
            if pgid != ours {
                let _ = unsafe { libc::killpg(pgid, libc::SIGKILL) };
            }
        }
    }
}

#[derive(Clone)]
struct StderrRing {
    bytes: Arc<Mutex<VecDeque<u8>>>,
}

impl StderrRing {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_RING_BYTES))),
        }
    }

    async fn capture<R>(self, mut stderr: R)
    where
        R: AsyncRead + Unpin,
    {
        let mut buf = [0u8; 512];
        loop {
            match tokio::io::AsyncReadExt::read(&mut stderr, &mut buf).await {
                Ok(0) => break,
                Ok(n) => self.push(&buf[..n]),
                Err(_) => break,
            }
        }
    }

    fn push(&self, data: &[u8]) {
        let Ok(mut ring) = self.bytes.lock() else {
            return;
        };
        ring.extend(data);
        while ring.len() > STDERR_RING_BYTES {
            ring.pop_front();
        }
    }

    fn snapshot(&self) -> String {
        let Ok(mut ring) = self.bytes.lock() else {
            return String::new();
        };
        String::from_utf8_lossy(ring.make_contiguous())
            .trim()
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;
    use tempfile::TempDir;

    const CLIENT_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymAAAAJgFEUUfBRFF
HwAAAAtzc2gtZWQyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymA
AAAEDC+zNpo65+8VYQLiNUGNNxWqEww8yfkOaqMHdwmfuTzhCKCm5OLslts14nPEyRyftp
MaHALDvYi2xPw/SMSDKYAAAAEnplcm9mcy1jbGllbnQtdGVzdAECAw==
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

    struct Fixture {
        _dir: TempDir,
        endpoint: crate::config::SftpEndpoint,
        identity: PathBuf,
        known_hosts: PathBuf,
        program: PathBuf,
        sha: String,
    }

    impl Fixture {
        fn with_program(body: &str) -> Self {
            let dir = TempDir::new().unwrap();
            let identity = dir.path().join("id_ed25519");
            fs::write(&identity, CLIENT_KEY).unwrap();
            fs::set_permissions(&identity, fs::Permissions::from_mode(0o600)).unwrap();
            let known_hosts = dir.path().join("known_hosts");
            fs::write(
                &known_hosts,
                "example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeHostKeyForTestsOnly\n",
            )
            .unwrap();
            let program = dir.path().join("hpnssh");
            fs::write(&program, wrap_hpn_stub(body)).unwrap();
            fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).unwrap();
            let sha = sha256_hex(program.as_path());
            Self {
                _dir: dir,
                endpoint: crate::config::SftpEndpoint {
                    host: "example.com".to_owned(),
                    port: 23,
                    username: "alice".to_owned(),
                },
                identity,
                known_hosts,
                program,
                sha,
            }
        }

        fn factory(&self) -> HpnSessionFactory {
            HpnSessionFactory::new(
                self.endpoint.clone(),
                self.identity.clone(),
                self.known_hosts.clone(),
                self.program.clone(),
                self.sha.clone(),
            )
            .unwrap()
        }
    }

    fn wrap_hpn_stub(body: &str) -> String {
        let rest = body.strip_prefix("#!/bin/sh\n").unwrap_or(body);
        format!(
            "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"-V\" ]; then\n    printf '%s\\n' 'OpenSSH_9.0_hpn14v15' >&2\n    exit 0\n  fi\ndone\n{rest}"
        )
    }

    fn sha256_hex(path: &Path) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
    }

    fn process_exists(pid: u32) -> bool {
        // SAFETY: signal 0 is an existence probe and does not deliver a signal.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    #[test]
    fn hpn_command_uses_pinned_program_and_strict_options() {
        let fixture = Fixture::with_program("#!/bin/sh\nexit 0\n");
        let args = hpn_args(&fixture.endpoint, &fixture.identity, &fixture.known_hosts);
        let joined = args.join("\x1f");
        assert!(args.contains(&"-T".to_owned()));
        assert!(args.contains(&"-s".to_owned()));
        assert_eq!(args[args.len() - 3], "--");
        assert_eq!(args[args.len() - 2], "example.com");
        assert_eq!(args[args.len() - 1], "sftp");
        assert!(args.windows(2).any(|w| w[0] == "-p" && w[1] == "23"));
        assert!(args.windows(2).any(|w| w[0] == "-l" && w[1] == "alice"));
        assert!(args.windows(2).any(|w| w[0] == "-F" && w[1] == "/dev/null"));
        assert!(joined.contains("BatchMode=yes"));
        assert!(joined.contains("PasswordAuthentication=no"));
        assert!(joined.contains("KbdInteractiveAuthentication=no"));
        assert!(joined.contains("IdentitiesOnly=yes"));
        assert!(args.windows(2).any(|pair| {
            pair[0] == "-i" && pair[1] == fixture.identity.to_string_lossy().as_ref()
        }));
        assert!(joined.contains("StrictHostKeyChecking=yes"));
        assert!(!joined.contains("StrictHostKeyChecking=no"));
        assert!(!joined.contains("accept-new"));
        assert!(joined.contains("ControlMaster=no"));
        assert!(joined.contains("ControlPersist=no"));
        assert!(joined.contains("IdentityAgent=none"));
        assert!(joined.contains("ForwardAgent=no"));
        assert!(!joined.contains("AllowTcpForwarding="));
        assert!(!joined.contains("DisableForwarding="));
        assert!(joined.contains("ForwardX11=no"));
        assert!(joined.contains("RequestTTY=no"));
        assert!(joined.contains("PermitLocalCommand=no"));
        assert!(joined.contains("ProxyCommand=none"));
        assert!(joined.contains("ProxyJump=none"));
        assert!(
            !args.iter().any(|arg| arg
                .split('=')
                .nth(1)
                .unwrap_or(arg)
                .split(',')
                .any(|alg| alg == "ssh-rsa")),
            "legacy ssh-rsa must stay off the algorithm lists: {args:?}"
        );
        let command = format!("{}", fixture.program.display());
        assert_ne!(command, "/usr/bin/ssh");
        assert!(command.ends_with("hpnssh"));
    }

    #[test]
    fn hpn_command_preserves_paths_with_spaces() {
        let endpoint = crate::config::SftpEndpoint {
            host: "example.com".to_owned(),
            port: 23,
            username: "alice".to_owned(),
        };
        let identity = PathBuf::from("/tmp/identity key");
        let known_hosts = PathBuf::from("/tmp/known hosts");

        let args = hpn_args(&endpoint, &identity, &known_hosts);

        assert!(
            args.windows(2)
                .any(|pair| { pair[0] == "-i" && pair[1] == identity.to_string_lossy().as_ref() })
        );
        assert!(
            args.iter()
                .any(|arg| { arg == &format!("UserKnownHostsFile=\"{}\"", known_hosts.display()) })
        );
    }

    #[test]
    fn hpn_factory_rejects_an_encrypted_identity() {
        let fixture = Fixture::with_program("#!/bin/sh\nexit 0\n");
        fs::write(&fixture.identity, ENCRYPTED_KEY).unwrap();
        fs::set_permissions(&fixture.identity, fs::Permissions::from_mode(0o600)).unwrap();
        let error = HpnSessionFactory::new(
            fixture.endpoint.clone(),
            fixture.identity.clone(),
            fixture.known_hosts.clone(),
            fixture.program.clone(),
            fixture.sha.clone(),
        )
        .expect_err("encrypted identities must fail before HPN child startup");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("identity_file"),
                    "failure must name the invalid configured identity: {message}"
                );
                assert!(
                    message.contains("unencrypted"),
                    "encrypted keys must fail closed: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[test]
    fn hpn_factory_rejects_a_sha256_mismatch() {
        let fixture = Fixture::with_program("#!/bin/sh\nexit 0\n");
        let error = HpnSessionFactory::new(
            fixture.endpoint.clone(),
            fixture.identity.clone(),
            fixture.known_hosts.clone(),
            fixture.program.clone(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        )
        .expect_err("pin mismatch must fail closed");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("hpn_sha256") || message.contains("SHA-256"),
                    "failure must name the pin: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[test]
    fn hpn_from_config_fails_closed_without_a_pin() {
        let error = HpnSessionFactory::from_config(
            crate::config::SftpEndpoint {
                host: "example.com".to_owned(),
                port: 22,
                username: "alice".to_owned(),
            },
            &crate::config::SftpConfig {
                transport: crate::config::SftpSshTransport::HpnOpenSsh,
                ..Default::default()
            },
        )
        .expect_err("missing pin must fail closed");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("hpn_program") || message.contains("hpn_sha256"),
                    "missing pin must fail closed: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hpn_owner_kills_and_reaps_child_process_group() {
        let dir = TempDir::new().unwrap();
        let script = dir.path().join("group.sh");
        let leader_file = dir.path().join("leader.pid");
        let member_file = dir.path().join("member.pid");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s' $$ > {leader}\nsleep 30 &\nprintf '%s' $! > {member}\nwait\n",
                leader = leader_file.display(),
                member = member_file.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let mut command = Command::new(&script);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            command.process_group(0);
        }
        let mut child = command.spawn().unwrap();
        let stderr = child.stderr.take().unwrap();
        let ring = StderrRing::new();
        let stderr_task = tokio::spawn(ring.capture(stderr));
        let leader = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(text) = fs::read_to_string(&leader_file) {
                    if let Ok(pid) = text.parse::<u32>() {
                        if process_exists(pid) {
                            return pid;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader pid");
        let member = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(text) = fs::read_to_string(&member_file) {
                    if let Ok(pid) = text.parse::<u32>() {
                        if process_exists(pid) {
                            return pid;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("member pid");

        let owner = HpnConnectionOwner {
            child: Some(child),
            stderr_task: Some(stderr_task),
        };
        Box::new(owner)
            .close(CancellationToken::new())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while process_exists(leader) || process_exists(member) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("process group must be killed and reaped");
    }

    #[tokio::test]
    async fn hpn_open_fails_closed_and_reaps_the_child() {
        let dir = TempDir::new().unwrap();
        let pid_file = dir.path().join("child.pid");
        let fixture = Fixture::with_program(&format!(
            "#!/bin/sh\nprintf '%s' $$ > {pid}\nprintf 'not-sftp\\n'\nexit 1\n",
            pid = pid_file.display(),
        ));
        let factory = fixture.factory();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("garbage child output must fail closed");
        assert!(
            matches!(error, TransportError::Open(_)),
            "HPN must not fall back after a child failure: {error:?}"
        );

        let pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(text) = fs::read_to_string(&pid_file) {
                    if let Ok(pid) = text.parse::<u32>() {
                        return pid;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("child pid");
        tokio::time::timeout(Duration::from_secs(2), async {
            while process_exists(pid) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed handshake must reap the HPN child");
    }

    #[tokio::test]
    async fn hpn_open_fails_closed_when_the_pin_changes() {
        let fixture = Fixture::with_program("#!/bin/sh\nexit 0\n");
        let factory = fixture.factory();
        fs::write(
            &fixture.program,
            wrap_hpn_stub("#!/bin/sh\nprintf 'mutated'\nexit 0\n"),
        )
        .unwrap();
        fs::set_permissions(&fixture.program, fs::Permissions::from_mode(0o755)).unwrap();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("a mutated binary must fail the pin before spawn");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("SHA-256")
                        || message.contains("hpn_sha256")
                        || message.contains("does not match"),
                    "pin change must fail closed: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hpn_loopback_handshakes_sftp_over_child_stdio() {
        let fixture = Fixture::with_program(
            "#!/bin/sh\nprintf '\\000\\000\\000\\005\\002\\000\\000\\000\\003'\nsleep 30\n",
        );
        let session = fixture
            .factory()
            .open(CancellationToken::new())
            .await
            .expect("fake child must complete the SFTP handshake");
        session
            .close(CancellationToken::new())
            .await
            .expect("loopback session must reap the child");
    }
}
