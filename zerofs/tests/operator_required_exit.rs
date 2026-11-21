//! `zerofs run` exit statuses as a supervisor sees them.
//!
//! A refusal that needs a person (a damaged journal, a failing metadata disk,
//! a missing pool catalog) exits with status 10 and ends stderr with one plain
//! line that says so. An ordinary failure (a config file that does not parse,
//! a port in use) still exits with status 1. Each test starts the real binary
//! against a prepared directory.

use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// The status supervisors such as the TrueNAS app package match. Spelled out
/// here, not imported, so that a change to the constant fails this test.
const OPERATOR_REQUIRED: i32 = 10;
const NOTICE: &str =
    "zerofs stopped: this failure needs an operator decision, and restarting will refuse again";

/// Startup either refuses or fails well inside this bound.
const BOUND: Duration = Duration::from_secs(120);

fn zerofs_binary() -> PathBuf {
    std::env::var_os("ZEROFS_BIN").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_zerofs")),
        PathBuf::from,
    )
}

struct Stopped {
    status: ExitStatus,
    stderr: String,
}

impl Stopped {
    fn last_line(&self) -> &str {
        self.stderr.trim_end().lines().last().unwrap_or("")
    }

    fn assert_operator_required(&self, cause: &str) {
        assert_eq!(
            self.status.code(),
            Some(OPERATOR_REQUIRED),
            "expected the operator-required status; stderr:\n{}",
            self.stderr
        );
        assert_eq!(self.last_line(), NOTICE, "stderr:\n{}", self.stderr);
        assert!(
            self.stderr.contains(cause),
            "the refusal did not name its cause {cause:?}; stderr:\n{}",
            self.stderr
        );
        let error = self.stderr.find("Error: ").expect("the error is printed");
        assert!(
            error < self.stderr.rfind(NOTICE).unwrap(),
            "the notice must follow the error and its causes; stderr:\n{}",
            self.stderr
        );
    }

    fn assert_ordinary_failure(&self, cause: &str) {
        assert_eq!(
            self.status.code(),
            Some(1),
            "expected status 1; stderr:\n{}",
            self.stderr
        );
        assert!(
            !self.stderr.contains(NOTICE),
            "an ordinary failure must not ask for an operator; stderr:\n{}",
            self.stderr
        );
        assert!(
            self.stderr.contains(cause),
            "the failure did not name its cause {cause:?}; stderr:\n{}",
            self.stderr
        );
    }
}

/// A `zerofs run` child, killed and reaped if a test leaves it running.
struct Server {
    child: Child,
    stderr: Option<std::thread::JoinHandle<String>>,
}

impl Server {
    fn start(config: &Path) -> Self {
        let mut child = Command::new(zerofs_binary())
            .args(["run", "-c"])
            .arg(config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start zerofs run");
        let mut pipe = child.stderr.take().expect("server stderr");
        let stderr = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            pipe.read_to_end(&mut bytes).unwrap();
            String::from_utf8_lossy(&bytes).into_owned()
        });
        Self {
            child,
            stderr: Some(stderr),
        }
    }

    fn wait(mut self) -> Stopped {
        let deadline = Instant::now() + BOUND;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll zerofs run") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let stderr = self.stderr.take().unwrap().join().unwrap();
                panic!("zerofs run did not stop within {BOUND:?}; stderr:\n{stderr}");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        Stopped {
            status,
            stderr: self.stderr.take().unwrap().join().unwrap(),
        }
    }

    /// Wait until the server accepts 9P connections on `socket`, then stop it
    /// with SIGTERM and require a clean exit.
    fn serve_then_stop(mut self, socket: &Path) {
        let deadline = Instant::now() + BOUND;
        while std::os::unix::net::UnixStream::connect(socket).is_err() {
            if let Some(status) = self.child.try_wait().unwrap() {
                let stderr = self.stderr.take().unwrap().join().unwrap();
                panic!("zerofs run exited with {status} before serving; stderr:\n{stderr}");
            }
            assert!(
                Instant::now() < deadline,
                "zerofs run did not serve within {BOUND:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let pid = i32::try_from(self.child.id()).unwrap();
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
        let stopped = self.wait();
        assert!(
            stopped.status.success(),
            "zerofs run did not stop cleanly; stderr:\n{}",
            stopped.stderr
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// A file:// volume serving 9P on a Unix socket, with optional extra TOML.
struct Volume {
    root: tempfile::TempDir,
}

impl Volume {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        for directory in ["data", "cache"] {
            std::fs::create_dir_all(root.path().join(directory)).unwrap();
        }
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn config(&self, extra: &str) -> PathBuf {
        let config = self.path("zerofs.toml");
        std::fs::write(
            &config,
            format!(
                r#"
[cache]
dir = "{cache}"
disk_size_gb = 0.01
memory_size_gb = 0.01
warm_metadata = "off"

[storage]
url = "{data}"
encryption_password = "operator-required-exit"

[servers]

[servers.ninep]
unix_socket = "{socket}"

[telemetry]
enabled = false
{extra}
"#,
                cache = self.path("cache").display(),
                data = file_url(&self.path("data")),
                socket = self.path("9p.sock").display(),
            ),
        )
        .unwrap();
        config
    }

    /// Start the volume once and stop it cleanly, so that its metadata store
    /// (and journal, when `extra` enables writeback) exist on disk.
    fn create(&self, extra: &str) -> PathBuf {
        let config = self.config(extra);
        Server::start(&config).serve_then_stop(&self.path("9p.sock"));
        config
    }
}

fn files_named(root: &Path, name: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|file| file == name) {
                found.push(path);
            }
        }
    }
    found
}

fn chmod_tree(root: &Path, file_mode: u32) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(file_mode))
                    .unwrap();
            }
        }
    }
}

const WRITEBACK: &str = r#"
[writeback]
enabled = true
dir = "{dir}"
memory_size_gb = 0.01
disk_size_gb = 0.1
min_free_gb = 0.001
"#;

/// Offline recovery must acquire the stopped journal's owner lock and match
/// its persisted identity before updating its predecessor record.
#[test]
fn offline_recovery_checks_the_journal_lock_and_identity() {
    let volume = Volume::new();
    let writeback = WRITEBACK.replace("{dir}", &volume.path("writeback").display().to_string());
    let config = volume.create(&writeback);
    let journals = files_named(&volume.path("writeback"), "journal.redb");
    assert_eq!(journals.len(), 1);
    let db = &journals[0];
    let journal = db.parent().unwrap();
    let object = volume.path("data/predecessor");
    std::fs::write(&object, b"predecessor").unwrap();
    let recover = |config: &Path| {
        Command::new(zerofs_binary())
            .args(["debug", "reseed-writeback-predecessor", "--config"])
            .arg(config)
            .arg("--journal")
            .arg(journal)
            .arg("--path")
            .arg(object.strip_prefix("/").unwrap())
            .args(["--sequence", "0"])
            .output()
            .unwrap()
    };

    let original = std::fs::read(db).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(journal.join("LOCK"))
        .unwrap();
    lock.try_lock().unwrap();
    let busy = recover(&config);
    let diagnostic = String::from_utf8_lossy(&busy.stderr);
    assert!(
        !busy.status.success() && diagnostic.contains("already locked"),
        "{diagnostic}"
    );
    assert!(
        std::fs::read(db).unwrap() == original,
        "refusal modified the journal"
    );
    drop(lock);

    let other = Volume::new();
    let other_writeback =
        WRITEBACK.replace("{dir}", &other.path("writeback").display().to_string());
    let mismatch = recover(&other.create(&other_writeback));
    let diagnostic = String::from_utf8_lossy(&mismatch.stderr);
    assert!(
        !mismatch.status.success() && diagnostic.contains("identity mismatch"),
        "{diagnostic}"
    );
    assert!(
        std::fs::read(db).unwrap() == original,
        "refusal modified the journal"
    );

    let matching = recover(&config);
    assert!(
        matching.status.success(),
        "{}",
        String::from_utf8_lossy(&matching.stderr)
    );
}

/// The journal device lost acknowledged writes: the database ends before the
/// end its last durable commit describes.
#[test]
fn a_journal_shorter_than_its_last_commit_stops_for_an_operator() {
    let volume = Volume::new();
    let writeback = WRITEBACK.replace("{dir}", &volume.path("writeback").display().to_string());
    let config = volume.create(&writeback);
    let journals = files_named(&volume.path("writeback"), "journal.redb");
    assert_eq!(journals.len(), 1, "one journal database: {journals:?}");
    let journal = &journals[0];
    let described = std::fs::metadata(journal).unwrap().len();
    assert!(
        described > 4096,
        "the journal must hold more than its header page to be truncated: {described} bytes"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(journal)
        .unwrap()
        .set_len(described - 4096)
        .unwrap();

    Server::start(&config)
        .wait()
        .assert_operator_required("lost writes after acknowledging them");
}

/// A metadata file the local filesystem refuses to read (EACCES here, EIO on
/// a bad block) is not transient, and startup stops instead of retrying.
#[test]
fn a_metadata_read_fault_at_startup_stops_for_an_operator() {
    let volume = Volume::new();
    let config = volume.create("");
    chmod_tree(&volume.path("data"), 0o000);

    let stopped = Server::start(&config).wait();
    chmod_tree(&volume.path("data"), 0o600);
    stopped.assert_operator_required("EACCES");
}

/// A pool whose catalog is missing must be initialised or restored on
/// purpose; normal startup never recreates it.
#[test]
fn a_missing_pool_catalog_stops_for_an_operator() {
    let volume = Volume::new();
    for member in ["disk-a", "disk-b"] {
        std::fs::create_dir_all(volume.path(member)).unwrap();
    }
    let pool = format!(
        r#"
[storage.pool]
catalog_dir = "{catalog}"

[[storage.pool.members]]
id = "disk-a"
url = "{a}"
capacity_gb = 0.008
reserve_free_gb = 0.0

[[storage.pool.members]]
id = "disk-b"
url = "{b}"
capacity_gb = 0.008
reserve_free_gb = 0.0
"#,
        catalog = volume.path("catalog").display(),
        a = file_url(&volume.path("disk-a")),
        b = file_url(&volume.path("disk-b")),
    );
    let config = volume.config(&pool);

    Server::start(&config)
        .wait()
        .assert_operator_required("pool catalog is missing");
}

#[test]
fn a_config_that_does_not_parse_still_exits_with_status_one() {
    let volume = Volume::new();
    let config = volume.path("zerofs.toml");
    std::fs::write(&config, "[storage\nurl = ").unwrap();

    Server::start(&config)
        .wait()
        .assert_ordinary_failure("zerofs.toml");
}

#[test]
fn a_port_in_use_still_exits_with_status_one() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let volume = Volume::new();
    let config = volume.path("zerofs.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[cache]
dir = "{cache}"
disk_size_gb = 0.01
memory_size_gb = 0.01
warm_metadata = "off"

[storage]
url = "memory:///operator-required-exit"
encryption_password = "operator-required-exit"

[servers]

[servers.nbd]
addresses = ["{address}"]

[telemetry]
enabled = false
"#,
            cache = volume.path("cache").display(),
        ),
    )
    .unwrap();

    Server::start(&config)
        .wait()
        .assert_ordinary_failure("Address already in use");
}
