//! Acknowledged namespace operations survive a process kill.
//!
//! NFSv3 requires CREATE, MKDIR, REMOVE, RMDIR, RENAME, LINK, SYMLINK and
//! SETATTR to be on stable storage when they reply, and SMB clients expect
//! the same. The TrueNAS app serves both through a 9P mount of this engine
//! and neither UNFS3 nor Samba fsyncs a directory after a namespace change,
//! so the engine reply itself must be durable. Each test starts the real
//! binary with volatile write acknowledgement in front of a local writeback
//! journal, the packaged profile's shape, then SIGKILLs it right after the
//! replies, with no fsync, and restarts it.

use ninep_client::{NOFID, NinePClient, SetattrBuilder};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(120);
// The client allocates fids from 1 upwards; the root uses 0, as the mount does.
const ROOT: u32 = 0;

fn zerofs_binary() -> PathBuf {
    std::env::var_os("ZEROFS_BIN").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_zerofs")),
        PathBuf::from,
    )
}

struct Volume {
    root: tempfile::TempDir,
}

impl Volume {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        // The engine creates its private writeback directory itself.
        for directory in ["data", "cache"] {
            std::fs::create_dir_all(root.path().join(directory)).unwrap();
        }
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn socket(&self) -> PathBuf {
        self.path("9p.sock")
    }

    /// Volatile filesystem acknowledgement in front of the local journal,
    /// with the periodic flush pushed out of the test's reach so that only
    /// the reply path can make a mutation durable.
    fn config(&self) -> PathBuf {
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
url = "file://{data}"
encryption_password = "namespace-durability"

[writeback]
enabled = true
dir = "{writeback}"
ack_mode = "memory"
memory_size_gb = 0.02
disk_size_gb = 0.2
min_free_gb = 0.001
shutdown_flush = "local"

[filesystem]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 0.016777216
volatile_max_operations = 128

[lsm]
flush_interval_secs = 3600

[servers]

[servers.ninep]
unix_socket = "{socket}"

[telemetry]
enabled = false
"#,
                cache = self.path("cache").display(),
                data = self.path("data").display(),
                writeback = self.path("writeback").display(),
                socket = self.socket().display(),
            ),
        )
        .unwrap();
        config
    }
}

/// A `zerofs run` child, killed and reaped if a test leaves it running.
struct Server {
    child: Child,
    stderr: Option<std::thread::JoinHandle<String>>,
}

impl Server {
    async fn start(volume: &Volume, config: &Path) -> Self {
        let _ = std::fs::remove_file(volume.socket());
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
        let mut server = Self {
            child,
            stderr: Some(stderr),
        };
        let deadline = Instant::now() + BOUND;
        while std::os::unix::net::UnixStream::connect(volume.socket()).is_err() {
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!(
                    "zerofs run exited with {status} before serving; stderr:\n{}",
                    server.stderr()
                );
            }
            assert!(
                Instant::now() < deadline,
                "zerofs run did not serve within {BOUND:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        server
    }

    fn stderr(&mut self) -> String {
        self.stderr
            .take()
            .map(|reader| reader.join().unwrap())
            .unwrap_or_default()
    }

    fn signal(&mut self, signal: i32) {
        let pid = i32::try_from(self.child.id()).unwrap();
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
        let deadline = Instant::now() + BOUND;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "zerofs run ignored signal {signal}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let stderr = self.stderr();
        if signal == libc::SIGTERM {
            assert!(status.success(), "unclean stop; stderr:\n{stderr}");
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn attach(volume: &Volume) -> Arc<NinePClient> {
    let client = NinePClient::connect_unix(volume.socket(), 1 << 20)
        .await
        .expect("connect 9P");
    client
        .attach(ROOT, NOFID, "root", "", 0)
        .await
        .expect("attach root");
    client
}

async fn create(client: &NinePClient, name: &[u8]) {
    let fid = client.alloc_fid();
    client.walk(ROOT, fid, &[]).await.unwrap();
    client
        .lcreate(
            fid,
            name,
            (libc::O_RDWR | libc::O_CREAT | libc::O_EXCL) as u32,
            libc::S_IFREG | 0o644,
            0,
        )
        .await
        .unwrap();
    client.clunk(fid).await.unwrap();
    client.free_fid(fid);
}

/// The entry's mode, or `None` when the name does not resolve.
async fn mode_of(client: &NinePClient, name: &[u8]) -> Option<u32> {
    let fid = client.alloc_fid();
    let mode = match client.walk(ROOT, fid, &[name]).await {
        Ok(qids) if qids.len() == 1 => {
            let stat = client.getattr(fid, ninep_proto::GETATTR_ALL).await.unwrap();
            client.clunk(fid).await.unwrap();
            Some(stat.mode)
        }
        _ => None,
    };
    client.free_fid(fid);
    mode
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_namespace_operations_survive_sigkill_without_fsync() {
    let volume = Volume::new();
    let config = volume.config();

    // The baseline reaches the journal through an orderly stop.
    let mut server = Server::start(&volume, &config).await;
    {
        let client = attach(&volume).await;
        for name in [&b"doomed"[..], b"old", b"kept"] {
            create(&client, name).await;
        }
        client
            .mkdir(ROOT, b"emptied", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
    }
    server.signal(libc::SIGTERM);

    // Every reply below is a success, and no fsync follows.
    let mut server = Server::start(&volume, &config).await;
    {
        let client = attach(&volume).await;
        client.unlinkat(ROOT, b"doomed", 0).await.unwrap();
        client
            .unlinkat(ROOT, b"emptied", libc::AT_REMOVEDIR as u32)
            .await
            .unwrap();
        client.renameat(ROOT, b"old", ROOT, b"new").await.unwrap();
        create(&client, b"fresh").await;
        client
            .mkdir(ROOT, b"made", libc::S_IFDIR | 0o755, 0)
            .await
            .unwrap();
        let fid = client.alloc_fid();
        client.walk(ROOT, fid, &[b"kept"]).await.unwrap();
        client
            .setattr(SetattrBuilder::new(fid).mode(Some(0o600)).build())
            .await
            .unwrap();
        client.clunk(fid).await.unwrap();
        client.free_fid(fid);
    }
    server.signal(libc::SIGKILL);

    let mut server = Server::start(&volume, &config).await;
    let client = attach(&volume).await;
    let mut lost = Vec::new();
    for (name, present) in [
        ("doomed", false),
        ("emptied", false),
        ("old", false),
        ("new", true),
        ("fresh", true),
        ("made", true),
    ] {
        if mode_of(&client, name.as_bytes()).await.is_some() != present {
            lost.push(format!(
                "{name} is {}",
                if present { "missing" } else { "back" }
            ));
        }
    }
    if mode_of(&client, b"kept").await.map(|mode| mode & 0o7777) != Some(0o600) {
        lost.push("kept lost its chmod".to_string());
    }
    drop(client);
    server.signal(libc::SIGTERM);
    assert!(
        lost.is_empty(),
        "acknowledged operations were lost across SIGKILL: {lost:?}"
    );
}

/// Cost of stable namespace replies through 9P. Ignored by default; run with
/// `cargo test --test namespace_durability -- --ignored --nocapture`.
/// `ZEROFS_BENCH_FILES` sets the per-phase file count (default 500).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark"]
async fn bench_namespace_operations() {
    let files: usize = std::env::var("ZEROFS_BENCH_FILES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);
    let volume = Volume::new();
    let config = volume.config();
    let mut server = Server::start(&volume, &config).await;
    let client = attach(&volume).await;
    let rate =
        |started: Instant, operations: usize| operations as f64 / started.elapsed().as_secs_f64();
    let name = |prefix: &str, index: usize| format!("{prefix}{index}").into_bytes();

    let started = Instant::now();
    for index in 0..files {
        create(&client, &name("a", index)).await;
    }
    let create_rate = rate(started, files);

    let started = Instant::now();
    for index in 0..files {
        client
            .renameat(ROOT, &name("a", index), ROOT, &name("b", index))
            .await
            .unwrap();
    }
    let rename_rate = rate(started, files);

    let started = Instant::now();
    for index in 0..files {
        client.unlinkat(ROOT, &name("b", index), 0).await.unwrap();
    }
    let remove_rate = rate(started, files);

    // What tar does per member: create, write 4 KiB, chmod, close.
    let payload = vec![0x5a; 4096];
    let started = Instant::now();
    for index in 0..files {
        let fid = client.alloc_fid();
        client.walk(ROOT, fid, &[]).await.unwrap();
        client
            .lcreate(
                fid,
                &name("t", index),
                (libc::O_RDWR | libc::O_CREAT | libc::O_EXCL) as u32,
                libc::S_IFREG | 0o644,
                0,
            )
            .await
            .unwrap();
        client.write(fid, 0, &payload).await.unwrap();
        client
            .setattr(SetattrBuilder::new(fid).mode(Some(0o640)).build())
            .await
            .unwrap();
        client.clunk(fid).await.unwrap();
        client.free_fid(fid);
    }
    let untar_rate = rate(started, files);

    // Sixteen concurrent creators share each group commit.
    let writers = 16;
    let started = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|writer| {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                for index in 0..files / writers {
                    create(&client, format!("c{writer}-{index}").as_bytes()).await;
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    let concurrent_rate = rate(started, files / writers * writers);

    drop(client);
    server.signal(libc::SIGTERM);
    println!(
        "NAMESPACE_BENCH {{\"files\":{files},\"create_per_s\":{create_rate:.1},\
         \"rename_per_s\":{rename_rate:.1},\"remove_per_s\":{remove_rate:.1},\
         \"untar_files_per_s\":{untar_rate:.1},\"concurrent16_create_per_s\":{concurrent_rate:.1}}}"
    );
}
