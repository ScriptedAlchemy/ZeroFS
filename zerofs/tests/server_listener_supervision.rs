use std::io::Read;
use std::process::ExitStatus;
use std::process::Stdio;
use std::time::Duration;

fn wait_for_exit(child: &mut std::process::Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll child process") {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn occupied_sole_listener_exits_promptly_with_addr_in_use() {
    let binary = env!("CARGO_BIN_EXE_zerofs");
    let mut warmup = std::process::Command::new(binary)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start zerofs warmup");
    let warmup_status = wait_for_exit(&mut warmup, Duration::from_secs(60)).unwrap_or_else(|| {
        warmup.kill().expect("kill stuck zerofs warmup");
        panic!("{binary} did not start within 60 seconds");
    });
    assert!(warmup_status.success(), "zerofs warmup failed");

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();

    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let config_path = root.path().join("zerofs.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
[cache]
dir = {cache_dir:?}
disk_size_gb = 0.01
memory_size_gb = 0.01
warm_metadata = "off"

[storage]
url = "memory:///listener-supervision"
encryption_password = "test-password"

[servers]

[servers.nbd]
addresses = ["{address}"]

[telemetry]
enabled = false
"#,
            cache_dir = cache_dir.display(),
        ),
    )
    .unwrap();

    let mut command = std::process::Command::new(binary);
    command
        .arg("run")
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = command.spawn().expect("run zerofs server");
    let mut child_stderr = child.stderr.take().expect("capture server stderr");
    let stderr_reader = std::thread::spawn(move || {
        let mut stderr = Vec::new();
        child_stderr.read_to_end(&mut stderr).unwrap();
        stderr
    });

    let status = if let Some(status) = wait_for_exit(&mut child, Duration::from_secs(15)) {
        status
    } else {
        child.kill().expect("kill stuck zerofs server");
        let _ = child.wait();
        let stderr = String::from_utf8_lossy(&stderr_reader.join().unwrap()).into_owned();
        panic!("{binary} stayed alive after its sole listener failed to bind:\n{stderr}");
    };
    let stderr = String::from_utf8_lossy(&stderr_reader.join().unwrap()).into_owned();

    assert!(!status.success(), "server unexpectedly succeeded");
    assert!(
        stderr.contains("Address already in use") || stderr.contains("AddrInUse"),
        "listener failure was not returned: {stderr}"
    );
}
