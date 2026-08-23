#![cfg(feature = "hotpath-profile")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use serde_json::Value;

const COMPLETED_LABEL: &str = "zerofs.hotpath.lifecycle.completed";
const CANCELLED_LABEL: &str = "zerofs.hotpath.lifecycle.cancelled";

#[test]
fn hotpath_json_distinguishes_completed_and_cancelled_futures() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let address = listener.local_addr().expect("read reserved loopback port");
    drop(listener);
    let report_dir = tempfile::tempdir().expect("create lifecycle report directory");
    let report_path = report_dir.path().join("hotpath-lifecycle.json");

    let mut child = Command::new(env!("CARGO_BIN_EXE_hotpath_lifecycle"))
        .env("HOTPATH_METRICS_PORT", address.port().to_string())
        .env("HOTPATH_METRICS_SERVER_OFF", "false")
        .env("HOTPATH_OUTPUT_FORMAT", "json")
        .env("HOTPATH_OUTPUT_PATH", &report_path)
        .env("HOTPATH_REPORT", "functions-timing,futures,threads")
        .env("HOTPATH_TOKIO_RUNTIME_INTERVAL_MS", "10")
        .env("HOTPATH_CPU_BASELINE_OFF", "true")
        .env("ZEROFS_HOTPATH_LIFECYCLE_HOLD_MS", "250")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start hotpath lifecycle fixture");

    let result = (|| {
        wait_for_lifecycle_evidence(address)?;
        wait_for_tokio_runtime_snapshot(address)?;
        wait_for_graceful_exit(&mut child)?;
        verify_static_report(&report_path)
    })();
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result.expect("Hotpath must retain lifecycle, Tokio, and static JSON evidence");
}

fn wait_for_lifecycle_evidence(address: SocketAddr) -> Result<(), String> {
    for _ in 0..80 {
        if let Some(futures) = get_json(address, "/futures") {
            let completed = lifecycle_state(address, &futures, COMPLETED_LABEL);
            let cancelled = lifecycle_state(address, &futures, CANCELLED_LABEL);
            if completed.as_deref() == Some("ready") && cancelled.as_deref() == Some("cancelled") {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err("did not observe ready and cancelled lifecycle states from Hotpath".to_owned())
}

fn wait_for_tokio_runtime_snapshot(address: SocketAddr) -> Result<(), String> {
    for _ in 0..80 {
        if let Some(snapshot) = get_json(address, "/tokio_runtime")
            && snapshot
                .get("num_workers")
                .and_then(Value::as_u64)
                .is_some_and(|workers| workers > 0)
            && snapshot
                .get("workers")
                .and_then(Value::as_array)
                .is_some_and(|workers| !workers.is_empty())
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err("did not observe a Tokio runtime metrics snapshot from Hotpath".to_owned())
}

fn wait_for_graceful_exit(child: &mut std::process::Child) -> Result<(), String> {
    for _ in 0..80 {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            return status
                .success()
                .then_some(())
                .ok_or_else(|| format!("lifecycle fixture exited unsuccessfully: {status}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err("lifecycle fixture did not exit gracefully".to_owned())
}

fn verify_static_report(path: &std::path::Path) -> Result<(), String> {
    let report = std::fs::read_to_string(path)
        .map_err(|error| format!("read Hotpath JSON report: {error}"))?;
    let report: Value = serde_json::from_str(&report)
        .map_err(|error| format!("parse Hotpath JSON report: {error}"))?;
    if report.get("type").and_then(Value::as_str) != Some("hotpath_report") {
        return Err("Hotpath JSON report did not have type hotpath_report".to_owned());
    }
    for required in ["functions_timing", "futures", "threads"] {
        if report.get(required).is_none() {
            return Err(format!(
                "Hotpath JSON report omitted required {required} section"
            ));
        }
    }
    for forbidden in [
        "functions_alloc",
        "functions_cpu",
        "channels",
        "streams",
        "rw_locks",
        "mutexes",
        "sql",
        "http",
        "io",
        "debug",
        "cpu_baseline",
    ] {
        if report.get(forbidden).is_some() {
            return Err(format!(
                "Hotpath JSON report included forbidden {forbidden} section"
            ));
        }
    }
    Ok(())
}

fn lifecycle_state(address: SocketAddr, futures: &Value, label: &str) -> Option<String> {
    let id = futures
        .get("data")?
        .as_array()?
        .iter()
        .find(|entry| entry.get("label").and_then(Value::as_str) == Some(label))?
        .get("id")?
        .as_u64()?;
    let logs = get_json(address, &format!("/futures/{id}/logs"))?;
    logs.get("calls")?
        .as_array()?
        .iter()
        .find_map(|call| call.get("state").and_then(Value::as_str).map(str::to_owned))
}

fn get_json(address: SocketAddr, path: &str) -> Option<Value> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(25)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = Vec::new();
    let mut buffer = [0; 4 * 1024];
    loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..read]);
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")?
            + 4;
        let headers = std::str::from_utf8(&response[..header_end]).ok()?;
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))?
            .parse::<usize>()
            .ok()?;
        if response.len() >= header_end + content_length {
            return serde_json::from_slice(&response[header_end..header_end + content_length]).ok();
        }
    }
    None
}
