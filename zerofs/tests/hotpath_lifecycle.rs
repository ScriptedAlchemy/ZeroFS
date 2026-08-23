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

    let mut child = Command::new(env!("CARGO_BIN_EXE_hotpath_lifecycle"))
        .env("HOTPATH_METRICS_PORT", address.port().to_string())
        .env("HOTPATH_METRICS_SERVER_OFF", "false")
        .env("HOTPATH_OUTPUT_FORMAT", "none")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start hotpath lifecycle fixture");

    let result = wait_for_lifecycle_evidence(address);
    let _ = child.kill();
    let _ = child.wait();
    result.expect("Hotpath JSON must retain completed and cancelled future lifecycles");
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
