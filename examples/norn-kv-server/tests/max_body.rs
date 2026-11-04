//! A `--max-body` beyond the protocol's 32-bit body length is rejected by
//! `ServerConfig::validate` instead of aborting the process on the first
//! connection, where every connection preallocates `max_body_len + 24` bytes.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const HEADER_LEN: usize = 24;

fn noop_request() -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[0] = 0x80;
    bytes[1] = 0x0a;
    bytes[12..16].copy_from_slice(&1_u32.to_be_bytes());
    bytes
}

#[test]
fn oversized_max_body_is_rejected_instead_of_aborting() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_norn-kv-server"))
        .args(["--listen=127.0.0.1:0", "--max-body=1099511627776"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let address: SocketAddr = line
        .trim()
        .strip_prefix("listening on ")
        .expect("server did not start")
        .parse()
        .unwrap();

    // Before validation rejected the limit, the first connection aborted the
    // process. Whether or not the server is still up, poke it once.
    if let Ok(mut stream) = TcpStream::connect(address) {
        let _ = stream.write_all(&noop_request());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server neither rejected the limit nor aborted");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert_eq!(
        status.code(),
        Some(1),
        "expected a clean error exit: {status:?}, stderr: {stderr}"
    );
    assert!(
        stderr.contains("exceeds the protocol's 32-bit limit"),
        "stderr: {stderr}"
    );
}
