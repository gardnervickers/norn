//! Regression: norn-kv-bench accepts responses that differ from the
//! request's correct memcached-binary response.
//!
//! Each fake server answers every GET and SET correctly except for one field.
//! The README promises that every response field except the CAS and a miss's
//! error text is validated, so each corrupted run must be reported as
//! `valid=false`.
#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::sync::{Arc, Mutex};

const HEADER_LEN: usize = 24;

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    /// GET hits return flags 0xdeadbeef instead of the stored flags (0).
    GetFlags,
    /// GET hits echo the key (GETK layout) although the request was GET.
    GetEchoesKey,
    /// SET replies carry a non-empty body.
    SetReplyBody,
    /// Every reply has a nonzero data type byte.
    DataType,
    /// GET misses carry flags extras.
    MissExtras,
    /// GET misses echo the key.
    MissEchoesKey,
}

fn fake_server(fault: Fault) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::default();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let store = Arc::clone(&store);
            std::thread::spawn(move || serve(stream, &store, fault));
        }
    });
    address
}

fn serve(mut stream: TcpStream, store: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, fault: Fault) {
    stream.set_nodelay(true).unwrap();
    let mut header = [0_u8; HEADER_LEN];
    while stream.read_exact(&mut header).is_ok() {
        let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let extras = header[4] as usize;
        let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut body = vec![0; body_len];
        if stream.read_exact(&mut body).is_err() {
            return;
        }
        let key = body[extras..extras + key_len].to_vec();
        let mut response = vec![0_u8; HEADER_LEN];
        response[0] = 0x81;
        response[1] = header[1];
        response[12..16].copy_from_slice(&header[12..16]);
        match header[1] {
            0x01 => {
                store
                    .lock()
                    .unwrap()
                    .insert(key, body[extras + key_len..].to_vec());
                if matches!(fault, Fault::SetReplyBody) {
                    response.extend_from_slice(b"junk");
                }
            }
            0x00 => match store.lock().unwrap().get(&key) {
                Some(value) => {
                    response[4] = 4;
                    if matches!(fault, Fault::GetFlags) {
                        response.extend_from_slice(&0xdead_beef_u32.to_be_bytes());
                    } else {
                        response.extend_from_slice(&[0; 4]);
                    }
                    if matches!(fault, Fault::GetEchoesKey) {
                        response[2..4].copy_from_slice(&(key.len() as u16).to_be_bytes());
                        response.extend_from_slice(&key);
                    }
                    response.extend_from_slice(value);
                }
                None => {
                    response[7] = 1;
                    if matches!(fault, Fault::MissExtras) {
                        response[4] = 4;
                        response.extend_from_slice(&[0; 4]);
                    }
                    if matches!(fault, Fault::MissEchoesKey) {
                        response[2..4].copy_from_slice(&(key.len() as u16).to_be_bytes());
                        response.extend_from_slice(&key);
                    }
                    // Real memcached explains a miss in the body.
                    response.extend_from_slice(b"Not found");
                }
            },
            _ => response[7] = 0x81,
        }
        if matches!(fault, Fault::DataType) {
            response[5] = 1;
        }
        let body_len = (response.len() - HEADER_LEN) as u32;
        response[8..12].copy_from_slice(&body_len.to_be_bytes());
        if stream.write_all(&response).is_err() {
            return;
        }
    }
}

/// Run the real benchmark binary against `address` with a shortened smoke
/// profile and return its first summary line.
fn run_bench(address: SocketAddr, name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "norn-kv-bench-validation-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let smoke = concat!(env!("CARGO_MANIFEST_DIR"), "/profiles/smoke.json");
    let profile = std::fs::read_to_string(smoke)
        .unwrap()
        .replace("\"warmup_ms\": 200", "\"warmup_ms\": 100")
        .replace("\"capacity_ms\": 700", "\"capacity_ms\": 300")
        .replace("\"settle_ms\": 200", "\"settle_ms\": 100")
        .replace("\"segment_ms\": 500", "\"segment_ms\": 300")
        .replace("\"rates\": [5000, 20000]", "\"rates\": [2000]");
    let profile_path = dir.join("profile.json");
    std::fs::write(&profile_path, profile).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_norn-kv-bench"))
        .arg("run")
        .arg(format!("--profile={}", profile_path.display()))
        .arg(format!("--address={address}"))
        .arg(format!("--out={}", dir.join("out").display()))
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find(|line| line.contains("valid="))
        .unwrap_or_else(|| {
            panic!(
                "no summary; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
        .to_owned()
}

#[test]
fn reference_server_is_valid() {
    let summary = run_bench(fake_server(Fault::None), "none");
    assert!(summary.contains("valid=true"), "{summary}");
}

#[test]
fn bench_rejects_wrong_get_flags() {
    let summary = run_bench(fake_server(Fault::GetFlags), "flags");
    assert!(
        summary.contains("valid=false"),
        "GET replies with flags 0xdeadbeef were accepted: {summary}"
    );
}

#[test]
fn bench_rejects_get_reply_that_echoes_the_key() {
    let summary = run_bench(fake_server(Fault::GetEchoesKey), "key");
    assert!(
        summary.contains("valid=false"),
        "GET replies carrying a key were accepted: {summary}"
    );
}

#[test]
fn bench_rejects_set_reply_with_a_body() {
    let summary = run_bench(fake_server(Fault::SetReplyBody), "setbody");
    assert!(
        summary.contains("valid=false"),
        "SET replies carrying a body were accepted: {summary}"
    );
}

#[test]
fn bench_rejects_nonzero_data_type() {
    let summary = run_bench(fake_server(Fault::DataType), "datatype");
    assert!(
        summary.contains("valid=false"),
        "replies with data type 0x01 were accepted: {summary}"
    );
}

#[test]
fn bench_rejects_miss_with_extras() {
    let summary = run_bench(fake_server(Fault::MissExtras), "missextras");
    assert!(
        summary.contains("valid=false"),
        "GET misses carrying extras were accepted: {summary}"
    );
}

#[test]
fn bench_rejects_miss_that_echoes_the_key() {
    let summary = run_bench(fake_server(Fault::MissEchoesKey), "misskey");
    assert!(
        summary.contains("valid=false"),
        "GET misses carrying a key were accepted: {summary}"
    );
}
