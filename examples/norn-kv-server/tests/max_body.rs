//! A large `--max-body` must not cost memory up front. A limit beyond the
//! protocol's 32-bit body length is rejected before the server listens, and a
//! large valid limit leaves connections serving under an address-space limit,
//! where a frame too large to buffer fails only its own connection.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const HEADER_LEN: usize = 24;
/// Address-space limit for the server process, far below the 4 GB limit the
/// tests configure.
const ADDRESS_SPACE_LIMIT: libc::rlim_t = 1 << 30;

fn noop_request() -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[0] = 0x80;
    bytes[1] = 0x0a;
    bytes[12..16].copy_from_slice(&1_u32.to_be_bytes());
    bytes
}

/// A SET header declaring `body_len` bytes of body, sent without the body.
fn set_header(body_len: u32) -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[0] = 0x80;
    bytes[1] = 0x01;
    bytes[2..4].copy_from_slice(&3_u16.to_be_bytes());
    bytes[4] = 8;
    bytes[8..12].copy_from_slice(&body_len.to_be_bytes());
    bytes[12..16].copy_from_slice(&2_u32.to_be_bytes());
    bytes
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server did not exit");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn oversized_max_body_is_rejected_before_listening() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_norn-kv-server"))
        .args(["--listen=127.0.0.1:0", "--max-body=1099511627776"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let status = wait_for_exit(&mut child);
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
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
    assert_eq!(stdout, "", "the server announced a listener it cannot run");
    assert!(
        stderr.contains("exceeds the protocol's 32-bit limit"),
        "stderr: {stderr}"
    );
}

/// Start the server with a 4 GB body limit under [`ADDRESS_SPACE_LIMIT`].
fn spawn_limited(mode: &str) -> (Child, SocketAddr) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_norn-kv-server"));
    command
        .args(["--listen=127.0.0.1:0", "--max-body=4000000000", mode])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // Safety: `setrlimit` is async-signal-safe and touches no parent state.
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: ADDRESS_SPACE_LIMIT,
                rlim_max: ADDRESS_SPACE_LIMIT,
            };
            if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let address = line
        .trim()
        .strip_prefix("listening on ")
        .unwrap_or_else(|| panic!("server did not start: {line:?}"))
        .parse()
        .unwrap();
    (child, address)
}

fn assert_noop_round_trips(address: SocketAddr) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(&noop_request()).unwrap();
    let mut reply = [0; HEADER_LEN];
    stream
        .read_exact(&mut reply)
        .expect("server did not answer NOOP");
    assert_eq!(&reply[12..16], &1_u32.to_be_bytes());
}

fn large_limit_serves_connections(mode: &str) {
    let (mut child, address) = spawn_limited(mode);
    assert_noop_round_trips(address);

    // A frame declaring a body larger than the address-space limit can only
    // fail its own connection.
    {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&set_header(3_900_000_000)).unwrap();
        stream.write_all(&[0; 64 * 1024]).unwrap();
        // Exact mode sizes the body buffer from the header and fails the
        // connection; multishot modes buffer only what arrives and keep
        // waiting.
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let _ = stream.read(&mut [0; 1]);
    }
    assert_noop_round_trips(address);

    assert!(
        child.try_wait().unwrap().is_none(),
        "server exited under the address-space limit"
    );
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn large_max_body_serves_connections_multishot() {
    large_limit_serves_connections("--recv-mode=multishot");
}

#[test]
fn large_max_body_serves_connections_exact() {
    large_limit_serves_connections("--recv-mode=exact");
}

#[test]
fn large_max_body_serves_connections_sharded() {
    large_limit_serves_connections("--workers=2");
}
