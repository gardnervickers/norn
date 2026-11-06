//! A large `--max-body` must not cost memory up front. A limit beyond the
//! protocol's 32-bit body length is rejected before the server listens, and a
//! large valid limit leaves connections serving under an address-space limit,
//! where a frame too large to buffer fails only its own connection and a
//! frame that was buffered but cannot be stored fails only its own request.
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
/// Body bytes streamed for one frame before giving up on the server failing
/// the connection; more than [`ADDRESS_SPACE_LIMIT`].
const STREAM_LIMIT: u32 = 3 << 30;

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
    // fail its own connection. Exact mode sizes the body buffer from the
    // header and fails at once; multishot modes buffer what arrives, so
    // stream the body until the server gives up on the connection.
    {
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream.write_all(&set_header(3_900_000_000)).unwrap();
        let chunk = vec![0; 1024 * 1024];
        let mut streamed = 0_u64;
        while streamed < u64::from(STREAM_LIMIT) {
            if stream.write_all(&chunk).is_err() {
                break;
            }
            streamed += chunk.len() as u64;
        }
        assert!(
            streamed < u64::from(STREAM_LIMIT),
            "server buffered {streamed} bytes of one frame without failing"
        );
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

/// Send one SET of `value_len` bytes and return the status of its reply, if
/// the server sent one.
fn send_large_set(address: SocketAddr, key: &[u8], value_len: usize) -> Option<u16> {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let body_len = 8 + key.len() + value_len;
    let mut header = [0_u8; HEADER_LEN];
    header[0] = 0x80;
    header[1] = 0x01;
    header[2..4].copy_from_slice(&(key.len() as u16).to_be_bytes());
    header[4] = 8;
    header[8..12].copy_from_slice(&(body_len as u32).to_be_bytes());
    header[12..16].copy_from_slice(&2_u32.to_be_bytes());
    let _ = stream.write_all(&header);
    let _ = stream.write_all(&[0; 8]);
    let _ = stream.write_all(key);
    let chunk = vec![b'v'; 1024 * 1024];
    let mut sent = 0;
    while sent < value_len {
        let n = chunk.len().min(value_len - sent);
        if stream.write_all(&chunk[..n]).is_err() {
            break;
        }
        sent += n;
    }
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut reply = [0; HEADER_LEN];
    stream.read_exact(&mut reply).ok()?;
    Some(u16::from_be_bytes([reply[6], reply[7]]))
}

/// A value that fits the receive buffers but whose copy into the store does
/// not must not abort the process: the SET is answered with the protocol's
/// out-of-memory status and the server keeps serving.
fn large_set_fails_only_its_request(mode: &str, keys: &[&[u8]], value_len: usize) {
    let (mut child, address) = spawn_limited(mode);
    for key in keys {
        let status = send_large_set(address, key, value_len);
        assert!(
            child.try_wait().unwrap().is_none(),
            "one client's SET killed the server"
        );
        assert_eq!(status, Some(0x0082), "SET of {value_len} bytes under limit");
    }
    assert_noop_round_trips(address);
    assert!(
        child.try_wait().unwrap().is_none(),
        "server exited under the address-space limit"
    );
    let _ = child.kill();
    let _ = child.wait();
}

/// Multishot: `pending` grows by doubling to 512 MiB, which fits; the 480 MB
/// store copy then does not.
#[test]
fn large_buffered_set_fails_only_its_request_multishot() {
    large_set_fails_only_its_request("--recv-mode=multishot", &[b"big"], 480_000_000);
}

/// Exact: the 540 MB body buffer fits; the 540 MB store copy does not.
#[test]
fn large_buffered_set_fails_only_its_request_exact() {
    large_set_fails_only_its_request("--recv-mode=exact", &[b"big"], 540_000_000);
}

/// Sharded: whichever worker owns the key, copying the value either to the
/// owner or into its store fails only that request. Several keys cover both
/// the local and the remote path.
#[test]
fn large_buffered_set_fails_only_its_request_sharded() {
    large_set_fails_only_its_request(
        "--workers=2",
        &[b"big0", b"big1", b"big2", b"big3"],
        480_000_000,
    );
}

fn get_request(key: &[u8], opaque: u32) -> Vec<u8> {
    let mut request = vec![0_u8; HEADER_LEN];
    request[0] = 0x80;
    request[2..4].copy_from_slice(&(key.len() as u16).to_be_bytes());
    request[8..12].copy_from_slice(&(key.len() as u32).to_be_bytes());
    request[12..16].copy_from_slice(&opaque.to_be_bytes());
    request.extend_from_slice(key);
    request
}

/// The server's virtual memory size in bytes, from `/proc/<pid>/status`.
fn vm_size(pid: u32) -> usize {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let kib: usize = status
        .lines()
        .find_map(|line| line.strip_prefix("VmSize:"))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse().ok())
        .expect("VmSize in /proc status");
    kib * 1024
}

/// Wait until the server's virtual memory size stops changing, so that memory
/// freed by earlier connections does not skew a later measurement.
fn settled_vm_size(pid: u32) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = vm_size(pid);
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let now = vm_size(pid);
        if now == last || Instant::now() >= deadline {
            return now;
        }
        last = now;
    }
}

/// How a pipelined GET connection ended.
#[derive(Debug, PartialEq, Eq)]
enum PipelineEnd {
    /// Every reply, up to the final NOOP's, arrived.
    Complete,
    /// The server closed the connection before the final reply.
    Closed,
}

/// Read replies until the one for `last_opaque` arrives (checking that the
/// GET of `get_opaque` carries a `value_len` value if it is answered) or the
/// connection ends.
fn read_pipeline(
    stream: &mut TcpStream,
    get_opaque: u32,
    value_len: usize,
    last_opaque: u32,
) -> PipelineEnd {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut sink = vec![0; 1024 * 1024];
    loop {
        let mut header = [0; HEADER_LEN];
        match stream.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return PipelineEnd::Closed
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                return PipelineEnd::Closed
            }
            Err(error) => panic!("connection neither answered nor closed: {error}"),
        }
        let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
        let mut body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        if opaque == get_opaque {
            assert_eq!(body_len, 4 + value_len, "GET reply length");
        }
        while body_len != 0 {
            let want = body_len.min(sink.len());
            match stream.read(&mut sink[..want]) {
                Ok(0) => return PipelineEnd::Closed,
                Ok(n) => body_len -= n,
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                    return PipelineEnd::Closed
                }
                Err(error) => panic!("connection neither answered nor closed: {error}"),
            }
        }
        if opaque == last_opaque {
            return PipelineEnd::Complete;
        }
    }
}

/// Store `stored` (the first entry is read back), then pipeline a GET for it
/// behind six misses (which, in sharded mode, queue the big reply behind
/// remote ones) and a final NOOP. `held_body` bytes of an unfinished SET on
/// another connection take up memory meanwhile. The server must survive and
/// keep answering whichever way the GET ends: with every reply, or, if the
/// reply cannot be buffered, with only that connection closed.
///
/// Returns how the GET connection ended.
fn large_get_fails_only_its_connection(
    mode: &str,
    stored: &[(&str, usize)],
    held_body: usize,
) -> PipelineEnd {
    const GET_OPAQUE: u32 = 100;
    const NOOP_OPAQUE: u32 = 999;
    let (mut child, address) = spawn_limited(mode);
    for (key, len) in stored {
        assert_eq!(
            send_large_set(address, key.as_bytes(), *len),
            Some(0),
            "setup SET of {key} must succeed"
        );
    }

    // Another connection holds `held_body` bytes of an unfinished SET. Wait
    // until the server has reserved them rather than guessing how long that
    // takes.
    let mut holder = TcpStream::connect(address).unwrap();
    if held_body != 0 {
        let before = settled_vm_size(child.id());
        let mut header = [0_u8; HEADER_LEN];
        header[0] = 0x80;
        header[1] = 0x01;
        header[2..4].copy_from_slice(&1_u16.to_be_bytes());
        header[4] = 8;
        header[8..12].copy_from_slice(&(9 + held_body as u32).to_be_bytes());
        header[12..16].copy_from_slice(&3_u32.to_be_bytes());
        holder.write_all(&header).unwrap();
        holder.write_all(&[0; 8]).unwrap();
        holder.write_all(b"h").unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while vm_size(child.id()) < before + held_body * 9 / 10 {
            assert!(
                Instant::now() < deadline,
                "server never reserved the held SET body"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    let mut stream = TcpStream::connect(address).unwrap();
    let mut pipeline = Vec::new();
    for (opaque, key) in [b"a", b"b", b"c", b"d", b"e", b"f"].iter().enumerate() {
        pipeline.extend_from_slice(&get_request(*key, 10 + opaque as u32));
    }
    pipeline.extend_from_slice(&get_request(stored[0].0.as_bytes(), GET_OPAQUE));
    let mut noop = noop_request();
    noop[12..16].copy_from_slice(&NOOP_OPAQUE.to_be_bytes());
    pipeline.extend_from_slice(&noop);
    stream.write_all(&pipeline).unwrap();
    let end = read_pipeline(&mut stream, GET_OPAQUE, stored[0].1, NOOP_OPAQUE);
    drop(stream);

    std::thread::sleep(Duration::from_millis(300));
    drop(holder);
    assert!(
        child.try_wait().unwrap().is_none(),
        "one client's GET pipeline killed the server"
    );
    assert_noop_round_trips(address);
    let _ = child.kill();
    let _ = child.wait();
    end
}

#[test]
fn large_get_fails_only_its_connection_exact() {
    // 600 MB stored and 350 MB held by an unfinished SET on another
    // connection: a 150 MB reply no longer fits under the 1 GiB limit.
    let stored: Vec<_> = ["k0", "k1", "k2", "k3"]
        .into_iter()
        .map(|key| (key, 150_000_000))
        .collect();
    let end = large_get_fails_only_its_connection("--recv-mode=exact", &stored, 350_000_000);
    assert_eq!(end, PipelineEnd::Closed, "the reply cannot have fit");
}

#[test]
fn large_get_fails_only_its_connection_sharded() {
    // The reply fits on the owning worker. Behind the misses it is either
    // handed over to the connection's batch without a copy, so the GET is
    // answered, or the copy does not fit and only this connection is closed.
    // Which one happens depends on how the replies interleave; the server
    // must survive both.
    let mut stored = vec![("big", 200_000_000)];
    stored.extend(
        [
            "filler0", "filler1", "filler2", "filler3", "filler4", "filler5",
        ]
        .into_iter()
        .map(|key| (key, 50_000_000)),
    );
    large_get_fails_only_its_connection("--workers=2", &stored, 0);
}
