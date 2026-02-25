# ping-pong-grpc

A minimal tonic gRPC ping/pong demo running on Norn's single-threaded runtime.

## What this shows

- Running tonic-generated server/client code on top of Norn + `norn-uring`.
- Using Hyper's low-level HTTP/2 connection APIs with Norn executor/timer adapters.
- Keeping handler state in `Rc<RefCell<_>>` (non-`Send`/non-`Sync`) via a strict
  thread-affinity wrapper (`PanicSyncSend`) to satisfy tonic transport bounds.
- Performing async disk I/O inside the handler (`open` + `write_at` + `sync` + `read_at`).

## Run (Linux)

```bash
cargo run -p ping-pong-grpc
```

Expected output:

```text
request: ping from norn
response: pong: ping from norn (count=1, disk_echo=ping from norn)
```

On non-Linux platforms the binary prints a message and exits, because `norn-uring`
is Linux-only.
