# Performance Notes

This file records benchmark-driven optimization attempts that were not kept, or
that were kept only as part of a larger change. Check this before revisiting
runtime benchmark work.

## 2026-05-12: `uring_realworld` UDP Request/Response

Target benchmark:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_udp_request_response/runtime=norn/recv=single/window=64/total_requests=4096/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Baseline and Kept Change

- Baseline Norn median: `10.259 ms`.
- Kept direct nonblocking UDP `recv_from`/`send_to` fast path plus inline single
  completion storage.
- Final Norn median: `7.751 ms`, `24.4%` faster than baseline.
- Same-run comparison runtime median: `9.399 ms`; final Norn result was `17.5%`
  faster.
- Commit: `fddead2` (`Improve UDP realworld path`).

### Tried and Rejected

- Pre-arm multishot UDP receive futures before request sends.
  - Result: no meaningful difference in the target shape, roughly `10.039 ms`
    versus `10.162 ms` in noisy single runs.
  - Reason rejected: futures should not perform I/O until polled, and the user
    explicitly asked to preserve that Rust async invariant.

- Inline completion storage by itself.
  - Result: initial target median moved from `10.259 ms` to about `9.971 ms`,
    but later repeat runs showed about `10.067 ms`.
  - Reason not counted alone: below the `5%` threshold and somewhat noisy.
  - It was kept in the final patch because it removes the common singleshot heap
    allocation and combined cleanly with the UDP fast path.

- Fast non-full SQ push path.
  - Change: added a direct `try_push` path before constructing `PushFuture`.
  - Result: no meaningful target improvement by itself. With the final UDP fast
    path it moved the median only from about `7.674 ms` to `7.633 ms`, inside
    observed run noise.
  - Reason rejected: added lifecycle/backpressure complexity for a tiny,
    unreliable win.

- Skip backpressure notification when there are no waiters.
  - Result: regressed the target median to about `10.345 ms`.
  - Reason rejected: clear regression.

- Direct raw socket address parser for receive completions.
  - Change: replaced `SockAddr::as_socket` conversion with unsafe raw IPv4/IPv6
    parsing.
  - Result: regressed the target median to about `7.83 ms`.
  - Reason rejected: slower and more unsafe code.

### Ideas That Need a Different Benchmark Shape

- Connected UDP sockets using `send`/`recv` instead of `send_to`/`recv_from`.
  This may expose a different cost profile, but it changes the API shape being
  compared.

- Server-side multishot or bufring receive. This may show Norn-specific upside,
  but it is no longer the same basic UDP socket workload.

- Spin/retry receive after send on loopback. This might help the current
  loopback request/response workload, but it is likely too benchmark-shaped
  without a broader runtime policy.

## 2026-05-12: `uring_realworld` TCP Request/Response

Target benchmark added in commit `abb6eeb`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response/runtime=norn/recv=normal/connections=8/requests_per_connection=64/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Baseline

- Focused apples-to-apples median, Norn normal: `1.335 ms`.
- Focused apples-to-apples median, comparison runtime normal: `1.393 ms`.
- Focused Norn bufring median: `1.457 ms`.
- Broader one-off checks:
  - `connections=1/requests_per_connection=512/payload=64`: Norn `1.542 ms`,
    comparison runtime `1.748 ms`.
  - `connections=64/requests_per_connection=64/payload=64`: Norn `11.468 ms`,
    comparison runtime `11.811 ms`.
  - `connections=8/requests_per_connection=64/payload=1024`: Norn `1.739 ms`,
    comparison runtime `1.796 ms`.

### Tried and Rejected

- Raw direct TCP stream `recv`/`send` helpers.
  - Change: bypassed per-call `socket2::Socket` wrapper creation in
    `TcpStreamReader`/`TcpStreamWriter` by calling `libc::recv`/`libc::send`
    directly after readiness.
  - Result: focused Norn normal median regressed from `1.335 ms` to about
    `1.368 ms`; focused bufring also regressed.
  - Reason rejected: slower and added unsafe raw socket code.

- Immediate no-op `TcpStreamWriter::poll_flush`.
  - Change: returned `Poll::Ready(Ok(()))` for TCP stream flush instead of
    polling write readiness and calling socket `flush`.
  - Result: focused Norn normal median regressed to about `1.405 ms`.
  - Reason rejected: plausible API cleanup, but not a performance win in this
    benchmark and below the bar.

- Direct nonblocking `accept4` fast path before io_uring accept.
  - Change: tried queued accepts directly before falling back to the existing
    io_uring accept operation.
  - Result: `connections=64/requests_per_connection=64/payload=64` moved only
    from a prior `11.468 ms` one-off baseline to an `11.345 ms` median, roughly
    `1%`; the focused 8-connection case was noisy/slower.
  - Reason rejected: real idea, but not material enough for this workload.

### Ideas That Need a Different Benchmark Shape

- Add a Norn-only TCP `recv=bufring_multi` shape using `recv_ring_multi`.
  The current `recv=bufring` variant intentionally uses single-shot bufring
  receives, which makes it a useful baseline but not the best-case registered
  buffer-ring path.

- Isolate bufring receive from send-path overhead. The current bufring variant
  uses `TcpSocket::send`, which submits io_uring send operations, while the
  normal stream path uses readiness plus direct socket sends. A future benchmark
  shape may need a split writer/socket API or a dedicated connected-socket send
  fast path to compare only receive-buffer strategy.
