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
