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

## 2026-05-12: `uring_realworld` TCP Multishot Bufring

Target benchmark added in commit `4fe518d`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response/runtime=norn/recv=bufring_multi/connections=8/requests_per_connection=64/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Baseline

- Focused median, Norn `recv=bufring_multi`: `1.055 ms`.
- Same focused case, Norn `recv=normal`: `1.370 ms`.
- Same focused case, Norn `recv=bufring`: `1.473 ms`.
- Longer-lived connection check:
  - `recv=bufring_multi/connections=8/requests_per_connection=512/payload=64`:
    `7.420 ms`.
  - `recv=normal/connections=8/requests_per_connection=512/payload=64`:
    `10.453 ms`.
  - `recv=bufring/connections=8/requests_per_connection=512/payload=64`:
    `10.992 ms`.

### Benchmark Bug Found and Fixed

- Initial `recv=bufring_multi` shape hung during cleanup.
  - Cause: the multishot receive op owns an `NornFd` reference, and the
    benchmark awaited `socket.close()` while the multishot op was still in
    scope.
  - Fix: scope the multishot op so it is dropped before closing the socket.

### Tried and Rejected

- Public `TcpSocket::send_all` using direct nonblocking socket sends.
  - Change: added an owned-buffer `send_all` API that tried direct socket sends
    and fell back to the existing io_uring send path for unsupported
    descriptors.
  - Result: focused `recv=bufring_multi` median regressed from `1.055 ms` to
    about `1.221 ms`; single-shot bufring also regressed.
  - Reason rejected: the existing io_uring send path is better for this
    workload.

- Increase inline completion storage from one CQE to four CQEs.
  - Change: changed `CompletionQueue` from `SmallVec<[CQEResult; 1]>` to
    `SmallVec<[CQEResult; 4]>`.
  - Result: focused `recv=bufring_multi` median regressed from `1.055 ms` to
    about `1.079 ms`; the 512-request case regressed from `7.420 ms` to about
    `7.568 ms`.
  - Reason rejected: larger inline storage did not help this multishot path and
    slightly slowed it down.

- Non-full submission queue fast-submit path.
  - Change: added a checked `Handle::try_push` path so `Op::poll_submit` could
    push immediately when the driver was running and the SQ had space, avoiding
    `PushFuture` construction in the common path.
  - Result: initially looked like about `2%` on the 512-request multishot case,
    but repeat/cross-checks did not hold: focused 64-request target was about
    baseline or slower, 512-request target was noise, and the UDP target
    regressed to about `7.904 ms`.
  - Reason rejected: not a repeatable material win, and it was already noisy on
    the earlier UDP work.

- Return the multishot `more` bit from the raw completion vtable.
  - Change: avoided a duplicate `CQEResult::more()` call by having
    `RawOp<T>::complete` return the `more` flag to `RawOpRef::complete`.
  - Result: focused 64-request target regressed to about `1.062 ms`; the
    512-request target stayed around baseline.
  - Reason rejected: no measurable improvement.

- Fast-path `pop()` for single queued multishot completions.
  - Change: used `SmallVec::pop()` instead of `remove(0)` when the completion
    queue length was exactly one.
  - Result: combined with SQ fast-submit it did not improve the focused target
    and only moved the 512-request case by about `1%`.
  - Reason rejected: sub-threshold and not reliable.

### Ideas That Need a Different Benchmark Shape

- Add a Norn-only `recv=bufring_bundle_multi` shape using `recv_bundle_multi`.
  Bundle receive may reduce per-completion overhead when frames span multiple
  buffers, but it is a distinct kernel/API path from `recv_ring_multi`.

- Use longer-lived connections as the primary target when optimizing persistent
  server behavior. The `requests_per_connection=512` case has lower setup and
  cancellation weight than the focused 64-request case.

## 2026-05-12: `uring_realworld` TCP Bundle Multishot Bufring

Target benchmark added in commit `204844d`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response/runtime=norn/recv=bufring_bundle_multi/connections=8/requests_per_connection=64/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Baseline

- Focused 64-request median, Norn `recv=bufring_bundle_multi`: `1.056 ms`.
- Same focused case, Norn `recv=bufring_multi`: `1.050 ms`.
- Longer-lived 512-request median, Norn `recv=bufring_bundle_multi`: `7.492 ms`.
- Same longer-lived case, Norn `recv=bufring_multi`: `7.395 ms`.

### Result

- Bundle multishot is a useful benchmark shape, but it was not a win for 64-byte
  fixed-frame loopback TCP.
- It was roughly tied with `recv=bufring_multi` on the short target and slower
  on the longer-lived target.
- Revisit with larger payloads or frames that naturally span multiple buffers;
  that is where bundle receive is more likely to matter.

## 2026-05-12: `uring_realworld` TCP Coordination

Target benchmark added in commit `9c56541`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response_coord/runtime=norn/coord=scan/recv=bufring_multi/connections=8/requests_per_connection=512/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Profile Signal

Profiled target:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response/runtime=norn/recv=bufring_multi/connections=8/requests_per_connection=512/payload=64
```

Top samples from the generated flamegraph data:

- `FuturesUnordered::next` poll path: `75.86%`.
- TCP client body: `34.14%`.
- TCP echo connection body: `31.03%`.
- Driver park path: `21.38%`.
- Send helper: `19.66%`.
- Driver CQ drain: `17.59%`.
- Multishot receive helper: `15.52%`.
- `RawOp<T>::complete`: `11.38%`.
- `FuturesUnordered` wake path: `7.59%`.

### Baseline

- Focused 64-request unordered median:
  `1.054 ms` (`1.064515`, `1.049078`, `1.053556` ms).
- Focused 64-request scan median:
  `1.015 ms` (`1.063173`, `1.013193`, `1.015427` ms).
- Longer-lived 512-request unordered median:
  `7.409 ms` (`7.408739`, `7.407505`, `7.470373` ms).
- Longer-lived 512-request scan median:
  `7.079 ms` (`7.052264`, `7.120222`, `7.078803` ms).

### Result

- Scan coordination was faster than `FuturesUnordered` fan-in for this fixed
  eight-connection benchmark: about `3.7%` on the 64-request target and `4.5%`
  on the 512-request target.
- This is useful benchmark evidence, but it is benchmark orchestration rather
  than a runtime fast path. Keep using the original unordered benchmark for
  apples-to-apples comparisons, and use the coordination benchmark to isolate
  fan-in overhead.
- Next coordination target: compare against `norn-util::PollSet` or a small
  reusable poll-set abstraction before changing runtime internals.

### PollSet Follow-up

`coord=pollset` was added in commit `7341409`.

Repeated three-way comparison after adding `coord=pollset`:

- Focused 64-request unordered median:
  `1.059 ms` (`1.059631`, `1.046450`, `1.058910` ms).
- Focused 64-request scan median:
  `1.012 ms` (`1.009356`, `1.020000`, `1.012157` ms).
- Focused 64-request PollSet median:
  `1.068 ms` (`1.065084`, `1.084633`, `1.067776` ms).
- Longer-lived 512-request unordered median:
  `7.444 ms` (`7.364495`, `7.443810`, `7.467562` ms).
- Longer-lived 512-request scan median:
  `7.380 ms` (`7.085961`, `7.394979`, `7.379787` ms).
- Longer-lived 512-request PollSet median:
  `7.403 ms` (`7.390006`, `7.402693`, `7.403087` ms).

Profiled target after the scan change:

```text
NORN_BENCH_PPROF=/tmp/norn-tcp-scan-pprof cargo bench -p benches \
  --bench uring_realworld -- \
  bench_tcp_request_response_coord/runtime=norn/coord=scan/recv=bufring_multi/connections=8/requests_per_connection=512/payload=64
```

Top inclusive samples from the generated flamegraph data:

- Scan coordination closure: `78.03%`.
- TCP client body: `41.26%`.
- TCP echo connection body: `35.43%`.
- Send helper: `19.28%`.
- Multishot receive helper: `17.94%`.
- Driver park path: `17.04%`.
- Driver CQ drain: `14.35%`.
- `RawOp<T>::complete`: `4.93%`.

Result:

- `PollSet` is not a win for this fixed eight-connection benchmark. It was
  slower than scan at 64 requests and roughly tied with the noisy 512-request
  results.
- The direct scan helper remains the best coordination shape measured so far,
  but the 512-request coordination delta is small enough that future runtime
  optimization work should use repeated runs.
- After removing `FuturesUnordered`, the remaining profile points at send,
  multishot receive, driver park/drain, and raw completion overhead.

Tried and rejected after this profile:

- Skip replacing an operation waker when `Waker::will_wake` says the current
  registered waker is equivalent.
  - Change: `Header::set_waker` checked the existing waker before cloning and
    storing the new one.
  - Result: improved the scan-shaped 512-request target from the noisy
    `7.380 ms` median to about `6.911 ms`, but did not carry over to the stable
    unordered/default shape: unordered 512 stayed about `7.46 ms`, and
    unordered 64 moved from about `1.059 ms` to about `1.084 ms`.
  - Reason rejected: the win appears tied to scan polling repeatedly with the
    same root waker, while the benchmark's default orchestration did not benefit
    and the short target regressed.

## 2026-05-12: `uring_realworld` TCP Lifecycle

Target benchmark added in commit `bca7814`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response_lifecycle/runtime=norn/recv=bufring_multi/connections=8/requests_per_connection=1/payload=64
```

Linux runs were executed through the Lima `norn-uring` VM.

### Matrix

One-pass lifecycle matrix:

- `requests_per_connection=1`
  - `recv=normal`: `96.579 us`.
  - `recv=bufring`: `158.908 us`.
  - `recv=bufring_multi`: `171.653 us`.
  - `recv=bufring_bundle_multi`: `170.325 us`.
- `requests_per_connection=4`
  - `recv=normal`: `164.027 us`.
  - `recv=bufring`: `225.079 us`.
  - `recv=bufring_multi`: `218.880 us`.
  - `recv=bufring_bundle_multi`: `218.359 us`.
- `requests_per_connection=16`
  - `recv=normal`: `417.931 us`.
  - `recv=bufring`: `466.575 us`.
  - `recv=bufring_multi`: `395.470 us`.
  - `recv=bufring_bundle_multi`: `390.741 us`.
- `requests_per_connection=64`
  - `recv=normal`: `1.431 ms`.
  - `recv=bufring`: `1.444 ms`.
  - `recv=bufring_multi`: `1.196 ms`, high variance in this matrix run.
  - `recv=bufring_bundle_multi`: `1.112 ms`.

Repeated key cases:

- `recv=normal/requests_per_connection=1` median:
  `93.716 us` (`93.929`, `92.625`, `93.716` us).
- `recv=bufring_multi/requests_per_connection=1` median:
  `170.426 us` (`169.805`, `170.426`, `171.153` us).
- `recv=bufring_bundle_multi/requests_per_connection=1` median:
  `171.368 us` (`171.368`, `170.877`, `172.373` us).
- `recv=normal/requests_per_connection=16` median:
  `417.367 us` (`417.367`, `417.515`, `417.153` us).
- `recv=bufring_multi/requests_per_connection=16` median:
  `395.709 us` (`395.709`, `394.715`, `398.830` us).

### Result

- Multishot receive is the wrong path for one-request connections in this
  benchmark: `recv=bufring_multi` was about `82%` slower than `recv=normal`.
- The multishot path crosses over by about 16 requests per connection:
  `recv=bufring_multi` was about `5.2%` faster than `recv=normal` in repeated
  16-request runs.
- This supports using `requests_per_connection=512` as the persistent-server
  optimization target and keeping a separate lifecycle target for setup,
  teardown, and multishot cancellation/drop sensitivity.

## 2026-05-12: `uring_realworld` TCP Linked Requests

Target benchmark added in commit `b9e768a`:

```text
cargo bench -p benches --bench uring_realworld -- \
  bench_tcp_request_response_linked/runtime=norn/recv=bufring_linked/connections=8/requests_per_connection=512/payload=1
```

Linux runs were executed through the Lima `norn-uring` VM.

### Baseline

One-pass linked matrix:

- `recv=bufring/requests_per_connection=64/payload=1`: `1.443 ms`.
- `recv=bufring_linked/requests_per_connection=64/payload=1`: `1.489 ms`.
- `recv=bufring/requests_per_connection=512/payload=1`: `10.237 ms`.
- `recv=bufring_linked/requests_per_connection=512/payload=1`: `10.890 ms`.

Repeated key cases:

- `recv=bufring/requests_per_connection=64` median:
  `1.457 ms` (`1.440772`, `1.456641`, `1.476816` ms).
- `recv=bufring_linked/requests_per_connection=64` median:
  `1.509 ms` (`1.504386`, `1.509079`, `1.527765` ms).
- `recv=bufring/requests_per_connection=512` median:
  `10.699 ms` (`10.584496`, `10.730947`, `10.699473` ms).
- `recv=bufring_linked/requests_per_connection=512` median:
  `10.968 ms` (`10.967942`, `10.954609`, `11.048699` ms).

### Result

- Linked send/receive submission was not a win for this loopback
  request/response shape.
- The linked path was about `3.6%` slower at 64 requests and about `2.5%`
  slower at 512 requests.
- Do not revisit request linking as a generic send/completion optimization
  without a benchmark shape that can benefit from deeper independent batches or
  from avoiding multiple explicit round trips.

## 2026-07-10: Timer and `io_uring` Follow-up

Portable profiling found three repeatable timer-wheel wins:

- Lazy minimum-expiration recomputation after timer cancellation reduced the
  4,096-timer cancellation benchmark from `11.818 ms` to `107.618 us` and also
  improved the 64- and 512-timer cases by about `38%` and `88%`.
- Storing the six wheel levels inline reduced the 256-timer benchmark by about
  `6.1%` to `6.4%` across the 1-, 32-, and 64-task cases.
- Removing `RefCell` borrow checks from the scoped timer-entry waker reduced
  those same cases by another `5.7%` to `9.3%`.

The latter two changes were checked with fresh-target Miri wheel tests and
reduced the cancellation target by a further `5.6%` in combination.

Tried and rejected after the retained timer changes:

- Rewrite `Sleep::poll` to collapse its initial-registration and reset paths.
  - Result: the 256-timer 1-, 32-, and 64-task cases regressed from about
    `8.373`, `6.104`, and `7.238 us` to about `13.5`, `8.9`, and `9.8 us`.
  - Reason rejected: large, consistent regressions in the primary timer target.
- Remove the raw-operation lifecycle vtable, retesting commit `e13cf44` against
  the current tree on Linux.
  - Result: file round trips moved from a `113.239 ms` median to `113.113 ms`
    (about `0.1%` faster), while noop submission moved from `28.107 ms` to
    `28.240 ms` (about `0.5%` slower).
  - Reason rejected: no material gain and a small regression in the focused
    submission target.
- Replace operation-header completion and waker `RefCell`s with scoped
  `UnsafeCell` access.
  - Result: file round trips improved from `113.239 ms` to `109.138 ms`
    (`3.6%`) and noop submission from `28.107 ms` to `27.683 ms` (`1.5%`).
  - Reason rejected: both results were below the `5%` materiality threshold and
    did not justify adding unsafe lifecycle code.
- Port fixed-buffer file I/O from commit `a3a72d6`.
  - Result at 16,384 4-KiB operations: fixed-buffer direct writes took
    `368.722 ms` versus `366.894 ms` for ordinary direct I/O; fixed-buffer
    direct reads took `368.425 ms` versus a noisy `383.217 ms` direct-I/O run
    and `368.008 ms` buffered run.
  - Reason rejected: no write gain, no gain over buffered reads, and at most a
    noisy `3.9%` gain over direct reads for roughly 1,000 lines of API and test
    surface.
- Port the batched UDP send-bundle branch (`0252622`/`8175bcf`).
  - Result for 2,048 4-KiB datagrams (16 x 256-byte segments): copy/coalesce
    median `4.136 ms`; bundle median `5.129 ms` (`24%` slower).
  - Result for 2,048 32-KiB datagrams (64 x 512-byte segments): copy/coalesce
    median `11.012 ms`; bundle median `12.437 ms` (`13%` slower).
  - Reason rejected: the bundle path lost even in the copy-heavy case where it
    had the best chance to win, while adding substantial buffer lifecycle and
    operation API complexity.

The unmerged ZCRX branch (`582d451`) was not treated as an optimization of the
existing Norn receive path. It adds a separate kernel/NIC facility, and its
microbenchmark measures only its own completion parsing and refill helper with
no existing-path comparator. Evaluate it as a feature on supported hardware,
not as evidence for a portable runtime fast-path change.

## 2026-07-11: Zen 3 Workstation Follow-up

Linux measurements used a Ryzen 9 5950X with the governor set to
`performance`. Focused cases were pinned to CPU 15, whose SMT sibling was
idle. The clean baseline was current master at `e719d3a`.

Retained changes:

- Submit an `io_uring` operation immediately when the SQ has capacity, and
  construct the backpressure waiter only after a known-full result.
  - 64 tasks / 100,000 noops: `15.907 ms` to `14.089 ms` median,
    `11.4%` faster.
  - Forced backpressure: `2.142 ms` to `2.186 ms`, `2.1%` slower and below
    the materiality threshold.
  - File round trips improved by `1.1%`; repeated loopback UDP/TCP checks were
    neutral to slightly faster.
- Inline `TaskQueue::next` and `Rc<Shared>::schedule`, the two task dispatch
  boundaries that remained visible in the profile.
  - 32 tasks x 32 yields: `11,205 ns` to `9,449 ns` median, `15.7%`
    faster.
  - 128 tasks x 32 yields: `45,437 ns` to `37,382 ns` median, `17.7%`
    faster.
  - Spawn 1,024 tasks: `39,369 ns` to `37,728 ns`, `4.2%` faster.
- Replace the executor TLS context's `RefCell` with the same scoped
  `UnsafeCell` discipline already used by the timer context.
  - Ready `block_on`: `5 ns` to `4 ns` median.
  - Yield-once `block_on`: `7 ns` to `6 ns` median.
  - `cargo miri test -p norn-executor` passed all seven tests.
- Pipeline up to 64 norn-kv recovery reads and preserve aligned Linux buffers
  across block I/O instead of copying through `Vec`.
  - 256 live slots, three current-master runs: `11.315`, `11.309`, and
    `11.308 ms`; median `11.309 ms`.
  - Optimized runs: `4.631`, `4.704`, and `4.680 ms`; median
    `4.680 ms`, `58.6%` faster.
  - The recovered prototype's buffer allocation was generalized so the
    non-Linux `Vec` backend still compiles and passes tests.

Profiles explained the retained wins:

- Before task dispatch inlining, `Schedule` and `TaskQueue::next` accounted
  for about `18.5%` and `12.4%` of samples. Both disappeared from the
  post-change profile.
- Before immediate submission, `PushFuture::poll` and `Handle::push`
  accounted for about `8.4%` and `5.4%`. The waiter path disappeared from
  the common-case profile after the change.
- Removing executor context borrow checks roughly halved the sampled
  `Context::enter` share in the yield-once target.

Tried and rejected:

- Widen task state flags from `u8` to `u32`: at most about `4.2%` on the
  largest yield case, with smaller results elsewhere.
- Inline the `Runnable::run` wrapper: no incremental gain after the two
  retained dispatch annotations.
- Force-inline the immediate SQ push helper: no measurable improvement.
- Replace the driver's ring `RefCell` with unchecked local `UnsafeCell`
  access: about `1.1%` on noop submission and `1.2%` on forced
  backpressure, below the threshold for added unsafe code.

### Workstation completion pass

The direct-buffer UDP benchmark change was revalidated against its immediate
parent (`2e1cd68`) on the same pinned CPU:

- Single receive, window 32 / 4,096 requests:
  - Before: `30.904`, `30.627`, and `30.554 ms`; median `30.627 ms`.
  - Current master: `24.529`, `24.293`, and `24.305 ms`; median
    `24.305 ms`, `20.6%` faster.
- Multishot receive, window 64 / 8,192 requests:
  - Before: `55.862`, `55.987`, and `55.926 ms`; median `55.926 ms`.
  - Current master: `49.519`, `49.486`, and `49.563 ms`; median
    `49.519 ms`, `11.5%` faster.

The current UDP profile was diffuse. The largest runtime-specific self costs
were `Handle::try_push` at `4.5%` and `Op::poll` at `4.0%`.
`FuturesUnordered` coordination was about `3%`; prior scan and `PollSet`
comparisons already showed no material general win.

The first concurrent KV recovery implementation still drained reads in
discrete batches of 64. Keeping one `FuturesUnordered` queue continuously
replenished removed the batch barriers. Window-size screening found:

- 64 reads: `3.477 ms` median (`3.477`, `3.645`, `3.312 ms`).
- 128 reads: `2.493 ms` median (`2.527`, `2.480`, `2.493 ms`).
- 256 reads: `2.846 ms` in the screening run.

The selected 128-read window was `46.7%` faster than the established current
baseline of `4.680 ms`. With the example's smaller 64-entry ring, a focused
check improved from `4.854 ms` to `2.501 ms`, confirming that submission
backpressure does not erase the win.

Profiling the sliding window showed `crc_fast::checksum_combine` at `53%` of
sampled CPU cycles. Streaming the zeroed header and payload through one
`crc_fast::Digest` preserves the existing on-disk checksum while avoiding
polynomial combination:

- Sliding window only: `2.493 ms` median.
- Streaming CRC: `2.132 ms` median (`2.024`, `2.132`, `2.301 ms`),
  another `14.5%` improvement.
- Cumulative versus current master: `54.4%` faster.

The final profile had no comparable CPU hotspot: block-read setup was `6.4%`,
memset `5.5%`, and CRC `2.2%`. Skipping the aligned-buffer zero fill with
tracked initialization was tested and rejected at `2.146 ms`, slightly slower
than the retained shape and below the materiality threshold.

## 2026-07-14: Thread-safe Root Waker

Goal: replace the executor root waker's `Rc<Cell<bool>>`-backed raw `Waker`
with a thread-safe implementation while measuring the cost on the root
`block_on` path. Lower `ns/iter` is better. Correctness requires executor tests
and a cross-thread wake test through `ThreadPark`.

Environment and methodology:

- Local NixOS Linux host, AMD Ryzen 9 5950X (16 cores / 32 threads), 62 GiB
  memory, Linux 6.18.38.
- Repository state: clean `codex/optimize-udp-kv-performance` at
  `4b369f50df8eb60152b8350f319cc885db50a941` before the experiment.
- Toolchain: `rustc 1.85.0-nightly (6d9f6ae36 2024-12-16)`, Cargo
  `1.85.0-nightly (769f622e1 2024-12-14)` from `nix develop`.
- Command: `taskset -c 2 nix develop -c cargo bench -p benches --bench executor`.
- Seven complete runs before and after the change. Primary metric is the median
  `bench_block_on_yield` result; `bench_block_on_ready` is secondary. Raw logs
  are stored under `/tmp/norn-root-waker-benchmark/`.
- Any movement is reported; a regression above 5% requires investigation.

### Baseline

- Seven `bench_block_on_ready` runs: `4`, `4`, `4`, `4`, `4`, `4`, and
  `4 ns/iter`; median `4 ns/iter`.
- Seven `bench_block_on_yield` runs: `6`, `6`, `6`, `6`, `6`, `6`, and
  `6 ns/iter`; median `6 ns/iter`.
- CPU 2 used the `performance` frequency governor. The harness reported zero
  integer-nanosecond dispersion in every run, so the baseline is stable but
  changes smaller than its one-nanosecond output resolution cannot be resolved.

### Attempt 1: `Arc<AtomicBool>` root notification

Hypothesis: a stable `Wake` implementation backed by `Arc<AtomicBool>` and the
park driver's `Unparker` closes the raw-waker thread-safety hole; latching
`ThreadPark` notifications prevents wake-before-park loss.

- Correctness: `cargo test -p norn-executor` passed all nine tests, including a
  root waker fired from another thread and a deterministic pre-park unpark test.
- Seven ready runs: `6`, `8`, `6`, `6`, `6`, `6`, and `6 ns/iter`; median
  `6 ns/iter`, 50% slower than baseline.
- Seven yield-once runs: `8`, `9`, `8`, `8`, `8`, `8`, and `8 ns/iter`;
  median `8 ns/iter`, 33.3% slower than baseline.
- Decision: revise. The atomic swap used to consume each notification is the
  leading candidate: ready performs one swap and yield-once performs two.

### Attempt 2: generation-counted notification

Hypothesis: use a monotonically increasing atomic generation so polling needs
only atomic loads; the wake side performs the sole read-modify-write. This
cannot lose a wake between observation and consumption.

- Correctness: all nine executor tests passed.
- Seven ready runs: all `5 ns/iter`; median `5 ns/iter`, 25% slower than
  baseline and one nanosecond faster than Attempt 1.
- Seven yield-once runs: all `7 ns/iter`; median `7 ns/iter`, 16.7% slower than
  baseline and one nanosecond faster than Attempt 1.
- Decision: revise. The remaining per-`block_on` difference includes creating
  and dropping a `Waker` backed by an atomically reference-counted `Arc`.

### Attempt 3: cache the root `Waker`

Hypothesis: retain the stable `Waker` alongside its `Arc` state across
`block_on` calls, avoiding one atomic clone/drop pair per call.

- Correctness: all nine executor tests passed.
- Seven ready runs: all `8 ns/iter`; median `8 ns/iter`, 100% slower than
  baseline and 60% slower than Attempt 2.
- Seven yield-once runs: all `8 ns/iter`; median `8 ns/iter`, 33.3% slower
  than baseline and 14.3% slower than Attempt 2.
- Decision: abandon and restore Attempt 2. The longer-lived state-plus-waker
  ownership shape consistently optimized worse than reconstructing the waker.

### Attempt 4: cache with separated hot fields

Hypothesis: cache the waker as in Attempt 3, but destructure the reusable bundle
so `FutureHarness` retains the same separate state and waker field layout as
Attempt 2.

- Correctness: all nine executor tests passed.
- Seven ready runs: all `7 ns/iter`; median `7 ns/iter`, 75% slower than
  baseline and 40% slower than Attempt 2.
- Seven yield-once runs: all `8 ns/iter`; median `8 ns/iter`, 33.3% slower
  than baseline and 14.3% slower than Attempt 2.
- Decision: abandon. Separating the fields recovered one nanosecond in the
  ready case versus Attempt 3, but both cached forms were clearly worse.

### Attempt 5: borrowed Arc-backed context waker

Hypothesis: keep one executor-owned `Arc` reference and construct a borrowed
raw `Waker` for each poll context. The borrowed waker is never dropped; only
clones that escape the poll increment the atomic strong count. Its vtable uses
`Arc` operations and is fully thread-safe.

- Correctness: all nine executor tests passed.
- Seven ready runs: `4`, `4`, `4`, `4`, `4`, `4`, and `5 ns/iter`; median
  `4 ns/iter`, equal to baseline.
- Seven yield-once runs: all `6 ns/iter`; median `6 ns/iter`, equal to
  baseline.
- Decision: revise before accepting. Inspection found that unconditionally
  invoking the park driver's unparker would make same-thread io_uring
  completion wakes touch the remote eventfd coordination path.

### Attempt 6: suppress owner-thread unparks

Hypothesis: record the executor owner thread and invoke the park driver's
unparker only for wakes from a different thread, avoiding redundant io_uring
eventfd coordination for local completions.

- Correctness: all ten executor tests passed, including explicit checks that a
  same-thread root wake does not unpark and a remote root wake does.
- Seven ready runs: `4`, `5`, `4`, `4`, `4`, `4`, and `4 ns/iter`; median
  `4 ns/iter`, equal to baseline.
- Seven yield-once runs: `7`, `8`, `7`, `7`, `7`, `7`, and `7 ns/iter`;
  median `7 ns/iter`, 16.7% slower than baseline.
- Decision: revise. Thread identity lookup adds one nanosecond to the direct
  borrowed-waker wake path even though that waker cannot escape the poll.

### Attempt 7: split borrowed and owned vtables

Hypothesis: the context's borrowed waker cannot escape `Future::poll` without
being cloned, so its direct wake path can increment the generation without an
owner check. Clones receive a distinct owned vtable that performs atomic Arc
ownership and owner-aware remote unparking.

- Correctness: all ten executor tests passed. The same-thread test exercises an
  owned clone, while the remote-thread test sends and consumes an owned clone
  on another thread.
- Seven ready runs: all `4 ns/iter`; median `4 ns/iter`, equal to baseline.
- Seven yield-once runs: all `5 ns/iter`; median `5 ns/iter`, 16.7% faster
  than the original baseline at the harness's integer-nanosecond resolution.
- Exact-tree baseline refresh from
  `4b369f50df8eb60152b8350f319cc885db50a941`: all seven ready runs were
  `4 ns/iter`, and all seven yield-once runs were `6 ns/iter`, confirming the
  comparison did not move with workstation drift.
- Decision: keep. `cargo test` passed the full workspace, including the
  `norn-uring` integration tests. `cargo fmt --all -- --check` and
  `cargo clippy --all-targets --all-features -- -D warnings` passed. Targeted
  `cargo miri test -p norn-executor` passed all ten executor tests and doc
  tests, covering both borrowed and owned raw-waker paths.

### Cumulative result

- Retained design: an executor-owned `Arc` notification generation, a borrowed
  context waker whose clones become owned Arc-backed wakers, remote-only
  unparking, and latched `ThreadPark` notifications.
- Ready `block_on`: `4 ns/iter` baseline and final median; no measurable
  regression.
- Yield-once `block_on`: `6 ns/iter` baseline to `5 ns/iter` final median;
  one nanosecond faster in this microbenchmark.
- Confidence: high for the focused executor microbench because both sides were
  stable across seven runs and the original tree was refreshed after the final
  candidate. Absolute deltas below one nanosecond remain below harness
  resolution.
