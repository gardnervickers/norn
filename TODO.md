# TODO (Prod-Readiness Gaps)

This repo is explicitly experimental today, but the items below are the major gaps to address before considering it production-ready.

## Build / Platform Support

- Gate `norn-uring` (and `io-uring` dependency) behind `cfg(target_os = "linux")` and/or a feature flag so the workspace builds on non-Linux hosts (currently fails on macOS due to `io-uring` crate).
- Decide on stance for non-Linux:
  - Option A: `norn-uring` is Linux-only and other crates still compile.
  - Option B: provide a stub `Park`/driver implementation for non-Linux (likely not worth it unless needed).

## `norn-uring` Driver: SQ Full / Backpressure

- Fix potential deadlock in remote wake path when SQ is full:
  - `Driver::prepare_park` sets the unparker "parked" bit and, if SQ is full, returns `false` without clearing the bit (`norn-uring/src/driver/mod.rs`).
  - Ensure failure to enqueue the eventfd read does not leave the unparker in a permanently “parked” state, and cannot block the event loop without a wake mechanism.
- Consider adding a bounded “SQE permit” / admission control mechanism:
  - Today, `Op::new` allocates the operation before it knows it can enqueue an SQE; `join_all`-style usage can allocate unbounded in-flight ops even with a small ring.
  - Introduce a global cap (e.g. ring depth or configurable limit) for outstanding/queued ops to bound memory and latency.
- Ensure CQ backpressure / `EBUSY` is handled consistently across all submit call-sites (not just `Driver::park`).

## `norn-uring` Driver: Submit Error Handling

- Centralize submit logic + retries:
  - `Driver::park` retries `EBUSY`/`EINTR` but other paths call `submit` and `unwrap`/propagate without the same logic (`shutdown`, `close_fd`, `cancel` paths).
  - Provide a single “submit with mode + retry/drain policy” helper and use it everywhere.
- Decide how to surface submit errors:
  - Consider exposing a driver health/error state so callers can fail fast (instead of panicking) once the driver is broken.

## Shutdown / Cancellation Semantics

- Remove panic-prone assumptions during shutdown:
  - `UnsubmittedOp` currently `expect`s that push cannot fail (“should not be polled during shutdown”) (`norn-uring/src/operation/mod.rs`).
  - Executor `block_on` currently `unwrap`s any `park` error (`norn-executor/src/lib.rs`).
- Define and document shutdown guarantees:
  - What happens to in-flight ops on shutdown? Are they canceled, completed with an error, or possibly never resolve?
  - Ensure “drain token” shutdown cannot spin forever and doesn’t rely on infallible submit.

## Resource Lifetime / Drop Safety

- Audit all drop paths that call `Handle::current()` (TLS-dependent) (e.g. `norn-uring/src/fd.rs`, `norn-uring/src/bufring.rs`):
  - Ensure these drops only happen inside an active driver context, or make them safe/fallible otherwise.
  - Consider explicit close/unregister APIs and avoid “best-effort in Drop” for critical resources.
- Replace internal `unwrap` in drop/cleanup paths with logged errors + graceful fallback where possible.

## Timer Driver

- Implement “fire everything on drop” for timers (TODO exists; `norn-timer/src/wheels.rs`):
  - Define the error returned to sleepers when the driver is dropped/shutdown.

## Test Coverage (High-Value Additions)

- Add tests for:
  - SQ full + `prepare_park` failure + remote `unpark()` wake correctness.
  - CQ full / `EBUSY` behavior for submit across *all* submit call-sites (park, shutdown, cancel, close).
  - Shutdown with many ops blocked on SQ backpressure (no panics, futures resolve deterministically).
  - Drop safety of `NornFd`/`BufRing` outside a driver context (either forbidden by API or gracefully handled).

