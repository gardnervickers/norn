# Repository Guidelines

## What This Repository Is

Norn is an experimental set of Rust crates for building **single-threaded async runtimes** with explicit layering:

1. `norn-task`: intrusive, refcounted local task system.
2. `norn-executor`: `LocalExecutor<P: Park>` event loop over a `TaskQueue`.
3. `norn-timer`: `Park` wrapper that adds timer-wheel scheduling.
4. `norn-uring`: `Park` implementation backed by `io_uring` plus fs/net APIs.
5. `norn-util`: scoped task polling utility (`PollSet`).

The project is intentionally not general-purpose today; APIs are still in flux in executor/uring layers.

## Workspace Layout

- `norn-task/`: local-only task runtime primitives (`TaskQueue`, `TaskSet`, `JoinHandle`, `Runnable`, `Schedule`).
- `norn-executor/`: single-thread executor and `Park` abstraction (`SpinPark`, `ThreadPark`).
- `norn-timer/`: timer wheel + `Clock::{system, simulated}` as a `Park` wrapper.
- `norn-uring/`: `io_uring` reactor, operation lifecycle, stable buffers, bufring, fs, tcp/udp.
- `norn-util/`: `PollSet` future for scoped local task orchestration.
- `benches/`: `bencher` harness (`schedule_task`, `noop_submit`, `timers`, `hyper`).
- `hack/`: helper scripts (`miri.sh`, `coverage.sh`).
- `.github/workflows/`: CI (`cargo build`, `cargo test`, `nix build` on Ubuntu).

## Runtime Architecture (Important)

### `norn-task` invariants

- Tasks are single-allocation (`TaskCell`) objects with type-erased vtables.
- Wakers and scheduling are **thread-affine**. `task_cell::check_caller` asserts same thread id.
- State machine is in `state.rs` (`NOTIFIED`, `RUNNING`, `COMPLETE`, `JOIN_HANDLE`, `CANCELLED`) with strict assertions.
- `abort_on_panic` is used in core state paths; invariant violations abort the process.
- `FutureCell` captures task panics into `TaskError::Panic`, and cancellation as `TaskError::Cancelled`.
- `TaskSet::bind` is `unsafe`; caller must guarantee captured lifetimes outlive task execution.

### `norn-executor` model

- `LocalExecutor::block_on` runs:
  1. root future harness poll,
  2. runnable task drain loop,
  3. `park(mode)`.
- `Handle::current()` comes from TLS context and panics outside executor context.
- `block_on` currently does `self.park.park(mode).unwrap()`: park errors panic.
- `Drop` enters context, shuts down `TaskQueue`, then calls `Park::shutdown`.

### `norn-timer` model

- `Driver<P>` wraps another `Park`, advances wheels each park cycle, and clamps timeout to next timer expiration.
- Hierarchical wheel: 6 levels, 64 slots per level, millisecond ticks.
- `Sleep` futures resolve to `Result<(), norn_timer::Error>`; shutdown path returns shutdown errors.
- Simulated clock is heavily used in tests/benchmarks.
- Known gap: dropping timer driver does not currently fire all outstanding sleepers (tracked TODO in tests/comments).

### `norn-uring` model

- `Driver` owns ring + backpressure notifier + status (`Running`, `Draining`, `Shutdown`).
- Submission queue pressure is handled by `PushFuture` waiting on `Notify`.
- Driver park loop retries `EBUSY`, loops on `EINTR`, drains CQ before parking.
- Remote unpark is eventfd-based (`driver/unpark.rs`).
- `Driver::drop` calls `Park::shutdown`; shutdown path uses cancellation + IO_DRAIN token.
- Special CQE user_data tokens are reserved (`<= 1024`).

### `norn-uring` operation lifecycle

- `Op<T>` has staged state:
  - `Unsubmitted` (waiting for SQ space),
  - `Submitted` (awaiting completions).
- Dropping submitted `Op` attempts cancellation (`cancel(user_data)`).
- `Operation::cleanup` is used for resource cleanup of dropped/unconsumed completions.
- `UnsubmittedOp` currently expects push success and can panic if polled during shutdown.

### FD and buffer lifetime constraints

- `NornFd` is refcounted and waits for `strong_count == 1` before close submission.
- `NornFd::Inner::drop` and `BufRing::drop` call `Handle::current()`; dropping outside driver context is unsafe/panic-prone.
- I/O buffers must implement `StableBuf`/`StableBufMut` (stable pointer guarantees).
- Bufring uses registered kernel buffer rings and returns buffers to ring on `BufRingBuf::drop`.

## Platform and Build Constraints

- This repo is actively developed on both macOS and Linux.
- `norn-task`, `norn-executor`, `norn-timer`, and `norn-util` are expected to build and benchmark on macOS and Linux.
- `norn-uring` is Linux-only and is explicitly gated with `cfg(target_os = "linux")` (crate + integration tests).
- On macOS, `norn-uring` is compiled out; workspace checks should still pass for non-uring crates.
- CI runs on Ubuntu:
  - `.github/workflows/rust.yml`: `cargo build`, `cargo test`.
  - `.github/workflows/build_nix.yml`: `nix build`.

## Build, Test, and Benchmark Commands

- `nix develop`: enter dev shell (fenix nightly toolchain + miri + rustfmt + clippy + cargo-udeps/outdated).
- `cargo build`: build workspace default members.
- `cargo test`: run all tests (requires Linux/io_uring for `norn-uring` tests).
- `cargo test -p norn-task`: targeted task runtime tests.
- `cargo test -p norn-timer`: timer wheel tests including proptests.
- `cargo test -p norn-uring --test udp`: targeted integration test.
- `cargo bench -p benches --bench schedule_task -- bench_join`: filtered bench via bencher filter arg.
- `cargo bench -p benches --bench timers -- bench_timers`: timer wheel/executor integration benchmarks.
- `./hack/miri.sh`: run `cargo miri test`.
- `./hack/coverage.sh`: run `cargo llvm-cov --lcov --output-path lcov.info`.

### Lima Workflow for macOS (`norn-uring`)

- `norn-uring` must be developed/tested on Linux. On macOS, use Lima and run Linux commands via flake apps.
- First ensure `limactl` is installed on macOS (`brew install lima`).
- `nix run .#uring-vm-up`: create/start the `norn-uring` Lima VM, ensure writable host mount, and install Nix in the guest when missing.
- `nix run .#uring-shell`: open a Linux shell in this repo path and enter `nix develop`.
- `nix run .#uring-test`: run `nix develop -c cargo test -p norn-uring` inside the Lima VM.
- Under the hood these commands call `hack/lima-norn-uring.sh`.
- Optional environment variables:
  - `LIMA_INSTANCE_NAME` (default `norn-uring`).
  - `LIMA_HOST_MOUNT` (default `$HOME`; mounted writable into guest).
  - `REPO_HOST_PATH` (default git root / current directory).

## Benchmarking Methodology (Performance Changes)

- Always benchmark before and after with the same command/filter.
- Use the `bencher` harness filters for focused cases (for example `bench_spawn/num_tasks=1024`).
- Collect a baseline full matrix first, then repeat key hot cases at least 3 times and compare medians.
- Keep runtime checks consistent for perf branches:
  - `cargo fmt --all`
  - `cargo test` (or targeted crate tests during iteration, full/expanded before commit)
  - `cargo clippy --all-targets --all-features -- -D warnings`
- Use profiling to pick targets instead of guessing:
  - macOS: `sample <bench-binary-name> 8 1 -wait -mayDie -file /tmp/<name>.sample.txt`
  - Build debuginfo bench binaries for readable symbols: `CARGO_PROFILE_BENCH_DEBUG=true cargo bench -p benches --bench <name> --no-run`
  - Use the `Sort by top of stack` section to identify dominant functions.
  - Linux (when available): use `perf record`/`perf report` for equivalent sampling.
- Only checkpoint a perf commit when the gain is material and repeatable; include exact before/after numbers and commands in the commit body.

## Test Coverage Map

- `norn-task/src/tests/`:
  - abort/drop/wake interactions,
  - panic behavior,
  - Tokio-inspired task combination matrix.
- `norn-executor/src/lib.rs` tests:
  - basic `block_on`,
  - spawn timing (before/in/after runtime),
  - context-based spawn.
- `norn-timer/src/tests/`:
  - smoke tests with simulated clock,
  - wheel property tests (`proptest`) for advance behavior/termination.
- `norn-uring/tests/`:
  - noop submission pressure,
  - file open/read/write,
  - tcp accept/echo,
  - udp send/recv, bufring usage, exhaustion, close behavior.

## Coding and Documentation Standards

- Rust 2021, `cargo fmt`.
- Prefer clippy-clean changes: `cargo clippy --all-targets --all-features -- -D warnings`.
- Public API additions usually require rustdoc; crates generally deny `missing_docs`.
- Naming follows Rust defaults:
  - modules/functions: `snake_case`,
  - types/traits: `CamelCase`,
  - constants: `SCREAMING_SNAKE_CASE`.

## Change Guidance for Agents

- Keep edits localized and behavior-preserving unless explicitly requested otherwise.
- When changing semantics, add or update tests in the crate that owns the behavior.
- For `norn-uring`, validate shutdown/cancel/drop implications explicitly; many paths are panic-sensitive.
- Avoid introducing cross-thread assumptions; core runtime is single-thread/local by design.
- Respect existing layering:
  - `norn-task` should not depend on executor/uring specifics,
  - `Park` integration belongs in executor/timer/uring boundaries.

## PR Expectations

- Use descriptive imperative commit subjects (avoid generic `WIP` for final PR commits).
- Include:
  - intent,
  - key design decisions,
  - exact test/bench commands run,
  - platform notes (especially Linux/io_uring constraints).
