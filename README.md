# Norn

Norn is a Rust workspace for composing single-threaded asynchronous runtimes.
It separates local task scheduling, executor control flow, timers, cross-thread
message delivery, and Linux `io_uring` I/O into independent crates.

The project is experimental, and its APIs are still evolving. Norn targets
custom runtimes built around thread-affine tasks and task wakers: they must be
polled, scheduled, and woken on the runtime thread that owns them.

## Workspace crates

| Crate | Role |
| --- | --- |
| [`norn-task`](norn-task/) | Local task allocation, scheduling, cancellation, and join handles. |
| [`norn-executor`](norn-executor/) | A local executor and the `Park`/`Unpark` interfaces used to compose runtime drivers. |
| [`norn-timer`](norn-timer/) | A timer-wheel `Park` layer with system and simulated clocks. |
| [`norn-uring`](norn-uring/) | A Linux-only `io_uring` driver with filesystem, TCP, UDP, and registered-buffer APIs. |
| [`norn-channel`](norn-channel/) | Bounded cross-thread channels that keep receive-side waking on the destination runtime thread. |
| [`norn-nursery`](norn-nursery/) | Scoped local concurrency for child futures that may borrow from their environment. |
| [`norn-util`](norn-util/) | Utilities for embedding and polling sets of local tasks. |

There is no top-level `norn` runtime crate. Applications select the layers they
need and place them under `norn-executor::LocalExecutor`. For example, the timer
driver wraps another `Park` implementation, while `norn-uring::Driver` can serve
as the Linux I/O layer.

## Minimal executor

```rust
use norn_executor::park::SpinPark;
use norn_executor::{spawn, LocalExecutor};

let mut executor = LocalExecutor::new(SpinPark);
let value = executor.block_on(async {
    spawn(async { 21 * 2 }).await.unwrap()
});

assert_eq!(value, 42);
```

`SpinPark` is useful for examples and cases where busy-spinning is intentional.
`ThreadPark` blocks the runtime thread on a condition variable. On Linux,
`norn-uring::Driver` implements the same `Park` interface and drives I/O
completion while the executor is parked.

## Platform support

`norn-task`, `norn-executor`, `norn-timer`, `norn-channel`, `norn-nursery`, and
`norn-util` are platform-independent Rust crates. `norn-uring` and its I/O APIs
are compiled only on Linux. The workspace is structured so the non-`io_uring`
crates can still be built on macOS.

## Examples

- [`examples/norn-kv`](examples/norn-kv/) is a small block-oriented key/value
  store. It uses `norn-uring` on Linux and a blocking backend elsewhere.
- [`examples/ping-pong-grpc`](examples/ping-pong-grpc/) runs a tonic gRPC
  client and server over Norn's executor, timer, TCP, and filesystem layers.
  This example requires Linux.
- [`norn-channel` crate documentation](norn-channel/src/lib.rs) demonstrates a
  sharded cross-thread channel topology.
- [`norn-nursery` crate documentation](norn-nursery/src/lib.rs) demonstrates
  scoped borrowing, nested child tasks, and early termination.

The [`benches`](benches/) package contains focused task, executor, timer,
channel, HTTP, buffer, and `io_uring` benchmarks. Results and methodology are
recorded alongside the benchmarks.

## Building and checking documentation

The repository includes a Nix development shell with the Rust tooling used by
the project:

```console
nix develop
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Linux and a kernel with `io_uring` support are required to run the
`norn-uring` tests. The other crates can be tested independently with commands
such as `cargo test -p norn-task` or `cargo test -p norn-timer`.

## Design sources

The task representation and scheduling state machine draw on techniques used
by [Tokio](https://github.com/tokio-rs/tokio). The `io_uring` submission model
also draws on [tokio-uring](https://github.com/tokio-rs/tokio-uring), and the
scoped-concurrency API is informed by the
[`moro`](https://github.com/nikomatsakis/moro) experiment. These projects are
design influences. Norn makes no API-compatibility claim.
