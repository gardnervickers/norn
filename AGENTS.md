# Repository Guidelines

## Project Structure & Module Organization

Rust workspace (`Cargo.toml`) with crate-per-module organization:

- `norn-task/`: task primitives (`TaskQueue`, `TaskSet`, `JoinHandle`, `Schedule`/`Runnable`). Tests in `norn-task/src/tests/` cover wake/abort/panic behavior.
- `norn-executor/`: `LocalExecutor<P: Park>` event loop + TLS handle access. `norn-executor/src/park/` defines the `Park` layering model (`SpinPark`, `ThreadPark`).
- `norn-timer/`: timer `Park` wrapper (`Driver<P>`) that produces `Sleep` futures; uses `Clock::{system,simulated}` and a hierarchical wheel. Includes `proptest`-based checks.
- `norn-uring/`: `io_uring` `Park` driver plus higher-level APIs: `operation/` (op lifecycle/cancel), `buf`/`bufring` (stable/registered buffers), `fs/` (files), `net/` (TCP/UDP). Integration tests live in `norn-uring/tests/` (see `tests/util.rs`).
- `norn-util/`: `PollSet` (a `Future` that runs a scoped set of local tasks).
- `benches/`: `bencher`-based microbenchmarks (`benches/*.rs`).

Tooling/ops: `hack/` (dev scripts), `flake.nix`/`default.nix`/`.envrc` (Nix + direnv).

## Build, Test, and Development Commands

- `nix develop`: enter the Nix dev shell (tooling includes `cargo`, `rustfmt`, `clippy`, `miri`).
- `cargo build`: build the workspace (default members).
- `cargo test`: run unit + integration tests.
- `cargo test -p norn-timer`: run tests for a single crate.
- `cargo bench -p benches -- bench_join`: run a filtered benchmark (the `bencher` harness uses the first arg as a filter).
- `./hack/miri.sh`: run `cargo miri test` (slower, catches UB).
- `./hack/coverage.sh`: produce `lcov.info` via `cargo llvm-cov` (requires `cargo-llvm-cov`).

## Coding Style & Naming Conventions

- Rust 2021 edition; format with `cargo fmt` (rustfmt defaults; 4-space indentation).
- Keep clippy clean: `cargo clippy --all-targets --all-features -- -D warnings`.
- Naming: crates `norn-*`, modules/functions `snake_case`, types `CamelCase`, constants `SCREAMING_SNAKE_CASE`.
- Most crates deny `missing_docs`; add rustdoc when introducing new public items.

## Testing Guidelines

- Unit tests live inline (`mod tests`) and in `*/src/tests/`.
- Integration tests live in `norn-uring/tests/` where appropriate.
- Prefer targeted runs while iterating (e.g., `cargo test -p norn-task`).
- `norn-uring` tests require a working `io_uring` environment; CI runs on Ubuntu.

## Commit & Pull Request Guidelines

- Git history contains many `WIP` commits; for PRs, use descriptive, imperative subjects (e.g., `timer: fix wheel rollover`).
- PRs should include: purpose, approach, how to test (`cargo test ...`), and any platform constraints (notably `io_uring`).
- CI runs `cargo build`, `cargo test`, and `nix build`; keep these passing.

## Agent-Specific Instructions

- Keep changes minimal and localized; update/add tests when behavior changes.
