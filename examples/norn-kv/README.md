# norn-kv

`norn-kv` is a small fixed-slot key/value store used as an end-to-end example
for Norn's file APIs. It is an example package, not a general-purpose storage
engine.

Each value occupies one fixed-size slot. Records include a generation number
and CRC; the generation is encoded into the returned `Key`, allowing the store
to reject keys for slots that have since been deleted and reused. Opening a
store scans its slots to rebuild the in-memory free list and generation table.

On Linux, the backend uses `norn-uring`, `O_DIRECT`, and aligned buffers. On
other platforms it uses blocking file operations behind the same async-facing
store API.

## Run

```console
cargo run -p norn-kv
```

The program creates a temporary store, writes and reads several values, deletes
one value, reopens the file, and verifies the remaining records.

## Test

```console
cargo test -p norn-kv
```

The tests cover configuration validation, stale keys, slot reuse, recovery,
corruption detection, and I/O behavior.
