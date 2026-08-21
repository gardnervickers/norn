# Norn disk-backed key/value server

Status: proposed

## Decision

Build a new Linux-only Norn application that implements the Memcached binary
protocol over TCP and stores every value on disk. The application keeps only
keys and record locations in memory. It uses a preallocated, double-buffered,
fixed-record file, `O_DIRECT | O_DSYNC`, registered aligned buffers, and a
single Norn executor.

The application is deliberately narrow:

- Memcached binary `GET`, `SET`, `DELETE`, `NOOP`, and `STAT` operations;
- keys up to 250 bytes and a configured value limit;
- one configured record size and one preallocated data file (initially 8 KiB
  extents for values up to 4 KiB, or 64 KiB extents for values up to 32 KiB);
- strict durability by default;
- no expiration, CAS, compression, replication, compaction, or online resize;
- one runtime thread, with a future instance-per-core sharding model.

The current `examples/norn-kv` should not become the engine. Its fixed-slot
format, CRC checks, aligned direct I/O, and recovery tests are useful source
material, but its opaque generated keys, single mutable store, scratch-buffer
reuse, and value-returning API conflict with a keyed concurrent server. Keep it
until the new application replaces its filesystem coverage, then remove it in
a separate change.

## Why this workload

This application makes Norn coordinate network input, protocol parsing,
bounded resource admission, concurrent disk I/O, checksums, cancellation,
network output, and shutdown. `memtier_benchmark` can drive it without a custom
client. Since values are absent from the in-memory index, each successful GET
performs a disk read and each mutation performs a disk write.

The Memcached binary protocol is important to the receive path. Its fixed
24-byte header declares the exact body length. For SET, the body is
`extras || key || value`. After reading the header, the server can receive that
body directly into its final location in an aligned disk record.

Memcached has deprecated this protocol for new clients in favor of its text and
meta protocols. The compatibility goal here is interoperability with an
existing load generator, not a forward-looking Memcached replacement. The
fixed header earns its place by removing an application value copy from SET.

The first implementation processes requests sequentially within each TCP
connection. Many concurrent connections provide disk queue depth while keeping
per-connection response ordering and cancellation rules simple. Pipelined
clients remain protocol-correct, but one connection contributes at most one
storage operation at a time. Per-connection concurrent dispatch is a later,
separately measured optimization.

## Architecture

```text
TCP connection task
  -> binary protocol codec
  -> bounded local command queue
  -> storage coordinator
       -> in-memory key -> cell index
       -> per-cell state and wait queues
       -> bounded read/write FuturesUnordered
       -> preallocated O_DIRECT | O_DSYNC file
  <- operation result + owned buffer
  <- ordinary send or SEND_ZC
```

One executor thread owns all application state. No application lock or
cross-thread channel is needed. A storage coordinator owns the index, free-cell
list, sequence allocator, per-cell state, file, and submitted operations.
Connection tasks exchange commands and replies with bounded local channels.
Once a mutation is admitted, the coordinator owns it through terminal kernel
completion; disconnecting a client only discards its reply.

The registered fixed-buffer table is wrapped by an application `BufferBank`.
It acquires every registered slot at startup and lends a class token plus an
owned `FixedBuf` from separate ingress and egress queues. Generic network I/O
can own a stable lease wrapper. Fixed disk I/O currently requires the concrete
`FixedBuf`, so the coordinator retains the class token separately while the raw
buffer is kernel-owned and recombines them only after terminal completion. A
future fixed-buffer SEND_ZC operation must retain both objects by the same rule.
This partition prevents slow readers holding SEND_ZC buffers from starving
writes. Exhaustion suspends admission rather than allocating an unregistered
fallback buffer.

Initial configuration parameters are:

- cell count and aligned record size;
- ingress and egress fixed-buffer counts;
- maximum admitted storage operations;
- local command queue capacity;
- ordinary-send/SEND_ZC size threshold;
- strict or explicitly volatile durability mode.

The buffer counts, command capacity, and storage queue depth bound all major
per-request memory and kernel ownership.

## File geometry and record format

The file contains two aligned, CRC-protected superblock copies followed by a
fixed array of cells. Every cell contains record extents A and B. All offsets
and extent lengths are multiples of the filesystem direct-I/O alignment.
An 8 KiB extent is the initial default and accommodates the storage header,
a 250-byte key, protocol extras, and a 4 KiB value. A 64 KiB configuration with
a 32 KiB value limit supplies a large-payload workload without adding
multi-extent records.

The immutable superblock contains:

- magic and format version;
- store UUID;
- cell count, record size, and alignment;
- maximum key and value lengths;
- CRC32C.

Each record header contains:

- magic and format version;
- cell number;
- nonzero, nonwrapping global sequence number (`FREE` uses zero);
- kind: `FREE`, `PUT`, or `TOMBSTONE`;
- key length, value length, and Memcached flags;
- body offset and meaningful body length;
- CRC32C over the header with a zeroed CRC field and all meaningful body bytes.

A PUT body uses the received Memcached SET layout:

```text
[storage header][SET extras][key][value][unused padding]
```

The format guarantees at least 28 writable bytes immediately before every
value. GET validation snapshots all needed metadata before that prefix is
overwritten with the response header and flags in the in-memory read buffer.

CRC and length fields make padding irrelevant. The backing allocation is fully
initialized before registration; stale padding is outside the CRC, is never
returned to a client, and need not be cleared on the write path. The file is
created with mode `0600`. A confidentiality requirement for retired raw bytes
would add explicit clearing as a separately measured policy.

Store creation is intentionally slow and explicit. It builds a temporary file,
preallocates its final geometry, writes a valid `FREE` record to copy A of every
cell, writes both superblocks, performs a full file sync covering data and file
metadata, atomically renames the file, and syncs the parent directory. Recovery
thereafter requires at least one valid record per cell; two invalid copies are
corruption rather than an ambiguous unused cell.

## Operation contracts

### SET

1. Read exactly 24 bytes into a small owned cursor buffer and validate the
   opcode, key length, extras length, and total body length. The cursor exposes
   only its unfilled tail on each receive, because a plain `Vec` would overwrite
   its beginning after a fragmented short receive.
2. Acquire an ingress fixed buffer and select the final record-body range.
3. Loop on `TcpSocket::recv`, calling `consume(n)` after each successful short
   read, until the exact `extras || key || value` body occupies the disk buffer.
4. Call `reset_range()` to restore the complete extent, then validate the extras
   and key, assign or locate a cell, assign a fresh sequence number, write the
   storage header, and calculate CRC32C in place.
5. Call `set_len(extent_len)` and submit a fixed-buffer write to the cell's
   inactive copy.
6. Require an exact-length successful completion. Only then publish the new
   current copy in the index and send success.

There is one unavoidable kernel-to-userspace receive copy on ordinary Linux
TCP. There is no application payload copy between network input and the disk
write. Header parsing and CRC calculation read the bytes but do not relocate
the value.

A zero-byte receive before the declared body is complete is a truncated
request. A receive deadline and the ingress-buffer bound prevent a drip-feeding
client from retaining resources indefinitely. Commands with empty bodies do
not attempt to select an empty fixed-buffer range.

### GET

1. Parse the key into small connection storage.
2. Look up and pin the cell's current committed extent. This lookup is the GET
   linearization point.
3. Acquire an egress fixed buffer, call `reset_range()` to discard its previous
   view, and submit a fixed-buffer read for the full extent.
4. Require an exact-length completion and validate cell, sequence, key, lengths,
   kind, and CRC in place. Any mismatch with the committed index is a storage
   failure, not a cache miss.
5. Snapshot the flags, value offset, and value length. Write the 24-byte binary
   response header and four-byte flags field into the format-guaranteed
   disposable prefix immediately preceding the stored value. Select the
   contiguous `response header || flags || value` range and call
   `set_len(28 + value_len)`.
6. Use ordinary send below a measured threshold and SEND_ZC above it. Retry
   short sends from the remaining range. Recycle the buffer only after the
   operation's terminal CQE; a successful zero-copy send terminates with its
   notification, while an immediate error may terminate without one.

The direct read, validation, and network transmission use the same allocation.
The read bypasses the page cache and the value is not copied by application
code. At startup the server enables and probes `SO_ZEROCOPY`; unsupported
systems use ordinary send and record that fallback. A successful SEND_ZC may
still fall back to a kernel copy, so the runtime must expose usage before the
application calls the path verified zero-copy.

### DELETE

DELETE writes a higher-sequence tombstone to the inactive copy and follows the
same exact-write and publish rule as SET. After durable completion, GET observes
the key as absent. The catalog retains the key-to-cell tombstone reservation
while later same-key mutations are queued; a queued recreate runs on that cell.
Only when its per-key queue is empty may the reservation be removed and the cell
enter the general free list. The tombstone remains the cell's current record
until a new key is durably written to the other copy, so a crash cannot
resurrect the deleted value.

### Concurrency and visibility

Distinct cells may have operations in flight concurrently up to the configured
storage limit. A cell has at most one mutation in flight; later mutations for
that cell queue in arrival order.

There is no global append offset, log mutex, or ordered completion barrier. The
file is fully preallocated, and the two offsets for every cell are known before
submission. The coordinator fills available storage credits from a fair queue
of ready cells and places every admitted read or write future in one
`FuturesUnordered`; it does not await one operation before submitting unrelated
work. io_uring, the filesystem, and the device may complete work independently
across cells. SQ pressure suspends the individual submission future without
blocking other ready or completing operations. Strict `O_DSYNC` writes can
still encounter filesystem or device flush serialization; the strict-versus-
volatile comparison measures that external limit.

Fairness is explicit. Command intake is drained with a budget, ready cells are
visited round-robin, and read/write admission has reserved credits or bounded
burst limits so a sustained write stream cannot indefinitely starve reads (or
vice versa). Queue metrics distinguish waiting for a same-cell dependency,
storage credit, SQ capacity, and a fixed buffer. Head-of-line blocking is
per-key by contract; it is never imposed across independent keys.

The ready-cell scan uses nonblocking credit and buffer acquisition. If a
resource is unavailable, it parks that cell on the corresponding wait queue and
continues the round-robin scan. It never awaits a buffer, credit, cell
dependency, or one SQ push inside dispatch.

A GET pins the selected physical copy until its fixed read reaches terminal
completion. A mutation may write the other copy while that read runs. A second
mutation waits rather than overwriting the pinned old copy. Once the read
completes, its response buffer is independent of the file extent and no longer
pins the cell.

The key catalog has explicit `Pending` and `Committed` states. Before awaiting
or submitting the first SET for a missing key, the coordinator inserts a
`Pending { cell }` reservation. Later mutations for that key queue on the same
cell, so two clients cannot create duplicate live records. GET observes a miss
until the first write commits. A pre-submission failure may release the
reservation; after submission, the coordinator owns it across disconnect and
either commits it or poisons/quarantines it under the indeterminate-error rule.

A GET may return the version committed at its index lookup even if a later SET
commits before the read response is sent. A successful mutation linearizes when
its exact durable write completes and the coordinator publishes the new copy.
Operations are responded to in request order on each connection.

## Durability and error rules

Strict mode opens the data file with `O_DIRECT | O_DSYNC`. A mutation is
acknowledged only after an exact full-extent write completion. O_DIRECT alone is
not a durability guarantee.

An optional mode may acknowledge plain O_DIRECT write completion, but it must
be named `volatile` and documented as having no acknowledged-recovery promise.
Group commit can be added later: all writes in an epoch must complete before
its `fdatasync` is submitted, and no write in that epoch is published or
acknowledged before the sync completes.

Short positive reads or writes are errors unless the operation explicitly
retries the remaining aligned range. A write or sync error leaves persistence
indeterminate. The initial engine poisons itself, stops serving, drains terminal
operations, and requires reopen/recovery; it does not return the affected cell
or sequence number to general use.

The server's normal shutdown order is:

1. stop accepting connections;
2. stop admitting commands and request connection-task shutdown;
3. drain every network and coordinator-owned operation to terminal completion,
   including SEND_ZC notifications;
4. recover every ingress and egress buffer and drop the bank-held `FixedBuf`
   values back into the underlying pool;
5. unregister the fixed-buffer table;
6. close the data file and shut down the Norn driver.

## Recovery

Recovery may be arbitrarily slow in the first version. It validates the
superblock copies and exact file geometry, then reads both fixed offsets for
every cell. One valid superblock is sufficient if the other is invalid. Two
valid copies must have byte-equivalent immutable fields; disagreement or two
invalid copies fails open. Recovery never trusts a record length to locate the
next record.

For each extent, recovery validates the magic, version, cell number, kind,
bounds, sequence, and complete CRC. It chooses the valid record with the higher
sequence number. Equal nonzero sequences with different contents are
corruption. A `PUT` winner adds its key and cell to the index. `FREE` and
`TOMBSTONE` winners add their cells to the free list. Duplicate live keys are
corruption. The next sequence number is one greater than the maximum valid
sequence in the file; exhaustion fails closed.

This is the crash proof for a mutation:

- Before submission, the old copy remains current.
- During a partial or torn write, CRC rejects the inactive copy and recovery
  selects the old copy.
- After the full O_DSYNC write completes, the new copy is durable and has the
  higher sequence, whether or not the process sent the response.
- Publishing only after that completion means runtime visibility never leads
  strict-mode recovery.
- The old copy is not overwritten until it is inactive, unpinned, and selected
  by a later serialized mutation.

Consequently a lost response may leave a durably applied operation, which is
the normal unknown-outcome case for a disconnected client. The server never
acknowledges an operation that strict recovery is permitted to lose.

CRC detects torn writes and corruption; it does not repair arbitrary media
loss. If neither copy is valid, recovery fails rather than inventing a cache
miss or free cell.

## Zero-copy ledger

| Path | Boundary | Initial mechanism | Payload copy status |
| --- | --- | --- | --- |
| SET | NIC/kernel to userspace | TCP receive into selected `FixedBuf` range | one kernel copy |
| SET | userspace to NVMe | `WRITE_FIXED`, O_DIRECT | no application or page-cache copy |
| GET | storage to userspace | `READ_FIXED`, O_DIRECT | direct read into final response allocation, bypassing page cache |
| GET small | userspace to socket | ordinary send | kernel copies payload |
| GET large | userspace to NIC | SEND_ZC over the same `FixedBuf` | no application copy; kernel fallback must be reported |
| Recovery | NVMe to userspace | fixed reads, bounded or sequential | off the steady-state path |

The ownership seam is an owned stable range, not `Vec<u8>`. Protocol parsing,
storage operations, and socket operations accept and return buffer ownership.
That allows the application to add fixed-buffer SEND_ZC and future receive
backends without changing storage correctness.

Hardware zero-copy receive is not assumed. NIC ZCRX delivers variable fragments
at offsets chosen by the network stack, while O_DIRECT records require complete
aligned extents. ZCRX therefore does not automatically produce a disk-writable
record; a future ZCRX path may need coalescing or a different storage layout. It
should be a measured alternate ingress backend, not a claim that the current
end-to-end path has no copies.

Before calling the GET path verified zero-copy, Norn needs an observable
SEND_ZC result that distinguishes kernel zero-copy from copied fallback. A
fixed-buffer-specific SEND_ZC operation should also pass the registered buffer
index when the kernel API supports it. The existing terminal-CQE ownership rule
remains the reclamation boundary.

## Performance contract

The primary workload is a preloaded, fixed-cardinality set driven over
Memcached binary TCP, with a configurable GET/SET ratio, enough connections to
reach the configured disk queue depth, and no expiration. The main cases use a
small value, a 4 KiB value in an 8 KiB extent, and a 32 KiB value in a 64 KiB
extent. Cardinality and the in-memory index footprint are fixed and reported;
the dataset need not exceed RAM because O_DIRECT already bypasses the page
cache.

The minimum workload set is:

- persistent NOOP or GET miss as the network/codec control;
- GET hit and durable existing-key SET at queue depth one and at one screened
  saturation queue depth;
- one sustained 90/10 GET/SET mix at the saturation point;
- a large-value GET for ordinary-send versus SEND_ZC.

Headline results are successful operations per second; p50, p95, p99, and
p99.9 latency; CPU utilization; operation errors; storage-credit utilization;
queue-wait reasons; and ingress/egress buffer occupancy. Every report also
states live/free cells, logical live bytes, provisioned file bytes, record size,
cardinality, device, and durability mode.

Targeted diagnostic trials may add disk IOPS, bandwidth, request size and queue
depth; executor idle/park time; admitted/queued/rejected commands; allocation
counts; and ordinary-send, SEND_ZC, verified-zero-copy, and copied-fallback
counts. These counters are sampled outside the request path where possible.

Feature comparisons apply only where they discriminate: strict versus volatile
durability on SET, ordinary versus fixed disk buffers on GET/SET, and ordinary
send versus SEND_ZC on large GET. Queue depth and SEND_ZC threshold are screened
once, then frozen before optimization comparisons; the suite does not cross
every mode with every value size.

The scheduler has a separate head-of-line test shape: hold one cell's mock I/O
future pending while continuously admitting operations for independent cells.
Those cells must consume every remaining storage credit and complete without
waiting for the held cell. A matching integration run uses a hot contended key
among uniformly distributed keys and reports independent-key throughput and
credit utilization separately. This makes accidental global serialization a
test failure instead of a profile interpretation.

Exact benchmark commands, repetitions, acceptance thresholds, and result paths
remain unset until the target benchmark machine is agreed. Baseline and changed
runs must use the same machine, file/device, build, workload, and configuration,
with repeated interleaved trials and median comparison.

## Discriminating tests

Format and recovery tests must cover:

- a crash before submission, during each partial extent write, after durable
  completion, after publish, and after response construction;
- a valid old copy plus a torn, corrupt, or out-of-bounds new copy;
- reversed completion order across cells;
- DELETE followed by crash at every step of cell reuse;
- equal sequences with unequal records, duplicate live keys, bad cell numbers,
  sequence exhaustion, and invalid superblocks;
- short reads, short writes, write errors, sync errors, and corrupt reads;
- an old-copy GET concurrent with one and then two same-cell mutations;
- one permanently pending cell while unrelated cells fill and retire all other
  storage credits, with read/write fairness checked under bounded bursts;
- connection cancellation before admission and after mutation submission;
- shutdown with receives, reads, writes, and SEND_ZC notifications in flight.

Protocol and resource tests must split every request at every byte boundary,
coalesce multiple requests in one TCP arrival, exercise short sends, and prove
that command, storage, and buffer bounds hold under slow or disconnected
clients. Model tests should compare the cell state machine and recovery result
after every injected crash point.

## Reviewable implementation sequence

1. Add a pure record codec and cell state model with crash-point recovery tests.
   It has no Norn or socket dependency and establishes the persistence contract.
2. Add store creation/recovery and a Norn storage coordinator using ordinary
   aligned stable buffers. Validate durability and cancellation before tuning.
3. Register and partition fixed buffers, then implement fixed reads/writes and
   direct SET-body receive. Prove all ownership returns on error and shutdown.
4. Add the sequential Memcached binary TCP connection task and run the first
   agreed end-to-end baseline with ordinary send.
5. Add observable SEND_ZC/fixed-buffer support to `norn-uring`, enable it above
   a measured threshold, and compare it against ordinary send.
6. Only after profiles identify a limit, consider per-connection parallel
   dispatch, group commit, multiple value classes, or one application instance
   per core with client-side key sharding.

Each step is independently testable and keeps a simple implementation available
to distinguish format, runtime, and optimization failures.

## Rejected initial designs

Extending the current `norn-kv` preserves the wrong external contract and
serializes mutations behind one mutable store. An append-only log has a simpler
scan but cannot sustain a fixed-cardinality SET workload without a cleaner or
unbounded disk growth. In-place single-copy records cannot distinguish a torn
update from the last committed value. A WAL plus data file adds two write paths,
checkpointing, and ordering before the workload has shown a need. Text protocol
receives do not declare the value position until after variable-length parsing,
making direct receive into the final aligned record substantially harder.

## References

- [Memcached protocol status](https://docs.memcached.org/protocols/)
- [Memcached binary protocol](https://github.com/memcached/memcached/blob/master/doc/protocol-binary.txt)
- [`memtier_benchmark` options and Memcached binary support](https://github.com/redis/memtier_benchmark/blob/master/memtier_benchmark.1)
- [`io_uring_prep_send_zc` ownership and fallback behavior](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html)
