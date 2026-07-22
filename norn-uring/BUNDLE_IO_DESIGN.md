# Bundle I/O design

Status: implemented and validated on Linux; approved by independent API,
lifecycle/safety, and test/benchmark reviewers.

## Shared buffer-ring core

Send and receive bundles use the same kernel provided-buffer-ring mechanism,
but expose different ownership semantics. Norn therefore uses concrete typed
wrappers over one private implementation rather than a public direction
generic:

```rust,ignore
struct RegisteredBufRing {
    // Registration, mapped ring storage, buffer allocation, publication
    // ledger, BID ownership, driver identity, and availability generation.
}

#[derive(Clone)]
pub struct RecvBufRing {
    inner: Rc<RegisteredBufRing>,
}

pub struct SendBufRing {
    inner: Rc<RegisteredBufRing>,
}
```

`RecvBufRing` is cloneable because its visible entries are fungible empty
capacity. Clones may be used by multiple sockets; each selected buffer becomes
uniquely owned by the completion that received into it.

`SendBufRing` is deliberately not `Clone`. Its visible entries contain ordered
data committed to one socket, so its public ownership token must be moved into
an attachment. Internal `Rc` references held by checked-out buffers, the pump,
and cleanup state do not weaken that public exclusivity.

Both wrappers reuse registration, memory, publication-position accounting,
BID ownership, and terminal-lifetime machinery. They do not share a generic
automatic buffer-return path. A dropped receive buffer republishes its BID to
the kernel ring; a dropped uncommitted send buffer returns its BID only to a
private userspace free list. A receive ring is initially fully published, while
a send ring is initially empty.

Direction-specific attachment and queue state remain outside
`RegisteredBufRing`. Rings are constructed with their direction already fixed;
there is no conversion between send and receive wrappers while registered.
Incremental provided-buffer consumption (`IOU_PBUF_RING_INC` and
`IORING_CQE_F_BUF_MORE`) is explicitly unsupported initially because it needs a
different per-BID partial-ownership model.

### Publication ledger

A single cached receive head is insufficient when several sockets share one
ring: independent CQEs can be observed in a different order from the order in
which the kernel consumed ring entries. The shared core therefore records every
publication under a monotonically increasing absolute position:

```text
absolute position -> BID + BID generation + published/claimed/returned state
BID -> its one currently unclaimed publication position
```

The first BID in a bundle CQE identifies its unclaimed publication position.
The result length identifies how many consecutive published positions the
kernel selected. Completion processing atomically claims that exact range,
independent of CQE order. Ledger entries remain until a sequential retirement
watermark passes them, so ring wrap and BID republication cannot overwrite
history needed by an older completion.

Ledger storage is bounded. If an unresolved position prevents retirement and
the ledger reaches its configured window, subsequently returned BIDs remain in
a userspace held list instead of being republished. Resolution advances the
watermark and republishes held BIDs. This temporarily reduces receive capacity
but prevents an indefinitely delayed CQE from causing unbounded bookkeeping.

A BID may be republished only after its prior publication was claimed and the
owned buffer was dropped. Thus there can be only one unclaimed publication for
a BID. An absent, duplicated, overlapping, or otherwise irreconcilable range
quarantines the entire receive ring, cancels all of its consumers, and prevents
further publication; it is never treated as a per-socket recoverable error.

### Socket-direction permits

Move-only ring wrappers do not by themselves prevent an ordinary operation
created before attachment from being polled afterward. Each `NornFd` therefore
has independent read-side and write-side mode state.

Constructing an ordinary operation acquires the corresponding ordinary permit
immediately, before SQ submission. The permit remains owned by operation state
until an unsubmitted operation is dropped or a submitted operation reaches its
terminal CQE, including the notification CQE for zero-copy sends. Entering
bundled mode succeeds only when that side has no ordinary permits and then
prevents new ordinary permits. Leaving bundled mode occurs only after its pump
reaches the same terminal fence.

The public ownership of `TcpSocket::into_stream`, `TcpStreamReader`, and
`TcpStreamWriter` prevents new conflicting calls once an adapter consumes the
relevant value. The fd-side permits close the stale-operation hole that Rust
value ownership alone cannot see.

## Decision

Send bundles do not require transactional SQ submission when a provided-buffer
ring is treated as an outbound queue temporarily attached to one socket.

Publishing a buffer is a commit to send it. A send-bundle SQE is an
interchangeable kick that drains the socket's FIFO; it does not own a distinct
batch of buffers. If the io_uring SQ is full after buffers become visible, the
queue remains in `NeedKick` until normal submission backpressure clears. No
provided-buffer entry is rolled back.

The generic `Operation::{prepare_submission, rollback_submission}` design is
therefore rejected for this use case. It imposed a difficult retractability
contract whose only proposed consumer disappears under queue ownership.

## Resource ownership

Applications manage send-ring policy:

- buffer-group IDs;
- allocation and registration;
- buffer count and capacity;
- one-ring-per-connection versus leasing from an application-owned pool; and
- when an idle connection releases its ring.

Norn enforces mechanism and safety:

- a send ring is move-only;
- attachment consumes the ring;
- one attached ring has exactly one socket consumer;
- committed buffers remain queued across SQ pressure and waiter cancellation;
- the runtime pump owns pending and active SQEs through terminal CQEs; and
- a ring returned from bundled mode contains no stale visible data.

The ownership flow is:

```text
user-owned ring
          |
          | attach_send_ring
          v
socket-owned send FIFO + runtime pump
          |
          | finish_send_ring or asynchronous Drop cleanup
          v
clean ring returned to user
```

Norn does not provide a ring pool, lease trait, recycling callback, or pool
policy. An application-owned pool hands out a plain `SendBufRing` and accepts
the clean ring returned by explicit finish. On writer Drop the runtime pump
sanitizes and destroys the ring; Drop cannot return ownership to application
code. Applications that want to recycle rings must use explicit finish.

## TCP usage

Ordinary streaming continues to use `AsyncWrite`:

```rust,ignore
let send_ring = pool.acquire().await?;

let stream = socket.into_stream();
let (reader, writer) = stream.owned_split();
let mut writer = writer.attach_send_ring(send_ring)?;

tokio::io::copy(&mut file, &mut writer).await?;
writer.flush().await?;

match writer.finish_send_ring().await {
    FinishOutcome::Drained { writer, ring } => {
        pool.release(ring);
        handle_next_request(reader, writer).await?;
    }
    FinishOutcome::SendFailed { error, ring } => {
        pool.release(ring);
        return Err(error.into());
    }
}
```

Applications that can produce data directly into registered buffers use the
owned-buffer path:

```rust,ignore
let mut offset = 0;

loop {
    let buffer = writer.acquire().await?;
    let (result, buffer) = file.read_at(buffer, offset).await;
    let bytes = result?;

    if bytes == 0 {
        drop(buffer);
        break;
    }

    writer.enqueue(buffer, bytes)?;
    offset += bytes as u64;
}

writer.flush().await?;
```

`SendBuf` is owned, implements `StableBuf` and `StableBufMut`, and retains its
originating ring. This permits direct file reads and other owned-buffer
operations. Dropping an unqueued `SendBuf` returns it immediately. Enqueue
validates the ring identity and initialized length before transferring the
buffer to the committed FIFO; an enqueue error returns the buffer.

## TCP API shape

The names below record semantics rather than final signatures:

```rust,ignore
impl TcpStreamWriter {
    fn attach_send_ring(
        self,
        ring: SendBufRing,
    ) -> Result<BundledTcpWriter, AttachSendRingError<Self, SendBufRing>>;
}

impl BundledTcpWriter {
    async fn acquire(&mut self) -> Result<SendBuf, SendError>;
    fn try_acquire(&mut self) -> Result<Option<SendBuf>, SendError>;

    fn enqueue(
        &mut self,
        buffer: SendBuf,
        initialized: usize,
    ) -> Result<(), EnqueueError<SendBuf>>;

    async fn flush(&mut self) -> Result<(), SendError>;
    async fn shutdown(&mut self) -> Result<(), SendError>;

    async fn finish_send_ring(
        self,
    ) -> FinishOutcome<TcpStreamWriter>;
}

enum FinishOutcome<W> {
    Drained { writer: W, ring: SendBufRing },
    SendFailed { error: SendError, ring: SendBufRing },
    CleanupFailed { error: SendError },
}

struct AttachSendRingError<W, R> {
    kind: AttachErrorKind,
    writer: W,
    ring: R,
}

struct EnqueueError<B> {
    kind: EnqueueErrorKind,
    buffer: B,
}

impl<'a> UdpDatagramBuilder<'a> {
    fn push(
        &mut self,
        buffer: SendBuf,
        initialized: usize,
    ) -> Result<(), DatagramPushError<SendBuf>>;

    async fn commit(self) -> Result<(), DatagramCommitError<'a>>;
}
```

`AttachSendRingError` returns the unchanged writer and ring. It covers driver
mismatch, a dirty or already attached ring, unsupported kernel capability, a
different active executor, and a send side with any ordinary permit, including
an unpolled or SQ-waiting operation. `EnqueueError` owns and returns the rejected buffer. Enqueue validates
ring identity and that the requested initialized length does not exceed the
buffer's actually initialized bytes before transferring ownership.
`DatagramPushError` does the same. A failed consuming datagram commit returns
the complete private builder, so no staged segment is lost or published.

`BundledTcpWriter` implements `tokio::io::AsyncWrite`:

- `poll_write` copies into available send buffers and reports acceptance into
  the bounded userspace FIFO;
- `poll_write` never copies bytes and then returns `Pending` for that call;
- `poll_flush` waits until all bytes accepted before that call have reached the
  local socket; and
- `poll_shutdown` flushes and then performs `SHUT_WR`.

Later socket failures are surfaced by `flush`, `shutdown`, or a later write.
Like other buffered writers, applications must flush or shut down to observe
final local acceptance.

## Backpressure

The registered ring capacity is the queue bound. There is no separate byte
budget:

```text
maximum retained storage = buffer_count * buffer_capacity
```

`acquire().await` waits for a free send buffer. Checked-out, committed,
selected, and partially consumed buffers all count against capacity. Terminal
completion accounting returns buffers and wakes blocked acquisitions.

Once `acquire` returns, that buffer is reserved; a valid `enqueue` cannot fail
with queue-full. `AsyncWrite::poll_write` returns `Pending` when no free buffer
is available. This gives streaming applications bounded memory and ordinary
backpressure without exposing SQ capacity.

Owned buffers may outlive the public writer. `finish_send_ring` cannot return
the ring until all checked-out buffers have been dropped or returned. A leaked
`SendBuf` consequently retains its portion of ring capacity rather than
allowing premature reuse.

## Runtime pump and cancellation

Attaching the ring creates a persistent runtime-owned pump, or registers an
equivalent driver-owned pending source. The pump owns the pending SQ submission
and active `Op`; temporary application futures own only waiters.

```text
Idle --enqueue--> NeedKick --SQ space--> Active
                     |                    |
               SQ full stays         MORE stays
                 NeedKick              Active
                                          |
                           terminal + queued --> NeedKick
                           terminal + empty  --> Idle

any ambiguous completion --> Failed
```

Dropping `flush`, `acquire`, or `shutdown` removes only that waiter. It never
withdraws committed data or cancels the pump. A later call observes the same
queue and completion watermarks.

The terminal path rechecks the FIFO after clearing `Active`, preventing an
enqueue-versus-terminal lost wake: queued data always has either an active
consumer or a pending kick.

## Writer Drop

`Drop` cannot synchronously detach a ring because cancellation and reclamation
may require a terminal CQE. Dropping `BundledTcpWriter` instead:

1. marks the public sender abandoned;
2. wakes the runtime pump;
3. stops accepting buffers;
4. cancels or finishes the active request as appropriate;
5. waits for terminal kernel ownership through normal driver cleanup;
6. unregisters and resets the ring when ownership is ambiguous; and
7. destroys the sanitized ring.

If checked-out `SendBuf`s remain, cleanup waits for their references to be
released. During runtime teardown, driver-owned operation references retain the
fd, ring mapping, buffers, and bookkeeping until the terminal completion fence.

## Finish and error recovery

`flush()` drains current data but keeps bundled mode attached.
`finish_send_ring(self)` consumes the bundled writer, drains if healthy, and
leaves bundled mode.

On the healthy path it waits for an empty FIFO and terminal CQE, then returns
the ordinary writer and clean ring.

The future returned by `finish_send_ring(self)` owns the writer and ring.
Dropping that future is defined abandonment: it cannot be retried and returns
neither value to the caller. The runtime pump performs the same asynchronous
terminal cleanup as writer Drop and eventually destroys the sanitized ring.

On a send failure, directly reusing the ring on another socket would be unsafe:
stale visible entries could send data from the old connection. The finish path
therefore waits for terminal kernel ownership, unregisters the provided-buffer
group, resets its userspace tail and Norn bookkeeping, and returns a clean
ring. The failed TCP writer is not returned because stream progress may be
ambiguous.

The ring object may internally be clean-but-unregistered if re-registration
fails. A later attachment can retry registration. The public postcondition is
that it contains no visible buffers from the old socket and cannot send stale
data when attached elsewhere.

With the initially supported non-incremental buffer-ring mode, a terminal
completion ending
inside one selected entry while later entries remain visible is not safely
recoverable in-place. The writer fails closed and never tail-republishes the
suffix. Future incremental-mode support requires its own design; it is not
silently enabled as an optimization.

## UDP mapping

UDP uses the same user-managed rings, owned `SendBuf`s, runtime pump,
finish behavior, and sanitation guarantee. Its queue framing is different.

A send bundle gathers all currently selected buffers into one `sock_sendmsg`,
which is one datagram. The ring has no end-of-datagram marker. Publishing two
logical packets together could merge them.

UDP therefore has a private datagram builder and queue:

```rust,ignore
let send_ring = pool.acquire().await?;
let mut socket = socket.attach_send_ring(send_ring)?;

let mut datagram = socket.datagram();

let header = datagram.acquire().await?;
datagram.push(header, header_len)?;

let payload = datagram.acquire().await?;
datagram.push(payload, payload_len)?;

datagram.commit().await?;
socket.flush().await?;
```

`datagram(&mut self)` permits only one live builder per sender. This prevents
multiple builders from each retaining part of the bounded ring while waiting
for the other to commit. Builders and later committed datagrams remain
userspace-private. All segments of the head datagram, and no segments of later
datagrams, become visible. The next datagram is not published until the
previous send produces its terminal CQE.

The focused bundle API requires a connected UDP socket. Ordinary unconnected
`send_to` and ancillary-data APIs remain separate. An empty datagram uses an
ordinary zero-length send; zero-length segments never enter the bundle ring.
It reserves one otherwise-empty `SendBuf` as a capacity token until that send
reaches its terminal CQE, so empty datagrams cannot form an unbounded side
queue.

Each datagram has a segment limit equal to the smaller of total ring capacity
and `SEND_BUNDLE_MAX_SEGMENTS` (256), matching Linux's maximum one-shot
buffer-ring import. Every successful acquisition counts immediately, before
`push`, so a builder that reaches the limit fails another acquisition rather
than awaiting buffers that it owns itself. An empty commit with an acquired but
unpushed segment likewise fails immediately.

UDP `flush()` snapshots the committed-datagram watermark when called and waits
only for that watermark to complete. It does not wait for uncommitted
`SendBuf`s that escaped a discarded builder.

## Receive bundles

Receive does not mirror the send-ring attachment lifecycle. The application keeps a
cloneable `RecvBufRing`; a receiver holds one clone while each yielded bundle
owns the BIDs selected for that completion. Dropping a `RecvBuf` returns its BID
to the shared ring, and dropping a `RecvBufBundle` returns all of its BIDs.

The low-level socket API remains useful for applications that want explicit
operation control:

```rust,ignore
impl TcpSocket {
    fn recv_bundle(&self, ring: &RecvBufRing) -> Op<RecvBundle>;
    fn recv_bundle_multi(&self, ring: &RecvBufRing) -> Op<RecvBundleMulti>;
}
```

The streaming API consumes the TCP read half so a different receive operation
cannot overlap it. It owns only a clone of the receive ring, not the ring
itself:

```rust,ignore
let recv_ring = recv_pool.ring();
let stream = socket.into_stream();
let (reader, writer) = stream.owned_split();

let mut incoming = reader.recv_bundles(recv_ring.clone())?;
let mut outgoing = writer.attach_send_ring(send_pool.acquire().await?)?;

while let Some(bundle) = incoming.next().await {
    let mut bundle = bundle?;
    handle_stream_bytes(&mut bundle, &mut outgoing).await?;
    // Any remaining buffers return to recv_ring here.
}

let reader = incoming.finish().await?;
let FinishOutcome::Drained { writer, ring } =
    outgoing.finish_send_ring().await
else {
    // Handle the recorded send failure.
};
```

`TcpRecvBundles` implements `Stream<Item = io::Result<RecvBufBundle>>`.
`RecvBufBundle` represents arbitrary TCP stream bytes, not a message boundary.
It exposes total length, buffer count, slice iteration, cursor-style userspace
consumption, and conversion into its owned `RecvBuf`s. This cursor is unrelated
to kernel incremental provided-buffer mode. A zero-byte TCP completion ends
the stream.

`finish()` stops receiving, waits for the active multishot request's terminal
CQE, and returns the ordinary reader. Dropping the adapter abandons the read
half and lets runtime-owned cancellation/cleanup reach the same terminal fence;
it never invalidates the shared receive ring. Bundles already delivered to the
application may outlive the adapter and socket.

Temporary receive-ring exhaustion is backpressure, not a terminal I/O error.
If a multishot request ends with `ENOBUFS`, the adapter waits for a BID to be
returned and rearms the receive. The ring maintains an availability generation
and broadcast notification. A waiter records the generation, arms its wait,
then rechecks both availability and generation before sleeping. Every paused
consumer becomes eligible on a generation change; losers may observe
`ENOBUFS` again, but no receiver can miss the return and remain stranded. Other
terminal socket errors are yielded and end the adapter.

For TCP, exhaustion leaves unread bytes in the socket and ultimately applies
transport backpressure.

Kernel receive bundles apply only to plain `RECV`, not `RECVMSG`. The ergonomic
receive-bundle adapter is therefore TCP-only in the initial API. A connected
UDP socket may retain an explicitly low-level `recv_bundle` operation, but it
cannot provide peer addresses, message flags, ancillary data, or a reliable
high-level truncation contract. We do not disguise it as the general UDP
datagram stream.

The high-level UDP API continues to use multishot `recvmsg` with one provided
buffer per datagram. It yields peer/message metadata and explicit original
length versus copied length so truncation is observable. An empty datagram is a
valid item rather than EOF. Consequently UDP shares `RecvBufRing` storage and
ownership machinery, but not the multi-buffer receive-bundle adapter.

Dropping the future returned by consuming `TcpRecvBundles::finish(self)` is
also defined abandonment. The read half is not returned, runtime cleanup waits
through the terminal CQE, and the shared receive ring remains valid unless its
publication ledger itself was quarantined.

## Combined ownership model

```text
receive: shared RecvBufRing -> kernel selects capacity -> owned RecvBufBundle
                                                    Drop -> shared ring

send:    owned SendBufRing -> socket-owned committed FIFO -> clean ring
```

The common core answers where memory and BIDs live. The wrappers answer who may
publish or consume them. SQ capacity is hidden in both directions: senders wait
for free send buffers, while receivers automatically pause and rearm when
receive buffers return.

## Implemented structure

The implementation follows the plan below. The main separation boundaries are:

- registration and mapped storage in `bufring/registered.rs`;
- receive publication history in `bufring/ledger.rs`;
- send ownership and completion reconciliation in `bufring/send.rs`;
- fd-direction exclusion in `fd/mode.rs`;
- physical send-bundle operations in `net/socket/bundle_send.rs`;
- the protocol-independent lifecycle pump in `net/socket/bundle_pump.rs`; and
- typed TCP/UDP adapters in their own `bundled_recv.rs` and `bundled_send.rs`
  modules.

The implementation sequence was:

1. Add independent per-`NornFd` read/write mode permits and make every ordinary
   operation acquire its permit at construction and retain it to its true
   terminal fence.
2. Refactor the existing `BufRing` storage and registration into private
   `RegisteredBufRing`, then add typed `RecvBufRing`, `RecvBuf`, and
   `RecvBufBundle` wrappers with receive-specific republication.
3. Replace cached-head inference with the absolute-position/BID-generation
   publication ledger. Prove out-of-order completion, wrap, republication, and
   quarantine invariants in deterministic unit tests before adding adapters.
4. Add availability-generation broadcast/recheck and prove its no-lost-wake
   invariant with multiple simulated consumers.
5. Add the consuming TCP receive-stream adapter. Make `ENOBUFS` wait/rearm
   transparent and fence successful finish on the terminal CQE. Keep the
   existing recvmsg-backed UDP datagram stream separate from receive bundles.
6. Add move-only `SendBufRing` over the same core. Pooling remains entirely an
   application concern. Implement checked-out, private, visible, selected,
   partially consumed, failed, and clean states without yet attaching them to
   high-level sockets.
7. Implement the runtime-owned send pump. Prove that enqueue-versus-terminal,
   SQ-full, cancelled waiters, partial completion, shutdown, and abandoned
   writer paths cannot strand data or release kernel-owned memory.
8. Add `BundledTcpWriter`, its `AsyncWrite` implementation, the owned-buffer
   path, `flush`, `shutdown`, and `finish_send_ring`.
9. Add connected UDP send datagram builders with private staging and exactly-one-
   datagram visibility, reusing the send pump and sanitation lifecycle.
10. Run focused Linux tests after every phase, then the full workspace tests,
   formatting, and clippy. Keep capability-dependent tests explicit for older
   kernels.
11. Obtain independent API and lifecycle review of the exact implementation.
   Iterate until the reviewer agrees the typed wrappers, streaming use,
   backpressure, error recovery, and Drop behavior are clean.

## Validation record

The implementation is covered by deterministic unit and Linux integration
tests for ledger ordering/quarantine, ring wrap and BID reuse, availability
broadcasts, direction permits, SQ-wait cancellation, TCP receive exhaustion and
shared-ring contention, TCP streaming/backpressure/direct-file input, cancelled
flush and active finish, send-failure sanitation plus cross-socket reuse, UDP
multi-segment boundaries, head-only publication/failure containment, snapshot
flush, pushed and unpushed builder exhaustion, the kernel's 256-segment bundle
ceiling, and bounded empty datagrams. Attachment is tested outside an executor
and on the wrong active executor. A non-skipping SQPOLL run
is available with `NORN_REQUIRE_SQPOLL=1` and has passed on the development
host.

`benches/bundle_send.rs` compares ordinary and bundled TCP byte streaming and
ordinary and two-segment bundled UDP streaming with connection setup outside
the timed loop. The benchmark is a baseline and regression tool; the current
implementation does not claim a performance win.

Useful future hardening includes instrumented SQE/CQE-count assertions,
randomized shutdown/error stress, and extending high-contention shared-ring
integration coverage to more socket combinations. Completion accounting,
stable BID ownership, partial/error handling, and shutdown lifetime remain the
areas to keep auditing as the API evolves. Transactional SQ submission would
not solve those obligations.
