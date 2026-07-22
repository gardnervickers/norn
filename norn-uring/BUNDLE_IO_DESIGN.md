# Bundle I/O design

Status: staged. This change contains only the socket-lifecycle prerequisite for
bundle I/O. Registered send and receive rings, FIFO publication, pumps, and
TCP/UDP adapters belong to later review stages.

## Socket-direction lifecycle

Move-only adapters cannot by themselves prevent an ordinary operation created
before attachment from being polled afterward. Each `NornFd` therefore tracks
the read and write directions independently.

An ordinary direction may have multiple permits. A bundled direction has one
exclusive permit whose internal clones share a single lifetime. Read and write
state do not conflict with each other. Operations that affect both directions,
such as `SHUT_RDWR`, validate both sides before changing either side, so a
failed acquisition cannot partially reserve the descriptor.

Constructing an ordinary io_uring socket operation acquires its direction
permit before SQ submission. This closes three stale-operation windows:

- an operation that has not been polled yet;
- an operation waiting for submission-queue capacity; and
- a submitted operation whose user-facing future has been dropped.

Dropping an unsubmitted operation releases its permit. Once submitted, the
operation allocation retains the permit until the kernel reports a terminal
CQE. `IORING_CQE_F_MORE` keeps the permit active; for zero-copy sends this means
the primary CQE does not release the permit before the notification CQE.
Synthetic terminal completions used for configuration and submission failures
release the permit through the same path.

The raw operation implementation has separate guarded and unguarded layouts
with a shared prefix. File operations, noops, and other unguarded operations do
not pay storage for a socket terminal guard. The driver submission seam accepts
any internal terminal guard and does not depend on ordinary or bundled socket
policy.

Direct nonblocking TCP reads and writes acquire a permit only around the system
call. Readiness polling does not transfer bytes and does not reserve a data
direction.

## Stacking boundary

The bundled-acquisition entry point is intentionally internal and has no
production caller in this prerequisite. Narrow dead-code allowances mark that
stacking seam; the first bundle mechanism PR removes them when it consumes the
exclusive permit.

No buffer publication or send semantics are established here. The later core
send PR remains responsible for the durable invariant that publishing a buffer
commits it to the socket-owned FIFO while an SQE is only a retryable drain kick.
