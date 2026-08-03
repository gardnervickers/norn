# Registered Buffers API Design

Status: implemented v1 candidate on the current feature branch. The API and
safety model have completed independent design and implementation reviews;
timed performance acceptance is still pending.

## Decision

V1 is an owned, dense, immutable fixed-buffer pool:

- callers choose the concrete buffer type and recover the original values;
- a safe registration method backed by an unsafe trait covers common and
  application-owned types;
- an unsafe registration closure covers foreign types blocked by Rust's orphan
  rules;
- the pool grants one non-cloneable `FixedBuf` per registered slot;
- fixed operations consume and return that `FixedBuf`; and
- the implementation caches raw descriptors after registration, so custom
  buffer code and dynamic dispatch never occur on the I/O path.

Sparse tables, replacement, borrowed buffers, and disjoint leases are separate
future designs. They are not hidden behind underspecified v1 hooks.

## Goals

- Accept caller-selected heap, inline, arena, slab, or wrapper buffer types.
- Keep every registered address valid until successful unregistration or ring
  destruction, including cancellation, shutdown, error, panic, and `Drop`.
- Add no fixed-buffer-specific allocation or dynamic dispatch per I/O. Norn's
  existing `RawOp` allocation remains.
- Preserve Norn's convention that an operation owns its buffer and returns it
  with the completion result.
- Make the common pool API ergonomic without exposing kernel table management.
- Recover the exact original `Vec<B>` after successful unregistration.

## Non-goals for v1

- Non-`'static` borrowed registrations. They require a scoped-operation model.
- Registering uninitialized spare capacity. A future API must model
  `MaybeUninit<u8>` explicitly.
- More than one simultaneous view of a registered slot, even for disjoint
  ranges.
- Sparse registration, slot replacement, or resource-tag CQE handling.
- Async waiting for a free slot.
- Enforcing filesystem-specific `O_DIRECT` alignment rules.

## Public API at a glance

```rust
use norn_uring::fixedbuf::{FixedBufPool, FixedBuffer};

let handle = Handle::current();
let pool = handle.register_fixed_buffers(vec![
    MyAlignedBlock::new(),
    MyAlignedBlock::new(),
])?;

let mut buf = pool.try_acquire()?;
buf.set_range(0..4096)?;
let (result, mut buf) = file.read_fixed_at(buf, 0).await;
let n = result?;
consume(&buf[..n]);

buf.set_payload(payload)?;
let mut offset = 4096;
while !buf.is_empty() {
    let (result, returned) = file.write_fixed_at(buf, offset).await;
    let written = result?;
    buf = returned;
    if written == 0 {
        return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
    }
    buf.consume(written);
    offset += written as u64;
}
drop(buf);

let original: Vec<MyAlignedBlock> = pool.unregister()?;
```

One slot is one in-flight-I/O credit. A queue depth of 64 requires at least 64
registered slots. Registering one large arena as one `FixedBuffer` still gives
only one v1 lease; callers should register its blocks as separate values when
they need concurrent operations.

## Buffer-region contract

The long-lived registered-memory contract belongs beside `StableBuf` and
`StableBufMut` in `norn_uring::buf`, and is re-exported by `fixedbuf`:

```rust
/// A fully initialized writable region suitable for long-lived kernel
/// registration.
///
/// # Safety
///
/// `fixed_region` must return the exact region owned by `self` that may remain
/// registered. From the call until successful unregistration or ring
/// destruction:
///
/// - the address and length must not change;
/// - the entire region must remain allocated, initialized, and writable;
/// - no external alias may read or write it while the kernel or a `FixedBuf`
///   may write it;
/// - regions selected for one pool must be pairwise disjoint;
/// - no safe method or interior-mutation path may expose, free, resize,
///   relocate, or overlap it; and
/// - moving `self` is permitted only before registration and after release.
pub unsafe trait FixedBuffer {
    /// Return the exact initialized region to register.
    fn fixed_region(&mut self) -> &mut [u8];
}
```

The trait itself is not `'static`; v1's owned pool and operations impose that
bound. This leaves the memory contract reusable if Norn later gains scoped
operations.

Norn provides implementations for:

- `Vec<u8>` (its initialized length; spare capacity is excluded),
- `Box<[u8]>`,
- `BytesMut` (its initialized length),
- `[u8; N]`, and
- `Box<T> where T: FixedBuffer + ?Sized`.

The last implementation permits heterogeneous pools without hot-path dynamic
dispatch:

```rust
let buffers: Vec<Box<dyn FixedBuffer>> = vec![
    Box::new(vec![0u8; 4096]),
    Box::new(MyAlignedBlock::new()),
];
let pool = handle.register_fixed_buffers(buffers)?;
```

Unregistration recovers `Vec<Box<dyn FixedBuffer>>`, so callers that need typed
recovery should use an application enum. The trait deliberately does not add
`Any` or downcasting.

### Foreign-type escape hatch

Rust's orphan rules prevent an application from implementing Norn's trait for
some third-party types. Requiring a newtype for every such case would undercut
the generic design, so `Handle` also exposes:

```rust
pub unsafe fn register_fixed_buffers_with<B, F>(
    &self,
    buffers: Vec<B>,
    region: F,
) -> Result<FixedBufPool<B>, RegisterError<B>>
where
    B: 'static,
    F: for<'a> FnMut(&'a mut B) -> &'a mut [u8];
```

The function invokes `region` at most once per buffer, in input order, after
every `B` is in final storage; on success every projection was invoked exactly
once. Projection is not a pinning event: if registration fails before becoming
active, cached pointers are discarded and recovered values may move. Its
safety contract is the same as `FixedBuffer`: each slice must remain stable,
live, writable, and unaliased until release. Registration also rejects
pairwise-overlapping address intervals. The safe trait-based registration
method delegates to this primitive.

## Pool construction and errors

```rust
pub struct FixedBufPool<B: 'static> { /* private Rc */ }
pub struct FixedBuf<B: 'static> { /* private Rc + cached descriptor */ }

impl Handle {
    pub fn register_fixed_buffers<B>(
        &self,
        buffers: Vec<B>,
    ) -> Result<FixedBufPool<B>, RegisterError<B>>
    where
        B: FixedBuffer + 'static;
}
```

`FixedBufPool` and `FixedBuf` are intentionally not `Clone`. The pool can be
borrowed to acquire multiple slots; each fixed buffer has one unambiguous owner.

Registration errors retain every input value in original order:

```rust
impl<B> RegisterError<B> {
    pub fn kind(&self) -> &RegisterErrorKind;
    pub fn into_buffers(self) -> Vec<B>;
    pub fn into_parts(self) -> (RegisterErrorKind, Vec<B>);
}
```

`RegisterErrorKind` distinguishes invalid input, an existing table, driver
shutdown, unsupported kernel functionality, resource exhaustion, and other OS
errors. Error formatting may mention locked-memory limits for `ENOMEM` without
claiming that is always the cause.

After cheaply reserving the driver's one fixed table, the implementation moves
the buffers into final non-moving `Box<[UnsafeCell<B>]>` storage. Only then does
it invoke `fixed_region`, validate the results and pairwise disjointness, and
cache each pointer and length in separate dense metadata. `UnsafeCell` is
required: an `Rc` gives shared access to the pool while fixed reads let the
kernel mutate inline `B` storage.

## Acquisition and views

```rust
impl<B: 'static> FixedBufPool<B> {
    /// Acquire any free slot. The common path uses an internal O(1) free list.
    pub fn try_acquire(&self) -> Result<FixedBuf<B>, AcquireError>;

    /// Acquire a specific kernel-table slot.
    pub fn try_acquire_at(&self, index: usize)
        -> Result<FixedBuf<B>, AcquireError>;

    pub fn unregister(self) -> Result<Vec<B>, UnregisterError<B>>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}

impl<B: 'static> FixedBuf<B> {
    /// Select a non-empty range relative to the whole registered slot.
    pub fn set_range(&mut self, range: Range<usize>)
        -> Result<(), RangeError>;
    pub fn reset_range(&mut self);
    pub fn range(&self) -> Range<usize>;

    /// Logical payload length. Fixed writes use exactly this many bytes.
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize;
    pub fn clear(&mut self);
    pub fn set_len(&mut self, len: usize) -> Result<(), LengthError>;
    pub fn set_payload(&mut self, payload: &[u8]) -> Result<(), LengthError>;
    pub fn consume(&mut self, n: usize);

    pub fn as_slice(&self) -> &[u8];
    pub fn as_mut_slice(&mut self) -> &mut [u8];
    pub fn as_full_slice(&self) -> &[u8];
    pub fn as_full_slice_mut(&mut self) -> &mut [u8];

    pub fn index(&self) -> usize;
}
```

`AcquireError` distinguishes exhaustion, an invalid explicit index, and an
already-acquired explicit index. Public indices are `usize`; the implementation
validates and stores the kernel index as `u16` at registration/acquisition
rather than converting on each SQE construction. Any-slot acquisition is O(1);
explicit indexed acquisition searches the free list and is intended for setup,
not a large-pool per-request hot path.

A newly acquired slot selects the whole registered region and treats the whole
initialized region as payload. Changing or resetting the range sets the
logical length to zero. This prevents a view change from silently re-enabling
bytes for a later write. Callers then fill and `set_len`, or issue a fixed read,
which uses capacity rather than logical length.

`FixedBuf` implements `AsRef<[u8]>`, `AsMut<[u8]>`, `Deref<Target = [u8]>`, and
`DerefMut` over the logical payload. It also implements `StableBuf` and
`StableBufMut`, so the memory can be used by ordinary Norn operations. Ordinary
operations may use it through another driver because they carry a process
address rather than a ring-local buffer index.

The common any-slot acquisition path maintains a free list. Moving a
`FixedBuf` through repeated I/O does not touch that list or clone its pool
`Rc`; only initial acquisition and final release do. A fixed buffer caches its
pointer, kernel index, view, capacity, and logical length, so SQE construction
does not touch user `B`, dispatch through a trait object, or traverse pool
metadata.

The underlying `B` is deliberately inaccessible while registered. Applications
with per-buffer metadata keep it externally and associate it through
`FixedBuf::index()`; exact `B` values become accessible again after successful
unregistration.

## Fixed file operations

```rust
impl File {
    pub fn read_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        offset: u64,
    ) -> impl Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static;

    pub fn write_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        offset: u64,
    ) -> impl Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static;
}
```

The methods compare driver identity before allocating an operation or
constructing an SQE. A mismatch is a programmer error and panics eagerly with
`#[track_caller]`, matching Norn's request-linking behavior.

`ReadFixed` submits the selected capacity. A successful completion replaces
the logical length with the validated CQE byte count. An error preserves the
previous logical length. `WriteFixed` submits only the logical length. Both
operations own and return the fixed buffer, so it remains unavailable while an
operation is unpolled, queued, submitted, cancelled, or completion-pending.

The internal operation is based on `NornFd`, keeping the lifecycle reusable if
fixed operations are later exposed for pipes or sockets.

## Raw-operation soundness prerequisite

`Operation` is currently a safe public trait, while `Handle::submit` is safe.
That lets safe downstream code construct arbitrary pointer-bearing SQEs and
bypass the ownership represented by `StableBuf` or `FixedBuf`. The registered
buffer API cannot make a sound claim while this escape exists.

As part of this feature, `Operation` becomes an `unsafe trait`. Its safety
contract requires every pointer, file descriptor, kernel index, and referenced
resource in the configured SQE to remain valid and correctly aliased through
all terminal CQEs and cleanup. Every built-in implementation becomes an
audited `unsafe impl`. This preserves low-level customization without pretending
raw io_uring submission is a safe extension point.

## Registration lifetime and panic safety

The pool's inner allocation stores a weak driver token plus a generation. It
does not hold a strong `Handle`. Fixed operations already carry the file
descriptor's driver ownership; the pool must not add a
`RawOp -> FixedBuf -> pool -> Shared` cycle
if fail-soft shutdown abandons completions. If the weak token cannot upgrade,
ring destruction has already made the storage safe to release.

Norn's pre-existing `RawOp -> NornFd -> Handle/Shared` ownership can itself
form a fail-soft shutdown leak when an operation is permanently abandoned. A
fixed operation adds no new edge to that cycle. It can increase the leaked
footprint by retaining its pool and registered pages. Repairing that
general operation/descriptor shutdown cycle is tracked as separate runtime
work rather than weakening fixed-buffer lifetime safety.

The driver depends on an internal `registered_buffers::Registry` component
which owns the ring-wide fixed-table state machine:

```text
Empty -> Registering(generation) -> Registered(generation)
                                      |
                                      v
                              Released(generation) -> Empty
```

`Registry` owns the active state, generation allocation, failed-unregister
retention, and retry logic. The driver supplies access to its `IoUring` and a
small `Weak<Shared>` adapter used by published pools. Only the matching
generation may unregister, mark release, or clear state. A state mismatch is
treated conservatively: backing memory is retained rather than freed.

Registration is a panic-safe transaction:

1. Validate the cheap table-count constraints and reserve a fresh driver
   generation with a rollback guard.
2. Move every `B` to final storage.
3. Invoke the region provider, validate regions and pairwise disjointness, and
   preallocate all metadata/iovecs, pool state, and error-recovery state.
4. Arm the pool's conservative registered-storage guard.
5. Call `register_buffers`; disarm the storage guard if the syscall reports
   failure.
6. Publish `Registered(generation)` and disarm the reservation guard with an
   asserted state transition.

From the instant the kernel call succeeds, unwinding can only successfully
unregister or retain the storage. No panic path may free a registered address.

`unregister(self)` first proves unique pool ownership, which also proves there
are no fixed buffers or operations. On success it unregisters the matching
table, clears the generation, and returns `Vec<B>` in original order. On busy,
kernel error, or state mismatch, `UnregisterError<B>` owns the intact pool:

```rust
impl<B> UnregisterError<B> {
    pub fn kind(&self) -> &UnregisterErrorKind;
    pub fn into_pool(self) -> FixedBufPool<B>;
    pub fn into_parts(self) -> (UnregisterErrorKind, FixedBufPool<B>);
}
```

The last-owner `Drop` path also attempts release. On any failure or unwind it
moves the type-erased storage into a driver-owned retention list. That list is
dropped only after the `IoUring` field, so ring destruction precedes storage
release. If even retention cannot be completed without panicking, the storage
is leaked. Leaking is the safe fallback; freeing registered memory is not. A
subsequent registration retries unregistration of retained storage before it
reports the table busy, which recovers from transient borrow and syscall
failures.

## Kernel validation

Registration rejects before the syscall:

- an empty pool;
- an empty region;
- pairwise-overlapping regions;
- more than 16,384 regions (`IORING_MAX_FIXED_BUFS`);
- a region or selected view larger than `u32::MAX`;
- a stopped driver; and
- any already registering or registered fixed-buffer table.

The kernel can still reject locked-memory usage, unsupported functionality, or
other resource limits. Fixed-buffer table registration is ring-wide and
separate from provided buffer rings.

CQE byte counts are checked against the submitted length before logical
metadata is updated. `O_DIRECT` address, file-offset, and transfer-length
alignment remain the caller's responsibility because they vary by filesystem
and device.

## Evolution boundaries

Dense `unregister() -> Vec<B>` is intentionally permanent for this type.
Sparse slots need indexed optional recovery and resource retirement, so they
should use a separate `SparseFixedBufTable<B>` (or builder-selected table type)
rather than changing `FixedBufPool<B>` semantics.

Safe replacement requires slot generations and retaining old buffers until a
resource-tag CQE proves the kernel has released every old reference. Norn's CQ
router must also recognize those tags before updates are exposed. Neither is a
v1 implementation detail to guess at now.

Disjoint leases similarly require a new exclusion model. V1's whole-slot rule
is simple, auditable, and maps queue depth directly to registered entries.

Uninitialized spare-capacity registration needs a parallel trait that exposes
`MaybeUninit<u8>` and tracks initialized ranges. The fully initialized
`FixedBuffer` contract will not be silently broadened.

## Required implementation validation

- A custom inline `#[repr(align(4096))] [u8; N]` wrapper completes fixed reads
  and writes, proving pointers are captured after final placement.
- `Vec<u8>`, `Box<[u8]>`, `BytesMut`, `[u8; N]`, a foreign type registered via
  the region closure, and `Box<dyn FixedBuffer>` work as pools.
- Invalid inputs, indices, ranges, double acquisition, and pool exhaustion fail
  before submission.
- Any-slot acquisition recycles released indices without per-I/O free-list work.
- Range changes clear logical length; failed reads preserve it; successful reads
  validate and replace it; writes submit only it.
- A fixed buffer from ring A cannot construct an operation on ring B.
- The same fixed buffer remains usable in an ordinary operation on ring B.
- Dropping unpolled and submitted/cancelled operations does not release a slot
  or backing memory early.
- Explicit unregistration recovers original values in order and permits a new
  registration on the same ring.
- Injected panics immediately after kernel registration cannot free storage.
- Injected unregister failure and generation mismatch retain storage.
- Driver destruction before pool destruction safely releases storage.
- Miri exercises pool/view/lease state; Linux integration tests exercise actual
  registration and fixed I/O.

## Performance validation contract to finalize before running

Registration cost is measured separately from steady-state I/O. The I/O matrix
compares ordinary direct and fixed direct reads/writes with persistent,
pre-acquired buffers in a continuously replenished window. The primary case is
4 KiB at queue depths 1, 32, and 128; secondary block sizes are 16 and 64 KiB.
Registration is outside the timed loop.

The benchmark must confirm:

- exactly the existing one `RawOp` allocation for both ordinary and fixed
  request construction, with zero fixed-specific allocations or hot-path
  dynamic dispatch;
- no more than 2% QD1 regression and no statistically credible 5% or greater
  QD32-128 regression;
- an untimed, offset-derived exact-data pass over the full operation window
  before timing;
- at least seven alternating paired ordinary/fixed trials per primary case;
- no regression in existing ordinary noop and file-I/O cases; and
- separate registration/unregistration scaling.

The exact command, repetitions, CPU pinning, success threshold, and durable
results file are agreed with the user before measurements are run.
