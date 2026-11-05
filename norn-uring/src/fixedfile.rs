//! A managed table of registered ("fixed" or "direct") file descriptors.
//!
//! [`Handle::register_fixed_files`] registers a sparse `io_uring` file table
//! with one driver. Each [`FixedFd`] owns one occupied slot in that table. A
//! request that names the slot with `IOSQE_FIXED_FILE` makes the kernel use the
//! table's file reference instead of looking up and reference-counting a
//! process descriptor for every request.
//!
//! ```no_run
//! use std::os::fd::AsFd;
//!
//! use norn_uring::Handle;
//!
//! # async fn example(socket: std::net::TcpStream) -> std::io::Result<()> {
//! let table = Handle::current().register_fixed_files(64)?;
//! let fixed = table.insert_fd(socket.as_fd())?;
//! // The table holds its own reference to the socket.
//! drop(socket);
//! println!("socket installed in slot {}", fixed.index());
//! fixed.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Slot ownership
//!
//! Norn, not the kernel, chooses every slot index. A slot is reused only
//! after its previous file can no longer be observed:
//!
//! - every request retains its [`FixedFd`]'s slot through the terminal
//!   completion, so a queued, submitted, or cancelled request can never
//!   resolve its index to a different file;
//! - dropping the last owner clears the kernel entry synchronously with
//!   `IORING_REGISTER_FILES_UPDATE` before the index is reused; and
//! - an index whose entry cannot be cleared is quarantined and retried before
//!   the next allocation instead of being reused.
//!
//! Kernel-side automatic slot allocation (`IORING_FILE_INDEX_ALLOC`) is never
//! requested, because it would race with Norn's allocator. Another process
//! holding this ring's descriptor could still place files into the table with
//! `IORING_OP_MSG_RING`; Norn treats its own slots as authoritative and
//! replaces such entries on the next install into the same index.
//!
//! # Kernel requirements
//!
//! Sparse table registration requires Linux 5.19. [`FixedFd::install`]
//! requires Linux 6.8. Kernels without a feature report an OS error from
//! the corresponding call or request.
//!
//! Linux 6.13 or newer is recommended. Earlier kernels track registered files
//! and buffers in shared generations: a file removed from a slot is closed
//! only after every request that began before the removal and uses a
//! registered file or buffer has completed. One long-lived request, such as a
//! receive on an idle connection, can then keep other closed sockets open
//! indefinitely. The same generations make unregistering a table wait for
//! those requests, so Norn never unregisters from a drop; see
//! [`Handle::register_fixed_files`].

use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::io;
use std::ops::Range;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{ready, Context, Poll};

use futures_core::Stream;

use io_uring::types::{self, DestinationSlot, FsyncFlags};
use io_uring::{opcode, IoUring};
use log::warn;

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRingBuf, RecvBufRing};
use crate::fd::{FdTarget, NornFd, OpFd, UringFd};
use crate::fixedbuf::FixedBuf;
use crate::fs::{self, OpenOptions};
use crate::net::socket::{RecvRingMulti, RingReceive, RingRecvStream};
use crate::net::{Recv, Send};
use crate::operation::{CQEResult, Op, Operation, Singleshot};
use crate::Handle;

const LOG: &str = "norn_uring::fixedfile";

impl Handle {
    /// Register a sparse table of `slots` fixed files with this driver.
    ///
    /// A ring has at most one file table. Another can be registered once the
    /// returned [`FixedFileTable`] and every [`FixedFd`] allocated from it are
    /// dropped. Dropping them clears every slot but leaves the kernel table
    /// registered until the next registration unregisters it or the ring is
    /// destroyed. On kernels before Linux 6.13 that unregistration waits for
    /// every in-flight request that uses a registered file or buffer, so
    /// register a new table only when no such request depends on this thread
    /// to finish.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when `slots` is zero or larger
    /// than `i32::MAX`, [`io::ErrorKind::AlreadyExists`] while another table is
    /// registered, [`io::ErrorKind::BrokenPipe`] after the driver stops, and
    /// the kernel's error when unregistering the previous table or
    /// registering this one fails. Linux reports `EMFILE` when `slots` exceeds
    /// `RLIMIT_NOFILE`, and `EINVAL` when it exceeds the kernel's table limit
    /// or sparse registration is unsupported.
    pub fn register_fixed_files(&self, slots: u32) -> io::Result<FixedFileTable> {
        if slots == 0 || i32::try_from(slots).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed-file table size must be between 1 and i32::MAX",
            ));
        }
        self.register_files_sparse(slots)?;
        Ok(FixedFileTable {
            table: Rc::new(Table {
                handle: self.clone(),
                capacity: slots,
                high_water: Cell::new(0),
                free: RefCell::new(Vec::new()),
                quarantined: RefCell::new(Vec::new()),
            }),
        })
    }
}

/// A driver's registered file table.
///
/// The table allocates slots for [`FixedFd`] values. It is not `Clone`;
/// share it by reference, or wrap it in an `Rc` when several tasks install
/// files.
pub struct FixedFileTable {
    table: Rc<Table>,
}

impl std::fmt::Debug for FixedFileTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedFileTable")
            .field("capacity", &self.table.capacity)
            .field("available", &self.table.available())
            .finish()
    }
}

impl FixedFileTable {
    /// Total number of slots in the registered table.
    pub fn capacity(&self) -> u32 {
        self.table.capacity
    }

    /// Number of slots that can currently be allocated.
    ///
    /// Slots owned by a [`FixedFd`], retained by an operation, or quarantined
    /// after a failed release are excluded.
    pub fn available(&self) -> u32 {
        self.table.available()
    }

    /// Install a process descriptor into a free slot with `IORING_OP_FILES_UPDATE`.
    ///
    /// The table takes its own reference to the open file. `fd` remains open
    /// and independently owned; closing it after this request completes does
    /// not affect the returned [`FixedFd`]. The request retains `fd` until its
    /// terminal completion, so [`UringFd::close`] reports
    /// [`io::ErrorKind::WouldBlock`] meanwhile.
    ///
    /// A slot is reserved when the request is first polled, and returned to
    /// the table if the request fails or is dropped before completing.
    ///
    /// The request resolves to an error with raw OS error `ENFILE` when the
    /// table has no free slot, and with [`io::ErrorKind::BrokenPipe`] when it
    /// is created after the driver stops.
    pub fn insert(&self, fd: &UringFd) -> impl crate::Request<Output = io::Result<FixedFd>> {
        let insert = Insert {
            table: Rc::clone(&self.table),
            source: fd.lease(),
            fds: [-1],
            slot: None,
        };
        self.table.handle.submit(insert)
    }

    /// Synchronously install a borrowed process descriptor into a free slot.
    ///
    /// This uses `IORING_REGISTER_FILES_UPDATE` and is useful for descriptors
    /// that are not owned by Norn. The table takes its own reference to the
    /// open file; `fd` remains owned by the caller.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::BrokenPipe`] after the driver stops, raw OS
    /// error `ENFILE` when the table has no free slot, or the kernel's error
    /// when `fd` cannot be installed.
    pub fn insert_fd(&self, fd: BorrowedFd<'_>) -> io::Result<FixedFd> {
        let slot = self.table.reserve()?;
        let installed = self
            .table
            .handle
            .update_files(slot.index(), &[fd.as_raw_fd()])?;
        if installed != 1 {
            return Err(not_installed());
        }
        slot.inner.occupied.set(true);
        Ok(FixedFd { slot })
    }

    /// Open a file directly into a free slot.
    ///
    /// The file never occupies a process descriptor. Use
    /// [`FixedFd::install`] when a process descriptor is needed later.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid option combination or path,
    /// [`io::ErrorKind::BrokenPipe`] when called after the driver stops, raw
    /// OS error `ENFILE` when the table has no free slot, or the kernel's
    /// error when the file cannot be opened.
    pub async fn open<P: AsRef<Path>>(
        &self,
        path: P,
        options: &OpenOptions,
    ) -> io::Result<FixedFd> {
        let open = OpenDirect::new(path.as_ref(), options, Rc::clone(&self.table))?;
        self.table.handle.submit(open).await
    }
}

/// An open file installed in one slot of a [`FixedFileTable`].
///
/// Requests submitted through this value retain its slot until their
/// terminal completion. Dropping the last owner removes the file from the
/// table, which closes it unless another reference, such as the descriptor
/// passed to [`FixedFileTable::insert_fd`], keeps it open.
///
/// Dropping the value does not require an active driver context.
pub struct FixedFd {
    slot: Slot,
}

impl std::fmt::Debug for FixedFd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedFd")
            .field("index", &self.slot.index())
            .finish()
    }
}

impl FixedFd {
    /// The slot's index in its driver's registered file table.
    ///
    /// The index is valid only while this value lives. A custom
    /// [`Operation`] that targets it with `IOSQE_FIXED_FILE` must be submitted
    /// to the same driver and own this `FixedFd` until its terminal completion.
    pub fn index(&self) -> u32 {
        self.slot.index()
    }

    /// Read bytes from the file into `buf`, starting at `offset`.
    ///
    /// The offset is ignored for streams such as pipes and sockets.
    pub fn read_at<B>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBufMut + 'static,
    {
        let read = fs::ReadAt::new(self.slot.clone(), buf, offset);
        self.slot.handle().submit(read)
    }

    /// Write `buf` to the file, starting at `offset`.
    ///
    /// The offset is ignored for streams such as pipes and sockets.
    pub fn write_at<B>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBuf + 'static,
    {
        let write = fs::WriteAt::new(self.slot.clone(), buf, offset);
        self.slot.handle().submit(write)
    }

    /// Read bytes into a registered fixed buffer, starting at `offset`.
    ///
    /// This behaves like [`fs::File::read_fixed_at`].
    ///
    /// # Panics
    ///
    /// Panics when the fixed buffer was registered with another driver.
    #[track_caller]
    pub fn read_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static,
    {
        assert!(
            buf.same_driver(self.slot.handle()),
            "fixed buffer and fixed file must target the same driver"
        );
        let read = fs::ReadFixedAt::new(self.slot.clone(), buf, offset);
        self.slot.handle().submit(read)
    }

    /// Write a registered fixed buffer's logical payload, starting at `offset`.
    ///
    /// This behaves like [`fs::File::write_fixed_at`].
    ///
    /// # Panics
    ///
    /// Panics when the fixed buffer was registered with another driver.
    #[track_caller]
    pub fn write_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static,
    {
        assert!(
            buf.same_driver(self.slot.handle()),
            "fixed buffer and fixed file must target the same driver"
        );
        let write = fs::WriteFixedAt::new(self.slot.clone(), buf, offset);
        self.slot.handle().submit(write)
    }

    /// Read bytes into several segments of one registered fixed buffer.
    ///
    /// This behaves like [`fs::File::readv_fixed_at`], including its segment
    /// validation and Linux 6.15 requirement.
    ///
    /// # Panics
    ///
    /// Panics when the fixed buffer was registered with another driver.
    #[track_caller]
    pub fn readv_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        segments: &[Range<usize>],
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static,
    {
        assert!(
            buf.same_driver(self.slot.handle()),
            "fixed buffer and fixed file must target the same driver"
        );
        let read =
            fs::VectoredFixedAt::<B, false, Slot>::new(self.slot.clone(), buf, segments, offset);
        self.slot.handle().submit(read)
    }

    /// Write bytes gathered from several segments of one registered fixed
    /// buffer.
    ///
    /// This behaves like [`fs::File::writev_fixed_at`], including its segment
    /// validation and Linux 6.15 requirement.
    ///
    /// # Panics
    ///
    /// Panics when the fixed buffer was registered with another driver.
    #[track_caller]
    pub fn writev_fixed_at<B>(
        &self,
        buf: FixedBuf<B>,
        segments: &[Range<usize>],
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, FixedBuf<B>)>
    where
        B: 'static,
    {
        assert!(
            buf.same_driver(self.slot.handle()),
            "fixed buffer and fixed file must target the same driver"
        );
        let write =
            fs::VectoredFixedAt::<B, true, Slot>::new(self.slot.clone(), buf, segments, offset);
        self.slot.handle().submit(write)
    }

    /// Sync the file and its metadata to disk.
    pub fn sync(&self) -> impl crate::Request<Output = io::Result<()>> {
        let sync = fs::Sync::new(self.slot.clone(), FsyncFlags::empty());
        self.slot.handle().submit(sync)
    }

    /// Sync only the file's data to disk.
    pub fn datasync(&self) -> impl crate::Request<Output = io::Result<()>> {
        let sync = fs::Sync::new(self.slot.clone(), FsyncFlags::DATASYNC);
        self.slot.handle().submit(sync)
    }

    /// Receive bytes from a connected socket into `buf`.
    pub fn recv<B>(&self, buf: B) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBufMut + 'static,
    {
        let recv = Recv::<B, false, Slot>::new(self.slot.clone(), buf, 0);
        self.slot.handle().submit(recv)
    }

    /// Send `buf` on a connected socket.
    pub fn send<B>(&self, buf: B) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBuf + 'static,
    {
        let send = Send::new(self.slot.clone(), buf, 0);
        self.slot.handle().submit(send)
    }

    /// Receive from a connected socket with a multishot receive into a
    /// provided buffer ring.
    ///
    /// See [`FixedRecvRingMulti`].
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    #[track_caller]
    pub fn recv_ring_multi(&self, ring: &RecvBufRing) -> FixedRecvRingMulti {
        assert!(
            ring.same_driver(self.slot.handle()),
            "buffer ring and fixed file must target the same driver"
        );
        FixedRecvRingMulti {
            inner: Some(RingRecvStream::new(self.slot.clone(), ring, 0)),
        }
    }

    /// Install this file into the process file table with `IORING_OP_FIXED_FD_INSTALL`.
    ///
    /// The returned descriptor is close-on-exec and refers to the same open
    /// file; this `FixedFd` remains valid and independently owned. Use it for
    /// APIs that require a process descriptor, for example with
    /// [`fs::File::from_uring_fd`].
    ///
    /// Requires Linux 6.8. Older kernels complete with `EINVAL`.
    pub fn install(&self) -> impl crate::Request<Output = io::Result<UringFd>> {
        let install = Install {
            slot: self.slot.clone(),
        };
        self.slot.handle().submit(install)
    }

    /// Remove the file from its slot with an asynchronous fixed-file close.
    ///
    /// Like dropping the value, this closes the file unless another
    /// reference keeps it open. On success the slot becomes available
    /// immediately.
    ///
    /// After the driver stops, the close cannot be submitted, so the slot is
    /// cleared synchronously instead, as dropping the value would.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while an operation still retains
    /// the slot; the slot is then cleared when the last operation releases it.
    /// Other errors are reported by the kernel, after which the slot is
    /// cleared synchronously before reuse.
    pub async fn close(self) -> io::Result<()> {
        if Rc::strong_count(&self.slot.inner) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "explicit close requires sole fixed-file ownership",
            ));
        }
        let handle = self.slot.handle().clone();
        if handle.check_accepting_fixed_files().is_err() {
            return self.slot.clear_now();
        }
        handle.submit(CloseFixed { slot: self.slot }).await
    }
}

pin_project_lite::pin_project! {
    /// A multishot receive into a provided buffer ring on a registered socket.
    ///
    /// Returned by [`FixedFd::recv_ring_multi`]. Like
    /// [`TcpRecvRingMulti`](crate::net::TcpRecvRingMulti), it yields received
    /// buffers in order, re-arms when the kernel ends the receive, pauses at the
    /// ring's unconsumed limit without losing data, and ends at the peer's
    /// orderly shutdown. Each receive retains the slot, so the file stays in
    /// the table until the stream and its in-flight receive are gone.
    pub struct FixedRecvRingMulti {
        #[pin]
        inner: Option<RingRecvStream<RecvRingMulti<Slot>>>,
    }
}

impl std::fmt::Debug for FixedRecvRingMulti {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedRecvRingMulti")
            .field("ended", &self.inner.is_none())
            .finish()
    }
}

impl Stream for FixedRecvRingMulti {
    type Item = io::Result<BufRingBuf>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let Some(inner) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        match ready!(inner.poll_next(cx)) {
            // An orderly shutdown completes with an empty buffer.
            Some(Ok(buffer)) if buffer.capacity() == 0 => {
                this.inner.set(None);
                Poll::Ready(None)
            }
            item @ Some(_) => Poll::Ready(item),
            None => {
                this.inner.set(None);
                Poll::Ready(None)
            }
        }
    }
}

impl RingReceive for RecvRingMulti<Slot> {
    type Buffer = BufRingBuf;
    type Source = Slot;

    fn receive(slot: &Slot, ring: &RecvBufRing, flags: i32) -> Op<Self> {
        let op = RecvRingMulti::new(slot.clone(), ring.clone(), flags);
        slot.handle().submit(op)
    }
}

/// A retained reference to one allocated slot.
///
/// The index stays reserved while any clone lives. When the last clone drops,
/// an occupied kernel entry is cleared before the index is reused.
#[derive(Clone)]
pub(crate) struct Slot {
    inner: Rc<SlotInner>,
}

struct SlotInner {
    table: Rc<Table>,
    index: u32,
    /// Whether the kernel entry may hold a file.
    occupied: Cell<bool>,
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot")
            .field("index", &self.inner.index)
            .field("occupied", &self.inner.occupied.get())
            .finish()
    }
}

impl Slot {
    fn index(&self) -> u32 {
        self.inner.index
    }

    fn handle(&self) -> &Handle {
        &self.inner.table.handle
    }

    /// Clear the kernel entry synchronously and release the index. If the
    /// clear fails, releasing the index retries it and quarantines the index
    /// when that fails too.
    fn clear_now(self) -> io::Result<()> {
        let result = self.inner.table.clear(self.inner.index);
        if result.is_ok() {
            self.inner.occupied.set(false);
        }
        result
    }
}

// Safety: the index is returned to the allocator only after the last `Slot`
// clone drops and its kernel entry is cleared, and `FixedFd` submits every
// operation to the driver that owns the table.
unsafe impl OpFd for Slot {
    #[inline]
    fn target(&self) -> FdTarget {
        FdTarget::Fixed(types::Fixed(self.inner.index))
    }
}

impl Drop for SlotInner {
    fn drop(&mut self) {
        self.table.release(self.index, self.occupied.get());
    }
}

/// Userspace slot allocator for one registered table.
struct Table {
    handle: Handle,
    capacity: u32,
    /// Indices at or above this value have never been allocated.
    high_water: Cell<u32>,
    /// Allocated-before indices whose kernel entries are empty.
    free: RefCell<Vec<u32>>,
    /// Indices whose kernel entries could not be cleared yet.
    quarantined: RefCell<Vec<u32>>,
}

impl Table {
    fn available(&self) -> u32 {
        let free = self.free.try_borrow().map_or(0, |free| free.len());
        // `free` holds distinct indices below `high_water <= capacity`.
        self.capacity - self.high_water.get() + free as u32
    }

    /// Allocate an empty slot for a new file. Every insertion path reserves
    /// through here, so a stopped driver admits no new files; releasing
    /// existing slots does not pass through this check.
    fn reserve(self: &Rc<Self>) -> io::Result<Slot> {
        self.handle.check_accepting_fixed_files()?;
        self.retry_quarantined();
        let recycled = self
            .free
            .try_borrow_mut()
            .ok()
            .and_then(|mut free| free.pop());
        let index = match recycled {
            Some(index) => index,
            None => {
                let index = self.high_water.get();
                if index >= self.capacity {
                    return Err(io::Error::from_raw_os_error(libc::ENFILE));
                }
                self.high_water.set(index + 1);
                index
            }
        };
        Ok(Slot {
            inner: Rc::new(SlotInner {
                table: Rc::clone(self),
                index,
                occupied: Cell::new(false),
            }),
        })
    }

    /// Return an index whose last owner dropped. This runs from `Drop` and
    /// must not panic; an index that cannot be proven empty is never freed.
    fn release(&self, index: u32, occupied: bool) {
        if occupied {
            if let Err(err) = self.clear(index) {
                warn!(target: LOG, "release.clear_failed index={index} {err}");
                // Leak rather than free: free indices must be empty in the
                // kernel table, and the stale file must not stay reachable.
                push_or_leak(&self.quarantined, index);
                return;
            }
        }
        self.recycle(index);
    }

    fn recycle(&self, index: u32) {
        push_or_leak(&self.free, index);
    }

    fn clear(&self, index: u32) -> io::Result<()> {
        if self.handle.update_files(index, &[-1])? != 1 {
            return Err(io::Error::other("fixed-file slot was not cleared"));
        }
        Ok(())
    }

    fn retry_quarantined(&self) {
        let pending = match self.quarantined.try_borrow_mut() {
            Ok(mut quarantined) if !quarantined.is_empty() => std::mem::take(&mut *quarantined),
            _ => return,
        };
        for index in pending {
            if self.clear(index).is_ok() {
                self.recycle(index);
            } else {
                push_or_leak(&self.quarantined, index);
            }
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        // Every slot retains the table, so only quarantined entries can still
        // hold files. Give them one more chance to clear; one that still fails
        // keeps its file until the next registration unregisters the kernel
        // table or the ring is destroyed. Unregistering here could block this
        // thread on kernels before 6.13.
        self.retry_quarantined();
        self.handle.retire_files();
    }
}

/// Record `index` without panicking, leaking it if the list is unavailable.
fn push_or_leak(list: &RefCell<Vec<u32>>, index: u32) {
    let Ok(mut list) = list.try_borrow_mut() else {
        warn!(target: LOG, "slot.leaked index={index}");
        return;
    };
    if list.try_reserve(1).is_ok() {
        list.push(index);
    } else {
        warn!(target: LOG, "slot.leaked index={index}");
    }
}

fn not_installed() -> io::Error {
    io::Error::other("io_uring did not install the descriptor")
}

fn reserve_with(table: &Rc<Table>, slot: &mut Option<Slot>) -> io::Result<u32> {
    debug_assert!(slot.is_none(), "fixed-file operation configured twice");
    let reserved = table.reserve()?;
    let index = reserved.index();
    *slot = Some(reserved);
    Ok(index)
}

/// Install a retained process descriptor with `IORING_OP_FILES_UPDATE`.
struct Insert {
    table: Rc<Table>,
    /// Keeps the descriptor number open until the terminal CQE.
    source: NornFd,
    /// The update array read by the kernel.
    fds: [RawFd; 1],
    slot: Option<Slot>,
}

// Safety: `fds` lives inline in the stably placed operation, `source` keeps
// the copied descriptor number open, and `slot` reserves an empty index
// through the terminal CQE. `reap` converts a successful update into an owned
// `FixedFd`; on failure the kernel leaves the empty index unchanged.
unsafe impl Operation for Insert {
    type Completion = io::Result<FixedFd>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let index = reserve_with(&self.table, &mut self.slot)?;
        self.fds[0] = self.source.fd().0;
        // `register_fixed_files` bounds every index by `i32::MAX`.
        Ok(opcode::FilesUpdate::new(self.fds.as_ptr(), 1)
            .offset(index as i32)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let slot = self.slot.take();
        let installed = result.into_result()?;
        let slot = slot.ok_or_else(not_installed)?;
        if installed != 1 {
            return Err(not_installed());
        }
        slot.inner.occupied.set(true);
        Ok(FixedFd { slot })
    }
}

impl Singleshot for Insert {
    type Output = io::Result<FixedFd>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

/// Open a path directly into a reserved slot.
struct OpenDirect {
    path: CString,
    how: types::OpenHow,
    table: Rc<Table>,
    slot: Option<Slot>,
}

impl OpenDirect {
    fn new(path: &Path, options: &OpenOptions, table: Rc<Table>) -> io::Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        let creation_mode = options.get_creation_mode()?;
        // A direct descriptor has no close-on-exec state, and the kernel
        // rejects `O_CLOEXEC` when a destination slot is supplied.
        let flags = options.get_access_mode()? | creation_mode;
        let mut how = types::OpenHow::new().flags(flags as u64);
        if creation_mode & libc::O_CREAT != 0 {
            how = how.mode(u64::from(options.mode));
        }
        Ok(Self {
            path,
            how,
            table,
            slot: None,
        })
    }
}

// Safety: `path` and `how` are owned by the stably placed operation, and
// `slot` reserves an empty index through the terminal CQE. The kernel installs
// a file only on success, which `reap` converts into an owned `FixedFd`.
unsafe impl Operation for OpenDirect {
    type Completion = io::Result<FixedFd>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let index = reserve_with(&self.table, &mut self.slot)?;
        let destination = DestinationSlot::try_from_slot_target(index).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "fixed-file index out of range")
        })?;
        Ok(
            opcode::OpenAt2::new(types::Fd(libc::AT_FDCWD), self.path.as_ptr(), &self.how)
                .file_index(Some(destination))
                .build(),
        )
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let slot = self.slot.take();
        result.into_result()?;
        let slot = slot.ok_or_else(not_installed)?;
        slot.inner.occupied.set(true);
        Ok(FixedFd { slot })
    }
}

impl Singleshot for OpenDirect {
    type Output = io::Result<FixedFd>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

/// Create a process descriptor for a fixed file.
struct Install {
    slot: Slot,
}

// Safety: `slot` retains the source entry through the terminal CQE, and a
// successful CQE is converted into an owned descriptor before it is queued.
unsafe impl Operation for Install {
    type Completion = io::Result<OwnedFd>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        // Flags 0 makes the new descriptor close-on-exec.
        Ok(opcode::FixedFdInstall::new(types::Fixed(self.slot.index()), 0).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let fd = result.into_result()?;
        // Safety: a successful install CQE transfers ownership of a new descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    }
}

impl Singleshot for Install {
    type Output = io::Result<UringFd>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        let fd = completion?;
        Ok(UringFd::from_fd_on(
            fd.into_raw_fd(),
            self.slot.handle().clone(),
        ))
    }
}

/// Remove a fixed file with `IORING_OP_CLOSE`.
struct CloseFixed {
    slot: Slot,
}

// Safety: `slot` retains the index through the terminal CQE. Only a
// successful close marks the entry empty; otherwise releasing the slot clears
// it synchronously before reuse.
unsafe impl Operation for CloseFixed {
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(opcode::Close::new(types::Fixed(self.slot.index())).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result.into_result()?;
        self.slot.inner.occupied.set(false);
        Ok(())
    }
}

impl Singleshot for CloseFixed {
    type Output = io::Result<()>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableState {
    Empty,
    Registered,
    /// The table's last owner dropped. The kernel table stays registered, with
    /// its slots cleared, until the next registration unregisters it.
    Retired,
}

/// Tracks the single registered file table owned by one ring.
pub(crate) struct Registry {
    state: Cell<TableState>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("state", &self.state.get())
            .finish()
    }
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            state: Cell::new(TableState::Empty),
        }
    }

    pub(crate) fn register(&self, ring: &RefCell<IoUring>, slots: u32) -> io::Result<()> {
        match self.state.get() {
            TableState::Empty => {}
            TableState::Registered => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "a fixed-file table is already registered with this driver",
                ))
            }
            TableState::Retired => self.unregister(ring)?,
        }
        let ring = borrow_ring(ring)?;
        retry_interrupted(|| ring.submitter().register_files_sparse(slots))?;
        self.state.set(TableState::Registered);
        Ok(())
    }

    pub(crate) fn update(
        &self,
        ring: &RefCell<IoUring>,
        offset: u32,
        fds: &[RawFd],
    ) -> io::Result<usize> {
        if self.state.get() != TableState::Registered {
            return Err(io::Error::from_raw_os_error(libc::ENXIO));
        }
        let ring = borrow_ring(ring)?;
        retry_interrupted(|| ring.submitter().register_files_update(offset, fds))
    }

    /// Record that the registered table's last owner dropped.
    pub(crate) fn retire(&self) {
        if self.state.get() == TableState::Registered {
            self.state.set(TableState::Retired);
        }
    }

    /// Unregister a retired kernel table. On failure the table stays retired
    /// and the error is returned.
    fn unregister(&self, ring: &RefCell<IoUring>) -> io::Result<()> {
        let result = borrow_ring(ring)
            .and_then(|ring| retry_interrupted(|| ring.submitter().unregister_files()));
        match result {
            Ok(()) => {}
            // The kernel has no table, so there is nothing left to release.
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) => {}
            Err(err) => return Err(err),
        }
        self.state.set(TableState::Empty);
        Ok(())
    }
}

#[cfg(test)]
impl Registry {
    pub(crate) fn test_is_retired(&self) -> bool {
        self.state.get() == TableState::Retired
    }
}

fn borrow_ring(ring: &RefCell<IoUring>) -> io::Result<std::cell::Ref<'_, IoUring>> {
    ring.try_borrow().map_err(|_| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring is already borrowed during fixed-file registration",
        )
    })
}

fn retry_interrupted<T>(mut f: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match f() {
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::os::fd::AsFd;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::Driver;

    fn driver() -> Driver {
        Driver::new(IoUring::builder(), 8).unwrap()
    }

    fn pipe() -> (OwnedFd, OwnedFd) {
        let mut fds = [0; 2];
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        assert_eq!(rc, 0);
        // Safety: `pipe2` returned two new descriptors owned by this test.
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
    }

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Returns true once every writer of the pipe is closed.
    fn reader_sees_eof(reader: &OwnedFd) -> bool {
        let mut byte = 0_u8;
        let n = unsafe { libc::read(reader.as_raw_fd(), (&mut byte as *mut u8).cast(), 1) };
        assert!(n == 0 || io::Error::last_os_error().kind() == io::ErrorKind::WouldBlock);
        n == 0
    }

    #[test]
    fn registration_is_exclusive_and_reusable() {
        let driver = driver();
        let handle = driver.handle();
        assert_eq!(
            handle.register_fixed_files(0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let table = handle.register_fixed_files(4).unwrap();
        assert_eq!(
            handle.register_fixed_files(4).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );

        // A live slot keeps the table registered after the table handle drops.
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(table);
        assert_eq!(
            handle.register_fixed_files(4).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        drop(fixed);
        drop((reader, writer));

        let table = handle.register_fixed_files(2).unwrap();
        assert_eq!(table.capacity(), 2);
    }

    #[test]
    fn slots_are_allocated_densely_and_recycled_after_release() {
        let driver = driver();
        let table = driver.handle().register_fixed_files(2).unwrap();
        let (reader, writer) = pipe();

        let first = table.insert_fd(writer.as_fd()).unwrap();
        let second = table.insert_fd(reader.as_fd()).unwrap();
        assert_eq!((first.index(), second.index()), (0, 1));
        assert_eq!(table.available(), 0);
        let err = table.insert_fd(writer.as_fd()).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));

        // A retained operation lease keeps the index reserved.
        let lease = first.slot.clone();
        drop(first);
        assert_eq!(table.available(), 0);
        drop(lease);
        assert_eq!(table.available(), 1);
        assert_eq!(table.insert_fd(writer.as_fd()).unwrap().index(), 0);
    }

    #[test]
    fn failed_install_returns_the_empty_slot() {
        let driver = driver();
        let table = driver.handle().register_fixed_files(1).unwrap();
        // Linux refuses to register io_uring instances in a file table.
        let ring = IoUring::new(2).unwrap();
        let err = table.insert_fd(ring.as_fd()).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EBADF));
        assert_eq!(table.available(), 1);
    }

    #[test]
    fn dropping_a_slot_removes_the_table_reference() {
        let driver = driver();
        let table = driver.handle().register_fixed_files(1).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(writer);
        assert!(!reader_sees_eof(&reader), "table must keep the writer open");
        drop(fixed);
        assert!(reader_sees_eof(&reader));
    }

    #[test]
    fn failed_clear_quarantines_until_a_later_allocation() {
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(writer);

        handle.test_with_ring_borrowed_mut(|| drop(fixed));
        assert_eq!(table.available(), 0);
        assert!(
            !reader_sees_eof(&reader),
            "quarantined entry must stay installed"
        );

        // The next allocation clears and reuses the quarantined index.
        let (other_reader, other_writer) = pipe();
        let fixed = table.insert_fd(other_writer.as_fd()).unwrap();
        assert_eq!(fixed.index(), 0);
        assert!(reader_sees_eof(&reader));
        drop((fixed, other_reader, other_writer));
    }

    #[test]
    fn slots_release_after_driver_shutdown() {
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(writer);

        drop(driver);
        assert_eq!(
            handle.register_fixed_files(1).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(!reader_sees_eof(&reader));
        // Release needs neither a running driver nor a driver context.
        drop(fixed);
        assert!(reader_sees_eof(&reader));
        assert_eq!(table.available(), 1);
        drop(table);
    }

    #[test]
    fn stopped_driver_rejects_insertions_but_releases_slots() {
        // Another driver owns the async insert's source so the source can
        // still be closed through a running ring when the test ends.
        let other = driver();
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(2).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        let (_, source) = pipe();
        let source = UringFd::from_fd_on(source.into_raw_fd(), other.handle());

        drop(driver);
        let err = table.insert_fd(writer.as_fd()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(table.available(), 1);

        let Poll::Ready(Err(err)) = poll_once(table.insert(&source)) else {
            panic!("insert after shutdown must fail immediately");
        };
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        let mut options = OpenOptions::new();
        options.read(true);
        let Poll::Ready(Err(err)) = poll_once(table.open("/dev/null", &options)) else {
            panic!("open after shutdown must fail immediately");
        };
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(table.available(), 1);

        // Clearing an existing entry still works after shutdown.
        drop(writer);
        assert!(!reader_sees_eof(&reader));
        drop(fixed);
        assert!(reader_sees_eof(&reader));
        assert_eq!(table.available(), 2);
        drop((table, source, other));
    }

    #[test]
    fn close_waits_for_sole_ownership_and_frees_the_slot() {
        let driver = driver();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        ex.block_on(async {
            let table = Handle::current().register_fixed_files(1).unwrap();
            let (reader, writer) = pipe();
            let fixed = table.insert_fd(writer.as_fd()).unwrap();
            drop(writer);

            // An operation's lease keeps the slot, so close is refused.
            let lease = fixed.slot.clone();
            let err = fixed.close().await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
            assert!(!reader_sees_eof(&reader));
            // The refused close dropped its owner; the last lease clears the slot.
            drop(lease);
            assert!(reader_sees_eof(&reader));
            assert_eq!(table.available(), 1);

            let (reader, writer) = pipe();
            let fixed = table.insert_fd(writer.as_fd()).unwrap();
            drop(writer);
            fixed.close().await.unwrap();
            assert!(reader_sees_eof(&reader));
            assert_eq!(table.available(), 1);
        });
    }

    #[test]
    fn dropped_table_stays_registered_until_the_next_registration() {
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        // Dropping makes no syscall, so a borrowed ring does not matter.
        handle.test_with_ring_borrowed_mut(|| drop(table));
        assert!(handle.test_fixed_files_retired());

        // The next registration unregisters the old table first, and reports
        // why it could not.
        let mut result = None;
        handle.test_with_ring_borrowed_mut(|| result = Some(handle.register_fixed_files(1)));
        let err = result.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert!(handle.test_fixed_files_retired());

        let table = handle.register_fixed_files(2).unwrap();
        assert_eq!(table.capacity(), 2);
    }

    #[test]
    fn dropping_the_table_retries_quarantined_slots() {
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(writer);
        handle.test_with_ring_borrowed_mut(|| drop(fixed));
        assert!(!reader_sees_eof(&reader));

        drop(table);
        assert!(reader_sees_eof(&reader));
    }

    #[test]
    fn close_after_shutdown_clears_the_slot_synchronously() {
        use futures_util::FutureExt;

        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();
        drop(writer);

        drop(driver);
        let result = fixed
            .close()
            .now_or_never()
            .expect("close completes at once");
        result.unwrap();
        assert!(reader_sees_eof(&reader));
        assert_eq!(table.available(), 1);
    }
}
