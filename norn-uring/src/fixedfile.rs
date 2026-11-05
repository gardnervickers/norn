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
//! Sparse table registration requires Linux 5.19. Kernels without it report
//! an OS error from [`Handle::register_fixed_files`].

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::rc::Rc;

use io_uring::types;
use io_uring::{opcode, IoUring};
use log::warn;

use crate::operation::{CQEResult, Operation, Singleshot};
use crate::Handle;

const LOG: &str = "norn_uring::fixedfile";

impl Handle {
    /// Register a sparse table of `slots` fixed files with this driver.
    ///
    /// A ring has at most one file table. It remains registered until the
    /// returned [`FixedFileTable`] and every [`FixedFd`] allocated from it are
    /// dropped; a new table can be registered after that.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when `slots` is zero or larger
    /// than `i32::MAX`, [`io::ErrorKind::AlreadyExists`] while another table is
    /// registered, [`io::ErrorKind::BrokenPipe`] after the driver stops, and
    /// the kernel's error when registration fails. Linux reports `EMFILE` when
    /// `slots` exceeds `RLIMIT_NOFILE`, and `EINVAL` when it exceeds the
    /// kernel's table limit or sparse registration is unsupported.
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

    /// Remove the file from its slot with an asynchronous fixed-file close.
    ///
    /// Like dropping the value, this closes the file unless another
    /// reference keeps it open. On success the slot becomes available
    /// immediately.
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
        handle.submit(CloseFixed { slot: self.slot }).await
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
        // hold files. Unregistering releases them all.
        self.handle.unregister_files();
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
    /// Unregistration failed; it is retried before the next registration.
    Stale,
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
        if self.state.get() == TableState::Stale {
            self.unregister(ring);
        }
        if self.state.get() != TableState::Empty {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a fixed-file table is already registered with this driver",
            ));
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

    pub(crate) fn unregister(&self, ring: &RefCell<IoUring>) {
        if self.state.get() == TableState::Empty {
            return;
        }
        let result = borrow_ring(ring)
            .and_then(|ring| retry_interrupted(|| ring.submitter().unregister_files()));
        match result {
            Ok(()) => self.state.set(TableState::Empty),
            // The kernel has no table, so there is nothing left to release.
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) => {
                self.state.set(TableState::Empty)
            }
            Err(err) => {
                warn!(target: LOG, "unregister.deferred {err}");
                self.state.set(TableState::Stale);
            }
        }
    }
}

#[cfg(test)]
impl Registry {
    pub(crate) fn test_is_stale(&self) -> bool {
        self.state.get() == TableState::Stale
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
    use std::os::fd::{AsFd, FromRawFd, OwnedFd};

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
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(2).unwrap();
        let (reader, writer) = pipe();
        let fixed = table.insert_fd(writer.as_fd()).unwrap();

        drop(driver);
        let err = table.insert_fd(writer.as_fd()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(table.available(), 1);

        // Clearing an existing entry still works after shutdown.
        drop(writer);
        assert!(!reader_sees_eof(&reader));
        drop(fixed);
        assert!(reader_sees_eof(&reader));
        assert_eq!(table.available(), 2);
        drop(table);
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
    fn failed_unregister_is_retried_before_registration() {
        let driver = driver();
        let handle = driver.handle();
        let table = handle.register_fixed_files(1).unwrap();
        handle.test_with_ring_borrowed_mut(|| drop(table));
        assert!(
            handle.test_fixed_files_stale(),
            "a borrowed ring defers unregistration"
        );
        let table = handle.register_fixed_files(1).unwrap();
        assert_eq!(table.capacity(), 1);
    }
}
