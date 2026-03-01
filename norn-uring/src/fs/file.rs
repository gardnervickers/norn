use std::io;
use std::path::Path;
use std::pin::Pin;

use io_uring::types::FsyncFlags;
use io_uring::{opcode, types};

use crate::buf::{StableBuf, StableBufMut};
use crate::fd::{FdKind, NornFd};
use crate::fs::opts;
use crate::operation::{CQEResult, Operation, Singleshot};

/// A reference to an open file on the filesystem.
pub struct File {
    fd: NornFd,
    handle: crate::Handle,
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File").finish()
    }
}

impl File {
    /// Open a file with the specified options at the provided
    /// path.
    pub(crate) async fn open_with_options<P: AsRef<Path>>(
        path: P,
        opts: opts::OpenOptions,
    ) -> io::Result<Self> {
        let access_mode = opts.get_access_mode()?;
        let creation_mode = opts.get_creation_mode()?;
        let open = Open::new(path.as_ref(), access_mode, creation_mode)?;
        let handle = crate::Handle::current();
        let fd = handle.submit(open).await?;
        Ok(Self { fd, handle })
    }

    /// Open a file in read-only mode at the provided path.
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut opts = opts::OpenOptions::new();
        opts.read(true).open(path).await
    }

    /// Returns a new [`OpenOptions`] object which can be used to open a file.
    pub fn with_options() -> opts::OpenOptions {
        opts::OpenOptions::new()
    }

    /// Read bytes from the file into the specified buffer.
    ///
    /// The read will start at the provided offset.
    pub fn read_at<B>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBufMut + 'static,
    {
        let read = ReadAt::new(self.fd.clone(), buf, offset);
        self.handle.submit(read)
    }

    /// Write the specified buffer to the file.
    ///
    /// The write will start at the provided offset.
    pub fn write_at<B>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)>
    where
        B: StableBuf + 'static,
    {
        let write = WriteAt::new(self.fd.clone(), buf, offset);
        self.handle.submit(write)
    }

    /// Read bytes from the file into a set of buffers.
    ///
    /// The read starts at the provided offset and fills each buffer in order.
    pub async fn readv_at<B>(&self, bufs: Vec<B>, offset: u64) -> (io::Result<usize>, Vec<B>)
    where
        B: StableBufMut + 'static,
    {
        let read = ReadVectoredAt::new(self.fd.clone(), bufs, offset);
        self.handle.submit(read).await
    }

    /// Write bytes from a set of buffers to the file.
    ///
    /// The write starts at the provided offset and consumes each buffer in order.
    pub async fn writev_at<B>(&self, bufs: Vec<B>, offset: u64) -> (io::Result<usize>, Vec<B>)
    where
        B: StableBuf + 'static,
    {
        let write = WriteVectoredAt::new(self.fd.clone(), bufs, offset);
        self.handle.submit(write).await
    }

    /// Sync the file and metadata to disk.
    pub fn sync(&self) -> impl crate::Request<Output = io::Result<()>> {
        let flags = FsyncFlags::empty();
        let sync = Sync::new(self.fd.clone(), flags);
        self.handle.submit(sync)
    }

    /// Sync only the data in the file to disk.
    pub fn datasync(&self) -> impl crate::Request<Output = io::Result<()>> {
        let flags = FsyncFlags::DATASYNC;
        let sync = Sync::new(self.fd.clone(), flags);
        self.handle.submit(sync)
    }

    /// Sync a range of the file.
    pub fn sync_range(
        &self,
        offset: u64,
        len: u32,
        flags: u32,
    ) -> impl crate::Request<Output = io::Result<()>> {
        let sync = SyncRange::new(self.fd.clone(), offset, len, flags);
        self.handle.submit(sync)
    }

    /// Call `fallocate` on the file.
    pub fn fallocate(
        &self,
        offset: u64,
        len: u64,
        mode: i32,
    ) -> impl crate::Request<Output = io::Result<()>> {
        let fallocate = Fallocate::new(self.fd.clone(), offset, len, mode);
        self.handle.submit(fallocate)
    }

    /// Truncate or extend the underlying file, updating the file length.
    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        let truncate = Truncate::new(self.fd.clone(), len);
        self.handle.submit(truncate).await
    }

    /// Allocate additional space in the file without changing the file length metadata.
    ///
    /// This is akin to fallocate with `FALLOC_FL_ZERO_RANGE` and `FALLOC_FL_KEEP_SIZE` set.
    pub fn allocate(&self, offset: u64, len: u64) -> impl crate::Request<Output = io::Result<()>> {
        self.fallocate(
            offset,
            len,
            libc::FALLOC_FL_ZERO_RANGE | libc::FALLOC_FL_KEEP_SIZE,
        )
    }
    /// Punches a hole in the file at the specified offset range.
    ///
    /// This can be used to discard portions of a file to save space. The file size will not be
    /// changed.
    pub fn discard(&self, offset: u64, len: u64) -> impl crate::Request<Output = io::Result<()>> {
        self.fallocate(
            offset,
            len,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
        )
    }

    /// Close the file.
    pub async fn close(self) -> io::Result<()> {
        self.fd.close().await
    }
}

struct Open {
    path: std::ffi::CString,
    how: types::OpenHow,
}

impl Open {
    fn new(path: &Path, access_mode: i32, creation_mode: i32) -> io::Result<Self> {
        let path = path
            .to_str()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
        let path = std::ffi::CString::new(path)?;
        let flags = access_mode | creation_mode | libc::O_CLOEXEC;
        let how = types::OpenHow::new().flags(flags as u64);
        Ok(Self { path, how })
    }
}

impl Operation for Open {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        let ptr = this.path.as_ptr();
        opcode::OpenAt2::new(types::Fd(libc::AT_FDCWD), ptr, &this.how).build()
    }

    fn cleanup(&mut self, result: CQEResult) {
        if let Ok(res) = result.result {
            drop(NornFd::from_fd(res as _));
        }
    }
}

impl Singleshot for Open {
    type Output = io::Result<NornFd>;

    fn complete(self, result: CQEResult) -> Self::Output {
        let res = result.result?;
        Ok(NornFd::from_fd(res as _))
    }
}

#[derive(Debug)]
struct ReadAt<B> {
    fd: NornFd,
    buf: B,
    offset: u64,
}

impl<B> ReadAt<B> {
    fn new(fd: NornFd, buf: B, offset: u64) -> Self {
        Self { fd, buf, offset }
    }
}

impl<B> Operation for ReadAt<B>
where
    B: StableBufMut,
{
    fn configure(mut self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let buf = self.buf.stable_ptr_mut();
        let len = self.buf.bytes_remaining();
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::Read::new(*fd, buf, len as _),
            FdKind::Fixed(fd) => opcode::Read::new(*fd, buf, len as _),
        }
        .offset(self.offset)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl<B> Singleshot for ReadAt<B>
where
    B: StableBufMut,
{
    type Output = (io::Result<usize>, B);

    fn complete(mut self, result: CQEResult) -> Self::Output {
        match result.result {
            Ok(n) => {
                let n = n as usize;
                unsafe {
                    self.buf.set_init(n);
                }
                (Ok(n), self.buf)
            }
            Err(err) => {
                let buf = self.buf;
                (Err(err), buf)
            }
        }
    }
}

struct WriteAt<B> {
    fd: NornFd,
    buf: B,
    offset: u64,
}

impl<B> WriteAt<B> {
    fn new(fd: NornFd, buf: B, offset: u64) -> Self {
        Self { fd, buf, offset }
    }
}

impl<B> Operation for WriteAt<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let buf = self.buf.stable_ptr();
        let len = self.buf.bytes_init();
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::Write::new(*fd, buf, len as _),
            FdKind::Fixed(fd) => opcode::Write::new(*fd, buf, len as _),
        }
        .offset(self.offset)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl<B> Singleshot for WriteAt<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, B);

    fn complete(self, result: CQEResult) -> Self::Output {
        match result.result {
            Ok(n) => (Ok(n as usize), self.buf),
            Err(err) => (Err(err), self.buf),
        }
    }
}

struct ReadVectoredAt<B> {
    fd: NornFd,
    bufs: Vec<B>,
    iovecs: Vec<libc::iovec>,
    offset: u64,
}

impl<B> ReadVectoredAt<B> {
    fn new(fd: NornFd, bufs: Vec<B>, offset: u64) -> Self {
        Self {
            fd,
            bufs,
            iovecs: Vec::new(),
            offset,
        }
    }
}

impl<B> Operation for ReadVectoredAt<B>
where
    B: StableBufMut,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        this.iovecs.clear();
        this.iovecs.reserve(this.bufs.len());
        for buf in &mut this.bufs {
            this.iovecs.push(libc::iovec {
                iov_base: buf.stable_ptr_mut() as *mut libc::c_void,
                iov_len: buf.bytes_remaining(),
            });
        }

        let ptr = this.iovecs.as_ptr();
        let len = this.iovecs.len() as u32;
        match this.fd.kind() {
            FdKind::Fd(fd) => opcode::Readv::new(*fd, ptr, len),
            FdKind::Fixed(fd) => opcode::Readv::new(*fd, ptr, len),
        }
        .offset(this.offset)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl<B> Singleshot for ReadVectoredAt<B>
where
    B: StableBufMut,
{
    type Output = (io::Result<usize>, Vec<B>);

    fn complete(mut self, result: CQEResult) -> Self::Output {
        match result.result {
            Ok(n) => {
                let mut remaining = n as usize;
                for buf in &mut self.bufs {
                    let init = remaining.min(buf.bytes_remaining());
                    unsafe {
                        buf.set_init(init);
                    }
                    remaining = remaining.saturating_sub(init);
                    if remaining == 0 {
                        break;
                    }
                }
                (Ok(n as usize), self.bufs)
            }
            Err(err) => (Err(err), self.bufs),
        }
    }
}

struct WriteVectoredAt<B> {
    fd: NornFd,
    bufs: Vec<B>,
    iovecs: Vec<libc::iovec>,
    offset: u64,
}

impl<B> WriteVectoredAt<B> {
    fn new(fd: NornFd, bufs: Vec<B>, offset: u64) -> Self {
        Self {
            fd,
            bufs,
            iovecs: Vec::new(),
            offset,
        }
    }
}

impl<B> Operation for WriteVectoredAt<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        this.iovecs.clear();
        this.iovecs.reserve(this.bufs.len());
        for buf in &this.bufs {
            this.iovecs.push(libc::iovec {
                iov_base: buf.stable_ptr() as *mut libc::c_void,
                iov_len: buf.bytes_init(),
            });
        }

        let ptr = this.iovecs.as_ptr();
        let len = this.iovecs.len() as u32;
        match this.fd.kind() {
            FdKind::Fd(fd) => opcode::Writev::new(*fd, ptr, len),
            FdKind::Fixed(fd) => opcode::Writev::new(*fd, ptr, len),
        }
        .offset(this.offset)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl<B> Singleshot for WriteVectoredAt<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, Vec<B>);

    fn complete(self, result: CQEResult) -> Self::Output {
        match result.result {
            Ok(n) => (Ok(n as usize), self.bufs),
            Err(err) => (Err(err), self.bufs),
        }
    }
}

struct Sync {
    fd: NornFd,
    flags: FsyncFlags,
}

impl Sync {
    fn new(fd: NornFd, flags: FsyncFlags) -> Self {
        Self { fd, flags }
    }
}

impl Operation for Sync {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::Fsync::new(*fd),
            FdKind::Fixed(fd) => opcode::Fsync::new(*fd),
        }
        .flags(self.flags)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for Sync {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}
struct SyncRange {
    fd: NornFd,
    offset: u64,
    len: u32,
    flags: u32,
}

impl SyncRange {
    fn new(fd: NornFd, offset: u64, len: u32, flags: u32) -> Self {
        Self {
            fd,
            offset,
            len,
            flags,
        }
    }
}

impl Operation for SyncRange {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::SyncFileRange::new(*fd, self.len),
            FdKind::Fixed(fd) => opcode::SyncFileRange::new(*fd, self.len),
        }
        .offset(self.offset)
        .flags(self.flags)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for SyncRange {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct Fallocate {
    fd: NornFd,
    offset: u64,
    len: u64,
    mode: i32,
}

impl Fallocate {
    fn new(fd: NornFd, offset: u64, len: u64, mode: i32) -> Self {
        Self {
            fd,
            offset,
            len,
            mode,
        }
    }
}

impl Operation for Fallocate {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::Fallocate::new(*fd, self.len),
            FdKind::Fixed(fd) => opcode::Fallocate::new(*fd, self.len),
        }
        .offset(self.offset)
        .mode(self.mode)
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for Fallocate {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct Truncate {
    fd: NornFd,
    len: u64,
}

impl Truncate {
    fn new(fd: NornFd, len: u64) -> Self {
        Self { fd, len }
    }
}

impl Operation for Truncate {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        match self.fd.kind() {
            FdKind::Fd(fd) => opcode::Ftruncate::new(*fd, self.len),
            FdKind::Fixed(fd) => opcode::Ftruncate::new(*fd, self.len),
        }
        .build()
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for Truncate {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}
