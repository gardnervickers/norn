//! Per-operation options for positional file reads and writes.
//!
//! These types expose the subset of `preadv2(2)`/`pwritev2(2)` flags whose
//! every accepted value preserves Norn's positional `*_at` semantics and buffer
//! ownership. Kernels that do not recognize a requested flag reject the
//! operation instead of ignoring it.

use std::num::NonZeroU8;

// `RWF_DONTCACHE` from `include/uapi/linux/fs.h` (Linux 6.14).
const RWF_DONTCACHE: i32 = 0x0000_0080;

/// Per-operation options for positional file reads.
///
/// Set them on a request from [`File::read_at`], [`File::read_fixed_at`], or
/// [`File::readv_at`] with [`WithOptions::options`]. The default options
/// submit the same request as not setting any.
///
/// ```no_run
/// # async fn f(file: &norn_uring::fs::File) {
/// use norn_uring::{fs::ReadOptions, WithOptions};
///
/// let cached_only = ReadOptions::new().nowait(true);
/// let (res, buf) = file.read_at(vec![0; 4096], 0).options(cached_only).await;
/// # }
/// ```
///
/// [`File::read_at`]: crate::fs::File::read_at
/// [`File::read_fixed_at`]: crate::fs::File::read_fixed_at
/// [`File::readv_at`]: crate::fs::File::readv_at
/// [`WithOptions::options`]: crate::WithOptions::options
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ReadOptions {
    rw_flags: i32,
}

impl ReadOptions {
    /// Create read options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self { rw_flags: 0 }
    }

    /// Configure whether the read fails instead of waiting for data.
    ///
    /// This sets `RWF_NOWAIT`. A read that would block completes with
    /// [`io::ErrorKind::WouldBlock`] and is not retried by the kernel. A read
    /// that can only partly complete without blocking may return fewer bytes
    /// than requested. Files that do not support nonblocking reads fail with
    /// `EOPNOTSUPP`.
    ///
    /// [`io::ErrorKind::WouldBlock`]: std::io::ErrorKind::WouldBlock
    #[must_use]
    pub const fn nowait(self, enabled: bool) -> Self {
        self.with_flag(libc::RWF_NOWAIT, enabled)
    }

    /// Configure whether buffered page-cache pages are dropped after the read.
    ///
    /// This sets `RWF_DONTCACHE`. It requires Linux 6.14 or newer and a
    /// filesystem that supports uncached buffered I/O; otherwise the read fails
    /// with `EOPNOTSUPP`.
    #[must_use]
    pub const fn dontcache(self, enabled: bool) -> Self {
        self.with_flag(RWF_DONTCACHE, enabled)
    }

    const fn with_flag(mut self, flag: i32, enabled: bool) -> Self {
        self.rw_flags = set_flag(self.rw_flags, flag, enabled);
        self
    }
}

/// Per-operation options for positional file writes.
///
/// Set them on a request from [`File::write_at`], [`File::write_fixed_at`],
/// or [`File::writev_at`] with [`WithOptions::options`]. The default options
/// submit the same request as not setting any. Options are `Copy` and can be
/// built in a `const`:
///
/// ```no_run
/// # async fn f(file: &norn_uring::fs::File) {
/// use norn_uring::{fs::WriteOptions, WithOptions};
///
/// const DURABLE: WriteOptions = WriteOptions::new().dsync(true);
/// let (res, buf) = file.write_at(b"record".to_vec(), 0).options(DURABLE).await;
/// # }
/// ```
///
/// [`File::write_at`]: crate::fs::File::write_at
/// [`File::write_fixed_at`]: crate::fs::File::write_fixed_at
/// [`File::writev_at`]: crate::fs::File::writev_at
/// [`WithOptions::options`]: crate::WithOptions::options
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct WriteOptions {
    rw_flags: i32,
    stream: Option<WriteStream>,
}

impl WriteOptions {
    /// Create write options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self {
            rw_flags: 0,
            stream: None,
        }
    }

    /// Configure whether the write has `O_DSYNC` semantics.
    ///
    /// This sets `RWF_DSYNC`. A successful completion means the written data,
    /// and the metadata required to retrieve it, reached stable storage.
    #[must_use]
    pub const fn dsync(self, enabled: bool) -> Self {
        self.with_flag(libc::RWF_DSYNC, enabled)
    }

    /// Configure whether the write has `O_SYNC` semantics.
    ///
    /// This sets `RWF_SYNC`. A successful completion means the written data
    /// and all associated file metadata reached stable storage.
    #[must_use]
    pub const fn sync(self, enabled: bool) -> Self {
        self.with_flag(libc::RWF_SYNC, enabled)
    }

    /// Configure whether the write fails instead of waiting.
    ///
    /// This sets `RWF_NOWAIT`. A write that would block completes with
    /// [`io::ErrorKind::WouldBlock`] and is not retried by the kernel. A write
    /// that can only partly complete without blocking may return fewer bytes
    /// than requested; `io_uring` still counts such a write as failed, so a
    /// request linked after it with [`Request::then`](crate::Request::then) is
    /// canceled. Files that do not support nonblocking writes fail with
    /// `EOPNOTSUPP`. A buffered write to a filesystem that cannot attempt one
    /// without blocking, such as ext4, fails with `WouldBlock` every time, so
    /// retrying it does not help.
    ///
    /// [`io::ErrorKind::WouldBlock`]: std::io::ErrorKind::WouldBlock
    #[must_use]
    pub const fn nowait(self, enabled: bool) -> Self {
        self.with_flag(libc::RWF_NOWAIT, enabled)
    }

    /// Configure whether buffered page-cache pages are dropped after writeback.
    ///
    /// This sets `RWF_DONTCACHE`. It requires Linux 6.14 or newer and a
    /// filesystem that supports uncached buffered I/O; otherwise the write
    /// fails with `EOPNOTSUPP`.
    #[must_use]
    pub const fn dontcache(self, enabled: bool) -> Self {
        self.with_flag(RWF_DONTCACHE, enabled)
    }

    /// Configure the write stream used for data placement, or `None` for the
    /// default placement.
    ///
    /// Write streams are a placement hint, such as `NVMe` flexible data
    /// placement; they never change which bytes are written or where in the
    /// file they land. See [`WriteStream`] for which targets honor them.
    #[must_use]
    pub const fn stream(mut self, stream: Option<WriteStream>) -> Self {
        self.stream = stream;
        self
    }

    const fn with_flag(mut self, flag: i32, enabled: bool) -> Self {
        self.rw_flags = set_flag(self.rw_flags, flag, enabled);
        self
    }
}

/// A nonzero write stream identifier.
///
/// Linux 6.16 added per-I/O write streams. Only direct (`O_DIRECT`) writes to
/// block devices consume them; such a write fails with `EINVAL` when the
/// stream exceeds the device's `queue/max_write_streams` limit, including on
/// devices that advertise no streams. Kernels older than 6.16, buffered
/// writes, and regular files ignore the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WriteStream(NonZeroU8);

impl WriteStream {
    /// Create a write stream identifier, or `None` when `id` is zero.
    ///
    /// Stream zero means "no stream"; use [`WriteOptions::stream`] with `None`
    /// to request default placement.
    pub const fn new(id: u8) -> Option<Self> {
        match NonZeroU8::new(id) {
            Some(id) => Some(Self(id)),
            None => None,
        }
    }

    /// Return the stream identifier.
    pub const fn get(self) -> u8 {
        self.0.get()
    }
}

const fn set_flag(flags: i32, flag: i32, enabled: bool) -> i32 {
    if enabled {
        flags | flag
    } else {
        flags & !flag
    }
}

impl ReadOptions {
    /// The `rw_flags` SQE field.
    pub(crate) const fn rw_flags(&self) -> i32 {
        self.rw_flags
    }

    #[cfg(test)]
    const fn write_stream(&self) -> u8 {
        0
    }
}

impl WriteOptions {
    /// The `rw_flags` SQE field.
    pub(crate) const fn rw_flags(&self) -> i32 {
        self.rw_flags
    }

    /// The `write_stream` SQE field; zero selects default placement.
    pub(crate) const fn write_stream(&self) -> u8 {
        match self.stream {
            Some(stream) => stream.get(),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_encode_no_flags_or_stream() {
        assert_eq!(ReadOptions::new(), ReadOptions::default());
        assert_eq!(WriteOptions::new(), WriteOptions::default());
        assert_eq!(ReadOptions::new().rw_flags(), 0);
        assert_eq!(WriteOptions::new().rw_flags(), 0);
        assert_eq!(WriteOptions::new().write_stream(), 0);
    }

    #[test]
    fn read_options_set_and_clear_flags() {
        let opts = ReadOptions::new().nowait(true).dontcache(true);
        assert_eq!(opts.rw_flags(), libc::RWF_NOWAIT | RWF_DONTCACHE);
        assert_eq!(opts.write_stream(), 0);

        let opts = opts.nowait(false);
        assert_eq!(opts.rw_flags(), RWF_DONTCACHE);
    }

    #[test]
    fn write_options_set_and_clear_flags() {
        let opts = WriteOptions::new()
            .dsync(true)
            .sync(true)
            .nowait(true)
            .dontcache(true);
        assert_eq!(
            opts.rw_flags(),
            libc::RWF_DSYNC | libc::RWF_SYNC | libc::RWF_NOWAIT | RWF_DONTCACHE
        );

        let opts = opts.sync(false).nowait(false);
        assert_eq!(opts.rw_flags(), libc::RWF_DSYNC | RWF_DONTCACHE);
    }

    #[test]
    fn write_stream_zero_means_no_stream() {
        assert_eq!(WriteStream::new(0), None);
        assert_eq!(WriteStream::new(255).map(WriteStream::get), Some(255));

        let opts = WriteOptions::new().stream(WriteStream::new(7));
        assert_eq!(opts.write_stream(), 7);
        assert_eq!(opts.stream(None).write_stream(), 0);
    }
}
