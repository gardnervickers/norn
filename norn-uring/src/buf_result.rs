//! The result of an operation that owns a buffer.

use std::{error, fmt, io};

/// The output of an operation that takes ownership of a buffer.
///
/// The operation hands the buffer back with its result, whether it
/// succeeded or not, so the caller can reuse the buffer either way.
/// Destructure it when the buffer matters on both paths:
///
/// ```no_run
/// # async fn f(file: &norn_uring::fs::File) {
/// use norn_uring::BufResult;
///
/// let BufResult(res, buf) = file.read_at(vec![0; 4096], 0).await;
/// # }
/// ```
///
/// Use [`BufResult::buf_ok`] to propagate errors with `?` while keeping the
/// buffer on success, or [`BufResult::drop_buf`] when the buffer is no longer
/// needed:
///
/// ```no_run
/// # async fn f(
/// #     socket: &norn_uring::net::TcpSocket,
/// #     file: &norn_uring::fs::File,
/// # ) -> std::io::Result<()> {
/// let (n, mut buf) = socket.recv(vec![0; 4096]).await.buf_ok()?;
/// buf.truncate(n);
/// file.write_at(buf, 0).await.drop_buf()?;
/// # Ok(())
/// # }
/// ```
#[must_use = "this `BufResult` may hold an error, and holds the operation's buffer"]
#[derive(Debug)]
pub struct BufResult<T, B>(pub io::Result<T>, pub B);

impl<T, B> BufResult<T, B> {
    /// Convert into a `Result` that carries the buffer on both paths.
    ///
    /// The error converts into [`io::Error`], so `?` works in functions that
    /// return [`io::Result`]. Match on [`BufError`] to recover the buffer
    /// from a failure.
    ///
    /// # Errors
    ///
    /// Returns the operation's error, with the buffer, if it failed.
    pub fn buf_ok(self) -> Result<(T, B), BufError<B>> {
        match self.0 {
            Ok(value) => Ok((value, self.1)),
            Err(error) => Err(BufError { error, buf: self.1 }),
        }
    }

    /// Drop the buffer and return the operation's result.
    ///
    /// # Errors
    ///
    /// Returns the operation's error if it failed.
    pub fn drop_buf(self) -> io::Result<T> {
        self.0
    }

    /// Split into the operation's result and the buffer.
    pub fn into_parts(self) -> (io::Result<T>, B) {
        (self.0, self.1)
    }

    /// Map the success value, keeping the buffer.
    pub fn map<U, F>(self, f: F) -> BufResult<U, B>
    where
        F: FnOnce(T) -> U,
    {
        BufResult(self.0.map(f), self.1)
    }

    /// Map the buffer, keeping the result.
    pub fn map_buf<C, F>(self, f: F) -> BufResult<T, C>
    where
        F: FnOnce(B) -> C,
    {
        BufResult(self.0, f(self.1))
    }
}

impl<T, B> From<BufResult<T, B>> for (io::Result<T>, B) {
    fn from(result: BufResult<T, B>) -> Self {
        result.into_parts()
    }
}

impl<T, B> From<(io::Result<T>, B)> for BufResult<T, B> {
    fn from((result, buf): (io::Result<T>, B)) -> Self {
        BufResult(result, buf)
    }
}

/// An operation's error, together with the buffer the operation returned.
///
/// Produced by [`BufResult::buf_ok`]. It converts into [`io::Error`] by
/// dropping the buffer, so `?` propagates it from functions that return
/// [`io::Result`].
pub struct BufError<B> {
    error: io::Error,
    buf: B,
}

impl<B> BufError<B> {
    /// Return the error.
    pub fn error(&self) -> &io::Error {
        &self.error
    }

    /// Return the buffer.
    pub fn buf(&self) -> &B {
        &self.buf
    }

    /// Return a mutable reference to the buffer.
    pub fn buf_mut(&mut self) -> &mut B {
        &mut self.buf
    }

    /// Drop the buffer and return the error.
    pub fn into_error(self) -> io::Error {
        self.error
    }

    /// Drop the error and return the buffer.
    pub fn into_buf(self) -> B {
        self.buf
    }

    /// Split into the error and the buffer.
    pub fn into_parts(self) -> (io::Error, B) {
        (self.error, self.buf)
    }
}

impl<B> From<BufError<B>> for io::Error {
    fn from(error: BufError<B>) -> Self {
        error.error
    }
}

// The buffer is left out so that `BufError` is `Debug` and `Error` for any
// buffer type, and so that a failure does not print the buffer's contents.
impl<B> fmt::Debug for BufError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<B> fmt::Display for BufError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl<B> error::Error for BufError<B> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn propagate(result: BufResult<usize, Vec<u8>>) -> io::Result<(usize, Vec<u8>)> {
        let (n, buf) = result.buf_ok()?;
        Ok((n, buf))
    }

    #[test]
    fn buf_ok_keeps_the_buffer_on_success() {
        let (n, buf) = propagate(BufResult(Ok(3), vec![1, 2, 3])).unwrap();
        assert_eq!(n, 3);
        assert_eq!(buf, [1, 2, 3]);
    }

    #[test]
    fn question_mark_converts_to_io_error() {
        let err = propagate(BufResult(
            Err(io::Error::from(io::ErrorKind::ConnectionReset)),
            vec![1],
        ))
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
    }

    #[test]
    fn buf_error_returns_the_buffer() {
        let err = BufResult::<(), _>(Err(io::Error::from_raw_os_error(libc::EAGAIN)), vec![7])
            .buf_ok()
            .unwrap_err();
        assert_eq!(err.error().raw_os_error(), Some(libc::EAGAIN));
        let (error, buf) = err.into_parts();
        assert_eq!(error.raw_os_error(), Some(libc::EAGAIN));
        assert_eq!(buf, [7]);
    }

    #[test]
    fn drop_buf_returns_the_result() {
        assert_eq!(BufResult(Ok(5), vec![0u8]).drop_buf().unwrap(), 5);
    }
}
