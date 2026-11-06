//! The result of an operation that owns a buffer.

use std::{fmt, io};

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
/// needed. Both drop the buffer on failure:
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
pub struct BufResult<T, B>(pub io::Result<T>, pub B);

impl<T, B> BufResult<T, B> {
    /// Return the value and the buffer on success.
    ///
    /// On failure the buffer is dropped, so `?` releases it right away, as
    /// it would for any other `io::Result`. Destructure the `BufResult`
    /// instead when the buffer is needed after a failure.
    ///
    /// # Errors
    ///
    /// Returns the operation's error if it failed.
    pub fn buf_ok(self) -> io::Result<(T, B)> {
        let BufResult(result, buf) = self;
        result.map(|value| (value, buf))
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

// The buffer is left out, so `BufResult` is `Debug` for any buffer type and
// does not print the buffer's contents.
impl<T: fmt::Debug, B> fmt::Debug for BufResult<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BufResult")
            .field(&self.0)
            .finish_non_exhaustive()
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
    fn question_mark_keeps_the_error() {
        let err = propagate(BufResult(
            Err(io::Error::from_raw_os_error(libc::ECONNRESET)),
            vec![1],
        ))
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(err.raw_os_error(), Some(libc::ECONNRESET));
    }

    #[test]
    fn buf_ok_drops_the_buffer_on_failure() {
        let buf = std::rc::Rc::new(());
        let result = BufResult::<(), _>(Err(io::Error::other("failed")), buf.clone()).buf_ok();
        assert!(result.is_err());
        assert_eq!(std::rc::Rc::strong_count(&buf), 1);
    }

    #[test]
    fn destructuring_recovers_the_buffer_on_failure() {
        let BufResult(res, buf) = BufResult::<(), _>(Err(io::Error::other("failed")), vec![7]);
        assert!(res.is_err());
        assert_eq!(buf, [7]);
    }

    #[test]
    fn drop_buf_returns_the_result() {
        assert_eq!(BufResult(Ok(5), vec![0u8]).drop_buf().unwrap(), 5);
    }

    #[test]
    fn debug_omits_the_buffer() {
        struct NotDebug;
        let shown = format!("{:?}", BufResult(Ok::<_, io::Error>(1), NotDebug));
        assert_eq!(shown, "BufResult(Ok(1), ..)");
    }
}
