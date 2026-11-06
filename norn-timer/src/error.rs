/// Error returned from timer operations.
#[derive(thiserror::Error, Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// The timer driver shut down before the timer could complete.
    #[error("the timer has shut down")]
    Shutdown,
}

impl From<Error> for std::io::Error {
    /// Convert into an [`std::io::Error`] of kind
    /// [`Other`](std::io::ErrorKind::Other), so `?` works on timer results in
    /// functions that return [`std::io::Result`]. The original error is kept
    /// as the source.
    fn from(err: Error) -> Self {
        std::io::Error::other(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleep_like() -> std::io::Result<()> {
        Err(Error::Shutdown)?;
        Ok(())
    }

    #[test]
    fn converts_into_io_error_with_source() {
        let err = sleep_like().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        let inner = err.get_ref().and_then(|e| e.downcast_ref::<Error>());
        assert_eq!(inner, Some(&Error::Shutdown));
    }
}
