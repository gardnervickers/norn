/// Error returned from timer operations.
#[derive(thiserror::Error, Debug, Clone, Copy)]
#[error(transparent)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    pub(super) fn shutdown() -> Self {
        Self {
            kind: ErrorKind::Shutdown,
        }
    }
}

/// The kind of timer error.
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum ErrorKind {
    /// The timer driver shut down before the timer could complete.
    #[error("the timer has shut down")]
    Shutdown,
}
