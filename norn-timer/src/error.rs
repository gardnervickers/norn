/// Error returned from timer operations.
#[derive(thiserror::Error, Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// The timer driver shut down before the timer could complete.
    #[error("the timer has shut down")]
    Shutdown,
}
