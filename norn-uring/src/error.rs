use std::io;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("reactor is shutting down")]
    ShuttingDown,
    #[error("reactor submit path failed: {0}")]
    Broken(#[source] io::Error),
    #[error("request batch of {batch_len} SQEs exceeds submission queue capacity {capacity}")]
    BatchTooLarge { batch_len: usize, capacity: usize },
}

impl SubmitError {
    pub(crate) fn shutting_down() -> Self {
        Self::ShuttingDown
    }

    pub(crate) fn broken(err: io::Error) -> Self {
        Self::Broken(err)
    }

    pub(crate) fn batch_too_large(batch_len: usize, capacity: usize) -> Self {
        Self::BatchTooLarge {
            batch_len,
            capacity,
        }
    }

    pub(crate) fn to_io_error(&self) -> io::Error {
        match self {
            Self::ShuttingDown => io::Error::other("reactor is shutting down"),
            Self::Broken(err) => {
                io::Error::new(err.kind(), format!("reactor submit path failed: {err}"))
            }
            Self::BatchTooLarge {
                batch_len,
                capacity,
            } => io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "request batch of {batch_len} SQEs exceeds submission queue capacity {capacity}"
                ),
            ),
        }
    }
}

impl From<SubmitError> for io::Error {
    fn from(value: SubmitError) -> Self {
        match value {
            SubmitError::ShuttingDown => io::Error::other(SubmitError::ShuttingDown),
            SubmitError::Broken(err) => {
                io::Error::new(err.kind(), format!("reactor submit path failed: {err}"))
            }
            SubmitError::BatchTooLarge {
                batch_len,
                capacity,
            } => io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "request batch of {batch_len} SQEs exceeds submission queue capacity {capacity}"
                ),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_errors_preserve_io_error_kinds_and_messages() {
        let shutting_down = SubmitError::shutting_down().to_io_error();
        assert_eq!(shutting_down.kind(), io::ErrorKind::Other);
        assert_eq!(shutting_down.to_string(), "reactor is shutting down");

        let broken = SubmitError::broken(io::Error::new(io::ErrorKind::TimedOut, "timed out"));
        let broken = broken.to_io_error();
        assert_eq!(broken.kind(), io::ErrorKind::TimedOut);
        assert_eq!(broken.to_string(), "reactor submit path failed: timed out");

        let batch = SubmitError::batch_too_large(9, 8).to_io_error();
        assert_eq!(batch.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            batch.to_string(),
            "request batch of 9 SQEs exceeds submission queue capacity 8"
        );

        let converted: io::Error = SubmitError::shutting_down().into();
        assert_eq!(converted.kind(), io::ErrorKind::Other);
        assert_eq!(converted.to_string(), "reactor is shutting down");
    }
}
