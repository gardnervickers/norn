use std::io;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct SubmitError {
    kind: SubmitErrorKind,
}

impl SubmitError {
    pub(crate) fn shutting_down() -> Self {
        Self {
            kind: SubmitErrorKind::ShuttingDown,
        }
    }

    pub(crate) fn broken(err: io::Error) -> Self {
        Self {
            kind: SubmitErrorKind::Broken(err),
        }
    }

    pub(crate) fn batch_too_large(batch_len: usize, capacity: usize) -> Self {
        Self {
            kind: SubmitErrorKind::BatchTooLarge {
                batch_len,
                capacity,
            },
        }
    }

    pub(crate) fn to_io_error(&self) -> io::Error {
        match &self.kind {
            SubmitErrorKind::ShuttingDown => {
                io::Error::new(io::ErrorKind::Other, "reactor is shutting down")
            }
            SubmitErrorKind::Broken(err) => {
                io::Error::new(err.kind(), format!("reactor submit path failed: {err}"))
            }
            SubmitErrorKind::BatchTooLarge {
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

#[derive(Debug, thiserror::Error)]
enum SubmitErrorKind {
    #[error("reactor is shutting down")]
    ShuttingDown,
    #[error("reactor submit path failed: {0}")]
    Broken(#[source] io::Error),
    #[error("request batch of {batch_len} SQEs exceeds submission queue capacity {capacity}")]
    BatchTooLarge { batch_len: usize, capacity: usize },
}

impl From<SubmitError> for io::Error {
    fn from(value: SubmitError) -> Self {
        match value.kind {
            SubmitErrorKind::ShuttingDown => io::Error::new(io::ErrorKind::Other, value),
            SubmitErrorKind::Broken(err) => {
                io::Error::new(err.kind(), format!("reactor submit path failed: {err}"))
            }
            SubmitErrorKind::BatchTooLarge {
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
