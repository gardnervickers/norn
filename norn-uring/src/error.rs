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
}

#[derive(Debug, thiserror::Error)]
enum SubmitErrorKind {
    #[error("reactor is shutting down")]
    ShuttingDown,
    #[error("reactor submit path failed: {0}")]
    Broken(#[source] io::Error),
}

impl From<SubmitError> for io::Error {
    fn from(value: SubmitError) -> Self {
        match value.kind {
            SubmitErrorKind::ShuttingDown => io::Error::new(io::ErrorKind::Other, value),
            SubmitErrorKind::Broken(err) => {
                io::Error::new(err.kind(), format!("reactor submit path failed: {err}"))
            }
        }
    }
}
