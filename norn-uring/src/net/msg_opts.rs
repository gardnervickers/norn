//! Per-operation options for socket receives.

/// Per-operation options for a socket receive.
///
/// Set them on a receive with [`WithOptions::options`](crate::WithOptions::options).
/// The default options submit the same request as a receive without options.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecvOptions {
    pub(crate) flags: i32,
}

impl RecvOptions {
    /// Create receive options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self { flags: 0 }
    }

    /// Configure whether the receive waits until the whole buffer is filled
    /// (`MSG_WAITALL`).
    #[must_use]
    pub const fn wait_all(self, enabled: bool) -> Self {
        self.with_flag(libc::MSG_WAITALL, enabled)
    }

    /// Configure whether the receive leaves the data queued (`MSG_PEEK`).
    #[must_use]
    pub const fn peek(self, enabled: bool) -> Self {
        self.with_flag(libc::MSG_PEEK, enabled)
    }

    const fn with_flag(mut self, flag: i32, enabled: bool) -> Self {
        self.flags = if enabled {
            self.flags | flag
        } else {
            self.flags & !flag
        };
        self
    }
}
