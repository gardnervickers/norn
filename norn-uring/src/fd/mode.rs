use std::rc::{Rc, Weak};

use super::Inner;

/// One or both independently coordinated socket I/O directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
    Both,
}

impl Direction {
    fn includes_read(self) -> bool {
        matches!(self, Self::Read | Self::Both)
    }

    fn includes_write(self) -> bool {
        matches!(self, Self::Write | Self::Both)
    }
}

/// The requested socket direction cannot enter the requested I/O mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModeConflict {
    direction: Direction,
}

impl ModeConflict {
    #[cfg(test)]
    pub(crate) fn direction(self) -> Direction {
        self.direction
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SideMode {
    ordinary: u32,
    bundled: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DirectionModes {
    read: SideMode,
    write: SideMode,
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the next stacked bundle API PR")
)]
impl DirectionModes {
    fn acquire_ordinary(&mut self, direction: Direction) -> Result<(), ModeConflict> {
        self.validate_ordinary(direction)?;
        if direction.includes_read() {
            self.read.ordinary = self
                .read
                .ordinary
                .checked_add(1)
                .expect("socket read permit count overflowed");
        }
        if direction.includes_write() {
            self.write.ordinary = self
                .write
                .ordinary
                .checked_add(1)
                .expect("socket write permit count overflowed");
        }
        Ok(())
    }

    fn validate_ordinary(self, direction: Direction) -> Result<(), ModeConflict> {
        if direction.includes_read() && self.read.bundled {
            return Err(ModeConflict {
                direction: Direction::Read,
            });
        }
        if direction.includes_write() && self.write.bundled {
            return Err(ModeConflict {
                direction: Direction::Write,
            });
        }
        Ok(())
    }

    fn acquire_bundled(&mut self, direction: Direction) -> Result<(), ModeConflict> {
        self.validate_bundled(direction)?;
        if direction.includes_read() {
            self.read.bundled = true;
        }
        if direction.includes_write() {
            self.write.bundled = true;
        }
        Ok(())
    }

    fn validate_bundled(self, direction: Direction) -> Result<(), ModeConflict> {
        if direction.includes_read() && (self.read.bundled || self.read.ordinary != 0) {
            return Err(ModeConflict {
                direction: Direction::Read,
            });
        }
        if direction.includes_write() && (self.write.bundled || self.write.ordinary != 0) {
            return Err(ModeConflict {
                direction: Direction::Write,
            });
        }
        Ok(())
    }

    fn release_ordinary(&mut self, direction: Direction) {
        if direction.includes_read() {
            assert!(!self.read.bundled);
            self.read.ordinary = self
                .read
                .ordinary
                .checked_sub(1)
                .expect("socket read permit count underflowed");
        }
        if direction.includes_write() {
            assert!(!self.write.bundled);
            self.write.ordinary = self
                .write
                .ordinary
                .checked_sub(1)
                .expect("socket write permit count underflowed");
        }
    }

    fn release_bundled(&mut self, direction: Direction) {
        if direction.includes_read() {
            assert!(self.read.bundled);
            assert_eq!(self.read.ordinary, 0);
            self.read.bundled = false;
        }
        if direction.includes_write() {
            assert!(self.write.bundled);
            assert_eq!(self.write.ordinary, 0);
            self.write.bundled = false;
        }
    }
}

/// A permit retained by an ordinary socket operation until its terminal CQE.
#[derive(Debug)]
pub(crate) struct OrdinaryPermit {
    inner: Weak<Inner>,
    direction: Direction,
    active: bool,
}

impl OrdinaryPermit {
    pub(super) fn acquire(inner: &Rc<Inner>, direction: Direction) -> Result<Self, ModeConflict> {
        let mut modes = inner.modes.get();
        modes.acquire_ordinary(direction)?;
        inner.modes.set(modes);
        Ok(Self {
            inner: Rc::downgrade(inner),
            direction,
            active: true,
        })
    }

    pub(crate) fn release(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut modes = inner.modes.get();
        modes.release_ordinary(self.direction);
        inner.modes.set(modes);
    }
}

impl Drop for OrdinaryPermit {
    fn drop(&mut self) {
        self.release();
    }
}

impl crate::operation::TerminalGuard for OrdinaryPermit {}

/// Exclusive ownership of socket directions while they are in bundled mode.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the next stacked bundle API PR")
)]
pub(crate) struct BundledPermit {
    lifetime: Rc<BundledPermitInner>,
}

#[derive(Debug)]
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the next stacked bundle API PR")
)]
struct BundledPermitInner {
    inner: Weak<Inner>,
    direction: Direction,
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "consumed by the next stacked bundle API PR")
)]
impl BundledPermit {
    pub(super) fn acquire(inner: &Rc<Inner>, direction: Direction) -> Result<Self, ModeConflict> {
        let mut modes = inner.modes.get();
        modes.acquire_bundled(direction)?;
        inner.modes.set(modes);
        Ok(Self {
            lifetime: Rc::new(BundledPermitInner {
                inner: Rc::downgrade(inner),
                direction,
            }),
        })
    }
}

impl Clone for BundledPermit {
    fn clone(&self) -> Self {
        Self {
            lifetime: Rc::clone(&self.lifetime),
        }
    }
}

impl crate::operation::TerminalGuard for BundledPermit {}

impl std::fmt::Debug for BundledPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundledPermit")
            .field("owners", &Rc::strong_count(&self.lifetime))
            .finish()
    }
}

impl Drop for BundledPermitInner {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut modes = inner.modes.get();
        modes.release_bundled(self.direction);
        inner.modes.set(modes);
    }
}
