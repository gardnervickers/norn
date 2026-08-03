use std::io;

use crate::park::{Park, ParkMode, Unpark};

/// A [`Park`] implementation which will
/// spin the CPU when there is no work to do.
#[derive(Debug)]
pub struct SpinPark;

#[derive(Debug, Clone, Copy)]
/// The no-op [`Unpark`] handle associated with [`SpinPark`].
pub struct NoopUnparker;

impl Unpark for NoopUnparker {
    fn unpark(&self) {}
}

impl Park for SpinPark {
    type Unparker = NoopUnparker;

    type Guard = ();

    fn park(&mut self, _: ParkMode) -> Result<(), io::Error> {
        Ok(())
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnparker
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}
