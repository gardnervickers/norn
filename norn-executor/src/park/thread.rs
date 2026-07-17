use std::sync::{Arc, Condvar, Mutex};

use crate::park::{Park, ParkMode, Unpark};

/// [`Park`] implementation that will park the
/// calling thread on a [`Condvar`] and wake it
/// when [`Unpark`] is called.
#[derive(Default)]
pub struct ThreadPark {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ThreadPark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPark").finish()
    }
}

#[derive(Clone)]
pub struct ThreadUnpark {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ThreadUnpark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadUnpark").finish()
    }
}

#[derive(Default)]
struct Inner {
    notified: Mutex<bool>,
    condvar: Condvar,
}

impl Unpark for ThreadUnpark {
    fn unpark(&self) {
        self.inner.unpark();
    }
}

impl Park for ThreadPark {
    type Unparker = ThreadUnpark;

    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> Result<(), std::io::Error> {
        self.inner.park(mode);
        Ok(())
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        ThreadUnpark {
            inner: Arc::clone(&self.inner),
        }
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}

impl Inner {
    fn unpark(&self) {
        let mut notified = self.notified.lock().unwrap();
        *notified = true;
        self.condvar.notify_one();
    }

    fn park(&self, mode: ParkMode) {
        match mode {
            ParkMode::NoPark => (),
            ParkMode::NextCompletion => {
                let notified = self.notified.lock().unwrap();
                let mut notified = self
                    .condvar
                    .wait_while(notified, |notified| !*notified)
                    .unwrap();
                *notified = false;
            }
            ParkMode::Timeout(timeout) => {
                let notified = self.notified.lock().unwrap();
                let (mut notified, _) = self
                    .condvar
                    .wait_timeout_while(notified, timeout, |notified| !*notified)
                    .unwrap();
                *notified = false;
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpark_before_park_is_observed() {
        let mut park = ThreadPark::default();
        park.unparker().unpark();
        park.park(ParkMode::NextCompletion).unwrap();
    }
}
