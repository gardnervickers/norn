use std::time::Duration;

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::LocalExecutor;

use crate::clock::Clock;
use crate::{Driver, Handle};

#[test]
fn smoke() {
    let clock = Clock::simulated();
    let park = FastPark(clock.clone());
    let timer = Driver::new(park, clock);
    let mut executor = LocalExecutor::new(timer);

    executor.block_on(async {
        let handle = Handle::current();
        let time = handle.clock().now();
        for _ in 0..5 {
            let sleep = handle.sleep(std::time::Duration::from_secs(1));
            sleep.await.unwrap();
        }
        let elapsed = handle.clock().now() - time;
        assert!(elapsed == std::time::Duration::from_secs(5));
    });
}

#[test]
fn zero_duration() {
    let clock = Clock::simulated();
    let park = FastPark(clock.clone());
    let timer = Driver::new(park, clock);
    let mut executor = LocalExecutor::new(timer);

    executor.block_on(async {
        let handle = Handle::current();
        let sleep = handle.sleep(Duration::from_millis(0));
        sleep.await.unwrap();
    });
}

struct FastPark(Clock);

#[derive(Debug, Clone, Copy)]
struct Unparker;

impl Unpark for Unparker {
    fn unpark(&self) {}
}

impl Park for FastPark {
    type Unparker = Unparker;

    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> Result<(), std::io::Error> {
        match mode {
            ParkMode::NoPark => Ok(()),
            ParkMode::NextCompletion => unimplemented!(),
            ParkMode::Timeout(duration) => {
                self.0.advance(duration / 2);
                Ok(())
            }
        }
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        Unparker
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}
