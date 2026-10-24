use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_timer::{Clock, Driver, Error, Sleep};
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

#[derive(Debug)]
struct RecordPark(Rc<RefCell<Vec<ParkMode>>>);

impl Park for RecordPark {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> std::io::Result<()> {
        self.0.borrow_mut().push(mode);
        Ok(())
    }

    fn enter(&self) {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnpark
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}

#[derive(Clone, Debug)]
enum Op {
    Sleep { micros: u64, poll: bool },
    SleepUntil { micros: u64, poll: bool },
    Advance { micros: u64, park: bool },
    Park,
    Poll(usize),
    Drop(usize),
    Reset(usize),
}

/// Spans sub-millisecond offsets and every wheel level, including deadlines
/// beyond one top-level turn.
fn micros() -> impl Strategy<Value = u64> {
    const MILLIS: u64 = 1_000;
    prop_oneof![
        Just(0),
        1..MILLIS,
        1..64 * MILLIS,
        1..(1 << 12) * MILLIS,
        1..(1 << 18) * MILLIS,
        1..(1 << 24) * MILLIS,
        1..(1 << 30) * MILLIS,
        1..(1 << 38) * MILLIS,
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (micros(), any::<bool>()).prop_map(|(micros, poll)| Op::Sleep { micros, poll }),
        1 => (micros(), any::<bool>()).prop_map(|(micros, poll)| Op::SleepUntil { micros, poll }),
        3 => (micros(), any::<bool>()).prop_map(|(micros, park)| Op::Advance { micros, park }),
        2 => Just(Op::Park),
        1 => any::<usize>().prop_map(Op::Poll),
        1 => any::<usize>().prop_map(Op::Drop),
        1 => any::<usize>().prop_map(Op::Reset),
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Unregistered,
    Registered,
    Fired,
}

struct Modeled {
    sleep: Pin<Box<Sleep>>,
    relative: Option<u64>,
    deadline: u64,
    polled: bool,
    state: State,
}

impl Modeled {
    fn new(sleep: Sleep, relative: Option<u64>, deadline: u64, polled: bool) -> Self {
        Self {
            sleep: Box::pin(sleep),
            relative,
            deadline,
            polled,
            state: State::Unregistered,
        }
    }
}

fn tick(now: u64) -> u64 {
    now / 1_000
}

fn relative_deadline(now: u64, micros: u64) -> u64 {
    if micros == 0 {
        tick(now)
    } else {
        (now + micros).div_ceil(1_000)
    }
}

fn absolute_deadline(target: u64, now: u64) -> u64 {
    if target <= now {
        0
    } else {
        target.div_ceil(1_000)
    }
}

/// Fire registered sleeps due at `now`, returning whether any fired.
fn fire_due(sleeps: &mut [Modeled], now: u64) -> bool {
    let mut fired = false;
    for modeled in sleeps {
        if modeled.state == State::Registered && modeled.deadline <= tick(now) {
            modeled.state = State::Fired;
            fired = true;
        }
    }
    fired
}

proptest! {
    #[cfg_attr(miri, ignore = "proptest is prohibitively slow under Miri")]
    #[test]
    fn timers_fire_and_park_like_a_sorted_model(ops in prop::collection::vec(op(), 1..200)) {
        let clock = Clock::simulated();
        let modes = Rc::new(RefCell::new(Vec::new()));
        let mut driver = Driver::new(RecordPark(Rc::clone(&modes)), clock.clone());
        let handle = driver.handle();
        let mut cx = Context::from_waker(Waker::noop());
        let mut now = 0_u64;
        let mut sleeps: Vec<Modeled> = Vec::new();

        for op in ops {
            match op {
                Op::Sleep { micros, poll } => sleeps.push(Modeled::new(
                    handle.sleep(Duration::from_micros(micros)),
                    Some(micros),
                    relative_deadline(now, micros),
                    poll,
                )),
                Op::SleepUntil { micros, poll } => sleeps.push(Modeled::new(
                    handle.sleep_until(clock.now() + Duration::from_micros(micros)),
                    None,
                    absolute_deadline(now + micros, now),
                    poll,
                )),
                Op::Advance { micros, park } => {
                    clock.advance(Duration::from_micros(micros));
                    now += micros;
                    if park {
                        driver.park(ParkMode::NoPark).unwrap();
                        fire_due(&mut sleeps, now);
                        prop_assert_eq!(modes.borrow().last().copied(), Some(ParkMode::NoPark));
                    }
                }
                Op::Park => {
                    driver.park(ParkMode::NextCompletion).unwrap();
                    let expected = if fire_due(&mut sleeps, now) {
                        ParkMode::NoPark
                    } else {
                        sleeps
                            .iter()
                            .filter(|modeled| modeled.state == State::Registered)
                            .map(|modeled| modeled.deadline - tick(now))
                            .min()
                            .map_or(ParkMode::NextCompletion, |delta| {
                                ParkMode::Timeout(Duration::from_millis(delta))
                            })
                    };
                    prop_assert_eq!(modes.borrow().last().copied(), Some(expected), "now={}us", now);
                }
                Op::Poll(index) => {
                    if !sleeps.is_empty() {
                        let index = index % sleeps.len();
                        sleeps[index].polled = true;
                    }
                }
                Op::Drop(index) => {
                    if !sleeps.is_empty() {
                        let index = index % sleeps.len();
                        sleeps.swap_remove(index);
                    }
                }
                Op::Reset(index) => {
                    if !sleeps.is_empty() {
                        let index = index % sleeps.len();
                        let modeled = &mut sleeps[index];
                        modeled.sleep.as_mut().reset();
                        modeled.state = State::Unregistered;
                        if let Some(micros) = modeled.relative {
                            modeled.deadline = relative_deadline(now, micros);
                        }
                    }
                }
            }

            for (index, modeled) in sleeps.iter_mut().enumerate() {
                if !modeled.polled {
                    continue;
                }
                if modeled.state == State::Unregistered {
                    // Registration consults the clock even when the wheel is stale.
                    modeled.state = if modeled.deadline <= tick(now) {
                        State::Fired
                    } else {
                        State::Registered
                    };
                }
                let ready = modeled.sleep.as_mut().poll(&mut cx).is_ready();
                prop_assert_eq!(
                    ready,
                    modeled.state == State::Fired,
                    "sleep {} deadline={} now={}us",
                    index,
                    modeled.deadline,
                    now
                );
            }
        }

        drop(driver);
        for modeled in &mut sleeps {
            let expected = if modeled.state == State::Fired {
                Ok(())
            } else {
                Err(Error::Shutdown)
            };
            prop_assert_eq!(modeled.sleep.as_mut().poll(&mut cx), Poll::Ready(expected));
        }
    }
}
