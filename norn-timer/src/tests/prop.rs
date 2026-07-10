use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use proptest::prelude::*;

use crate::entry;
use crate::wheels::Wheels;

fn new_sleep(wheels: &Rc<Wheels>, dur: Duration) -> Pin<Box<entry::Sleep<Rc<Wheels>>>> {
    Box::pin(entry::Sleep::new(wheels.clone(), dur))
}

proptest! {
    #[test]
    fn cancellations_preserve_exact_deadlines(
        cases in prop::collection::vec((1..64 * 128_u64, any::<bool>()), 1..1000)
    ) {
        let mut cx = futures_test::task::noop_context();
        let wheels = Rc::new(Wheels::new());
        let mut timers = Vec::with_capacity(cases.len());

        for &(timestamp, cancel) in &cases {
            let mut timer = new_sleep(&wheels, Duration::from_millis(timestamp));
            assert!(timer.as_mut().poll(&mut cx).is_pending());
            timers.push((timestamp, cancel, Some(timer)));
        }

        let mut expected = Vec::new();
        for (timestamp, cancel, timer) in &mut timers {
            if *cancel {
                drop(timer.take());
            } else {
                expected.push(*timestamp);
            }
        }
        expected.sort_unstable();

        let mut fired_total = 0;
        let mut now = 0;
        loop {
            let due = expected[fired_total..].partition_point(|deadline| *deadline <= now);
            let (fired, next) = wheels.advance(now);
            prop_assert_eq!(fired, due);
            fired_total += due;

            let Some(next) = next else {
                break;
            };
            prop_assert!(fired_total < expected.len());
            prop_assert_eq!(next.deadline(), expected[fired_total]);
            now = next.deadline();
        }

        prop_assert_eq!(fired_total, expected.len());
    }

    #[test]
    fn advance_prop(mut timestamps in prop::collection::vec(1..64*32u64, 1..10000),
                    mut poll_times in prop::collection::vec(0..64*32u64, 1..100)) {
        // 1. Generate a bunch of timestamps and a bunch of poll times.
        // 2. For each timestamp, make a timer and register it.
        // 3. Sort all of the poll times and timestamps.
        // 4. For each poll time, advance the wheel and check if it should return
        //    a timer.
        let mut cx = futures_test::task::noop_context();
        let wheels = Rc::new(Wheels::new());


        let mut timers = vec![];
        for &timestamp in timestamps.iter() {
            let mut timer = new_sleep(&wheels, Duration::from_millis(timestamp));
            // poll the timer to register it
            assert!(timer.as_mut().poll(&mut cx).is_pending());
            timers.push(timer);
        }

        timestamps.sort_unstable();
        poll_times.sort_unstable();


        for poll_time in poll_times {
            // remove all of the timers which should have fired by now
            let mut expect_fire = 0;
            while let Some(&first) = timestamps.first() {
                if first <= poll_time {
                    timestamps.remove(0);
                    expect_fire += 1;
                } else {
                    break;
                }
            }
            let (fired, next) = wheels.advance(poll_time);
            assert_eq!(fired, expect_fire);
            if let Some(next) = next {
                assert!(next.deadline() > poll_time);
            }
        }
    }

    #[test]
    fn advance_always_terminates_prop(timestamps in prop::collection::vec(1..64*32u64, 1..10000)) {
        // 1. Generate a bunch of timestamps.
        // 2. For each timestamp, make a timer and register it.
        // 3. Advance the wheel until it returns None.
        // 4. All of the timers should have fired.
        let mut cx = futures_test::task::noop_context();
        let wheels = Rc::new(Wheels::new());

        let mut timers = vec![];
        for &timestamp in timestamps.iter() {
            let mut timer = new_sleep(&wheels, Duration::from_millis(timestamp));
            // poll the timer to register it
            assert!(timer.as_mut().poll(&mut cx).is_pending());
            timers.push(timer);
        }

        let mut fired = 0;
        let mut next_tick = 0;
        loop {
            let (n, next) = wheels.advance(next_tick);
            fired += n;
            if next.is_none() {
                break;
            }
            next_tick = next.unwrap().deadline();
        }
        assert_eq!(fired, timestamps.len());

        // All of the timers should have fired
        for mut timer in timers.into_iter() {
            assert!(timer.as_mut().poll(&mut cx).is_ready());
        }
    }
}
