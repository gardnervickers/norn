//! Pass-4 randomized re-entrancy exploration for Miri.
//!
//! Tasks perform random task-system operations from their polls and from
//! their destructors: waking, joining, aborting and dropping other tasks,
//! spawning, running queued tasks inline, and shutting the queue down.
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, TaskQueue};

struct World {
    queue: RefCell<Option<TaskQueue>>,
    joins: RefCell<Vec<Option<JoinHandle<u32>>>>,
    wakers: RefCell<Vec<Option<Waker>>>,
    rng: Cell<u64>,
    spawned: Cell<u32>,
    depth: Cell<u32>,
    max_spawn: u32,
}

impl World {
    fn next(&self) -> u64 {
        let mut x = self.rng.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.set(x);
        x
    }

    fn pick(&self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }

    fn queue(&self) -> Option<TaskQueue> {
        self.queue.borrow().clone()
    }

    fn spawn(self: &Rc<Self>) {
        if self.spawned.get() >= self.max_spawn {
            return;
        }
        let Some(queue) = self.queue() else { return };
        let id = self.spawned.get();
        self.spawned.set(id + 1);
        self.wakers.borrow_mut().push(None);
        let polls = (self.next() % 4) as u32;
        let handle = queue.spawn(Actor {
            id,
            polls_left: polls,
            guard: DropActor {
                world: Rc::clone(self),
            },
        });
        self.joins.borrow_mut().push(Some(handle));
    }

    fn act(self: &Rc<Self>, cx: Option<&Context<'_>>, in_drop: bool) {
        let steps = self.next() % 3;
        for _ in 0..steps {
            let op = self.next() % 10;
            match op {
                0 => {
                    let n = self.wakers.borrow().len();
                    let i = self.pick(n);
                    let w = self.wakers.borrow().get(i).cloned().flatten();
                    if let Some(w) = w {
                        if self.next() % 2 == 0 {
                            w.wake_by_ref();
                        } else {
                            w.wake();
                        }
                    }
                }
                1 => {
                    let n = self.joins.borrow().len();
                    let i = self.pick(n);
                    let jh = self.joins.borrow_mut().get_mut(i).and_then(Option::take);
                    if let Some(mut jh) = jh {
                        let noop = Waker::noop();
                        let waker = cx.map(|cx| cx.waker()).unwrap_or(noop);
                        let mut cx2 = Context::from_waker(waker);
                        match Pin::new(&mut jh).poll(&mut cx2) {
                            Poll::Ready(_) => drop(jh),
                            Poll::Pending => {
                                let old = self.joins.borrow_mut()[i].replace(jh);
                                drop(old);
                            }
                        }
                    }
                }
                2 => {
                    let n = self.joins.borrow().len();
                    let i = self.pick(n);
                    let jh = self.joins.borrow_mut().get_mut(i).and_then(Option::take);
                    if let Some(jh) = jh {
                        jh.abort();
                        let old = self.joins.borrow_mut()[i].replace(jh);
                        drop(old);
                    }
                }
                3 => {
                    let n = self.joins.borrow().len();
                    let i = self.pick(n);
                    let jh = self.joins.borrow_mut().get_mut(i).and_then(Option::take);
                    drop(jh);
                }
                4 | 5 => self.spawn(),
                6 => {
                    if self.depth.get() < 3 {
                        if let Some(queue) = self.queue() {
                            if let Some(r) = queue.next() {
                                self.depth.set(self.depth.get() + 1);
                                r.run();
                                self.depth.set(self.depth.get() - 1);
                            }
                        }
                    }
                }
                7 => {
                    if self.next() % 4 == 0 {
                        if let Some(queue) = self.queue() {
                            queue.shutdown();
                        }
                    }
                }
                8 => {
                    let n = self.wakers.borrow().len();
                    let i = self.pick(n);
                    let w = self.wakers.borrow_mut().get_mut(i).and_then(Option::take);
                    drop(w);
                }
                _ => {
                    if in_drop && self.next() % 8 == 0 {
                        // Drop the world's queue handle from a destructor.
                        let q = self.queue.borrow_mut().take();
                        drop(q);
                    }
                }
            }
        }
    }
}

struct DropActor {
    world: Rc<World>,
}

impl Drop for DropActor {
    fn drop(&mut self) {
        let world = Rc::clone(&self.world);
        world.act(None, true);
    }
}

struct Actor {
    id: u32,
    polls_left: u32,
    guard: DropActor,
}

impl Future for Actor {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        let world = Rc::clone(&self.guard.world);
        let old = world.wakers.borrow_mut()[self.id as usize].replace(cx.waker().clone());
        drop(old);
        world.act(Some(cx), false);
        if self.polls_left == 0 {
            Poll::Ready(self.id)
        } else {
            self.polls_left -= 1;
            if world.next() % 3 == 0 {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

fn run_seed(seed: u64, max_spawn: u32) {
    let world = Rc::new(World {
        queue: RefCell::new(Some(TaskQueue::new())),
        joins: RefCell::new(Vec::new()),
        wakers: RefCell::new(Vec::new()),
        rng: Cell::new(seed | 1),
        spawned: Cell::new(0),
        depth: Cell::new(0),
        max_spawn,
    });
    for _ in 0..3 {
        world.spawn();
    }
    for _round in 0..6 {
        loop {
            let Some(queue) = world.queue() else { break };
            let Some(r) = queue.next() else { break };
            drop(queue);
            r.run();
        }
        world.act(None, false);
        // Wake everything that is still registered.
        let wakers: Vec<Waker> = world.wakers.borrow().iter().flatten().cloned().collect();
        for w in wakers {
            w.wake();
        }
    }
    let q = world.queue.borrow_mut().take();
    if let Some(q) = q {
        q.shutdown();
        drop(q);
    }
    // Break the world <-> task cycles.
    loop {
        let joins = std::mem::take(&mut *world.joins.borrow_mut());
        let wakers = std::mem::take(&mut *world.wakers.borrow_mut());
        if joins.is_empty() && wakers.is_empty() {
            break;
        }
        drop(joins);
        drop(wakers);
    }
    let q = world.queue.borrow_mut().take();
    drop(q);
    let joins = std::mem::take(&mut *world.joins.borrow_mut());
    drop(joins);
    let wakers = std::mem::take(&mut *world.wakers.borrow_mut());
    drop(wakers);
}

#[test]
fn random_reentrancy() {
    // Miri hides the host environment, so widen a Miri sweep with
    // `MIRIFLAGS="-Zmiri-env-forward=PASS4_SEEDS -Zmiri-env-forward=PASS4_START"`.
    let seeds: u64 = std::env::var("PASS4_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(if cfg!(miri) { 40 } else { 20000 });
    let start: u64 = std::env::var("PASS4_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    for seed in start..start + seeds {
        let s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xDEAD_BEEF;
        run_seed(s, 12);
    }
}
