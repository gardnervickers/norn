use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct CaptureWaker<T> {
    output: Option<T>,
    escaped: Rc<RefCell<Option<Waker>>>,
}

impl<T: Unpin> Future for CaptureWaker<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        *self.escaped.borrow_mut() = Some(cx.waker().clone());
        Poll::Ready(self.output.take().unwrap())
    }
}

struct ScopedOutput<'a> {
    scope_alive: &'a Cell<bool>,
    drops: Rc<Cell<usize>>,
}

impl Drop for ScopedOutput<'_> {
    fn drop(&mut self) {
        assert!(self.scope_alive.get());
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn dropping_join_destroys_scoped_output_before_escaped_waker() {
    let escaped = Rc::new(RefCell::new(None));
    let drops = Rc::new(Cell::new(0));

    {
        let scope_alive = Cell::new(true);
        let spawner = super::TestSpawner::new();
        let future = CaptureWaker {
            output: Some(ScopedOutput {
                scope_alive: &scope_alive,
                drops: Rc::clone(&drops),
            }),
            escaped: Rc::clone(&escaped),
        };
        // Safety: the join handle is dropped before `scope_alive`, and dropping
        // it must destroy the completed output even while a task waker escapes.
        let (runnable, handle) = unsafe {
            spawner
                .shared
                .owned
                .bind(future, Rc::clone(&spawner.shared))
        };

        runnable.unwrap().run();
        assert!(escaped.borrow().is_some());
        assert_eq!(drops.get(), 0);

        drop(handle);
        assert_eq!(drops.get(), 1);
        scope_alive.set(false);
    }

    drop(escaped.borrow_mut().take());
    assert_eq!(drops.get(), 1);
}
