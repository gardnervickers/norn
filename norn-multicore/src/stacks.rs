//! Worker stack capture for the stall watchdog.
//!
//! With the `stall-stacks` feature on Linux, the watchdog sends the stuck
//! worker thread a real-time signal. The handler unwinds the interrupted
//! stack into a static buffer and the watchdog symbolizes it afterwards, so
//! the handler neither allocates nor symbolizes. Unwinding uses the system
//! unwinder, which the watchdog loads before the first capture; it is not
//! guaranteed async-signal-safe, and could deadlock a worker that is stuck
//! inside the unwinder or the dynamic loader itself. A capture that does not
//! finish within a second is abandoned; later captures resume once its
//! handler finishes.

use crate::{GroupControl, WorkerId};

#[cfg(all(feature = "stall-stacks", target_os = "linux"))]
mod imp {
    use std::cell::UnsafeCell;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering};
    use std::sync::{Mutex, Once};
    use std::time::{Duration, Instant};

    use crate::{GroupControl, WorkerId};

    const MAX_FRAMES: usize = 128;
    const IDLE: u8 = 0;
    const REQUESTED: u8 = 1;
    const CAPTURING: u8 = 2;
    const DONE: u8 = 3;

    static STATE: AtomicU8 = AtomicU8::new(IDLE);
    static LEN: AtomicUsize = AtomicUsize::new(0);
    /// The kernel thread id the pending request targets. A late or stray
    /// signal on any other thread captures nothing.
    static TARGET: AtomicI64 = AtomicI64::new(0);
    static INSTALL: Once = Once::new();
    static INSTALLED: AtomicBool = AtomicBool::new(false);
    /// Serializes captures across every group's watchdog in the process.
    static CAPTURE: Mutex<()> = Mutex::new(());

    struct Frames(UnsafeCell<[usize; MAX_FRAMES]>);

    // Safety: only the signal handler writes the buffer, between winning
    // REQUESTED -> CAPTURING and publishing DONE, and only the watchdog reads
    // it, after observing DONE and before returning the state to IDLE.
    unsafe impl Sync for Frames {}

    static FRAMES: Frames = Frames(UnsafeCell::new([0; MAX_FRAMES]));

    fn signal() -> libc::c_int {
        libc::SIGRTMIN() + 3
    }

    fn gettid() -> i64 {
        // Safety: gettid has no preconditions and is async-signal-safe.
        unsafe { libc::syscall(libc::SYS_gettid) }
    }

    extern "C" fn on_signal(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut c_void) {
        // Only the requested thread captures: a signal queued for an earlier,
        // withdrawn request, or sent by someone else, writes nothing.
        if gettid() != TARGET.load(Ordering::Acquire)
            || STATE
                .compare_exchange(REQUESTED, CAPTURING, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        // Safety: errno is thread-local; restore what the interrupted code saw.
        let errno = unsafe { *libc::__errno_location() };
        let mut len = 0;
        // Safety: this handler is the only writer (see `Frames`), and the
        // callback neither allocates nor panics.
        unsafe {
            let frames = &mut *FRAMES.0.get();
            backtrace::trace_unsynchronized(|frame| {
                frames[len] = frame.ip() as usize;
                len += 1;
                len < MAX_FRAMES
            });
        }
        LEN.store(len, Ordering::Relaxed);
        STATE.store(DONE, Ordering::Release);
        // Safety: as above.
        unsafe { *libc::__errno_location() = errno };
    }

    /// Whether the signal is still handled by `on_signal`. An application
    /// that later took the signal over, or reset it so that it terminates the
    /// process, must not receive a capture request.
    fn still_installed() -> bool {
        // Safety: a sigaction query into a zeroed struct.
        unsafe {
            let mut current: libc::sigaction = std::mem::zeroed();
            libc::sigaction(signal(), std::ptr::null(), &mut current) == 0
                && current.sa_sigaction == on_signal as *const () as usize
        }
    }

    /// Install the handler once, unless the application already owns the
    /// signal.
    fn install() -> bool {
        INSTALL.call_once(|| {
            // Load the unwinder outside any handler: its first use may
            // allocate or take loader locks.
            backtrace::trace(|_| false);
            // Safety: plain sigaction calls on a zeroed, then filled, struct.
            unsafe {
                let mut old: libc::sigaction = std::mem::zeroed();
                if libc::sigaction(signal(), std::ptr::null(), &mut old) != 0
                    || old.sa_sigaction != libc::SIG_DFL
                {
                    return;
                }
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = on_signal as *const () as usize;
                action.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
                libc::sigemptyset(&mut action.sa_mask);
                if libc::sigaction(signal(), &action, std::ptr::null_mut()) == 0 {
                    INSTALLED.store(true, Ordering::Release);
                }
            }
        });
        INSTALLED.load(Ordering::Acquire)
    }

    pub(crate) fn register_worker(control: &GroupControl, worker: WorkerId) {
        // Safety: pthread_self has no preconditions.
        let thread = unsafe { libc::pthread_self() };
        let _ = control.threads[worker.index()].set((thread as u64, gettid()));
    }

    pub(crate) fn capture(control: &GroupControl, worker: WorkerId) -> Option<Vec<String>> {
        let (thread, tid) = *control.threads.get(worker.index())?.get()?;
        if !install() {
            return None;
        }
        let _one_at_a_time = CAPTURE.lock().unwrap_or_else(|poison| poison.into_inner());
        // A handler that finished after its capture was abandoned left DONE;
        // one still running (or deadlocked) leaves CAPTURING, and this
        // capture gives up.
        let _ = STATE.compare_exchange(DONE, IDLE, Ordering::AcqRel, Ordering::Relaxed);
        if !still_installed() {
            return None;
        }
        TARGET.store(tid, Ordering::Release);
        STATE
            .compare_exchange(IDLE, REQUESTED, Ordering::AcqRel, Ordering::Relaxed)
            .ok()?;
        // Safety: the thread is a worker of this group, registered on itself.
        // If it has exited and its id was reused, the handler's thread id
        // check keeps the other thread from capturing.
        if unsafe { libc::pthread_kill(thread as libc::pthread_t, signal()) } != 0 {
            STATE.store(IDLE, Ordering::Release);
            return None;
        }
        let start = Instant::now();
        loop {
            match STATE.load(Ordering::Acquire) {
                DONE => break,
                REQUESTED if start.elapsed() >= Duration::from_millis(100) => {
                    // The handler never ran; withdraw the request.
                    if STATE
                        .compare_exchange(REQUESTED, IDLE, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        return None;
                    }
                }
                CAPTURING if start.elapsed() >= Duration::from_secs(1) => return None,
                _ => std::thread::sleep(Duration::from_millis(1)),
            }
        }
        let len = LEN.load(Ordering::Relaxed);
        // Safety: DONE was observed with acquire ordering; see `Frames`.
        let ips: Vec<usize> = unsafe { (&*FRAMES.0.get())[..len].to_vec() };
        STATE.store(IDLE, Ordering::Release);
        Some(symbolize(&ips))
    }

    /// Symbolize the captured frames, dropping the handler's own frames.
    fn symbolize(ips: &[usize]) -> Vec<String> {
        let mut frames = Vec::new();
        for &ip in ips {
            let mut resolved = false;
            // Return addresses point after the call; look up the call itself.
            backtrace::resolve(ip.saturating_sub(1) as *mut c_void, |symbol| {
                resolved = true;
                let name = symbol
                    .name()
                    .map_or_else(|| format!("{ip:#x}"), |name| name.to_string());
                frames.push(match (symbol.filename(), symbol.lineno()) {
                    (Some(file), Some(line)) => format!("{name} at {}:{line}", file.display()),
                    _ => name,
                });
            });
            if !resolved {
                frames.push(format!("{ip:#x}"));
            }
        }
        // Everything up to the kernel's signal trampoline is the capture. The
        // trampoline often has no symbol, so it shows as a bare address right
        // after the handler's frames.
        if let Some(trampoline) = frames
            .iter()
            .position(|frame| frame.contains("__restore_rt"))
        {
            return frames.split_off(trampoline + 1);
        }
        let mut frames = frames
            .into_iter()
            .skip_while(|frame| frame.contains("backtrace") || frame.contains("on_signal"))
            .peekable();
        if frames.peek().is_some_and(|frame| frame.starts_with("0x")) {
            frames.next();
        }
        frames.collect()
    }
}

#[cfg(not(all(feature = "stall-stacks", target_os = "linux")))]
mod imp {
    use crate::{GroupControl, WorkerId};

    pub(crate) fn register_worker(_control: &GroupControl, _worker: WorkerId) {}

    pub(crate) fn capture(_control: &GroupControl, _worker: WorkerId) -> Option<Vec<String>> {
        None
    }
}

/// Record the calling worker thread, so its stack can be captured later.
pub(crate) fn register_worker(control: &GroupControl, worker: WorkerId) {
    imp::register_worker(control, worker);
}

/// Capture a stuck worker thread's stack, innermost frame first.
pub(crate) fn capture(control: &GroupControl, worker: WorkerId) -> Option<Vec<String>> {
    imp::capture(control, worker)
}
