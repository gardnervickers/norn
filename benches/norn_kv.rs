use std::borrow::Cow;
use std::future::Future;
use std::path::{Path, PathBuf};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_kv::{Store, StoreConfig};

mod support;

#[derive(Debug)]
struct BenchDir {
    path: PathBuf,
}

impl BenchDir {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("norn-kv-bench-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn join(&self, path: impl AsRef<Path>) -> PathBuf {
        self.path.join(path)
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct PutGetDeleteBench {
    value_len: usize,
    ops: usize,
}

struct PutDeleteBench {
    value_len: usize,
    ops: usize,
}

impl bencher::TDynBenchFn for PutGetDeleteBench {
    fn run(&self, b: &mut Bencher) {
        let dir = BenchDir::new("put-get-delete");
        let path = dir.join(format!("value_len={}.dat", self.value_len));
        let config = StoreConfig {
            slot_count: self.ops.next_power_of_two().max(1024),
            ..StoreConfig::default()
        };

        let mut runtime = Runtime::new();
        let mut store = runtime.block_on(Store::open(&path, config)).unwrap();
        let value = vec![0x5a; self.value_len];
        let ops = self.ops;

        b.iter(|| {
            runtime.block_on(async {
                for _ in 0..ops {
                    let key = store.put(value.clone()).await.unwrap();
                    let found = store.get(key).await.unwrap().unwrap();
                    assert_eq!(found.len(), value.len());
                    assert!(store.delete(key).await.unwrap());
                }
            });
        });
    }
}

impl bencher::TDynBenchFn for PutDeleteBench {
    fn run(&self, b: &mut Bencher) {
        let dir = BenchDir::new("put-delete");
        let path = dir.join(format!("value_len={}.dat", self.value_len));
        let config = StoreConfig {
            slot_count: self.ops.next_power_of_two().max(1024),
            ..StoreConfig::default()
        };

        let mut runtime = Runtime::new();
        let mut store = runtime.block_on(Store::open(&path, config)).unwrap();
        let value = vec![0x5a; self.value_len];
        let ops = self.ops;

        b.iter(|| {
            runtime.block_on(async {
                for _ in 0..ops {
                    let key = store.put(value.clone()).await.unwrap();
                    assert!(store.delete(key).await.unwrap());
                }
            });
        });
    }
}

struct RecoverBench {
    live_slots: usize,
}

impl bencher::TDynBenchFn for RecoverBench {
    fn run(&self, b: &mut Bencher) {
        let dir = BenchDir::new("recover");
        let path = dir.join(format!("live_slots={}.dat", self.live_slots));
        let config = StoreConfig {
            slot_count: self.live_slots.next_power_of_two().max(1024),
            ..StoreConfig::default()
        };

        let mut runtime = Runtime::new();
        runtime.block_on(async {
            let mut store = Store::open(&path, config).await.unwrap();
            for i in 0..self.live_slots {
                let value = (i as u64).to_le_bytes().to_vec();
                let _ = store.put(value).await.unwrap();
            }
        });

        b.iter(|| {
            runtime.block_on(async {
                let store = Store::open(&path, config).await.unwrap();
                std::hint::black_box(store);
            });
        });
    }
}

#[cfg(target_os = "linux")]
struct RawWriteBench {
    ops: usize,
}

#[cfg(target_os = "linux")]
impl bencher::TDynBenchFn for RawWriteBench {
    fn run(&self, b: &mut Bencher) {
        let dir = BenchDir::new("raw-write");
        let path = dir.join("raw.dat");
        let mut runtime = Runtime::new();
        let file = runtime.block_on(async {
            let mut opts = norn_uring::fs::OpenOptions::new();
            opts.create(true)
                .read(true)
                .write(true)
                .direct(true)
                .dsync(true);
            let file = opts.open(&path).await.unwrap();
            file.fallocate(0, (self.ops * 4096) as u64, 0)
                .await
                .unwrap();
            file
        });
        let ops = self.ops;
        let mut slot = 0_usize;

        b.iter(|| {
            runtime.block_on(async {
                for _ in 0..ops {
                    let mut buf = AlignedBuf::zeroed(4096).unwrap();
                    buf.as_mut_slice()[0..8].copy_from_slice(&(slot as u64).to_le_bytes());
                    file.write_at(buf, ((slot % ops) * 4096) as u64)
                        .await
                        .0
                        .unwrap();
                    slot = slot.wrapping_add(1);
                }
            });
        });
    }
}

#[cfg(target_os = "linux")]
struct Runtime {
    executor: norn_executor::LocalExecutor<norn_uring::Driver>,
}

#[cfg(target_os = "linux")]
impl Runtime {
    fn new() -> Self {
        let builder = io_uring::IoUring::builder();
        let driver = norn_uring::Driver::new(builder, 256).unwrap();
        Self {
            executor: norn_executor::LocalExecutor::new(driver),
        }
    }

    fn block_on<F: Future>(&mut self, future: F) -> F::Output {
        self.executor.block_on(future)
    }
}

#[cfg(not(target_os = "linux"))]
struct Runtime;

#[cfg(not(target_os = "linux"))]
impl Runtime {
    fn new() -> Self {
        Self
    }

    fn block_on<F: Future>(&mut self, future: F) -> F::Output {
        block_on_ready(future)
    }
}

#[cfg(not(target_os = "linux"))]
fn block_on_ready<F: Future>(future: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn clone(_: *const ()) -> RawWaker {
        raw_waker()
    }
    fn wake(_: *const ()) {}
    fn raw_waker() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, wake, wake, wake),
        )
    }

    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("blocking norn-kv bench future unexpectedly yielded"),
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
}

#[cfg(target_os = "linux")]
impl AlignedBuf {
    fn zeroed(len: usize) -> std::io::Result<Self> {
        let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        let res = unsafe { libc::posix_memalign(&mut ptr, 4096, len) };
        if res != 0 {
            return Err(std::io::Error::from_raw_os_error(res));
        }
        let ptr = std::ptr::NonNull::new(ptr.cast::<u8>()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, "posix_memalign failed")
        })?;
        let this = Self { ptr, len };
        unsafe {
            std::ptr::write_bytes(this.ptr.as_ptr(), 0, this.len);
        }
        Ok(this)
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

#[cfg(target_os = "linux")]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr.as_ptr().cast());
        }
    }
}

#[cfg(target_os = "linux")]
unsafe impl norn_uring::buf::StableBuf for AlignedBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len
    }
}

pub fn benches() -> Vec<TestDescAndFn> {
    let mut benches = Vec::new();

    for value_len in [64, 1024] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!("bench_put_delete/value_len={value_len}/ops=8")),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(PutDeleteBench { value_len, ops: 8 })),
        });

        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!("bench_put_get_delete/value_len={value_len}/ops=8")),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(PutGetDeleteBench { value_len, ops: 8 })),
        });
    }

    #[cfg(target_os = "linux")]
    benches.push(TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("bench_raw_write_4k_dsync/ops=8"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(RawWriteBench { ops: 8 })),
    });

    for live_slots in [64, 256] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!("bench_recover/live_slots={live_slots}")),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(RecoverBench { live_slots })),
        });
    }

    benches
}

fn main() {
    support::run(benches());
}
