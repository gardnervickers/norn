#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::fs::File as StdFile;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use futures::stream::{FuturesUnordered, StreamExt};

use norn_uring::buf::{FixedBuffer, StableBuf, StableBufMut};
use norn_uring::fixedbuf::FixedBuf;
use norn_uring::fs;

mod support;

const RING_DEPTH: u32 = 256;
const PROFILE_REPETITIONS_ENV: &str = "NORN_FIXEDBUF_PROFILE_REPETITIONS";
const WRITE_BYTE: u8 = 0xa5;
const READ_PATTERN_SEED: u64 = 0x42d4_94ca_2e45_7a15;
const WRITE_PATTERN_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

#[repr(C, align(4096))]
struct AlignedBlock<const N: usize>([u8; N]);

struct AlignedBuf<const N: usize>(Box<AlignedBlock<N>>);

impl<const N: usize> AlignedBuf<N> {
    fn filled(byte: u8) -> Self {
        Self(Box::new(AlignedBlock([byte; N])))
    }

    fn fill(&mut self, byte: u8) {
        self.0 .0.fill(byte);
    }
}

// Safety: every byte in the aligned heap allocation is initialized, and the
// box keeps its address stable while the operation owns this wrapper.
unsafe impl<const N: usize> StableBuf for AlignedBuf<N> {
    fn stable_ptr(&self) -> *const u8 {
        self.0 .0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        N
    }
}

// Safety: every byte in the aligned heap allocation is initialized and
// writable, and the operation owns the wrapper while the kernel may access it.
unsafe impl<const N: usize> StableBufMut for AlignedBuf<N> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.0 .0.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        N
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(init_len <= N);
    }
}

// Safety: the box keeps the aligned allocation stable and exclusively owned
// for the complete registration lifetime.
unsafe impl<const N: usize> FixedBuffer for AlignedBuf<N> {
    fn fixed_region(&mut self) -> &mut [u8] {
        &mut self.0 .0
    }
}

#[derive(Clone, Copy)]
enum IoMode {
    Ordinary,
    Fixed,
}

impl IoMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Fixed => "fixed",
        }
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Read,
    Write,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

struct FixedIoBench<const N: usize> {
    mode: IoMode,
    direction: Direction,
    queue_depth: usize,
    operations: usize,
}

struct RegistrationBench {
    buffer_count: usize,
}

impl RegistrationBench {
    fn new(buffer_count: usize) -> Self {
        Self { buffer_count }
    }
}

impl bencher::TDynBenchFn for RegistrationBench {
    fn run(&self, b: &mut Bencher) {
        let (_executor, handle) = new_executor();
        let mut buffers = Some(
            (0..self.buffer_count)
                .map(|_| AlignedBuf::<4096>::filled(0))
                .collect::<Vec<_>>(),
        );

        b.iter(|| {
            let pool = handle
                .register_fixed_buffers(buffers.take().expect("registration buffers missing"))
                .unwrap();
            buffers = Some(pool.unregister().unwrap());
        });

        assert_eq!(buffers.as_ref().unwrap().len(), self.buffer_count);
    }
}

impl<const N: usize> FixedIoBench<N> {
    fn new(mode: IoMode, direction: Direction, queue_depth: usize, operations: usize) -> Self {
        assert!(N >= 4096 && N.is_multiple_of(4096));
        assert!(operations >= queue_depth);
        Self {
            mode,
            direction,
            queue_depth,
            operations,
        }
    }

    fn open_file(
        &self,
        executor: &mut norn_executor::LocalExecutor<norn_uring::Driver>,
        path: &Path,
    ) -> fs::File {
        executor.block_on(async {
            let mut options = fs::OpenOptions::new();
            options.read(true).write(true).direct(true);
            options.open(path).await.unwrap()
        })
    }

    fn run_ordinary(&self, b: &mut Bencher) {
        let directory = BenchDir::new("fixed-buffer-ordinary");
        let path = directory.join("bench.dat");
        prepare_pattern_file::<N>(&path, self.operations, READ_PATTERN_SEED).unwrap();

        let (mut executor, _) = new_executor();
        let file = self.open_file(&mut executor, &path);
        let fill = match self.direction {
            Direction::Read => 0,
            Direction::Write => WRITE_BYTE,
        };
        let mut buffers = (0..self.queue_depth)
            .map(|_| AlignedBuf::<N>::filled(fill))
            .collect::<Vec<_>>();

        buffers = match self.direction {
            Direction::Read => {
                executor.block_on(run_ordinary_reads(&file, buffers, self.operations, true))
            }
            Direction::Write => {
                executor.block_on(run_ordinary_writes(&file, buffers, self.operations, true))
            }
        };
        if matches!(self.direction, Direction::Write) {
            executor.block_on(file.sync()).unwrap();
            validate_pattern_file::<N>(&path, self.operations, WRITE_PATTERN_SEED).unwrap();
            for buffer in &mut buffers {
                buffer.fill(WRITE_BYTE);
            }
        }

        let repetitions = profile_repetitions();
        b.iter(|| {
            for _ in 0..repetitions {
                buffers = match self.direction {
                    Direction::Read => executor.block_on(run_ordinary_reads(
                        &file,
                        std::mem::take(&mut buffers),
                        self.operations,
                        false,
                    )),
                    Direction::Write => executor.block_on(run_ordinary_writes(
                        &file,
                        std::mem::take(&mut buffers),
                        self.operations,
                        false,
                    )),
                };
            }
        });

        if matches!(self.direction, Direction::Write) {
            executor.block_on(file.sync()).unwrap();
        }
        drop(buffers);
        executor.block_on(file.close()).unwrap();
        if matches!(self.direction, Direction::Write) {
            validate_file(&path, self.operations * N, WRITE_BYTE).unwrap();
        }
    }

    fn run_fixed(&self, b: &mut Bencher) {
        let directory = BenchDir::new("fixed-buffer-fixed");
        let path = directory.join("bench.dat");
        prepare_pattern_file::<N>(&path, self.operations, READ_PATTERN_SEED).unwrap();

        let (mut executor, handle) = new_executor();
        let file = self.open_file(&mut executor, &path);
        let fill = match self.direction {
            Direction::Read => 0,
            Direction::Write => WRITE_BYTE,
        };
        let pool = handle
            .register_fixed_buffers(
                (0..self.queue_depth)
                    .map(|_| AlignedBuf::<N>::filled(fill))
                    .collect(),
            )
            .unwrap();
        let mut buffers = (0..self.queue_depth)
            .map(|_| pool.try_acquire().unwrap())
            .collect::<Vec<_>>();

        buffers = match self.direction {
            Direction::Read => {
                executor.block_on(run_fixed_reads(&file, buffers, self.operations, true))
            }
            Direction::Write => {
                executor.block_on(run_fixed_writes(&file, buffers, self.operations, true))
            }
        };
        if matches!(self.direction, Direction::Write) {
            executor.block_on(file.sync()).unwrap();
            validate_pattern_file::<N>(&path, self.operations, WRITE_PATTERN_SEED).unwrap();
            for buffer in &mut buffers {
                buffer.as_full_slice_mut().fill(WRITE_BYTE);
                buffer.set_len(N).unwrap();
            }
        }

        let repetitions = profile_repetitions();
        b.iter(|| {
            for _ in 0..repetitions {
                buffers = match self.direction {
                    Direction::Read => executor.block_on(run_fixed_reads(
                        &file,
                        std::mem::take(&mut buffers),
                        self.operations,
                        false,
                    )),
                    Direction::Write => executor.block_on(run_fixed_writes(
                        &file,
                        std::mem::take(&mut buffers),
                        self.operations,
                        false,
                    )),
                };
            }
        });

        if matches!(self.direction, Direction::Write) {
            executor.block_on(file.sync()).unwrap();
        }
        drop(buffers);
        pool.unregister().unwrap();
        executor.block_on(file.close()).unwrap();
        if matches!(self.direction, Direction::Write) {
            validate_file(&path, self.operations * N, WRITE_BYTE).unwrap();
        }
    }
}

fn profile_repetitions() -> usize {
    let Some(value) = std::env::var_os(PROFILE_REPETITIONS_ENV) else {
        return 1;
    };
    let value = value
        .to_str()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| panic!("{PROFILE_REPETITIONS_ENV} must be a positive integer"));
    value
}

impl<const N: usize> bencher::TDynBenchFn for FixedIoBench<N> {
    fn run(&self, b: &mut Bencher) {
        match self.mode {
            IoMode::Ordinary => self.run_ordinary(b),
            IoMode::Fixed => self.run_fixed(b),
        }
    }
}

async fn run_ordinary_reads<const N: usize>(
    file: &fs::File,
    buffers: Vec<AlignedBuf<N>>,
    operations: usize,
    validate: bool,
) -> Vec<AlignedBuf<N>> {
    let queue_depth = buffers.len();
    let mut pending = FuturesUnordered::new();
    for (index, buffer) in buffers.into_iter().enumerate() {
        pending.push(ordinary_read(file, buffer, index));
    }

    let mut next = queue_depth;
    let mut returned = Vec::with_capacity(queue_depth);
    while let Some((completed, result, buffer)) = pending.next().await {
        assert_eq!(result.unwrap(), N);
        if validate {
            assert_pattern(&buffer.0 .0, completed, READ_PATTERN_SEED);
        }
        if next < operations {
            pending.push(ordinary_read(file, buffer, next));
            next += 1;
        } else {
            returned.push(buffer);
        }
    }
    returned
}

async fn run_ordinary_writes<const N: usize>(
    file: &fs::File,
    buffers: Vec<AlignedBuf<N>>,
    operations: usize,
    patterned: bool,
) -> Vec<AlignedBuf<N>> {
    let queue_depth = buffers.len();
    let mut pending = FuturesUnordered::new();
    for (index, mut buffer) in buffers.into_iter().enumerate() {
        if patterned {
            fill_pattern(&mut buffer.0 .0, index, WRITE_PATTERN_SEED);
        }
        pending.push(ordinary_write(file, buffer, index));
    }

    let mut next = queue_depth;
    let mut returned = Vec::with_capacity(queue_depth);
    while let Some((_, result, mut buffer)) = pending.next().await {
        assert_eq!(result.unwrap(), N);
        if next < operations {
            if patterned {
                fill_pattern(&mut buffer.0 .0, next, WRITE_PATTERN_SEED);
            }
            pending.push(ordinary_write(file, buffer, next));
            next += 1;
        } else {
            returned.push(buffer);
        }
    }
    returned
}

async fn run_fixed_reads<const N: usize>(
    file: &fs::File,
    buffers: Vec<FixedBuf<AlignedBuf<N>>>,
    operations: usize,
    validate: bool,
) -> Vec<FixedBuf<AlignedBuf<N>>> {
    let queue_depth = buffers.len();
    let mut pending = FuturesUnordered::new();
    for (index, buffer) in buffers.into_iter().enumerate() {
        pending.push(fixed_read(file, buffer, index));
    }

    let mut next = queue_depth;
    let mut returned = Vec::with_capacity(queue_depth);
    while let Some((completed, result, buffer)) = pending.next().await {
        assert_eq!(result.unwrap(), N);
        if validate {
            assert_pattern(&buffer, completed, READ_PATTERN_SEED);
        }
        if next < operations {
            pending.push(fixed_read(file, buffer, next));
            next += 1;
        } else {
            returned.push(buffer);
        }
    }
    returned
}

async fn run_fixed_writes<const N: usize>(
    file: &fs::File,
    buffers: Vec<FixedBuf<AlignedBuf<N>>>,
    operations: usize,
    patterned: bool,
) -> Vec<FixedBuf<AlignedBuf<N>>> {
    let queue_depth = buffers.len();
    let mut pending = FuturesUnordered::new();
    for (index, mut buffer) in buffers.into_iter().enumerate() {
        if patterned {
            fill_pattern(buffer.as_full_slice_mut(), index, WRITE_PATTERN_SEED);
            buffer.set_len(N).unwrap();
        }
        pending.push(fixed_write(file, buffer, index));
    }

    let mut next = queue_depth;
    let mut returned = Vec::with_capacity(queue_depth);
    while let Some((_, result, mut buffer)) = pending.next().await {
        assert_eq!(result.unwrap(), N);
        if next < operations {
            if patterned {
                fill_pattern(buffer.as_full_slice_mut(), next, WRITE_PATTERN_SEED);
                buffer.set_len(N).unwrap();
            }
            pending.push(fixed_write(file, buffer, next));
            next += 1;
        } else {
            returned.push(buffer);
        }
    }
    returned
}

async fn ordinary_read<const N: usize>(
    file: &fs::File,
    buffer: AlignedBuf<N>,
    operation: usize,
) -> (usize, io::Result<usize>, AlignedBuf<N>) {
    let (result, buffer) = file.read_at(buffer, offset::<N>(operation)).await;
    (operation, result, buffer)
}

async fn ordinary_write<const N: usize>(
    file: &fs::File,
    buffer: AlignedBuf<N>,
    operation: usize,
) -> (usize, io::Result<usize>, AlignedBuf<N>) {
    let (result, buffer) = file.write_at(buffer, offset::<N>(operation)).await;
    (operation, result, buffer)
}

async fn fixed_read<const N: usize>(
    file: &fs::File,
    buffer: FixedBuf<AlignedBuf<N>>,
    operation: usize,
) -> (usize, io::Result<usize>, FixedBuf<AlignedBuf<N>>) {
    let (result, buffer) = file.read_fixed_at(buffer, offset::<N>(operation)).await;
    (operation, result, buffer)
}

async fn fixed_write<const N: usize>(
    file: &fs::File,
    buffer: FixedBuf<AlignedBuf<N>>,
    operation: usize,
) -> (usize, io::Result<usize>, FixedBuf<AlignedBuf<N>>) {
    let (result, buffer) = file.write_fixed_at(buffer, offset::<N>(operation)).await;
    (operation, result, buffer)
}

fn offset<const N: usize>(operation: usize) -> u64 {
    u64::try_from(operation.checked_mul(N).unwrap()).unwrap()
}

fn new_executor() -> (
    norn_executor::LocalExecutor<norn_uring::Driver>,
    norn_uring::Handle,
) {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let driver = norn_uring::Driver::new(builder, RING_DEPTH).unwrap();
    let handle = driver.handle();
    (norn_executor::LocalExecutor::new(driver), handle)
}

struct BenchDir {
    path: PathBuf,
}

impl BenchDir {
    fn new(tag: &str) -> Self {
        let parent = std::env::var_os("NORN_FIXEDBUF_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock went backwards")
            .as_nanos();
        let path = parent.join(format!("norn-{tag}-pid{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn join(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn prepare_pattern_file<const N: usize>(
    path: &Path,
    operations: usize,
    seed: u64,
) -> io::Result<()> {
    let file = StdFile::create(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut block = vec![0u8; N];
    for operation in 0..operations {
        fill_pattern(&mut block, operation, seed);
        writer.write_all(&block)?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()
}

fn validate_pattern_file<const N: usize>(
    path: &Path,
    operations: usize,
    seed: u64,
) -> io::Result<()> {
    let file = StdFile::open(path)?;
    assert_eq!(file.metadata()?.len(), (operations * N) as u64);
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut block = vec![0u8; N];
    for operation in 0..operations {
        reader.read_exact(&mut block)?;
        assert_pattern(&block, operation, seed);
    }
    Ok(())
}

fn fill_pattern(bytes: &mut [u8], operation: usize, seed: u64) {
    for (position, byte) in bytes.iter_mut().enumerate() {
        *byte = pattern_byte(operation, position, seed);
    }
}

fn assert_pattern(bytes: &[u8], operation: usize, seed: u64) {
    if let Some((position, actual)) = bytes
        .iter()
        .copied()
        .enumerate()
        .find(|(position, actual)| *actual != pattern_byte(operation, *position, seed))
    {
        panic!(
            "I/O pattern mismatch at operation {operation}, byte {position}: expected {}, got {actual}",
            pattern_byte(operation, position, seed)
        );
    }
}

fn pattern_byte(operation: usize, position: usize, seed: u64) -> u8 {
    let mut value = seed
        ^ (operation as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (position as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) as u8
}

fn validate_file(path: &Path, len: usize, byte: u8) -> io::Result<()> {
    let file = StdFile::open(path)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut checked = 0;
    while checked < len {
        let read = reader.read(&mut buffer[..(len - checked).min(1024 * 1024)])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "benchmark file ended before validation completed",
            ));
        }
        assert!(buffer[..read].iter().all(|actual| *actual == byte));
        checked += read;
    }
    Ok(())
}

fn add_cases<const N: usize>(
    benches: &mut Vec<TestDescAndFn>,
    queue_depths: &[usize],
    operations: usize,
) {
    for queue_depth in queue_depths {
        for direction in [Direction::Read, Direction::Write] {
            for mode in [IoMode::Ordinary, IoMode::Fixed] {
                benches.push(TestDescAndFn {
                    desc: TestDesc {
                        name: Cow::from(format!(
                            "fixed_file_io/mode={}/direction={}/storage=aligned_heap/block={N}/qd={queue_depth}/ops={operations}",
                            mode.as_str(),
                            direction.as_str(),
                        )),
                        ignore: false,
                    },
                    testfn: TestFn::DynBenchFn(Box::new(FixedIoBench::<N>::new(
                        mode,
                        direction,
                        *queue_depth,
                        operations,
                    ))),
                });
            }
        }
    }
}

fn benches() -> Vec<TestDescAndFn> {
    let mut benches = Vec::new();
    add_cases::<4096>(&mut benches, &[1, 32, 128], 16_384);
    add_cases::<16384>(&mut benches, &[32], 8_192);
    add_cases::<65536>(&mut benches, &[32], 2_048);
    for buffer_count in [1, 32, 128, 512, 1024] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!(
                    "fixed_registration_roundtrip/storage=aligned_heap/block=4096/buffers={buffer_count}"
                )),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(RegistrationBench::new(buffer_count))),
        });
    }
    benches
}

fn main() {
    support::run(benches());
}
