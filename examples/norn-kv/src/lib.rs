use std::io;
use std::path::Path;

use block::{BlockBuf, BlockFile};
use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;

const DEFAULT_SLOT_COUNT: usize = 1024;
const DEFAULT_SLOT_SIZE: usize = 4096;
const HEADER_SIZE: usize = 64;
const BLOCK_ALIGNMENT: usize = 4096;
const MAGIC: u32 = u32::from_le_bytes(*b"NKV1");
const VERSION: u16 = 1;
const FLAG_DELETED: u32 = 1 << 0;
const CRC_OFFSET: usize = 36;
const RECOVERY_READ_WINDOW: usize = 64;

/// Opaque handle returned by [`Store::put`] and accepted by [`Store::get`] and
/// [`Store::delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key(u64);

impl Key {
    /// Return the opaque integer representation.
    pub fn into_raw(self) -> u64 {
        self.0
    }

    /// Construct a key from its opaque integer representation.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

/// Configuration for a fixed-slot KV file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreConfig {
    /// Number of fixed-size slots in the file.
    pub slot_count: usize,
    /// Size of each slot in bytes.
    ///
    /// The example requires 4096-byte alignment so the Linux backend can use
    /// `O_DIRECT`.
    pub slot_size: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            slot_count: DEFAULT_SLOT_COUNT,
            slot_size: DEFAULT_SLOT_SIZE,
        }
    }
}

/// Errors returned by the example KV store.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid store configuration.
    #[error("invalid store config: {0}")]
    InvalidConfig(&'static str),
    /// The payload is larger than this store's fixed slot payload capacity.
    #[error("value length {len} exceeds max payload length {max}")]
    ValueTooLarge {
        /// Provided value length.
        len: usize,
        /// Maximum supported payload length.
        max: usize,
    },
    /// The underlying file operation failed.
    #[error("{op} failed")]
    Io {
        /// Operation being attempted.
        op: &'static str,
        /// Source I/O error.
        #[source]
        source: io::Error,
    },
}

/// Convenient result alias for [`Store`] operations.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    fn io(op: &'static str, source: io::Error) -> Self {
        Self::Io { op, source }
    }
}

/// Fixed-slot flat-file KV store backed by platform-selected block I/O.
#[derive(Debug)]
pub struct Store {
    file: BlockFile,
    config: StoreConfig,
    free: FreeList,
    generations: Vec<u64>,
    index_bits: u32,
    index_mask: u64,
    scratch: Option<BlockBuf>,
}

impl Store {
    /// Open, preallocate, and recover a fixed-slot KV file.
    pub async fn open(path: impl AsRef<Path>, config: StoreConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let total_len = config.total_len()?;
        let file = BlockFile::open(path, total_len, config.slot_size)
            .await
            .map_err(|err| Error::io("open", err))?;

        let index_bits = index_bits(config.slot_count);
        let index_mask = (1_u64 << index_bits) - 1;
        let mut store = Self {
            file,
            config,
            free: FreeList::new(config.slot_count),
            generations: vec![0; config.slot_count],
            index_bits,
            index_mask,
            scratch: None,
        };
        store.recover().await?;
        Ok(store)
    }

    /// Store bytes in a free slot and return an opaque key.
    pub async fn put(&mut self, value: Vec<u8>) -> Result<Key> {
        let payload_capacity = self.payload_capacity();
        if value.len() > payload_capacity {
            return Err(Error::ValueTooLarge {
                len: value.len(),
                max: payload_capacity,
            });
        }

        let Some(slot) = self.free.allocate() else {
            return Err(Error::io(
                "allocate slot",
                io::Error::new(io::ErrorKind::StorageFull, "norn-kv is full"),
            ));
        };

        let generation = self.next_generation(slot);
        let key = self.make_key(slot, generation);
        let mut buf = self.take_block()?;
        buf[HEADER_SIZE..HEADER_SIZE + value.len()].copy_from_slice(&value);
        write_header(
            &mut buf,
            Header {
                flags: 0,
                slot,
                generation,
                key,
                payload_len: value.len() as u32,
                crc: 0,
            },
        );

        match self.write_slot(slot, buf).await {
            Ok(()) => {
                self.generations[slot] = generation;
                Ok(Key(key))
            }
            Err(err) => {
                self.free.set_free(slot);
                Err(err)
            }
        }
    }

    /// Read a key, returning `Ok(None)` if the key is missing, stale, deleted,
    /// or its slot fails format/CRC validation.
    pub async fn get(&self, key: Key) -> Result<Option<Vec<u8>>> {
        let Some((slot, generation)) = self.decode_key(key) else {
            return Ok(None);
        };
        if self.free.is_free(slot) || self.generations[slot] != generation {
            return Ok(None);
        }

        let buf = self.read_slot(slot).await?;
        let Some(header) = self.valid_header(slot, &buf, false) else {
            return Ok(None);
        };
        if header.generation != generation || header.key != key.0 {
            return Ok(None);
        }

        let len = header.payload_len as usize;
        Ok(Some(buf[HEADER_SIZE..HEADER_SIZE + len].to_vec()))
    }

    /// Delete a key, returning whether a live slot was actually marked deleted.
    pub async fn delete(&mut self, key: Key) -> Result<bool> {
        let Some((slot, generation)) = self.decode_key(key) else {
            return Ok(false);
        };
        if self.free.is_free(slot) || self.generations[slot] != generation {
            return Ok(false);
        }

        let mut delete_buf = self.take_block()?;
        write_header(
            &mut delete_buf,
            Header {
                flags: FLAG_DELETED,
                slot,
                generation,
                key: key.0,
                payload_len: 0,
                crc: 0,
            },
        );
        self.write_slot(slot, delete_buf).await?;
        self.generations[slot] = generation;
        self.free.set_free(slot);
        Ok(true)
    }

    /// Maximum payload bytes accepted by [`Store::put`].
    pub fn payload_capacity(&self) -> usize {
        self.config.slot_size - HEADER_SIZE
    }

    async fn recover(&mut self) -> Result<()> {
        let mut next_slot = 0;
        while next_slot < self.config.slot_count {
            let end = (next_slot + RECOVERY_READ_WINDOW).min(self.config.slot_count);
            let mut reads = FuturesUnordered::new();
            for slot in next_slot..end {
                let file = &self.file;
                reads.push(async move { (slot, file.read_block(slot).await) });
            }

            while let Some((slot, buf)) = reads.next().await {
                let buf = buf.map_err(|err| Error::io("read slot", err))?;
                match self.valid_header(slot, &buf, true) {
                    Some(header) if header.is_deleted() => {
                        self.generations[slot] = header.generation;
                        self.free.set_free(slot);
                    }
                    Some(header) => {
                        self.generations[slot] = header.generation;
                        self.free.set_used(slot);
                    }
                    None => {
                        if let Some(header) = self.plausible_header(slot, &buf) {
                            self.generations[slot] = header.generation;
                        }
                        self.free.set_free(slot);
                    }
                }
            }

            next_slot = end;
        }
        Ok(())
    }

    async fn read_slot(&self, slot: usize) -> Result<BlockBuf> {
        self.file
            .read_block(slot)
            .await
            .map_err(|err| Error::io("read slot", err))
    }

    async fn write_slot(&mut self, slot: usize, buf: BlockBuf) -> Result<()> {
        let buf = self
            .file
            .write_block(slot, buf)
            .await
            .map_err(|err| Error::io("write slot", err))?;
        self.scratch = Some(buf);
        Ok(())
    }

    fn take_block(&mut self) -> Result<BlockBuf> {
        if let Some(mut buf) = self.scratch.take() {
            buf.fill(0);
            return Ok(buf);
        }
        block::zeroed(self.config.slot_size).map_err(|err| Error::io("allocate block", err))
    }

    fn decode_key(&self, key: Key) -> Option<(usize, u64)> {
        let slot = (key.0 & self.index_mask) as usize;
        let generation = key.0 >> self.index_bits;
        if slot < self.config.slot_count && generation > 0 {
            Some((slot, generation))
        } else {
            None
        }
    }

    fn make_key(&self, slot: usize, generation: u64) -> u64 {
        (generation << self.index_bits) | slot as u64
    }

    fn next_generation(&self, slot: usize) -> u64 {
        let max_generation = u64::MAX >> self.index_bits;
        match self.generations[slot] {
            generation if generation >= max_generation => 1,
            generation => generation + 1,
        }
    }

    fn valid_header(&self, slot: usize, buf: &[u8], allow_deleted: bool) -> Option<Header> {
        let header = read_header(buf)?;
        if header.slot != slot
            || header.generation == 0
            || header.payload_len as usize > self.payload_capacity()
            || header.key != self.make_key(slot, header.generation)
        {
            return None;
        }
        if header.is_deleted() && !allow_deleted {
            return None;
        }

        let len = header.payload_len as usize;
        if slot_crc32c(buf, len) == header.crc {
            Some(header)
        } else {
            None
        }
    }

    fn plausible_header(&self, slot: usize, buf: &[u8]) -> Option<Header> {
        let header = read_header(buf)?;
        if header.slot == slot
            && header.generation > 0
            && header.payload_len as usize <= self.payload_capacity()
            && header.key == self.make_key(slot, header.generation)
        {
            Some(header)
        } else {
            None
        }
    }
}

impl StoreConfig {
    fn validate(self) -> Result<()> {
        if self.slot_count == 0 {
            return Err(Error::InvalidConfig("slot_count must be greater than zero"));
        }
        if self.slot_count > u32::MAX as usize {
            return Err(Error::InvalidConfig("slot_count must fit in u32"));
        }
        if self.slot_size <= HEADER_SIZE {
            return Err(Error::InvalidConfig("slot_size must exceed header size"));
        }
        if !self.slot_size.is_multiple_of(BLOCK_ALIGNMENT) {
            return Err(Error::InvalidConfig(
                "slot_size must be a multiple of 4096 for O_DIRECT",
            ));
        }
        self.total_len()?;
        Ok(())
    }

    fn total_len(self) -> Result<u64> {
        let len = self
            .slot_count
            .checked_mul(self.slot_size)
            .ok_or(Error::InvalidConfig("file length overflow"))?;
        Ok(len as u64)
    }
}

#[derive(Debug, Clone, Copy)]
struct Header {
    flags: u32,
    slot: usize,
    generation: u64,
    key: u64,
    payload_len: u32,
    crc: u32,
}

impl Header {
    fn is_deleted(self) -> bool {
        self.flags & FLAG_DELETED != 0
    }
}

fn write_header(slot: &mut [u8], header: Header) {
    slot[..HEADER_SIZE].fill(0);
    slot[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    slot[4..6].copy_from_slice(&VERSION.to_le_bytes());
    slot[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    slot[8..12].copy_from_slice(&header.flags.to_le_bytes());
    slot[12..16].copy_from_slice(&(header.slot as u32).to_le_bytes());
    slot[16..24].copy_from_slice(&header.generation.to_le_bytes());
    slot[24..32].copy_from_slice(&header.key.to_le_bytes());
    slot[32..36].copy_from_slice(&header.payload_len.to_le_bytes());

    let crc = slot_crc32c(slot, header.payload_len as usize);
    slot[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
}

fn read_header(slot: &[u8]) -> Option<Header> {
    if slot.len() < HEADER_SIZE
        || read_u32(slot, 0) != MAGIC
        || read_u16(slot, 4) != VERSION
        || read_u16(slot, 6) as usize != HEADER_SIZE
    {
        return None;
    }

    Some(Header {
        flags: read_u32(slot, 8),
        slot: read_u32(slot, 12) as usize,
        generation: read_u64(slot, 16),
        key: read_u64(slot, 24),
        payload_len: read_u32(slot, 32),
        crc: read_u32(slot, CRC_OFFSET),
    })
}

fn slot_crc32c(slot: &[u8], payload_len: usize) -> u32 {
    let mut header = [0_u8; HEADER_SIZE];
    header.copy_from_slice(&slot[..HEADER_SIZE]);
    header[CRC_OFFSET..CRC_OFFSET + 4].fill(0);

    let header_crc = crc_fast::crc32_iscsi(&header);
    let payload = &slot[HEADER_SIZE..HEADER_SIZE + payload_len];
    let payload_crc = crc_fast::crc32_iscsi(payload);
    crc_fast::checksum_combine(
        crc_fast::CrcAlgorithm::Crc32Iscsi,
        header_crc as u64,
        payload_crc as u64,
        payload_len as u64,
    ) as u32
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap())
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

fn index_bits(slot_count: usize) -> u32 {
    let next = slot_count.next_power_of_two();
    usize::BITS - next.leading_zeros() - 1
}

#[derive(Debug)]
struct FreeList {
    slot_count: usize,
    levels: Vec<Vec<u64>>,
}

impl FreeList {
    fn new(slot_count: usize) -> Self {
        let leaf_words = slot_count.div_ceil(64);
        let mut levels = vec![vec![0; leaf_words]];
        while levels.len() < 2 || levels.last().unwrap().len() > 1 {
            let previous_len = levels.last().unwrap().len();
            levels.push(vec![0; previous_len.div_ceil(64)]);
        }
        Self { slot_count, levels }
    }

    fn allocate(&mut self) -> Option<usize> {
        let top_level = self.levels.len() - 1;
        let mut word_index = 0;
        if self.levels[top_level][word_index] == 0 {
            return None;
        }

        for level in (1..=top_level).rev() {
            let bit = self.levels[level][word_index].trailing_zeros() as usize;
            word_index = word_index * 64 + bit;
        }

        let bit = self.levels[0][word_index].trailing_zeros() as usize;
        let slot = word_index * 64 + bit;
        if slot >= self.slot_count {
            return None;
        }
        self.set_used(slot);
        Some(slot)
    }

    fn set_free(&mut self, slot: usize) {
        self.set_leaf(slot, true);
    }

    fn set_used(&mut self, slot: usize) {
        self.set_leaf(slot, false);
    }

    fn is_free(&self, slot: usize) -> bool {
        if slot >= self.slot_count {
            return true;
        }
        let word = self.levels[0][slot / 64];
        let mask = 1_u64 << (slot % 64);
        word & mask != 0
    }

    fn set_leaf(&mut self, slot: usize, free: bool) {
        if slot >= self.slot_count {
            return;
        }
        let mut index = slot / 64;
        let bit = slot % 64;
        set_bit(&mut self.levels[0][index], bit, free);

        for level in 1..self.levels.len() {
            let parent = index / 64;
            let parent_bit = index % 64;
            let child_has_free = self.levels[level - 1][index] != 0;
            set_bit(&mut self.levels[level][parent], parent_bit, child_has_free);
            index = parent;
        }
    }
}

fn set_bit(word: &mut u64, bit: usize, enabled: bool) {
    let mask = 1_u64 << bit;
    if enabled {
        *word |= mask;
    } else {
        *word &= !mask;
    }
}

mod block {
    #[cfg(not(target_os = "linux"))]
    pub use blocking::{BlockBuf, BlockFile};
    #[cfg(target_os = "linux")]
    pub use linux::{BlockBuf, BlockFile};

    #[cfg(target_os = "linux")]
    pub fn zeroed(len: usize) -> std::io::Result<BlockBuf> {
        BlockBuf::zeroed(len)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn zeroed(len: usize) -> std::io::Result<BlockBuf> {
        Ok(vec![0; len])
    }

    #[cfg(target_os = "linux")]
    mod linux {
        use std::io;
        use std::path::Path;
        use std::ptr::NonNull;

        use norn_uring::buf::{StableBuf, StableBufMut};
        use norn_uring::fs;

        pub type BlockBuf = AlignedBuf;

        #[derive(Debug)]
        pub struct BlockFile {
            file: fs::File,
            block_size: usize,
        }

        impl BlockFile {
            pub async fn open(path: &Path, total_len: u64, block_size: usize) -> io::Result<Self> {
                let needs_prealloc = match std::fs::metadata(path) {
                    Ok(metadata) => metadata.len() < total_len,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => true,
                    Err(err) => return Err(err),
                };

                let mut opts = fs::OpenOptions::new();
                opts.create(true)
                    .read(true)
                    .write(true)
                    .direct(true)
                    .dsync(true);
                let file = opts.open(path).await?;
                if needs_prealloc {
                    file.fallocate(0, total_len, 0).await?;
                }

                Ok(Self { file, block_size })
            }

            pub async fn read_block(&self, block: usize) -> io::Result<BlockBuf> {
                let buf = AlignedBuf::zeroed(self.block_size)?;
                let (res, buf) = self.file.read_at(buf, self.offset(block)).await;
                let n = res?;
                if n != self.block_size {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "short block read at block {block}: expected {}, got {n}",
                            self.block_size
                        ),
                    ));
                }
                Ok(buf)
            }

            pub async fn write_block(&self, block: usize, buf: BlockBuf) -> io::Result<BlockBuf> {
                if buf.len != self.block_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "block write length mismatch: expected {}, got {}",
                            self.block_size, buf.len
                        ),
                    ));
                }

                let (res, buf) = self.file.write_at(buf, self.offset(block)).await;
                let n = res?;
                if n != self.block_size {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "short block write at block {block}: expected {}, got {n}",
                            self.block_size
                        ),
                    ));
                }
                Ok(buf)
            }

            fn offset(&self, block: usize) -> u64 {
                (block * self.block_size) as u64
            }
        }

        #[derive(Debug)]
        pub struct AlignedBuf {
            ptr: NonNull<u8>,
            len: usize,
        }

        impl AlignedBuf {
            pub fn zeroed(len: usize) -> io::Result<Self> {
                let mut ptr = std::ptr::null_mut();
                let res =
                    unsafe { libc::posix_memalign(&mut ptr, super::super::BLOCK_ALIGNMENT, len) };
                if res != 0 {
                    return Err(io::Error::from_raw_os_error(res));
                }
                let ptr = NonNull::new(ptr.cast::<u8>()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::OutOfMemory, "posix_memalign failed")
                })?;
                let this = Self { ptr, len };
                unsafe {
                    std::ptr::write_bytes(this.ptr.as_ptr(), 0, this.len);
                }
                Ok(this)
            }

            pub fn as_slice(&self) -> &[u8] {
                unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
            }

            pub fn as_mut_slice(&mut self) -> &mut [u8] {
                unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
            }
        }

        impl std::ops::Deref for AlignedBuf {
            type Target = [u8];

            fn deref(&self) -> &Self::Target {
                self.as_slice()
            }
        }

        impl std::ops::DerefMut for AlignedBuf {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.as_mut_slice()
            }
        }

        impl Drop for AlignedBuf {
            fn drop(&mut self) {
                unsafe {
                    libc::free(self.ptr.as_ptr().cast());
                }
            }
        }

        unsafe impl StableBuf for AlignedBuf {
            fn stable_ptr(&self) -> *const u8 {
                self.ptr.as_ptr()
            }

            fn bytes_init(&self) -> usize {
                self.len
            }
        }

        unsafe impl StableBufMut for AlignedBuf {
            fn stable_ptr_mut(&mut self) -> *mut u8 {
                self.ptr.as_ptr()
            }

            fn bytes_remaining(&self) -> usize {
                self.len
            }

            unsafe fn set_init(&mut self, _: usize) {}
        }
    }

    #[cfg(not(target_os = "linux"))]
    mod blocking {
        use std::fs::File;
        use std::io::{self, Read, Seek, SeekFrom, Write};
        use std::path::Path;
        use std::sync::Mutex;

        pub type BlockBuf = Vec<u8>;

        #[derive(Debug)]
        pub struct BlockFile {
            file: Mutex<File>,
            block_size: usize,
        }

        impl BlockFile {
            pub async fn open(path: &Path, total_len: u64, block_size: usize) -> io::Result<Self> {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path)?;
                if file.metadata()?.len() < total_len {
                    file.set_len(total_len)?;
                }
                Ok(Self {
                    file: Mutex::new(file),
                    block_size,
                })
            }

            pub async fn read_block(&self, block: usize) -> io::Result<Vec<u8>> {
                let mut buf = vec![0; self.block_size];
                let mut file = self
                    .file
                    .lock()
                    .map_err(|_| io::Error::other("blocking file mutex poisoned"))?;
                file.seek(SeekFrom::Start(self.offset(block)))?;
                file.read_exact(&mut buf)?;
                Ok(buf)
            }

            pub async fn write_block(&self, block: usize, bytes: BlockBuf) -> io::Result<BlockBuf> {
                if bytes.len() != self.block_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "block write length mismatch: expected {}, got {}",
                            self.block_size,
                            bytes.len()
                        ),
                    ));
                }

                let mut file = self
                    .file
                    .lock()
                    .map_err(|_| io::Error::other("blocking file mutex poisoned"))?;
                file.seek(SeekFrom::Start(self.offset(block)))?;
                file.write_all(&bytes)?;
                file.sync_data()?;
                Ok(bytes)
            }

            fn offset(&self, block: usize) -> u64 {
                (block * self.block_size) as u64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;

    fn with_test_env<U, F>(f: impl FnOnce(std::path::PathBuf) -> F) -> U
    where
        F: Future<Output = U>,
    {
        with_test_env_inner(f)
    }

    #[cfg(target_os = "linux")]
    fn with_test_env_inner<U, F>(f: impl FnOnce(std::path::PathBuf) -> F) -> U
    where
        F: Future<Output = U>,
    {
        let builder = io_uring::IoUring::builder();
        let driver = norn_uring::Driver::new(builder, 64).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        with_test_dir(|root| ex.block_on(f(root)))
    }

    #[cfg(not(target_os = "linux"))]
    fn with_test_env_inner<U, F>(f: impl FnOnce(std::path::PathBuf) -> F) -> U
    where
        F: Future<Output = U>,
    {
        with_test_dir(|root| block_on_ready(f(root)))
    }

    fn with_test_dir<U>(f: impl FnOnce(std::path::PathBuf) -> U) -> U {
        let root = std::env::temp_dir().join(format!(
            "norn-kv-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let result = f(root.clone());
        let _ = std::fs::remove_dir_all(root);
        result
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
            Poll::Pending => panic!("blocking norn-kv test future unexpectedly yielded"),
        }
    }

    fn test_config(slot_count: usize) -> StoreConfig {
        StoreConfig {
            slot_count,
            slot_size: DEFAULT_SLOT_SIZE,
        }
    }

    #[test]
    fn put_get_delete() -> std::result::Result<(), Box<dyn std::error::Error>> {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let mut store = Store::open(&path, test_config(4)).await?;
            let key = store.put(b"hello".to_vec()).await?;

            assert_eq!(store.get(key).await?, Some(b"hello".to_vec()));
            assert!(store.delete(key).await?);
            assert_eq!(store.get(key).await?, None);
            assert!(!store.delete(key).await?);

            Ok(())
        })
    }

    #[test]
    fn reuse_after_delete_changes_key() -> std::result::Result<(), Box<dyn std::error::Error>> {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let mut store = Store::open(&path, test_config(1)).await?;
            let old = store.put(b"old".to_vec()).await?;
            assert!(store.delete(old).await?);

            let new = store.put(b"new".to_vec()).await?;
            assert_ne!(old, new);
            assert_eq!(store.get(old).await?, None);
            assert_eq!(store.get(new).await?, Some(b"new".to_vec()));

            Ok(())
        })
    }

    #[test]
    fn recovery_rebuilds_live_and_free_slots() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let config = test_config(3);
            let (deleted, live) = {
                let mut store = Store::open(&path, config).await?;
                let deleted = store.put(b"deleted".to_vec()).await?;
                let live = store.put(b"live".to_vec()).await?;
                assert!(store.delete(deleted).await?);
                (deleted, live)
            };

            let mut recovered = Store::open(&path, config).await?;
            assert_eq!(recovered.get(deleted).await?, None);
            assert_eq!(recovered.get(live).await?, Some(b"live".to_vec()));

            let reused = recovered.put(b"reused".to_vec()).await?;
            assert_ne!(deleted, reused);
            assert_eq!(recovered.get(reused).await?, Some(b"reused".to_vec()));

            Ok(())
        })
    }

    #[test]
    fn corrupt_header_recovers_as_free() -> std::result::Result<(), Box<dyn std::error::Error>> {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let config = test_config(1);
            let key = {
                let mut store = Store::open(&path, config).await?;
                store.put(b"live".to_vec()).await?
            };

            {
                use std::io::{Seek, SeekFrom, Write};

                let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(b"X")?;
                file.sync_all()?;
            }

            let mut recovered = Store::open(&path, config).await?;
            assert_eq!(recovered.get(key).await?, None);
            let replacement = recovered.put(b"replacement".to_vec()).await?;
            assert_eq!(
                recovered.get(replacement).await?,
                Some(b"replacement".to_vec())
            );

            Ok(())
        })
    }

    #[test]
    fn corrupt_payload_recovers_as_free() -> std::result::Result<(), Box<dyn std::error::Error>> {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let config = test_config(1);
            let key = {
                let mut store = Store::open(&path, config).await?;
                store.put(b"live".to_vec()).await?
            };

            {
                use std::io::{Seek, SeekFrom, Write};

                let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
                file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
                file.write_all(b"X")?;
                file.sync_all()?;
            }

            let mut recovered = Store::open(&path, config).await?;
            assert_eq!(recovered.get(key).await?, None);
            let replacement = recovered.put(b"replacement".to_vec()).await?;
            assert_ne!(key, replacement);
            assert_eq!(
                recovered.get(replacement).await?,
                Some(b"replacement".to_vec())
            );

            Ok(())
        })
    }

    #[test]
    fn oversized_value_fails() -> std::result::Result<(), Box<dyn std::error::Error>> {
        with_test_env(|root| async move {
            let path = root.join("kv.dat");
            let mut store = Store::open(&path, test_config(1)).await?;
            let value = vec![0; store.payload_capacity() + 1];

            assert!(matches!(
                store.put(value).await,
                Err(Error::ValueTooLarge { .. })
            ));

            Ok(())
        })
    }
}
