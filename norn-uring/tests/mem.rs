#![cfg(target_os = "linux")]

use std::io::Write;

use norn_uring::buf::StableBuf;
use norn_uring::mem::{self, Advice};

mod util;

/// An anonymous or file-backed memory mapping exposed as a stable buffer.
struct Mapping {
    ptr: *mut u8,
    len: usize,
}

impl Mapping {
    fn anonymous(len: usize) -> Self {
        Self::map(len, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1)
    }

    fn file(file: &std::fs::File, len: usize) -> Self {
        use std::os::fd::AsRawFd;
        Self::map(len, libc::MAP_SHARED, file.as_raw_fd())
    }

    fn map(len: usize, flags: libc::c_int, fd: libc::c_int) -> Self {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED, "{}", std::io::Error::last_os_error());
        Self {
            ptr: ptr.cast(),
            len,
        }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        assert_eq!(unsafe { libc::munmap(self.ptr.cast(), self.len) }, 0);
    }
}

// Safety: the mapping stays at a fixed address until the wrapper is dropped,
// and mapped pages are always initialized.
unsafe impl StableBuf for Mapping {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn bytes_init(&self) -> usize {
        self.len
    }
}

fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

/// Return the `VmFlags` of the mapping containing `addr`.
fn vm_flags(addr: usize) -> Vec<String> {
    let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
    let mut in_mapping = false;
    for line in smaps.lines() {
        if let Some((range, _)) = line.split_once(' ') {
            if let Some((start, end)) = range.split_once('-') {
                if let (Ok(start), Ok(end)) = (
                    usize::from_str_radix(start, 16),
                    usize::from_str_radix(end, 16),
                ) {
                    in_mapping = (start..end).contains(&addr);
                    continue;
                }
            }
        }
        if in_mapping {
            if let Some(flags) = line.strip_prefix("VmFlags:") {
                return flags.split_whitespace().map(str::to_owned).collect();
            }
        }
    }
    panic!("no mapping contains {addr:#x}");
}

fn madvise_supported() -> bool {
    let supported = util::opcode_supported(io_uring::opcode::Madvise::CODE);
    if !supported {
        eprintln!("skipping: IORING_OP_MADVISE is unavailable");
    }
    supported
}

#[test]
fn advise_applies_access_pattern_to_mapping() -> Result<(), Box<dyn std::error::Error>> {
    if !madvise_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mapping = Mapping::anonymous(page_size() * 4);
        let addr = mapping.ptr as usize;

        let (result, mapping) = mem::advise(mapping, Advice::Random).await;
        result?;
        let flags = vm_flags(addr);
        assert!(flags.iter().any(|flag| flag == "rr"), "{flags:?}");

        let (result, mapping) = mem::advise(mapping, Advice::Sequential).await;
        result?;
        let flags = vm_flags(addr);
        assert!(flags.iter().any(|flag| flag == "sr"), "{flags:?}");
        assert!(!flags.iter().any(|flag| flag == "rr"), "{flags:?}");

        let (result, mapping) = mem::advise(mapping, Advice::Normal).await;
        result?;
        let flags = vm_flags(addr);
        assert!(
            !flags.iter().any(|flag| flag == "sr" || flag == "rr"),
            "{flags:?}"
        );
        drop(mapping);
        Ok(())
    })
}

#[test]
fn advise_will_need_prefetches_file_mapping() -> Result<(), Box<dyn std::error::Error>> {
    if !madvise_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let len = page_size() * 8;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.join("mapped"))?;
        file.write_all(&vec![0x5a; len])?;
        let mapping = Mapping::file(&file, len);

        let (result, mapping) = mem::advise(mapping, Advice::WillNeed).await;
        result?;

        // Advice never changes the mapped contents.
        assert!(mapping.as_slice().iter().all(|byte| *byte == 0x5a));
        Ok(())
    })
}

#[test]
fn advise_preserves_heap_buffer_and_skips_partial_pages() -> Result<(), Box<dyn std::error::Error>>
{
    if !madvise_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        // Fewer bytes than one page cannot contain a whole page.
        let small = vec![7_u8; 100];
        let (result, small) = mem::advise(small, Advice::WillNeed).await;
        result?;
        assert_eq!(small, vec![7_u8; 100]);

        let large = vec![9_u8; page_size() * 3];
        let (result, large) = mem::advise(large, Advice::Sequential).await;
        result?;
        assert!(large.iter().all(|byte| *byte == 9));
        Ok(())
    })
}
