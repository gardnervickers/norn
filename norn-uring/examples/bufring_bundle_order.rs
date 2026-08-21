//! Direct-kernel probe for provided-buffer ring ordering assumptions.
//!
//! This intentionally bypasses Norn's operation and buffer-ring accounting. It
//! checks whether normal (non-`IOSQE_ASYNC`) receive completions expose buffer
//! selections in the same global order in which one shared buffer ring is
//! consumed. The oversized `MSG_TRUNC` edge case is opt-in because it probes a
//! deliberately inconsistent byte-count/buffer-count boundary.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("bufring_bundle_order is only supported on Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    if let Err(err) = linux::run() {
        eprintln!("FAIL: {err}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::VecDeque;
    use std::io::{self, Write};
    use std::net::{Shutdown, TcpListener, TcpStream, UdpSocket};
    use std::os::fd::AsRawFd;
    use std::ptr::{self, NonNull};
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::time::{Duration, Instant};

    use io_uring::{cqueue, opcode, squeue, types, IoUring};

    const BGID: u16 = 0x6e10;
    const RING_ENTRIES: u16 = 16;
    const BUFFER_LEN: usize = 256;
    const CONCURRENT_OPS: usize = 8;
    const DEFAULT_ITERATIONS: usize = 256;

    #[derive(Clone, Copy, Debug)]
    enum ReceiveKind {
        Bundle,
        Scalar,
    }

    #[derive(Debug)]
    struct ProbeSummary {
        iterations: usize,
        completions: usize,
    }

    #[derive(Debug)]
    struct Options {
        iterations: usize,
        probe_msg_trunc: bool,
    }

    pub(super) fn run() -> io::Result<()> {
        let options = parse_options()?;
        let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|version| version.trim().to_owned())
            .unwrap_or_else(|_| "unknown".to_owned());

        let uring = match IoUring::new(64) {
            Ok(uring) => uring,
            Err(err) if err.raw_os_error() == Some(libc::EPERM) => {
                println!("SKIP: io_uring is unavailable in this environment: {err}");
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if !uring.params().is_feature_recvsend_bundle() {
            println!("SKIP: kernel {kernel} does not advertise recv/send bundle support");
            return Ok(());
        }

        let pool = ProvidedRing::new(RING_ENTRIES, BUFFER_LEN)?;
        if let Err(err) = pool.register(&uring) {
            if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP | libc::ENOSYS)) {
                println!("SKIP: provided buffer rings are unavailable: {err}");
                return Ok(());
            }
            return Err(err);
        }
        let mut harness = Harness {
            uring,
            pool,
            registered: true,
        };

        let probe_result: io::Result<()> = (|| {
            harness.pool.publish_initial()?;

            let shared =
                run_order_probe(&mut harness, options.iterations, "shared bundles", |_| {
                    ReceiveKind::Bundle
                })?;
            println!(
                "PASS: {} iterations / {} CQEs from concurrent bundle receives shared one ring",
                shared.iterations, shared.completions
            );

            let mixed = run_order_probe(
                &mut harness,
                options.iterations,
                "mixed scalar and bundle",
                |index| {
                    if index % 2 == 0 {
                        ReceiveKind::Scalar
                    } else {
                        ReceiveKind::Bundle
                    }
                },
            )?;
            println!(
                "PASS: {} iterations / {} CQEs from mixed scalar and bundle receives shared one ring",
                mixed.iterations, mixed.completions
            );

            let max_bundle = run_multibuffer_layout_probe(&mut harness)?;
            println!(
                "PASS: multi-buffer bundles followed publication order (largest bundle: {max_bundle} buffers)"
            );

            run_waitall_reordering_probe(&mut harness)?;
            if options.probe_msg_trunc {
                run_msg_trunc_probe(&mut harness)?;
            } else {
                println!(
                    "SKIP: oversized MSG_TRUNC edge probe (pass --msg-trunc to run it explicitly)"
                );
            }
            Ok(())
        })();

        // On failure, field order drops the io_uring before the kernel-visible
        // mapping, safely terminating any operation left outstanding by a probe.
        probe_result?;
        match harness.unregister() {
            Err(err) => Err(io::Error::other(format!(
                "probe passed but unregistering buffer ring failed: {err}"
            ))),
            Ok(()) => {
                println!("DONE: completed enabled direct-kernel probes on {kernel}");
                println!(
                    "NOTE: this is evidence for the tested kernel path, not a substitute for a UAPI guarantee"
                );
                Ok(())
            }
        }
    }

    fn parse_options() -> io::Result<Options> {
        let mut args = std::env::args().skip(1);
        let mut iterations = None;
        let mut probe_msg_trunc = false;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--iterations" => {
                    let value = args.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "--iterations needs a value")
                    })?;
                    set_iterations(&mut iterations, value)?;
                }
                "--msg-trunc" if !probe_msg_trunc => probe_msg_trunc = true,
                value if !value.starts_with('-') => {
                    set_iterations(&mut iterations, value.to_owned())?;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "usage: bufring_bundle_order [--iterations N | N] [--msg-trunc]",
                    ));
                }
            }
        }
        Ok(Options {
            iterations: iterations.unwrap_or(DEFAULT_ITERATIONS),
            probe_msg_trunc,
        })
    }

    fn set_iterations(iterations: &mut Option<usize>, value: String) -> io::Result<()> {
        if iterations.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "iteration count was provided more than once",
            ));
        }
        let parsed = value.parse::<usize>().map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid iteration count {value:?}: {err}"),
            )
        })?;
        if parsed == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "iteration count must be nonzero",
            ));
        }
        *iterations = Some(parsed);
        Ok(())
    }

    fn run_order_probe(
        harness: &mut Harness,
        iterations: usize,
        label: &str,
        kind: impl Fn(usize) -> ReceiveKind,
    ) -> io::Result<ProbeSummary> {
        let pairs = (0..CONCURRENT_OPS)
            .map(|_| tcp_pair())
            .collect::<io::Result<Vec<_>>>()?;
        let (readers, mut writers): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();

        for iteration in 0..iterations {
            let entries = readers
                .iter()
                .enumerate()
                .map(|(index, reader)| {
                    let user_data = (index + 1) as u64;
                    match kind(index) {
                        ReceiveKind::Bundle => {
                            opcode::RecvBundle::new(types::Fd(reader.as_raw_fd()), BGID)
                                .build()
                                .user_data(user_data)
                        }
                        ReceiveKind::Scalar => {
                            opcode::Recv::new(types::Fd(reader.as_raw_fd()), ptr::null_mut(), 0)
                                .buf_group(BGID)
                                .build()
                                .flags(squeue::Flags::BUFFER_SELECT)
                                .user_data(user_data)
                        }
                    }
                })
                .collect::<Vec<_>>();
            push_all(&mut harness.uring, &entries)?;
            harness.uring.submit()?;

            let mut write_order = (0..CONCURRENT_OPS).collect::<Vec<_>>();
            write_order.rotate_left(iteration % CONCURRENT_OPS);
            if iteration % 2 == 1 {
                write_order.reverse();
            }
            for index in write_order {
                writers[index].write_all(&[payload_marker(iteration, index)])?;
            }

            let cqes = wait_for_cqes(&mut harness.uring, CONCURRENT_OPS)?;
            let mut selected = Vec::with_capacity(CONCURRENT_OPS);
            for (cqe_index, cqe) in cqes.into_iter().enumerate() {
                let op_index = cqe
                    .user_data()
                    .checked_sub(1)
                    .and_then(|value| usize::try_from(value).ok())
                    .filter(|index| *index < CONCURRENT_OPS)
                    .ok_or_else(|| {
                        io::Error::other(format!(
                            "{label}: completion {cqe_index} had unknown user_data {}",
                            cqe.user_data()
                        ))
                    })?;
                if cqe.result() != 1 {
                    return Err(io::Error::other(format!(
                        "{label}: iteration {iteration}, completion {cqe_index}, operation {op_index} returned {} instead of 1",
                        cqe.result()
                    )));
                }
                let actual_bid = selected_bid(&cqe, label)?;
                let expected_bid = harness.pool.consume_expected(1, actual_bid, label)?[0];
                let actual_byte = harness.pool.buffers[usize::from(expected_bid)][0];
                let expected_byte = payload_marker(iteration, op_index);
                if actual_byte != expected_byte {
                    return Err(io::Error::other(format!(
                        "{label}: iteration {iteration}, CQE {cqe_index} selected BID {actual_bid} for operation {op_index}, but the buffer held byte {actual_byte:#04x} instead of {expected_byte:#04x}"
                    )));
                }
                selected.push(expected_bid);
            }

            let selected_len = selected.len();
            selected.rotate_left(iteration % selected_len);
            selected.reverse();
            harness.pool.publish(&selected)?;
        }

        Ok(ProbeSummary {
            iterations,
            completions: iterations * CONCURRENT_OPS,
        })
    }

    fn run_multibuffer_layout_probe(harness: &mut Harness) -> io::Result<usize> {
        let (reader, mut writer) = tcp_pair()?;
        // Keep recycling each claimed range so legal TCP short reads cannot
        // exhaust the ring. Several ring rotations also exercise arbitrary
        // return order rather than just the initial publication sequence.
        let payload = stream_payload(harness.pool.capacity_bytes() * 4 + BUFFER_LEN / 2, 7);
        writer.write_all(&payload)?;

        let mut received = Vec::with_capacity(payload.len());
        let mut max_bundle = 0;
        while received.len() < payload.len() {
            let entry = opcode::RecvBundle::new(types::Fd(reader.as_raw_fd()), BGID)
                .build()
                .user_data(1);
            push_all(&mut harness.uring, &[entry])?;
            let cqe = wait_for_cqes(&mut harness.uring, 1)?
                .pop()
                .expect("wait_for_cqes returned one CQE");
            let result = positive_result(&cqe, "multi-buffer layout")?;
            let count = result.div_ceil(BUFFER_LEN);
            let first_bid = selected_bid(&cqe, "multi-buffer layout")?;
            let bids = harness
                .pool
                .consume_expected(count, first_bid, "multi-buffer layout")?;
            max_bundle = max_bundle.max(bids.len());

            let mut remaining = result;
            for bid in &bids {
                let len = remaining.min(BUFFER_LEN);
                received.extend_from_slice(&harness.pool.buffers[usize::from(*bid)][..len]);
                remaining -= len;
            }

            let mut returned = bids;
            returned.reverse();
            harness.pool.publish(&returned)?;
        }
        if received != payload {
            return Err(io::Error::other(
                "multi-buffer layout: reconstructed bytes did not match the TCP stream",
            ));
        }
        if max_bundle < 2 {
            return Err(io::Error::other(
                "multi-buffer layout: kernel never returned more than one buffer in a bundle",
            ));
        }
        Ok(max_bundle)
    }

    fn run_waitall_reordering_probe(harness: &mut Harness) -> io::Result<()> {
        let expected_a = harness.pool.available[0];
        let expected_b = harness.pool.available[1];
        let (a_reader, mut a_writer) = tcp_pair()?;
        let (b_reader, mut b_writer) = tcp_pair()?;

        a_writer.write_all(&[0xaa])?;
        wait_for_socket_bytes(&a_reader, 1)?;
        let a = opcode::RecvBundle::new(types::Fd(a_reader.as_raw_fd()), BGID)
            .flags(libc::MSG_WAITALL)
            .build()
            .user_data(0xa);
        push_all(&mut harness.uring, &[a])?;
        harness.uring.submit()?;
        wait_for_socket_bytes(&a_reader, 0)?;

        b_writer.write_all(&vec![0xbb; BUFFER_LEN])?;
        let b = opcode::Recv::new(
            types::Fd(b_reader.as_raw_fd()),
            ptr::null_mut(),
            BUFFER_LEN as u32,
        )
        .buf_group(BGID)
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(0xb);
        push_all(&mut harness.uring, &[b])?;
        let first = wait_for_cqe_with_timeout(&mut harness.uring, "MSG_WAITALL later receive")?;
        if first.user_data() != 0xb
            || first.result() != BUFFER_LEN as i32
            || selected_bid(&first, "MSG_WAITALL later receive")? != expected_b
        {
            return Err(io::Error::other(format!(
                "MSG_WAITALL counterexample: expected later scalar receive first with BID {expected_b}, got user_data={:#x}, res={}, bid={:?}, flags={:#x}",
                first.user_data(),
                first.result(),
                cqueue::buffer_select(first.flags()),
                first.flags()
            )));
        }

        a_writer.write_all(&vec![0xaa; BUFFER_LEN - 1])?;
        a_writer.shutdown(Shutdown::Write)?;
        b_writer.shutdown(Shutdown::Write)?;
        let second = wait_for_cqe_with_timeout(&mut harness.uring, "MSG_WAITALL earlier receive")?;
        if second.user_data() != 0xa
            || second.result() != BUFFER_LEN as i32
            || selected_bid(&second, "MSG_WAITALL earlier receive")? != expected_a
        {
            return Err(io::Error::other(format!(
                "MSG_WAITALL counterexample: expected earlier WAITALL receive second with BID {expected_a}, got user_data={:#x}, res={}, bid={:?}, flags={:#x}",
                second.user_data(),
                second.result(),
                cqueue::buffer_select(second.flags()),
                second.flags()
            )));
        }
        if !harness.pool.buffers[usize::from(expected_a)]
            .iter()
            .all(|byte| *byte == 0xaa)
            || !harness.pool.buffers[usize::from(expected_b)]
                .iter()
                .all(|byte| *byte == 0xbb)
        {
            return Err(io::Error::other(
                "MSG_WAITALL counterexample: selected buffer contents did not match their operations",
            ));
        }

        harness
            .pool
            .consume_expected(1, expected_a, "MSG_WAITALL consumption")?;
        harness
            .pool
            .consume_expected(1, expected_b, "later scalar consumption")?;
        harness.pool.publish(&[expected_b, expected_a])?;
        println!(
            "PASS: observed permitted MSG_WAITALL counterexample: ring consumed BIDs [{expected_a}, {expected_b}], but CQ order was [{expected_b}, {expected_a}]"
        );
        Ok(())
    }

    fn run_msg_trunc_probe(harness: &mut Harness) -> io::Result<()> {
        let receiver = UdpSocket::bind(("127.0.0.1", 0))?;
        let sender = UdpSocket::bind(("127.0.0.1", 0))?;
        sender.connect(receiver.local_addr()?)?;

        let entry = opcode::RecvBundle::new(types::Fd(receiver.as_raw_fd()), BGID)
            .flags(libc::MSG_TRUNC)
            .build()
            .user_data(1);
        push_all(&mut harness.uring, &[entry])?;
        harness.uring.submit()?;

        let datagram_len = harness.pool.capacity_bytes() * 2;
        let datagram = vec![0x5c; datagram_len];
        let sent = sender.send(&datagram)?;
        if sent != datagram_len {
            return Err(io::Error::other(format!(
                "MSG_TRUNC probe sent {sent} of {datagram_len} bytes"
            )));
        }

        let cqe = wait_for_cqes(&mut harness.uring, 1)?
            .pop()
            .expect("wait_for_cqes returned one CQE");
        if cqe.result() < 0 {
            let err = io::Error::from_raw_os_error(-cqe.result());
            if matches!(
                err.raw_os_error(),
                Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
            ) {
                println!("SKIP: this kernel rejects UDP bundle + MSG_TRUNC: {err}");
                return Ok(());
            }
            return Err(io::Error::other(format!(
                "MSG_TRUNC probe receive failed: {err}"
            )));
        }

        let result = cqe.result() as usize;
        let _ = selected_bid(&cqe, "MSG_TRUNC")?;
        let inferred = result.div_ceil(BUFFER_LEN);
        if result != datagram_len || inferred <= RING_ENTRIES as usize {
            return Err(io::Error::other(format!(
                "MSG_TRUNC probe expected a reported {datagram_len}-byte datagram and an impossible inferred count, got res={result}, inferred_buffers={inferred}"
            )));
        }
        println!(
            "PASS: MSG_TRUNC reported {result} bytes over a {RING_ENTRIES}-buffer ring; ceil(res / buffer_len) incorrectly implies {inferred} consumed buffers"
        );
        Ok(())
    }

    fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let writer = TcpStream::connect(listener.local_addr()?)?;
        let (reader, _) = listener.accept()?;
        writer.set_nodelay(true)?;
        reader.set_nodelay(true)?;
        Ok((reader, writer))
    }

    fn push_all(uring: &mut IoUring, entries: &[squeue::Entry]) -> io::Result<()> {
        let mut submission = uring.submission();
        unsafe {
            submission.push_multiple(entries).map_err(|_| {
                io::Error::other(format!(
                    "submission queue had room for fewer than {} entries",
                    entries.len()
                ))
            })?;
        }
        Ok(())
    }

    fn wait_for_cqes(uring: &mut IoUring, count: usize) -> io::Result<Vec<cqueue::Entry>> {
        let mut entries = Vec::with_capacity(count);
        while entries.len() < count {
            let timeout = types::Timespec::from(Duration::from_secs(5));
            let args = types::SubmitArgs::new().timespec(&timeout);
            uring
                .submitter()
                .submit_with_args(count - entries.len(), &args)?;
            let mut completion = uring.completion();
            entries.extend(completion.by_ref().take(count - entries.len()));
        }
        Ok(entries)
    }

    fn wait_for_cqe_with_timeout(uring: &mut IoUring, label: &str) -> io::Result<cqueue::Entry> {
        let timeout = types::Timespec::from(Duration::from_secs(5));
        let args = types::SubmitArgs::new().timespec(&timeout);
        uring
            .submitter()
            .submit_with_args(1, &args)
            .map_err(|err| io::Error::other(format!("{label}: timed wait failed: {err}")))?;
        uring
            .completion()
            .next()
            .ok_or_else(|| io::Error::other(format!("{label}: wait returned without a CQE")))
    }

    fn wait_for_socket_bytes(socket: &TcpStream, expected: i32) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut available = 0;
            let result = unsafe { libc::ioctl(socket.as_raw_fd(), libc::FIONREAD, &mut available) };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            if (expected == 0 && available == 0) || (expected != 0 && available >= expected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "socket did not reach FIONREAD={expected}; last observed {available}"
                )));
            }
            std::thread::yield_now();
        }
    }

    fn selected_bid(cqe: &cqueue::Entry, label: &str) -> io::Result<u16> {
        cqueue::buffer_select(cqe.flags()).ok_or_else(|| {
            io::Error::other(format!(
                "{label}: successful CQE for user_data {} had no selected BID (flags={:#x})",
                cqe.user_data(),
                cqe.flags()
            ))
        })
    }

    fn positive_result(cqe: &cqueue::Entry, label: &str) -> io::Result<usize> {
        if cqe.result() <= 0 {
            let detail = if cqe.result() < 0 {
                io::Error::from_raw_os_error(-cqe.result()).to_string()
            } else {
                "zero-byte receive".to_owned()
            };
            return Err(io::Error::other(format!(
                "{label}: receive failed: {detail}"
            )));
        }
        Ok(cqe.result() as usize)
    }

    fn payload_marker(iteration: usize, operation: usize) -> u8 {
        (iteration as u8)
            .wrapping_mul(17)
            .wrapping_add(operation as u8)
            .wrapping_add(1)
    }

    fn stream_payload(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let block = (index / BUFFER_LEN) as u8;
                let offset = (index % BUFFER_LEN) as u8;
                seed.wrapping_add(block.wrapping_mul(61))
                    .wrapping_add(offset.wrapping_mul(17))
            })
            .collect()
    }

    struct Harness {
        // Keep the io_uring first so it is dropped before the kernel-visible
        // provided-ring mapping if an error or panic bypasses explicit unregister.
        uring: IoUring,
        pool: ProvidedRing,
        registered: bool,
    }

    impl Harness {
        fn unregister(&mut self) -> io::Result<()> {
            if !self.registered {
                return Ok(());
            }
            self.uring.submitter().unregister_buf_ring(BGID)?;
            self.registered = false;
            Ok(())
        }
    }

    struct ProvidedRing {
        mapping: NonNull<libc::c_void>,
        mapping_len: usize,
        entries: u16,
        mask: u16,
        buffer_len: usize,
        buffers: Vec<Box<[u8]>>,
        local_tail: u16,
        available: VecDeque<u16>,
    }

    impl ProvidedRing {
        fn new(entries: u16, buffer_len: usize) -> io::Result<Self> {
            if entries == 0 || !entries.is_power_of_two() || buffer_len == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ring entries must be a nonzero power of two and buffer length must be nonzero",
                ));
            }
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if page_size <= 0 {
                return Err(io::Error::last_os_error());
            }
            let page_size = page_size as usize;
            let bytes = usize::from(entries) * std::mem::size_of::<types::BufRingEntry>();
            let mapping_len = bytes.div_ceil(page_size) * page_size;
            let mapping = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    mapping_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            if mapping == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            let mapping = NonNull::new(mapping).expect("MAP_FAILED was handled above");
            let buffers = (0..entries)
                .map(|_| vec![0; buffer_len].into_boxed_slice())
                .collect();
            Ok(Self {
                mapping,
                mapping_len,
                entries,
                mask: entries - 1,
                buffer_len,
                buffers,
                local_tail: 0,
                available: VecDeque::with_capacity(entries as usize),
            })
        }

        fn register(&self, uring: &IoUring) -> io::Result<()> {
            unsafe {
                uring.submitter().register_buf_ring_with_flags(
                    self.mapping.as_ptr() as u64,
                    self.entries,
                    BGID,
                    0,
                )
            }
        }

        fn publish_initial(&mut self) -> io::Result<()> {
            let order = (0..self.entries)
                .map(|index| (index.wrapping_mul(5).wrapping_add(7)) & self.mask)
                .collect::<Vec<_>>();
            self.publish(&order)
        }

        fn publish(&mut self, bids: &[u16]) -> io::Result<()> {
            if self.available.len() + bids.len() > self.entries as usize {
                return Err(io::Error::other(format!(
                    "publishing {} BIDs would make {} entries available in a {}-entry ring",
                    bids.len(),
                    self.available.len() + bids.len(),
                    self.entries
                )));
            }
            let mut pending = Vec::with_capacity(bids.len());
            for &bid in bids {
                if bid >= self.entries {
                    return Err(io::Error::other(format!(
                        "cannot publish out-of-range BID {bid}"
                    )));
                }
                if self.available.contains(&bid) || pending.contains(&bid) {
                    return Err(io::Error::other(format!("cannot publish BID {bid} twice")));
                }
                pending.push(bid);
            }

            let entries = self.mapping.as_ptr().cast::<types::BufRingEntry>();
            for &bid in bids {
                let ring_index = self.local_tail & self.mask;
                let entry = unsafe { &mut *entries.add(ring_index as usize) };
                entry.set_addr(self.buffers[usize::from(bid)].as_ptr() as u64);
                entry.set_len(self.buffer_len as u32);
                entry.set_bid(bid);
                self.local_tail = self.local_tail.wrapping_add(1);
                self.available.push_back(bid);
            }
            let shared_tail = unsafe { types::BufRingEntry::tail(entries) }.cast::<AtomicU16>();
            unsafe {
                (*shared_tail).store(self.local_tail, Ordering::Release);
            }
            Ok(())
        }

        fn consume_expected(
            &mut self,
            count: usize,
            actual_first: u16,
            label: &str,
        ) -> io::Result<Vec<u16>> {
            if count == 0 || count > self.available.len() {
                return Err(io::Error::other(format!(
                    "{label}: CQE implies {count} consumed buffers but {} were modeled as available",
                    self.available.len()
                )));
            }
            let expected_first = self.available.front().copied().expect("count was nonzero");
            if actual_first != expected_first {
                let next = self.available.iter().copied().take(8).collect::<Vec<_>>();
                return Err(io::Error::other(format!(
                    "{label}: CQ order diverged from ring consumption order: CQE first BID {actual_first}, expected {expected_first}; next modeled publications: {next:?}"
                )));
            }
            Ok((0..count)
                .map(|_| {
                    self.available
                        .pop_front()
                        .expect("count was bounds checked")
                })
                .collect())
        }

        fn capacity_bytes(&self) -> usize {
            self.entries as usize * self.buffer_len
        }
    }

    impl Drop for ProvidedRing {
        fn drop(&mut self) {
            let result = unsafe { libc::munmap(self.mapping.as_ptr(), self.mapping_len) };
            debug_assert_eq!(result, 0);
        }
    }
}
