#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use futures_util::stream::FuturesUnordered;
    use futures_util::StreamExt;
    use norn_executor::LocalExecutor;
    use norn_kv_server::protocol::{
        HEADER_LEN, OP_GET, OP_NOOP, OP_SET, REQUEST_MAGIC, RESPONSE_MAGIC,
    };
    use norn_uring::buf::{BufCursor, StableBuf, StableBufMut};
    use norn_uring::net::TcpSocket;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Workload {
        Noop,
        Get,
        Set,
    }

    impl Workload {
        fn parse(raw: &str) -> io::Result<Self> {
            match raw {
                "noop" => Ok(Self::Noop),
                "get" => Ok(Self::Get),
                "set" => Ok(Self::Set),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown workload '{raw}', expected noop|get|set"),
                )),
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::Noop => "noop",
                Self::Get => "get",
                Self::Set => "set",
            }
        }

        fn opcode(self) -> u8 {
            match self {
                Self::Noop => OP_NOOP,
                Self::Get => OP_GET,
                Self::Set => OP_SET,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct Config {
        address: SocketAddr,
        threads: usize,
        connections_per_thread: usize,
        warmup_requests: usize,
        requests: usize,
        sample_every: usize,
        value_size: usize,
        ring_entries: u32,
        workload: Workload,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                address: "127.0.0.1:11211".parse().unwrap(),
                threads: 8,
                connections_per_thread: 16,
                warmup_requests: 5_000,
                requests: 100_000,
                sample_every: 1_024,
                value_size: 64,
                ring_entries: 256,
                workload: Workload::Noop,
            }
        }
    }

    #[derive(Debug, Default)]
    struct WorkerResult {
        requests: usize,
        latency_ns: Vec<u64>,
    }

    struct Lane {
        socket: TcpSocket,
        request: Vec<u8>,
        response_header: Option<RecvBuffer>,
        response_body: Option<RecvBuffer>,
        workload: Workload,
        value_size: usize,
        opaque: u32,
    }

    impl Lane {
        async fn connect(
            address: SocketAddr,
            workload: Workload,
            worker: usize,
            connection: usize,
            value_size: usize,
        ) -> io::Result<Self> {
            let socket = TcpSocket::connect(address).await?;
            socket.set_nodelay(true).await?;
            let key = format!("bench-{worker}-{connection}").into_bytes();
            let value = vec![0x5a_u8; value_size];
            let response_capacity = value_size.saturating_add(256).max(256);
            let mut lane = Self {
                socket,
                request: build_request(workload, &key, &value, 0),
                response_header: Some(RecvBuffer::new(HEADER_LEN)),
                response_body: Some(RecvBuffer::new(response_capacity)),
                workload,
                value_size,
                opaque: 0,
            };

            if workload != Workload::Noop {
                let preload = build_request(Workload::Set, &key, &value, 0);
                lane.workload = Workload::Set;
                lane.request = preload;
                lane.round_trip().await?;
                lane.workload = workload;
                lane.request = build_request(workload, &key, &value, 0);
            }
            Ok(lane)
        }

        async fn warmup(mut self, requests: usize) -> io::Result<Self> {
            for _ in 0..requests {
                self.round_trip().await?;
            }
            Ok(self)
        }

        async fn measure(
            mut self,
            requests: usize,
            sample_every: usize,
        ) -> io::Result<WorkerResult> {
            let sample_capacity = requests.div_ceil(sample_every);
            let mut result = WorkerResult {
                requests,
                latency_ns: Vec::with_capacity(sample_capacity),
            };
            for request in 0..requests {
                if request % sample_every == 0 {
                    let started = Instant::now();
                    self.round_trip().await?;
                    result.latency_ns.push(elapsed_ns(started.elapsed()));
                } else {
                    self.round_trip().await?;
                }
            }
            self.socket.close().await?;
            Ok(result)
        }

        async fn round_trip(&mut self) -> io::Result<()> {
            self.opaque = self.opaque.wrapping_add(1);
            self.request[12..16].copy_from_slice(&self.opaque.to_be_bytes());
            let request = std::mem::take(&mut self.request);
            self.request = send_all(&self.socket, request).await?;

            let mut header = self.response_header.take().unwrap();
            header.reset(HEADER_LEN);
            let header = recv_exact(&self.socket, header).await?;
            let bytes = header.filled_slice();
            if bytes[0] != RESPONSE_MAGIC || bytes[1] != self.workload.opcode() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response magic or opcode mismatch",
                ));
            }
            let key_len = u16::from_be_bytes(bytes[2..4].try_into().unwrap()) as usize;
            let extras_len = bytes[4] as usize;
            let expected_extras_len = usize::from(self.workload == Workload::Get) * 4;
            if key_len != 0 || extras_len != expected_extras_len || bytes[5] != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response key, extras, or data-type field mismatch",
                ));
            }
            let status = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
            if status != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server returned status 0x{status:04x}"),
                ));
            }
            let body_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
            let opaque = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
            let cas = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
            self.response_header = Some(header);
            if opaque != self.opaque || cas != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "response opaque/CAS mismatch: opaque={opaque}, expected={}, cas={cas}",
                        self.opaque
                    ),
                ));
            }
            let mut body = self.response_body.take().unwrap();
            if body_len > body.capacity() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("response body {body_len} exceeds client buffer"),
                ));
            }
            body.reset(body_len);
            if body_len != 0 {
                body = recv_exact(&self.socket, body).await?;
            }
            match self.workload {
                Workload::Noop | Workload::Set if body_len == 0 => {}
                Workload::Get if body_len == self.value_size + 4 => {
                    let body = body.filled_slice();
                    if body[..4] != 0_u32.to_be_bytes()
                        || body[4..].iter().any(|&byte| byte != 0x5a)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "GET flags or value failed verification",
                        ));
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected response body length {body_len}"),
                    ));
                }
            }
            self.response_body = Some(body);
            Ok(())
        }
    }

    pub(crate) fn run() -> Result<(), Box<dyn std::error::Error>> {
        let config = parse_args()?;
        let barrier = Arc::new(Barrier::new(config.threads + 1));
        let mut workers = Vec::with_capacity(config.threads);
        for worker in 0..config.threads {
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || -> io::Result<WorkerResult> {
                let builder = io_uring::IoUring::builder();
                let driver = match norn_uring::Driver::new(builder, config.ring_entries) {
                    Ok(driver) => driver,
                    Err(error) => {
                        barrier.wait();
                        return Err(error);
                    }
                };
                let mut executor = LocalExecutor::new(driver);
                let prepared = executor.block_on(prepare_lanes(config, worker));
                let lanes = match prepared {
                    Ok(lanes) => lanes,
                    Err(error) => {
                        barrier.wait();
                        return Err(error);
                    }
                };
                barrier.wait();
                executor.block_on(measure_lanes(lanes, config.requests, config.sample_every))
            }));
        }

        barrier.wait();
        let started = Instant::now();
        let mut total = WorkerResult::default();
        for worker in workers {
            let result = worker
                .join()
                .map_err(|_| io::Error::other("load worker panicked"))??;
            total.requests += result.requests;
            total.latency_ns.extend(result.latency_ns);
        }
        let elapsed = started.elapsed();
        total.latency_ns.sort_unstable();
        let seconds = elapsed.as_secs_f64();
        let requests_per_second = total.requests as f64 / seconds;
        println!(
            "result workload={} threads={} connections={} value_size={} requests={} elapsed_s={seconds:.6} req_per_sec={requests_per_second:.3} samples={} p50_us={:.3} p95_us={:.3} p99_us={:.3} p999_us={:.3}",
            config.workload.name(),
            config.threads,
            config.threads * config.connections_per_thread,
            config.value_size,
            total.requests,
            total.latency_ns.len(),
            quantile_us(&total.latency_ns, 0.50),
            quantile_us(&total.latency_ns, 0.95),
            quantile_us(&total.latency_ns, 0.99),
            quantile_us(&total.latency_ns, 0.999),
        );
        Ok(())
    }

    async fn prepare_lanes(config: Config, worker: usize) -> io::Result<Vec<Lane>> {
        let mut lanes = Vec::with_capacity(config.connections_per_thread);
        for connection in 0..config.connections_per_thread {
            lanes.push(
                Lane::connect(
                    config.address,
                    config.workload,
                    worker,
                    connection,
                    config.value_size,
                )
                .await?,
            );
        }
        let mut warmups = FuturesUnordered::new();
        for lane in lanes {
            warmups.push(lane.warmup(config.warmup_requests));
        }
        let mut lanes = Vec::with_capacity(config.connections_per_thread);
        while let Some(lane) = warmups.next().await {
            lanes.push(lane?);
        }
        Ok(lanes)
    }

    async fn measure_lanes(
        lanes: Vec<Lane>,
        requests: usize,
        sample_every: usize,
    ) -> io::Result<WorkerResult> {
        let mut pending = FuturesUnordered::new();
        for lane in lanes {
            pending.push(lane.measure(requests, sample_every));
        }
        let mut total = WorkerResult::default();
        while let Some(result) = pending.next().await {
            let result = result?;
            total.requests += result.requests;
            total.latency_ns.extend(result.latency_ns);
        }
        Ok(total)
    }

    fn build_request(workload: Workload, key: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
        let extras_len = usize::from(workload == Workload::Set) * 8;
        let key = if workload == Workload::Noop { &[] } else { key };
        let value = if workload == Workload::Set {
            value
        } else {
            &[]
        };
        let body_len = extras_len + key.len() + value.len();
        let mut request = Vec::with_capacity(HEADER_LEN + body_len);
        request.push(REQUEST_MAGIC);
        request.push(workload.opcode());
        request.extend_from_slice(&(key.len() as u16).to_be_bytes());
        request.push(extras_len as u8);
        request.push(0);
        request.extend_from_slice(&0_u16.to_be_bytes());
        request.extend_from_slice(&(body_len as u32).to_be_bytes());
        request.extend_from_slice(&opaque.to_be_bytes());
        request.extend_from_slice(&0_u64.to_be_bytes());
        if workload == Workload::Set {
            request.extend_from_slice(&0_u32.to_be_bytes());
            request.extend_from_slice(&0_u32.to_be_bytes());
        }
        request.extend_from_slice(key);
        request.extend_from_slice(value);
        request
    }

    async fn send_all(socket: &TcpSocket, buffer: Vec<u8>) -> io::Result<Vec<u8>> {
        let mut cursor = BufCursor::new(buffer);
        while cursor.bytes_init() != 0 {
            let (result, returned) = socket.send(cursor).await;
            cursor = returned;
            let sent = result?;
            if sent == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            cursor.consume(sent);
        }
        Ok(cursor.into_inner())
    }

    async fn recv_exact(socket: &TcpSocket, mut buffer: RecvBuffer) -> io::Result<RecvBuffer> {
        while !buffer.is_full() {
            let (result, returned) = socket.recv(buffer).await;
            buffer = returned;
            if result? == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        }
        Ok(buffer)
    }

    fn elapsed_ns(elapsed: Duration) -> u64 {
        elapsed.as_nanos().min(u64::MAX as u128) as u64
    }

    fn quantile_us(sorted: &[u64], quantile: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
        sorted[index] as f64 / 1_000.0
    }

    #[derive(Debug)]
    struct RecvBuffer {
        storage: Box<[u8]>,
        filled: usize,
        limit: usize,
    }

    impl RecvBuffer {
        fn new(capacity: usize) -> Self {
            Self {
                storage: vec![0_u8; capacity].into_boxed_slice(),
                filled: 0,
                limit: capacity,
            }
        }

        fn reset(&mut self, limit: usize) {
            assert!(limit <= self.storage.len());
            self.filled = 0;
            self.limit = limit;
        }

        fn capacity(&self) -> usize {
            self.storage.len()
        }

        fn is_full(&self) -> bool {
            self.filled == self.limit
        }

        fn filled_slice(&self) -> &[u8] {
            &self.storage[..self.filled]
        }
    }

    // Safety: the exclusively owned box keeps its allocation stable and the
    // exposed writable tail remains within it for the operation lifetime.
    unsafe impl StableBufMut for RecvBuffer {
        fn stable_ptr_mut(&mut self) -> *mut u8 {
            unsafe { self.storage.as_mut_ptr().add(self.filled) }
        }

        fn bytes_remaining(&self) -> usize {
            self.limit - self.filled
        }

        unsafe fn set_init(&mut self, init_len: usize) {
            assert!(init_len <= self.bytes_remaining());
            self.filled += init_len;
        }
    }

    fn usage() {
        eprintln!(
            "Usage: norn-kv-loadgen [--address ADDR] [--workload noop|get|set] \
             [--threads N] [--connections-per-thread N] [--warmup-requests N] \
             [--requests N] [--sample-every N] [--value-size N] [--ring-entries N]"
        );
    }

    fn parse_args() -> io::Result<Config> {
        let mut config = Config::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if matches!(arg.as_str(), "--help" | "-h") {
                usage();
                std::process::exit(0);
            }
            let (name, raw) = if let Some((name, raw)) = arg.split_once('=') {
                (name.to_owned(), raw.to_owned())
            } else {
                let raw = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {arg}"),
                    )
                })?;
                (arg, raw)
            };
            match name.as_str() {
                "--address" => config.address = parse_number(&name, &raw)?,
                "--workload" => config.workload = Workload::parse(&raw)?,
                "--threads" => config.threads = parse_number(&name, &raw)?,
                "--connections-per-thread" => {
                    config.connections_per_thread = parse_number(&name, &raw)?
                }
                "--warmup-requests" => config.warmup_requests = parse_number(&name, &raw)?,
                "--requests" => config.requests = parse_number(&name, &raw)?,
                "--sample-every" => config.sample_every = parse_number(&name, &raw)?,
                "--value-size" => config.value_size = parse_number(&name, &raw)?,
                "--ring-entries" => config.ring_entries = parse_number(&name, &raw)?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument: {name}"),
                    ));
                }
            }
        }
        if config.threads == 0
            || config.connections_per_thread == 0
            || config.requests == 0
            || config.sample_every == 0
            || config.ring_entries == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "thread, connection, request, sample, and ring counts must be nonzero",
            ));
        }
        Ok(config)
    }

    fn parse_number<T>(name: &str, raw: &str) -> io::Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        raw.parse().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {name} value '{raw}': {error}"),
            )
        })
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("norn-kv-loadgen requires Linux and io_uring");
    std::process::exit(1);
}
