#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::io;
    use std::net::SocketAddr;

    use norn_executor::LocalExecutor;
    use norn_kv_server::handler::MemoryHandler;
    use norn_kv_server::memory::MemoryStore;
    use norn_kv_server::server::{serve, RecvMode, ServerConfig};
    use norn_uring::net::TcpListener;

    #[derive(Debug)]
    struct Config {
        listen: SocketAddr,
        backlog: u32,
        ring_entries: u32,
        server: ServerConfig,
    }

    fn usage() {
        eprintln!(
            "Usage: norn-kv-server [--listen ADDR] [--backlog N] [--ring-entries N] \
             [--max-body N] [--max-connections N] [--recv-mode exact|multishot] \
             [--max-batch-commands N] [--max-batch-response-bytes N]"
        );
    }

    fn value<'a>(arg: &'a str, name: &str) -> Option<&'a str> {
        arg.strip_prefix(name)
            .and_then(|rest| rest.strip_prefix('='))
    }

    fn parse() -> io::Result<Config> {
        let mut config = Config {
            listen: "127.0.0.1:11211".parse().unwrap(),
            backlog: 1_024,
            ring_entries: 256,
            server: ServerConfig::default(),
        };

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if matches!(arg.as_str(), "--help" | "-h") {
                usage();
                std::process::exit(0);
            }
            let (name, raw) = if let Some(raw) = value(&arg, "--listen") {
                ("--listen", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--backlog") {
                ("--backlog", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--ring-entries") {
                ("--ring-entries", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--max-body") {
                ("--max-body", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--max-connections") {
                ("--max-connections", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--recv-mode") {
                ("--recv-mode", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--max-batch-commands") {
                ("--max-batch-commands", raw.to_owned())
            } else if let Some(raw) = value(&arg, "--max-batch-response-bytes") {
                ("--max-batch-response-bytes", raw.to_owned())
            } else if matches!(
                arg.as_str(),
                "--listen"
                    | "--backlog"
                    | "--ring-entries"
                    | "--max-body"
                    | "--max-connections"
                    | "--recv-mode"
                    | "--max-batch-commands"
                    | "--max-batch-response-bytes"
            ) {
                let name = match arg.as_str() {
                    "--listen" => "--listen",
                    "--backlog" => "--backlog",
                    "--ring-entries" => "--ring-entries",
                    "--max-body" => "--max-body",
                    "--max-connections" => "--max-connections",
                    "--recv-mode" => "--recv-mode",
                    "--max-batch-commands" => "--max-batch-commands",
                    "--max-batch-response-bytes" => "--max-batch-response-bytes",
                    _ => unreachable!(),
                };
                let raw = args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {arg}"),
                    )
                })?;
                (name, raw)
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ));
            };

            match name {
                "--listen" => {
                    config.listen = raw.parse().map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("invalid --listen value '{raw}': {error}"),
                        )
                    })?;
                }
                "--backlog" => config.backlog = parse_number(name, &raw)?,
                "--ring-entries" => config.ring_entries = parse_number(name, &raw)?,
                "--max-body" => config.server.max_body_len = parse_number(name, &raw)?,
                "--max-connections" => config.server.max_connections = parse_number(name, &raw)?,
                "--max-batch-commands" => {
                    config.server.max_batch_commands = parse_number(name, &raw)?
                }
                "--max-batch-response-bytes" => {
                    config.server.max_batch_response_bytes = parse_number(name, &raw)?
                }
                "--recv-mode" => {
                    config.server.recv_mode = match raw.as_str() {
                        "exact" => RecvMode::Exact,
                        "multishot" => RecvMode::Multishot,
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("invalid --recv-mode value '{raw}'"),
                            ));
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        if config.backlog == 0
            || config.ring_entries == 0
            || config.server.max_body_len == 0
            || config.server.max_connections == 0
            || config.server.max_batch_commands == 0
            || config.server.max_batch_response_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "numeric arguments must be greater than zero",
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

    let config = parse()?;
    let builder = io_uring::IoUring::builder();
    let driver = norn_uring::Driver::new(builder, config.ring_entries).map_err(|error| {
        io::Error::new(error.kind(), format!("create io_uring driver: {error}"))
    })?;
    let mut executor = LocalExecutor::new(driver);
    executor.block_on(async move {
        let listener = TcpListener::bind(config.listen, config.backlog)
            .await
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("bind listener at {}: {error}", config.listen),
                )
            })?;
        let address = listener.local_addr()?;
        println!("listening on {address}");
        let handler = MemoryHandler::new(MemoryStore::new());
        serve(listener, handler, config.server).await
    })?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("norn-kv-server requires Linux and io_uring");
    std::process::exit(1);
}
