#[cfg(target_os = "linux")]
mod linux {
    use std::borrow::Cow;
    use std::cmp;
    use std::net::SocketAddr;

    use bencher::{run_tests_console, Bencher, TestDesc, TestDescAndFn, TestFn, TestOpts};
    use futures::StreamExt;
    use norn_executor::spawn;
    use norn_uring::net::{TcpListener, TcpSocket};

    #[derive(Clone, Copy)]
    enum AcceptMode {
        Single,
        Multi,
    }

    impl AcceptMode {
        fn as_str(self) -> &'static str {
            match self {
                AcceptMode::Single => "single",
                AcceptMode::Multi => "multi",
            }
        }
    }

    struct AcceptRecvBench {
        mode: AcceptMode,
        clients: usize,
        connections_per_client: usize,
    }

    impl AcceptRecvBench {
        fn total_connections(&self) -> usize {
            self.clients * self.connections_per_client
        }
    }

    impl bencher::TDynBenchFn for AcceptRecvBench {
        fn run(&self, b: &mut Bencher) {
            let mut builder = io_uring::IoUring::builder();
            builder
                .dontfork()
                .setup_coop_taskrun()
                .setup_defer_taskrun()
                .setup_single_issuer()
                .setup_submit_all();
            let ring = norn_uring::Driver::new(builder, 512).unwrap();
            let mut executor = norn_executor::LocalExecutor::new(ring);

            b.iter(|| {
                let mode = self.mode;
                let clients = self.clients;
                let connections_per_client = self.connections_per_client;
                let total_connections = self.total_connections();

                executor.block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 1024)
                        .await
                        .unwrap();
                    let addr = listener.local_addr().unwrap();

                    let mut handles = Vec::with_capacity(clients);
                    for _ in 0..clients {
                        handles.push(spawn(client_connect_close(addr, connections_per_client)));
                    }

                    match mode {
                        AcceptMode::Single => {
                            accept_and_close_single(&listener, total_connections).await
                        }
                        AcceptMode::Multi => {
                            accept_and_close_multi(&listener, total_connections).await
                        }
                    }

                    for handle in handles {
                        handle.await.unwrap().unwrap();
                    }
                })
            });
        }
    }

    async fn client_connect_close(addr: SocketAddr, connections: usize) -> std::io::Result<()> {
        for _ in 0..connections {
            let socket = TcpSocket::connect(addr).await?;
            socket.close().await?;
        }
        Ok(())
    }

    async fn accept_and_close_single(listener: &TcpListener, total_connections: usize) {
        for _ in 0..total_connections {
            let (socket, _addr) = listener.accept().await.unwrap();
            socket.close().await.unwrap();
        }
    }

    async fn accept_and_close_multi(listener: &TcpListener, total_connections: usize) {
        let mut incoming = std::pin::pin!(listener.incoming());
        for _ in 0..total_connections {
            let socket = incoming.next().await.unwrap().unwrap();
            socket.close().await.unwrap();
        }
    }

    pub fn benches() -> Vec<TestDescAndFn> {
        let mut benches = vec![];

        for mode in [AcceptMode::Single, AcceptMode::Multi] {
            for clients in [1usize, 8, 16] {
                for total in [256usize, 1024] {
                    let connections_per_client = cmp::max(total / clients, 1);
                    benches.push(TestDescAndFn {
                        desc: TestDesc {
                            name: Cow::from(format!(
                                "bench_accept_recv/mode={}/clients={}/connections={}",
                                mode.as_str(),
                                clients,
                                connections_per_client
                            )),
                            ignore: false,
                        },
                        testfn: TestFn::DynBenchFn(Box::new(AcceptRecvBench {
                            mode,
                            clients,
                            connections_per_client,
                        })),
                    });
                }
            }
        }

        benches
    }

    pub fn run_main() {
        let mut test_opts = TestOpts::default();
        if let Some(arg) = std::env::args().skip(1).find(|arg| *arg != "--bench") {
            test_opts.filter = Some(arg);
        }
        let mut all = Vec::new();
        all.extend(benches());
        run_tests_console(&test_opts, all).unwrap();
    }
}

#[cfg(target_os = "linux")]
fn main() {
    linux::run_main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("io_uring_accept_recv benchmark is only supported on Linux");
}
