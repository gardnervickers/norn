#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::io;
use std::mem::MaybeUninit;

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_executor::spawn;
use norn_uring::buf::StableBufMut;
use norn_uring::bufring::{SendBuf, SendBufRing};
use norn_uring::net::{
    FinishSendRingOutcome, FinishUdpSendRingOutcome, TcpListener, TcpSocket, UdpSocket,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;

const RING_DEPTH: u32 = 64;
const TCP_BYTES_PER_ITERATION: usize = 1024 * 1024;
const TCP_BUFFER_SIZE: usize = 4096;
// Keep one iteration below the default loopback UDP receive-buffer capacity so
// the throughput benchmark does not turn packet loss into a permanent wait.
const UDP_DATAGRAMS_PER_ITERATION: usize = 128;
const UDP_SEGMENT_SIZE: usize = 256;

fn new_executor() -> norn_executor::LocalExecutor<norn_uring::Driver> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let driver = norn_uring::Driver::new(builder, RING_DEPTH).unwrap();
    norn_executor::LocalExecutor::new(driver)
}

async fn connected_tcp_pair() -> io::Result<(TcpSocket, TcpSocket)> {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8).await?;
    let address = listener.local_addr()?;
    let client = spawn(async move { TcpSocket::connect(address).await });
    let (server, _) = listener.accept().await?;
    let client = client
        .await
        .map_err(|error| io::Error::other(error.to_string()))??;
    listener.close().await?;
    Ok((server, client))
}

async fn connected_udp_pair() -> io::Result<(UdpSocket, UdpSocket)> {
    let sender = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await?;
    let receiver = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await?;
    sender.connect(receiver.local_addr()?).await?;
    receiver.connect(sender.local_addr()?).await?;
    Ok((sender, receiver))
}

fn fill(mut buffer: SendBuf, byte: u8, len: usize) -> SendBuf {
    assert!(len <= buffer.capacity());
    buffer.spare_capacity_mut()[..len]
        .iter_mut()
        .for_each(|slot: &mut MaybeUninit<u8>| {
            slot.write(byte);
        });
    unsafe { buffer.set_init(len) };
    buffer
}

#[derive(Clone, Copy)]
enum TcpMode {
    Ordinary,
    Bundled,
}

struct TcpStreamBench(TcpMode);

impl bencher::TDynBenchFn for TcpStreamBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = new_executor();
        let (server, client) = executor.block_on(connected_tcp_pair()).unwrap();
        let (mut reader, server_writer) = server.into_stream().owned_split();
        let (client_reader, writer) = client.into_stream().owned_split();
        let payload = vec![0x5a; TCP_BYTES_PER_ITERATION];
        let mut received = vec![0; TCP_BYTES_PER_ITERATION];

        match self.0 {
            TcpMode::Ordinary => {
                let mut writer = writer;
                b.iter(|| {
                    executor
                        .block_on(async {
                            futures::try_join!(
                                async {
                                    writer.write_all(&payload).await?;
                                    writer.flush().await
                                },
                                async { reader.read_exact(&mut received).await.map(|_| ()) },
                            )?;
                            io::Result::Ok(())
                        })
                        .unwrap();
                });
                executor.block_on(async move {
                    writer.shutdown().await?;
                    drop(client_reader);
                    drop(server_writer);
                    drop(reader);
                    io::Result::Ok(())
                })
            }
            TcpMode::Bundled => {
                let ring = executor
                    .block_on(async {
                        SendBufRing::builder(60)
                            .buf_count(16)
                            .buf_len(TCP_BUFFER_SIZE)
                            .build()
                    })
                    .unwrap();
                let mut writer =
                    executor.block_on(async move { writer.attach_send_ring(ring).unwrap() });
                b.iter(|| {
                    executor
                        .block_on(async {
                            futures::try_join!(
                                async {
                                    writer.write_all(&payload).await?;
                                    writer.flush().await
                                },
                                async { reader.read_exact(&mut received).await.map(|_| ()) },
                            )?;
                            io::Result::Ok(())
                        })
                        .unwrap();
                });
                executor.block_on(async move {
                    let FinishSendRingOutcome::Drained { mut writer, ring } =
                        writer.finish_send_ring().await
                    else {
                        panic!("bundled TCP benchmark failed to detach cleanly");
                    };
                    writer.shutdown().await?;
                    drop(ring);
                    drop(client_reader);
                    drop(server_writer);
                    drop(reader);
                    io::Result::Ok(())
                })
            }
        }
        .unwrap();
    }
}

#[derive(Clone, Copy)]
enum UdpMode {
    Ordinary,
    Bundled,
}

struct UdpDatagramBench(UdpMode);

impl bencher::TDynBenchFn for UdpDatagramBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = new_executor();
        let (sender, receiver) = executor.block_on(connected_udp_pair()).unwrap();
        let datagram_len = UDP_SEGMENT_SIZE * 2;

        match self.0 {
            UdpMode::Ordinary => {
                let mut send_buffer = vec![0x5a; datagram_len];
                let mut receive_buffer = vec![0; datagram_len];
                b.iter(|| {
                    let next_send = std::mem::take(&mut send_buffer);
                    let next_receive = std::mem::take(&mut receive_buffer);
                    (send_buffer, receive_buffer) = executor
                        .block_on(async {
                            futures::try_join!(
                                async {
                                    let mut buffer = next_send;
                                    for _ in 0..UDP_DATAGRAMS_PER_ITERATION {
                                        let (sent, returned) = sender.send(buffer).await;
                                        buffer = returned;
                                        assert_eq!(sent?, datagram_len);
                                    }
                                    io::Result::Ok(buffer)
                                },
                                async {
                                    let mut buffer = next_receive;
                                    for _ in 0..UDP_DATAGRAMS_PER_ITERATION {
                                        let (received, returned) = receiver.recv(buffer).await;
                                        buffer = returned;
                                        assert_eq!(received?, datagram_len);
                                    }
                                    io::Result::Ok(buffer)
                                },
                            )
                        })
                        .unwrap();
                });
                executor
                    .block_on(async move {
                        sender.close().await?;
                        receiver.close().await
                    })
                    .unwrap();
            }
            UdpMode::Bundled => {
                let ring = executor
                    .block_on(async {
                        SendBufRing::builder(61)
                            .buf_count(16)
                            .buf_len(UDP_SEGMENT_SIZE)
                            .build()
                    })
                    .unwrap();
                let mut sender =
                    executor.block_on(async move { sender.attach_send_ring(ring).unwrap() });
                let mut receive_buffer = vec![0; datagram_len];
                b.iter(|| {
                    let next_receive = std::mem::take(&mut receive_buffer);
                    receive_buffer = executor
                        .block_on(async {
                            let ((), receive_buffer) = futures::try_join!(
                                async {
                                    for _ in 0..UDP_DATAGRAMS_PER_ITERATION {
                                        let mut datagram = sender.datagram();
                                        let first =
                                            fill(datagram.acquire().await?, 0x5a, UDP_SEGMENT_SIZE);
                                        datagram.push(first, UDP_SEGMENT_SIZE).unwrap();
                                        let second =
                                            fill(datagram.acquire().await?, 0x5a, UDP_SEGMENT_SIZE);
                                        datagram.push(second, UDP_SEGMENT_SIZE).unwrap();
                                        datagram.commit().await.unwrap();
                                    }
                                    sender.flush().await
                                },
                                async {
                                    let mut buffer = next_receive;
                                    for _ in 0..UDP_DATAGRAMS_PER_ITERATION {
                                        let (received, returned) = receiver.recv(buffer).await;
                                        buffer = returned;
                                        assert_eq!(received?, datagram_len);
                                    }
                                    io::Result::Ok(buffer)
                                },
                            )?;
                            io::Result::Ok(receive_buffer)
                        })
                        .unwrap();
                });
                executor
                    .block_on(async move {
                        let FinishUdpSendRingOutcome::Drained { socket, ring } =
                            sender.finish_send_ring().await
                        else {
                            panic!("bundled UDP benchmark failed to detach cleanly");
                        };
                        drop(ring);
                        socket.close().await?;
                        receiver.close().await
                    })
                    .unwrap();
            }
        }
    }
}

fn benches() -> Vec<TestDescAndFn> {
    [
        (
            "bundle_send/tcp/ordinary",
            TestFn::DynBenchFn(Box::new(TcpStreamBench(TcpMode::Ordinary))),
        ),
        (
            "bundle_send/tcp/bundled",
            TestFn::DynBenchFn(Box::new(TcpStreamBench(TcpMode::Bundled))),
        ),
        (
            "bundle_send/udp/ordinary",
            TestFn::DynBenchFn(Box::new(UdpDatagramBench(UdpMode::Ordinary))),
        ),
        (
            "bundle_send/udp/bundled_two_segments",
            TestFn::DynBenchFn(Box::new(UdpDatagramBench(UdpMode::Bundled))),
        ),
    ]
    .into_iter()
    .map(|(name, testfn)| TestDescAndFn {
        desc: TestDesc {
            name: Cow::Borrowed(name),
            ignore: false,
        },
        testfn,
    })
    .collect()
}

fn main() {
    support::run(benches());
}
