use std::io;
use std::net::SocketAddr;
use std::pin::pin;

use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::io::{AsyncWriteOwned, AsyncWriteOwnedExt};
use norn_uring::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod util;

#[test]
fn incoming_connections() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        // Bind
        let listener = TcpListener::bind("0.0.0.0:9090".parse()?, 32).await?;

        // Connect
        let handle = spawn(async {
            let _ = TcpStream::connect("0.0.0.0:9090".parse().unwrap()).await?;
            io::Result::Ok(())
        });

        let mut incoming = pin!(listener.incoming());
        let next = incoming.next().await.unwrap()?;
        next.close().await?;
        handle.await??;

        Ok(())
    })
}

fn echo() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let server = EchoServer::new().await?;

        let addr = server.local_addr()?;
        spawn(server.run()).detach();
        let mut conn = TcpStream::connect(addr).await?;

        // Create a 128KB buffer containing the string "hello" repeated.
        let mut buf = Vec::with_capacity(128 * 1024);
        for _ in 0..4 {
            buf.extend_from_slice(b"hello");
        }
        let (reader, writer) = conn.split();
        let mut writer = pin!(writer);
        let mut reader = pin!(reader);
        writer.write_all(&buf[..]).await?;
        //reader.read(buf.as_mut_slice()).await?;

        Ok(())
    })
}

struct EchoServer {
    listener: TcpListener,
}

impl EchoServer {
    async fn new() -> io::Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:0".parse().unwrap(), 32).await?;
        Ok(Self { listener })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    async fn run(self) -> io::Result<()> {
        let mut incoming = pin!(self.listener.incoming());
        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            spawn(async move {
                let (mut reader, mut writer) = stream.split();
                let mut reader = pin!(reader);
                let mut writer = pin!(writer);
                println!("starting copy");
                // Generate a 32MB buffer of the string hello repeated.
                if let Err(err) = tokio::io::copy(&mut reader, &mut writer).await {
                    log::error!("error copying: {:?}", err)
                }
            })
            .detach();
        }
        Ok(())
    }
}
