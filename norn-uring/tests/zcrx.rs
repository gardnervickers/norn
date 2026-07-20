#![cfg(target_os = "linux")]

use std::io;
use std::pin::pin;

use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::net::{TcpListener, TcpSocket};
use norn_uring::zcrx::ZcRxIfqConfig;

mod util;

#[test]
#[ignore = "requires Linux 6.15+, a ZCRX-capable NIC queue, and CAP_NET_ADMIN"]
fn recv_zc_multi_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let Some(if_index) = env_u32("NORN_ZCRX_IFINDEX")? else {
        eprintln!("NORN_ZCRX_IFINDEX is unset; skipping ZCRX smoke test");
        return Ok(());
    };
    let Some(rx_queue) = env_u32("NORN_ZCRX_RXQ")? else {
        eprintln!("NORN_ZCRX_RXQ is unset; skipping ZCRX smoke test");
        return Ok(());
    };

    let mut builder =
        io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry32>::builder();
    builder.setup_single_issuer().setup_defer_taskrun();
    let mut driver = match norn_uring::Driver::new_cqe32(builder, 128) {
        Ok(driver) => driver,
        Err(err) if util::zcrx_unsupported(&err) => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    let ifq = match driver.register_zcrx_ifq(ZcRxIfqConfig {
        if_index,
        rx_queue,
        rq_entries: 128,
        area_len: 132 * 4096,
    }) {
        Ok(ifq) => ifq,
        Err(err) if util::zcrx_unsupported(&err) => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;
        let client_task = spawn(async move { TcpSocket::connect(addr).await });
        let (server, _) = listener.accept().await?;
        let client = client_task.await??;

        let payload = b"norn-zcrx-smoke".to_vec();
        let (sent, payload) = client.send(payload).await;
        assert_eq!(sent?, payload.len());

        let mut recv = pin!(server.recv_zc_multi(4096, &ifq));
        let received = match recv.next().await {
            Some(Ok(buf)) => buf,
            Some(Err(err)) if util::zcrx_unsupported(&err) => return Ok(()),
            Some(Err(err)) => return Err(err.into()),
            None => return Err(io::Error::other("ZCRX stream ended before receiving data").into()),
        };
        assert_eq!(received.as_slice(), payload.as_slice());
        drop(received);
        Ok(())
    })
}

fn env_u32(name: &str) -> Result<Option<u32>, Box<dyn std::error::Error>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = value.into_string().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is not valid UTF-8"),
        )
    })?;
    Ok(Some(value.parse()?))
}
