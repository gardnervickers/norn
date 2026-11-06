#![cfg(target_os = "linux")]

//! On loopback the SYN is processed in softirq on the connecting thread's
//! CPU, so pinning the client selects the CPU the steering program sees.

use std::io;
use std::net::{SocketAddr, TcpStream};
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use norn_uring::net::{ReuseportCpuMap, TcpListener, TcpListenerOptions, TcpSocket};
use norn_uring::Handle;

mod util;

const CONNECTIONS_PER_CPU: usize = 8;

fn allowed_cpus() -> Vec<usize> {
    // SAFETY: the set is zero-initialized and pid zero names this thread.
    let set = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
            0
        );
        set
    };
    (0..libc::CPU_SETSIZE as usize)
        // SAFETY: every index is below CPU_SETSIZE.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect()
}

fn pin_current_thread(cpu: usize) -> io::Result<()> {
    // SAFETY: the set is zero-initialized, `cpu` is below CPU_SETSIZE, and
    // pid zero names this thread.
    let result = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Open `count` connections from a thread pinned to `cpu` and keep them open.
fn connect_from(cpu: usize, address: SocketAddr, count: usize) -> Vec<TcpStream> {
    std::thread::spawn(move || {
        pin_current_thread(cpu).unwrap();
        (0..count)
            .map(|_| TcpStream::connect(address).unwrap())
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap()
}

async fn bind_group(count: usize) -> io::Result<Vec<TcpListener>> {
    let options = TcpListenerOptions::new().reuse_port(true);
    let mut listeners =
        vec![TcpListener::bind_with_options("127.0.0.1:0".parse().unwrap(), 64, options).await?];
    let address = listeners[0].local_addr()?;
    for _ in 1..count {
        listeners.push(TcpListener::bind_with_options(address, 64, options).await?);
    }
    Ok(listeners)
}

/// Accept one connection, failing instead of hanging if none was queued.
async fn accept(listener: &TcpListener) -> io::Result<TcpSocket> {
    let accept = pin!(listener.accept());
    let deadline = pin!(Handle::current().timeout(Duration::from_secs(5)));
    match select(accept, deadline).await {
        Either::Left((accepted, _)) => Ok(accepted?.0),
        Either::Right(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "no connection was steered to this listener",
        )),
    }
}

#[test]
fn connections_land_on_the_listener_routed_for_their_syn_cpu(
) -> Result<(), Box<dyn std::error::Error>> {
    let allowed = allowed_cpus();
    if allowed.len() < 2 {
        eprintln!("skipping: steering needs at least two allowed CPUs");
        return Ok(());
    }
    // Route at most four CPUs, in reverse slot order so the test does not
    // pass by matching CPU order to listen order by accident.
    let cpus: Vec<usize> = allowed.iter().copied().take(4).collect();
    util::with_test_env(|| async move {
        let listeners = bind_group(cpus.len()).await?;
        let mut map = ReuseportCpuMap::new();
        for (index, &cpu) in cpus.iter().enumerate() {
            map.route(cpu, cpus.len() - 1 - index)?;
        }
        // Attaching through any member applies to the whole group.
        listeners[1].attach_reuseport_cpu_map(&map)?;

        let address = listeners[0].local_addr()?;
        let clients: Vec<_> = cpus
            .iter()
            .map(|&cpu| connect_from(cpu, address, CONNECTIONS_PER_CPU))
            .collect();

        for &cpu in &cpus {
            let listener = &listeners[map.slot(cpu).unwrap()];
            for _ in 0..CONNECTIONS_PER_CPU {
                let socket = accept(listener).await?;
                assert_eq!(socket.incoming_cpu()?, Some(cpu));
            }
        }
        drop(clients);
        Ok(())
    })
}

#[test]
fn unrouted_cpus_and_missing_slots_fall_back_to_hash_selection(
) -> Result<(), Box<dyn std::error::Error>> {
    let cpu = allowed_cpus()[0];
    util::with_test_env(|| async move {
        let listeners = bind_group(1).await?;
        let mut map = ReuseportCpuMap::new();
        // The group has one slot, so this route names a listener that does
        // not exist yet; every other CPU has no route at all.
        map.route(cpu, 3)?;
        listeners[0].attach_reuseport_cpu_map(&map)?;

        let address = listeners[0].local_addr()?;
        let mut clients = connect_from(cpu, address, 2);
        if let Some(&other) = allowed_cpus().iter().find(|&&other| other != cpu) {
            clients.extend(connect_from(other, address, 2));
        }
        for _ in 0..clients.len() {
            accept(&listeners[0]).await?;
        }
        Ok(())
    })
}

#[test]
fn attaching_to_a_listener_without_reuse_port_fails() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let error = listener
            .attach_reuseport_cpu_map(&ReuseportCpuMap::new())
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL), "{error}");
        Ok(())
    })
}
