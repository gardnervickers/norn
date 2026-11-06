//! CPU-keyed selection among `SO_REUSEPORT` listeners.

use std::collections::BTreeMap;
use std::io;
use std::os::fd::RawFd;

// `SKF_AD_OFF + SKF_AD_CPU`: an absolute load from this offset yields the
// CPU running the program, which for a TCP SYN is the CPU processing it.
const LOAD_CPU: u32 = (-0x1000i32 + 36) as u32;
// A slot index the kernel never has, so it selects by hash instead.
const FALLBACK: u32 = u32::MAX;
// `BPF_MAXINSNS`: one load, two instructions per route, one fallback return.
const MAX_ROUTES: usize = (4096 - 2) / 2;

/// Routes new connections on a reuse-port listener group by the CPU that
/// processed their SYN.
///
/// Attach it with [`TcpListener::attach_reuseport_cpu_map`]. The kernel
/// chooses a listener once per connection, when the SYN arrives, in softirq
/// on the CPU that received it. With RSS or RPS keeping a flow on one receive
/// queue, routing that CPU to the worker pinned there keeps the connection's
/// receive path on one core. Established packets never consult the map.
///
/// The map is a classic BPF program, so attaching it needs no privilege. A
/// CPU without a route, or a route to a slot the group does not have, falls
/// back to the kernel's hash selection, so steering never rejects a
/// connection; a wrong map only costs locality.
///
/// # Slots
///
/// A slot is a listener's position in its group: the *n*-th listener to start
/// listening is slot *n - 1*. Closing a listener moves the group's last
/// listener into the closed slot, so a group whose membership changes must
/// attach a rebuilt map. A group is shared by every socket with the same
/// owner that binds the address with `SO_REUSEPORT`, including other
/// processes, and attaching replaces the program for all of them.
///
/// # Size
///
/// Besides the classic BPF limit of 2,047 routes, the kernel charges the
/// converted program, about 24 bytes per route, against
/// `net.core.optmem_max`. The old 20 KiB default fits roughly 840 routes;
/// a larger map makes the attach fail with `ENOMEM`.
///
/// [`TcpListener::attach_reuseport_cpu_map`]: crate::net::TcpListener::attach_reuseport_cpu_map
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReuseportCpuMap {
    routes: BTreeMap<u32, u32>,
}

impl ReuseportCpuMap {
    /// Create a map without routes, which selects every listener by hash.
    pub fn new() -> Self {
        Self::default()
    }

    /// Route SYNs processed on `cpu` to the listener in `slot`.
    ///
    /// Several CPUs may route to one slot, for example a worker's CPU and its
    /// SMT sibling. Routing a CPU again replaces its slot.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `cpu` or `slot` does not
    /// fit the kernel's 32-bit values, or if the map would exceed the classic
    /// BPF program limit of 2,047 routes. See the type's size notes for
    /// the `optmem_max` limit, which the attach enforces.
    pub fn route(&mut self, cpu: usize, slot: usize) -> io::Result<&mut Self> {
        let cpu = u32::try_from(cpu).map_err(|_| invalid("CPU index exceeds u32"))?;
        let slot = u32::try_from(slot)
            .ok()
            .filter(|&slot| slot != FALLBACK)
            .ok_or_else(|| invalid("listener slot exceeds u32::MAX - 1"))?;
        if self.routes.len() == MAX_ROUTES && !self.routes.contains_key(&cpu) {
            return Err(invalid("reuseport CPU map exceeds 2047 routes"));
        }
        self.routes.insert(cpu, slot);
        Ok(self)
    }

    /// Return the slot routed for `cpu`, if any.
    pub fn slot(&self, cpu: usize) -> Option<usize> {
        let cpu = u32::try_from(cpu).ok()?;
        self.routes.get(&cpu).map(|&slot| slot as usize)
    }

    /// Return the number of routed CPUs.
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Return whether no CPU is routed.
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    fn program(&self) -> Vec<libc::sock_filter> {
        let mut program = Vec::with_capacity(2 + 2 * self.routes.len());
        program.push(statement(BPF_LD | BPF_W | BPF_ABS, LOAD_CPU));
        for (&cpu, &slot) in &self.routes {
            // On a match fall through to the return; otherwise skip it.
            program.push(libc::sock_filter {
                code: BPF_JMP | BPF_JEQ | BPF_K,
                jt: 0,
                jf: 1,
                k: cpu,
            });
            program.push(statement(BPF_RET | BPF_K, slot));
        }
        program.push(statement(BPF_RET | BPF_K, FALLBACK));
        program
    }

    /// Attach the map to the reuse-port group of the listener `fd`.
    pub(crate) fn attach(&self, fd: RawFd) -> io::Result<()> {
        let program = self.program();
        let fprog = libc::sock_fprog {
            len: program.len() as u16,
            filter: program.as_ptr().cast_mut(),
        };
        // SAFETY: `fprog` points at `program`, which outlives the call; the
        // kernel copies the program before returning.
        let result = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ATTACH_REUSEPORT_CBPF,
                std::ptr::from_ref(&fprog).cast(),
                std::mem::size_of::<libc::sock_fprog>() as libc::socklen_t,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

const BPF_LD: u16 = 0x00;
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;
const BPF_JMP: u16 = 0x05;
const BPF_JEQ: u16 = 0x10;
const BPF_K: u16 = 0x00;
const BPF_RET: u16 = 0x06;

fn statement(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_loads_the_cpu_then_tests_each_route_in_order() {
        let mut map = ReuseportCpuMap::new();
        map.route(24, 0)
            .unwrap()
            .route(8, 0)
            .unwrap()
            .route(9, 1)
            .unwrap();
        let program: Vec<_> = map
            .program()
            .iter()
            .map(|insn| (insn.code, insn.jt, insn.jf, insn.k))
            .collect();
        assert_eq!(
            program,
            [
                (0x20, 0, 0, LOAD_CPU),
                (0x15, 0, 1, 8),
                (0x06, 0, 0, 0),
                (0x15, 0, 1, 9),
                (0x06, 0, 0, 1),
                (0x15, 0, 1, 24),
                (0x06, 0, 0, 0),
                (0x06, 0, 0, FALLBACK),
            ]
        );
    }

    #[test]
    fn routes_replace_and_validate() {
        let mut map = ReuseportCpuMap::new();
        assert!(map.is_empty());
        map.route(3, 1).unwrap().route(3, 2).unwrap();
        assert_eq!((map.len(), map.slot(3), map.slot(4)), (1, Some(2), None));
        assert_eq!(
            map.route(0, FALLBACK as usize).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        for cpu in 0..MAX_ROUTES {
            map.route(cpu, 0).unwrap();
        }
        map.route(0, 1).unwrap();
        assert_eq!(
            map.route(MAX_ROUTES, 0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(map.program().len(), 4096);
    }
}
