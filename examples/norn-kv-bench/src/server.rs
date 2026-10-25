//! Server process lifecycle: launch, pin, readiness, and teardown.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `norn-kv-server`: started on port zero; the announced address is used.
    Norn,
    /// Any other server (e.g. memcached). `{port}` and `{workers}` in its
    /// arguments are substituted and readiness is a TCP connect probe.
    Generic,
}

#[derive(Debug, Clone)]
pub struct Spec {
    pub kind: Kind,
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub cpus: Vec<usize>,
    pub workers: usize,
}

pub struct Process {
    child: Child,
    pub pid: i32,
    pub address: SocketAddr,
    pub command: Vec<String>,
}

fn cpu_list(cpus: &[usize]) -> String {
    cpus.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

impl Process {
    pub fn start(spec: &Spec, log_dir: &Path) -> Result<Self, String> {
        let mut args = Vec::new();
        let mut port = 0;
        match spec.kind {
            Kind::Norn => {
                args.push("--listen=127.0.0.1:0".to_owned());
                args.push(format!("--workers={}", spec.workers));
                if !spec.cpus.is_empty() {
                    args.push(format!("--worker-cpus={}", cpu_list(&spec.cpus)));
                }
                args.extend(spec.args.iter().cloned());
            }
            Kind::Generic => {
                port = TcpListener::bind("127.0.0.1:0")
                    .and_then(|l| l.local_addr())
                    .map_err(|e| format!("choose port: {e}"))?
                    .port();
                for arg in &spec.args {
                    args.push(
                        arg.replace("{port}", &port.to_string())
                            .replace("{workers}", &spec.workers.to_string()),
                    );
                }
            }
        }
        let stderr = File::create(log_dir.join("server.stderr.log"))
            .map_err(|e| format!("create server log: {e}"))?;
        let mut command = Command::new(&spec.binary);
        command
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(stderr);
        let cpus = spec.cpus.clone();
        // SAFETY: the closure only calls async-signal-safe libc functions.
        unsafe {
            command.pre_exec(move || {
                // Do not outlive a coordinator that is killed before it can
                // stop the server.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if !cpus.is_empty() {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    for &cpu in &cpus {
                        libc::CPU_SET(cpu, &mut set);
                    }
                    if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", spec.binary.display()))?;
        let pid = child.id() as i32;
        let stdout = child.stdout.take().unwrap();
        let mut stdout_log = File::create(log_dir.join("server.stdout.log"))
            .map_err(|e| format!("create server log: {e}"))?;
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut announced = false;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let _ = writeln!(stdout_log, "{line}");
                if !announced {
                    if let Some(addr) = line.strip_prefix("listening on ") {
                        announced = true;
                        let _ = tx.send(addr.trim().to_owned());
                    }
                }
            }
        });
        let mut command_line = vec![spec.binary.display().to_string()];
        command_line.extend(args);
        let mut process = Self {
            child,
            pid,
            address: "127.0.0.1:0".parse().unwrap(),
            command: command_line,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        match spec.kind {
            Kind::Norn => {
                let announced = rx.recv_timeout(Duration::from_secs(30));
                match announced.ok().and_then(|a| a.parse().ok()) {
                    Some(address) => process.address = address,
                    None => {
                        let status = process.child.try_wait().ok().flatten();
                        process.kill();
                        return Err(format!(
                            "server did not announce a listener (exit status {status:?}); see {}",
                            log_dir.join("server.stderr.log").display()
                        ));
                    }
                }
            }
            Kind::Generic => {
                process.address = SocketAddr::from(([127, 0, 0, 1], port));
                loop {
                    if TcpStream::connect(process.address).is_ok() {
                        break;
                    }
                    if Instant::now() > deadline || process.exited() {
                        process.kill();
                        return Err(format!("server never accepted on {}", process.address));
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        Ok(process)
    }

    pub fn exited(&mut self) -> bool {
        !matches!(self.child.try_wait(), Ok(None))
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Terminate the server. Returns an error if it had already exited.
    pub fn stop(mut self) -> Result<(), String> {
        if let Ok(Some(status)) = self.child.try_wait() {
            return Err(format!("server exited during the run: {status}"));
        }
        // SAFETY: signalling our own child process.
        unsafe { libc::kill(self.pid, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.kill();
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            self.kill();
        }
    }
}
