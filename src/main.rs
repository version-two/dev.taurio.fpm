//! taurio-fpm — High-performance FastCGI process manager for php-cgi on Windows.
//!
//! Lightweight alternative to php-fpm. Spawns a pool of php-cgi workers,
//! load-balances incoming FastCGI connections with lock-free worker selection,
//! auto-scales under load, and reaps idle workers.
//!
//! Usage:
//!   taurio-fpm --port 9001 --cgi "C:\...\php-cgi.exe" [options]
//!
//! Architecture:
//!   [nginx/apache] -> port 9001 -> [taurio-fpm TCP listener]
//!                                        | pick idle worker (lock-free CAS)
//!                        [php-cgi :19001] [php-cgi :19002] [php-cgi :19003] ...

#![cfg_attr(target_os = "windows", windows_subsystem = "console")]

use serde::Serialize;
use std::env;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const INTERNAL_PORT_OFFSET: u16 = 10000;
const INITIAL_STARTUP_WAIT_MS: u64 = 150;
const WATCHDOG_INTERVAL_MS: u64 = 500;
const STATS_WRITE_INTERVAL_SECS: u64 = 2;
/// 64 KB pipe buffer — matches typical TCP window, much better than io::copy's 8 KB default
const PIPE_BUF_SIZE: usize = 65536;
/// Read/write timeout on piped streams
const STREAM_TIMEOUT_SECS: u64 = 120;
/// Max time to wait for a worker when all are busy (ms)
const BACKPRESSURE_WAIT_MS: u64 = 5000;
/// Poll interval when waiting for a busy worker to free up (ms)
const BACKPRESSURE_POLL_MS: u64 = 2;

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Stats output structs ────────────────────────────────────────────────

#[derive(Serialize)]
struct PoolStats {
    total_workers: usize,
    busy_workers: usize,
    idle_workers: usize,
    min_workers: usize,
    max_workers: usize,
    idle_timeout_secs: u64,
    total_connections: u64,
    workers: Vec<WorkerInfoOut>,
}

#[derive(Serialize)]
struct WorkerInfoOut {
    port: u16,
    busy: bool,
    pid: u32,
    idle_since_secs: f64,
}

// ── Worker slots (lock-free hot path) ───────────────────────────────────

/// Fixed-capacity worker slot array. Worker selection (acquire/release) is
/// entirely lock-free using atomic CAS. Only spawn/reap take the children lock.
struct WorkerSlot {
    port: u16,
    /// true if this slot holds a live worker process
    active: AtomicBool,
    /// true if currently handling a request
    busy: AtomicBool,
    pid: AtomicU32,
    last_active: AtomicU64,
}

struct Pool {
    slots: Vec<WorkerSlot>,
    /// Children vec (same indices as slots). Only locked for spawn/reap/stats.
    children: parking_lot::Mutex<Vec<Option<Child>>>,
    php_cgi_path: String,
    working_dir: String,
    base_port: u16,
    min_workers: usize,
    max_workers: usize,
    idle_timeout_secs: u64,
    total_connections: AtomicU64,
    /// Round-robin hint for acquire to reduce contention on slot 0
    acquire_hint: AtomicU32,
}

fn hide_window(cmd: &mut Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
}

fn spawn_php_cgi(php_cgi_path: &str, working_dir: &str, port: u16) -> Result<Child, String> {
    let mut cmd = Command::new(php_cgi_path);
    cmd.args(["-b", &format!("127.0.0.1:{}", port)]);
    if !working_dir.is_empty() {
        cmd.current_dir(working_dir);
    }
    cmd.env("PHP_FCGI_MAX_REQUESTS", "0");
    hide_window(&mut cmd);
    cmd.spawn()
        .map_err(|e| format!("Failed to spawn php-cgi on port {}: {}", port, e))
}

impl Pool {
    fn new(
        php_cgi_path: String,
        working_dir: String,
        base_port: u16,
        min_workers: usize,
        max_workers: usize,
        idle_timeout_secs: u64,
    ) -> Self {
        let mut slots = Vec::with_capacity(max_workers);
        let mut children = Vec::with_capacity(max_workers);
        for i in 0..max_workers {
            let port = base_port + INTERNAL_PORT_OFFSET + i as u16;
            slots.push(WorkerSlot {
                port,
                active: AtomicBool::new(false),
                busy: AtomicBool::new(false),
                pid: AtomicU32::new(0),
                last_active: AtomicU64::new(0),
            });
            children.push(None);
        }
        Pool {
            slots,
            children: parking_lot::Mutex::new(children),
            php_cgi_path,
            working_dir,
            base_port,
            min_workers,
            max_workers,
            idle_timeout_secs,
            total_connections: AtomicU64::new(0),
            acquire_hint: AtomicU32::new(0),
        }
    }

    /// Spawn a worker into slot `idx`. Takes the children lock.
    fn spawn_into(&self, idx: usize) -> Result<(), String> {
        let port = self.slots[idx].port;
        let child = spawn_php_cgi(&self.php_cgi_path, &self.working_dir, port)?;
        let pid = child.id();
        self.children.lock()[idx] = Some(child);
        self.slots[idx].pid.store(pid, Ordering::Release);
        self.slots[idx].last_active.store(now_epoch_secs(), Ordering::Release);
        self.slots[idx].busy.store(false, Ordering::Release);
        self.slots[idx].active.store(true, Ordering::Release);
        Ok(())
    }

    /// Pre-spawn initial workers (0..min_workers).
    fn warm_up(&self) -> Result<(), String> {
        for i in 0..self.min_workers {
            self.spawn_into(i)?;
        }
        Ok(())
    }

    /// Lock-free worker acquisition. Returns worker port or None.
    /// Uses round-robin hint to distribute load across workers.
    fn acquire(&self) -> Option<u16> {
        let n = self.max_workers;
        let start = self.acquire_hint.fetch_add(1, Ordering::Relaxed) as usize % n;

        // Pass 1: find an idle, active worker (lock-free)
        for offset in 0..n {
            let idx = (start + offset) % n;
            let slot = &self.slots[idx];
            if !slot.active.load(Ordering::Acquire) {
                continue;
            }
            if slot
                .busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.total_connections.fetch_add(1, Ordering::Relaxed);
                return Some(slot.port);
            }
        }

        // Pass 2: try to spawn a new worker (needs lock, but brief)
        {
            let mut children = self.children.lock();
            // Find first inactive slot
            for idx in 0..n {
                let slot = &self.slots[idx];
                if slot.active.load(Ordering::Acquire) {
                    continue;
                }
                // Spawn into this slot
                let port = slot.port;
                match spawn_php_cgi(&self.php_cgi_path, &self.working_dir, port) {
                    Ok(child) => {
                        let pid = child.id();
                        children[idx] = Some(child);
                        slot.pid.store(pid, Ordering::Release);
                        slot.last_active.store(now_epoch_secs(), Ordering::Release);
                        slot.busy.store(true, Ordering::Release);
                        slot.active.store(true, Ordering::Release);
                        self.total_connections.fetch_add(1, Ordering::Relaxed);
                        return Some(port);
                    }
                    Err(_) => continue,
                }
            }
        }

        None
    }

    /// Lock-free worker release.
    fn release(&self, port: u16) {
        let idx = (port - self.base_port - INTERNAL_PORT_OFFSET) as usize;
        if idx < self.max_workers {
            let slot = &self.slots[idx];
            slot.busy.store(false, Ordering::Release);
            slot.last_active.store(now_epoch_secs(), Ordering::Release);
        }
    }

    /// Watchdog: restart dead workers, reap excess idle workers. Takes children lock.
    fn maintain(&self) {
        let now = now_epoch_secs();
        let mut children = self.children.lock();

        let mut active_count = 0usize;

        for idx in 0..self.max_workers {
            let slot = &self.slots[idx];
            if !slot.active.load(Ordering::Acquire) {
                continue;
            }
            active_count += 1;

            // Check if worker process is still alive
            let alive = children[idx]
                .as_mut()
                .map(|c| matches!(c.try_wait(), Ok(None)))
                .unwrap_or(false);

            if !alive && !slot.busy.load(Ordering::Acquire) {
                // Worker died — respawn if we need it (under min_workers always respawn)
                if active_count <= self.min_workers {
                    let port = slot.port;
                    if let Ok(child) = spawn_php_cgi(&self.php_cgi_path, &self.working_dir, port) {
                        let pid = child.id();
                        children[idx] = Some(child);
                        slot.pid.store(pid, Ordering::Release);
                        slot.last_active.store(now, Ordering::Release);
                    } else {
                        slot.active.store(false, Ordering::Release);
                        children[idx] = None;
                        active_count -= 1;
                    }
                } else {
                    // Excess dead worker — just deactivate
                    slot.active.store(false, Ordering::Release);
                    children[idx] = None;
                    active_count -= 1;
                }
            }
        }

        // Reap excess idle workers beyond min_workers
        if active_count > self.min_workers {
            let mut excess = active_count - self.min_workers;
            for idx in (0..self.max_workers).rev() {
                if excess == 0 {
                    break;
                }
                let slot = &self.slots[idx];
                if !slot.active.load(Ordering::Acquire) || slot.busy.load(Ordering::Acquire) {
                    continue;
                }
                let idle = now.saturating_sub(slot.last_active.load(Ordering::Acquire));
                if idle >= self.idle_timeout_secs {
                    if let Some(ref mut child) = children[idx] {
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                    children[idx] = None;
                    slot.active.store(false, Ordering::Release);
                    excess -= 1;
                }
            }
        }

        // Ensure min_workers are running
        let current_active: usize = self.slots.iter()
            .filter(|s| s.active.load(Ordering::Acquire))
            .count();
        if current_active < self.min_workers {
            for idx in 0..self.max_workers {
                if current_active + (idx - idx) >= self.min_workers {
                    // Already enough after spawning
                    break;
                }
                let slot = &self.slots[idx];
                if slot.active.load(Ordering::Acquire) {
                    continue;
                }
                let port = slot.port;
                if let Ok(child) = spawn_php_cgi(&self.php_cgi_path, &self.working_dir, port) {
                    let pid = child.id();
                    children[idx] = Some(child);
                    slot.pid.store(pid, Ordering::Release);
                    slot.last_active.store(now, Ordering::Release);
                    slot.active.store(true, Ordering::Release);
                    let new_active: usize = self.slots.iter()
                        .filter(|s| s.active.load(Ordering::Acquire))
                        .count();
                    if new_active >= self.min_workers {
                        break;
                    }
                }
            }
        }
    }

    fn stats(&self) -> PoolStats {
        let now = now_epoch_secs();
        let mut workers = Vec::new();
        let mut busy = 0usize;
        for slot in &self.slots {
            if !slot.active.load(Ordering::Relaxed) {
                continue;
            }
            let is_busy = slot.busy.load(Ordering::Relaxed);
            if is_busy {
                busy += 1;
            }
            let last = slot.last_active.load(Ordering::Relaxed);
            workers.push(WorkerInfoOut {
                port: slot.port,
                busy: is_busy,
                pid: slot.pid.load(Ordering::Relaxed),
                idle_since_secs: if is_busy {
                    0.0
                } else {
                    now.saturating_sub(last) as f64
                },
            });
        }
        let total = workers.len();
        PoolStats {
            total_workers: total,
            busy_workers: busy,
            idle_workers: total - busy,
            min_workers: self.min_workers,
            max_workers: self.max_workers,
            idle_timeout_secs: self.idle_timeout_secs,
            total_connections: self.total_connections.load(Ordering::Relaxed),
            workers,
        }
    }

    /// Shut down all workers.
    fn shutdown(&self) {
        let mut children = self.children.lock();
        for idx in 0..self.max_workers {
            self.slots[idx].active.store(false, Ordering::Release);
            if let Some(ref mut child) = children[idx] {
                let _ = child.kill();
                let _ = child.wait();
            }
            children[idx] = None;
        }
    }
}

// ── High-performance bidirectional pipe ──────────────────────────────────

fn pipe_bidirectional(mut client: TcpStream, mut upstream: TcpStream) {
    let timeout = Some(Duration::from_secs(STREAM_TIMEOUT_SECS));
    let _ = client.set_read_timeout(timeout);
    let _ = client.set_write_timeout(timeout);
    let _ = upstream.set_read_timeout(timeout);
    let _ = upstream.set_write_timeout(timeout);

    let mut client2 = match client.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut upstream2 = match upstream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Client → Upstream (request body) in a separate thread
    let t = thread::spawn(move || {
        let mut buf = [0u8; PIPE_BUF_SIZE];
        loop {
            match client2.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if upstream2.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => break,
                Err(_) => break,
            }
        }
        let _ = upstream2.shutdown(std::net::Shutdown::Write);
    });

    // Upstream → Client (response) in the current thread
    let mut buf = [0u8; PIPE_BUF_SIZE];
    loop {
        match upstream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if client.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }
    let _ = client.shutdown(std::net::Shutdown::Write);
    let _ = t.join();
}

// ── CLI parsing ─────────────────────────────────────────────────────────

struct Config {
    port: u16,
    cgi_path: String,
    working_dir: String,
    min_workers: usize,
    max_workers: usize,
    idle_timeout: u64,
    stats_file: Option<String>,
}

fn parse_args() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();
    let mut config = Config {
        port: 9001,
        cgi_path: String::new(),
        working_dir: String::new(),
        min_workers: 2,
        max_workers: 12,
        idle_timeout: 30,
        stats_file: None,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                i += 1;
                config.port = args
                    .get(i)
                    .ok_or("--port requires a value")?
                    .parse()
                    .map_err(|_| "invalid port number")?;
            }
            "--cgi" | "-c" => {
                i += 1;
                config.cgi_path = args.get(i).ok_or("--cgi requires a value")?.clone();
            }
            "--dir" | "-d" => {
                i += 1;
                config.working_dir = args.get(i).ok_or("--dir requires a value")?.clone();
            }
            "--min" => {
                i += 1;
                config.min_workers = args
                    .get(i)
                    .ok_or("--min requires a value")?
                    .parse()
                    .map_err(|_| "invalid min workers")?;
            }
            "--max" => {
                i += 1;
                config.max_workers = args
                    .get(i)
                    .ok_or("--max requires a value")?
                    .parse()
                    .map_err(|_| "invalid max workers")?;
            }
            "--idle-timeout" | "-t" => {
                i += 1;
                config.idle_timeout = args
                    .get(i)
                    .ok_or("--idle-timeout requires a value")?
                    .parse()
                    .map_err(|_| "invalid idle timeout")?;
            }
            "--stats-file" | "-s" => {
                i += 1;
                config.stats_file =
                    Some(args.get(i).ok_or("--stats-file requires a value")?.clone());
            }
            "--help" | "-h" => {
                eprintln!(
                    "taurio-fpm - High-performance FastCGI process manager for php-cgi\n\
                     \n\
                     Usage: taurio-fpm --cgi <php-cgi-path> [options]\n\
                     \n\
                     Options:\n\
                     \x20 --port, -p <port>        Listen port (default: 9001)\n\
                     \x20 --cgi, -c <path>         Path to php-cgi.exe (required)\n\
                     \x20 --dir, -d <path>         Working directory for php-cgi\n\
                     \x20 --min <n>                Minimum workers (default: 2)\n\
                     \x20 --max <n>                Maximum workers (default: 12)\n\
                     \x20 --idle-timeout, -t <s>   Idle worker reap timeout (default: 30)\n\
                     \x20 --stats-file, -s <path>  Write JSON stats to this file every 2s\n\
                     \x20 --help, -h               Show this help"
                );
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {}", other));
            }
        }
        i += 1;
    }

    if config.cgi_path.is_empty() {
        return Err("--cgi is required".to_string());
    }
    if config.min_workers == 0 {
        config.min_workers = 1;
    }
    if config.max_workers < config.min_workers {
        config.max_workers = config.min_workers;
    }

    Ok(config)
}

// ── Main ────────────────────────────────────────────────────────────────

fn main() {
    let config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            eprintln!("Run with --help for usage.");
            std::process::exit(1);
        }
    };

    eprintln!(
        "taurio-fpm starting on port {} (workers {}-{}, idle timeout {}s, buf {}KB)",
        config.port, config.min_workers, config.max_workers, config.idle_timeout,
        PIPE_BUF_SIZE / 1024,
    );
    eprintln!("  php-cgi: {}", config.cgi_path);
    if let Some(ref sf) = config.stats_file {
        eprintln!("  stats:   {}", sf);
    }

    let shutdown = Arc::new(AtomicBool::new(false));

    {
        let shutdown_c = shutdown.clone();
        let _ = ctrlc_handler(move || {
            eprintln!("\ntaurio-fpm shutting down...");
            shutdown_c.store(true, Ordering::Relaxed);
        });
    }

    let pool = Arc::new(Pool::new(
        config.cgi_path.clone(),
        config.working_dir.clone(),
        config.port,
        config.min_workers,
        config.max_workers,
        config.idle_timeout,
    ));

    if let Err(e) = pool.warm_up() {
        eprintln!("Failed to spawn initial workers: {}", e);
        std::process::exit(1);
    }

    // Let initial workers bind
    thread::sleep(Duration::from_millis(INITIAL_STARTUP_WAIT_MS));

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", config.port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind port {}: {}", config.port, e);
            std::process::exit(1);
        }
    };
    let _ = listener.set_nonblocking(true);

    eprintln!(
        "taurio-fpm ready, listening on 127.0.0.1:{}",
        config.port
    );

    // Watchdog thread
    let shutdown_w = shutdown.clone();
    let pool_w = pool.clone();
    let stats_file = config.stats_file.clone();

    let watchdog = thread::Builder::new()
        .name("fpm-watchdog".into())
        .spawn(move || {
            let mut ms_elapsed = 0u64;
            loop {
                if shutdown_w.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(Duration::from_millis(WATCHDOG_INTERVAL_MS));
                ms_elapsed += WATCHDOG_INTERVAL_MS;

                pool_w.maintain();

                if ms_elapsed % (STATS_WRITE_INTERVAL_SECS * 1000) < WATCHDOG_INTERVAL_MS {
                    if let Some(ref path) = stats_file {
                        let stats = pool_w.stats();
                        if let Ok(json) = serde_json::to_string(&stats) {
                            let _ = std::fs::write(path, json);
                        }
                    }
                }
            }

            pool_w.shutdown();
            if let Some(ref path) = stats_file {
                let _ = std::fs::remove_file(path);
            }
        })
        .expect("failed to spawn watchdog thread");

    // Accept loop
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Err(_) => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
                continue;
            }
        };

        let _ = stream.set_nodelay(true);

        // Lock-free acquire — no mutex in the common case
        let worker_port = pool.acquire();

        let port = match worker_port {
            Some(p) => p,
            None => {
                // All workers busy — backpressure: hold the connection and
                // poll for a free worker. This prevents nginx from seeing a
                // connection reset (502) under load bursts.
                let start = Instant::now();
                let mut got = None;
                while start.elapsed().as_millis() < BACKPRESSURE_WAIT_MS as u128 {
                    thread::sleep(Duration::from_millis(BACKPRESSURE_POLL_MS));
                    if let Some(p) = pool.acquire() {
                        got = Some(p);
                        break;
                    }
                }
                match got {
                    Some(p) => p,
                    None => {
                        // Truly overloaded — drop connection, nginx returns 502.
                        // This is the correct back-pressure signal.
                        continue;
                    }
                }
            }
        };

        let pool_c = pool.clone();
        thread::spawn(move || {
            dispatch_to_worker(stream, port, &pool_c);
        });
    }

    drop(listener);
    let _ = watchdog.join();
    eprintln!("taurio-fpm stopped.");
}

/// Connect to a php-cgi worker and pipe the request/response.
fn dispatch_to_worker(stream: TcpStream, worker_port: u16, pool: &Pool) {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", worker_port).parse().unwrap();

    // Fast connect with brief retry for freshly-spawned workers
    let upstream = {
        let start = Instant::now();
        loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
                Ok(s) => break Some(s),
                Err(_) if start.elapsed() < Duration::from_millis(1500) => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break None,
            }
        }
    };

    if let Some(upstream) = upstream {
        let _ = upstream.set_nodelay(true);
        pipe_bidirectional(stream, upstream);
    }

    pool.release(worker_port);
}

/// Simple Ctrl+C handler using Windows API directly.
fn ctrlc_handler<F: Fn() + Send + Sync + 'static>(f: F) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        use std::sync::OnceLock;
        static HANDLER: OnceLock<Box<dyn Fn() + Send + Sync>> = OnceLock::new();
        HANDLER
            .set(Box::new(f))
            .map_err(|_| "handler already set")?;

        extern "system" fn ctrl_handler(_ctrl_type: u32) -> i32 {
            if let Some(f) = HANDLER.get() {
                f();
            }
            1 // TRUE — handled
        }

        extern "system" {
            fn SetConsoleCtrlHandler(
                handler: extern "system" fn(u32) -> i32,
                add: i32,
            ) -> i32;
        }

        unsafe {
            if SetConsoleCtrlHandler(ctrl_handler, 1) == 0 {
                return Err("SetConsoleCtrlHandler failed".into());
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = f;
    }

    Ok(())
}
