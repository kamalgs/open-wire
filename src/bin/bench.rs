//! open-wire binary-client benchmark tool.
//!
//! Measures raw throughput using the binary wire protocol (no NATS text overhead).
//! Connects to an open-wire server's binary port (`--binary-port` / `BINARY_PORT` env).
//!
//! Usage:
//! ```
//! bench [OPTIONS]
//!
//! Options:
//!   --addr HOST:PORT      Binary server address (sets both pub and sub) [default: 127.0.0.1:4223]
//!   --pub-addr HOST:PORT  Publisher address (overrides --addr for pub)
//!   --sub-addr HOST:PORT  Subscriber address (overrides --addr for sub; enables cross-node)
//!   --pub N               Publisher connections          [default: 1]
//!   --sub N               Subscriber connections         [default: 0]
//!   --size BYTES          Payload size                   [default: 128]
//!   --duration SECS       Benchmark duration             [default: 5]
//!   --subject SUBJECT     Subject to publish/subscribe   [default: bench.test]
//! ```
//!
//! Example — pub/sub throughput with 2 publishers and 1 subscriber:
//! ```
//! bench --addr 127.0.0.1:4223 --pub 2 --sub 1 --size 512 --duration 10
//! ```

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ── Binary frame encoding ────────────────────────────────────────────────────

const HEADER_LEN: usize = 9;

#[repr(u8)]
#[derive(Clone, Copy)]
enum Op {
    Pong = 0x02,
    Msg = 0x03,
    Sub = 0x05,
}

fn encode(op: Op, subject: &[u8], reply: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + subject.len() + reply.len() + payload.len());
    out.push(op as u8);
    out.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    out.extend_from_slice(&(reply.len() as u16).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(subject);
    out.extend_from_slice(reply);
    out.extend_from_slice(payload);
    out
}

fn write_msg(subject: &[u8], payload: &[u8], out: &mut Vec<u8>) {
    out.push(Op::Msg as u8);
    out.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // no reply
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(subject);
    out.extend_from_slice(payload);
}

fn write_pong(out: &mut Vec<u8>) {
    out.extend_from_slice(&encode(Op::Pong, b"", b"", b""));
}

fn write_sub(subject: &[u8], sid: u32, out: &mut Vec<u8>) {
    let sid_bytes = sid.to_le_bytes();
    out.extend_from_slice(&encode(Op::Sub, subject, b"", &sid_bytes));
}

// ── Frame reader ─────────────────────────────────────────────────────────────

struct FrameReader {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
}

impl FrameReader {
    fn new(stream: TcpStream) -> Self {
        FrameReader {
            stream,
            buf: vec![0u8; 65536],
            pos: 0,
            filled: 0,
        }
    }

    /// Read until we have `n` bytes available past `pos`.
    /// Returns `Err(WouldBlock)` on read timeout so the caller can check the stop flag.
    fn fill_to(&mut self, n: usize) -> io::Result<()> {
        while self.filled - self.pos < n {
            if self.pos > 0 {
                self.buf.copy_within(self.pos..self.filled, 0);
                self.filled -= self.pos;
                self.pos = 0;
            }
            if self.filled == self.buf.len() {
                self.buf.resize(self.buf.len() * 2, 0);
            }
            match self.stream.read(&mut self.buf[self.filled..]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed",
                    ))
                }
                Ok(n) => self.filled += n,
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Read one frame. Returns (op, payload_total_len). Skips Ping frames (sends Pong).
    /// Returns None on Pong (keepalive).
    fn next_frame_size(&mut self) -> io::Result<Option<(u8, usize)>> {
        self.fill_to(HEADER_LEN)?;
        let op = self.buf[self.pos];
        let subj_len =
            u16::from_le_bytes([self.buf[self.pos + 1], self.buf[self.pos + 2]]) as usize;
        let repl_len =
            u16::from_le_bytes([self.buf[self.pos + 3], self.buf[self.pos + 4]]) as usize;
        let pay_len = u32::from_le_bytes([
            self.buf[self.pos + 5],
            self.buf[self.pos + 6],
            self.buf[self.pos + 7],
            self.buf[self.pos + 8],
        ]) as usize;
        let total = HEADER_LEN + subj_len + repl_len + pay_len;
        self.fill_to(total)?;
        self.pos += total;
        Ok(Some((op, pay_len)))
    }
}

// ── Publisher ────────────────────────────────────────────────────────────────

fn run_publisher(
    addr: &str,
    subject: Arc<String>,
    payload_size: usize,
    stop: Arc<AtomicBool>,
    msgs_sent: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
) {
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("publisher: connect failed: {e}");
            return;
        }
    };
    stream.set_nodelay(true).ok();

    let payload = vec![b'x'; payload_size];
    let subj = subject.as_bytes();
    // Pre-build a batch of frames to amortize syscall overhead.
    const BATCH: usize = 64;
    let mut batch_buf: Vec<u8> =
        Vec::with_capacity((HEADER_LEN + subj.len() + payload_size) * BATCH);

    let mut local_msgs = 0u64;
    let mut local_bytes = 0u64;

    while !stop.load(Ordering::Relaxed) {
        batch_buf.clear();
        for _ in 0..BATCH {
            write_msg(subj, &payload, &mut batch_buf);
        }
        if stream.write_all(&batch_buf).is_err() {
            break;
        }
        local_msgs += BATCH as u64;
        local_bytes += batch_buf.len() as u64;

        // Periodically flush counters to shared atomics.
        if local_msgs.is_multiple_of(BATCH as u64 * 16) {
            msgs_sent.fetch_add(local_msgs, Ordering::Relaxed);
            bytes_sent.fetch_add(local_bytes, Ordering::Relaxed);
            local_msgs = 0;
            local_bytes = 0;
        }
    }
    msgs_sent.fetch_add(local_msgs, Ordering::Relaxed);
    bytes_sent.fetch_add(local_bytes, Ordering::Relaxed);
}

// ── Subscriber ───────────────────────────────────────────────────────────────

fn run_subscriber(
    addr: &str,
    subject: Arc<String>,
    stop: Arc<AtomicBool>,
    msgs_recv: Arc<AtomicU64>,
    bytes_recv: Arc<AtomicU64>,
) {
    let stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("subscriber: connect failed: {e}");
            return;
        }
    };
    stream.set_nodelay(true).ok();
    // Short read timeout so the receive loop can wake up and check the stop flag
    // after the publisher finishes, rather than blocking on the next frame forever.
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .ok();

    // Send Sub frame: subject=pattern, reply=queue(empty), payload=SID u32 LE.
    let mut out = Vec::new();
    write_sub(subject.as_bytes(), 1, &mut out);
    {
        let mut wr = stream.try_clone().expect("clone");
        wr.write_all(&out).ok();
    }

    let mut reader = FrameReader::new(stream);

    let mut local_msgs = 0u64;
    let mut local_bytes = 0u64;

    while !stop.load(Ordering::Relaxed) {
        match reader.next_frame_size() {
            Ok(Some((0x01, _))) => {
                // Ping — send Pong
                let mut pong = Vec::new();
                write_pong(&mut pong);
                reader.stream.write_all(&pong).ok();
            }
            Ok(Some((0x03 | 0x04, pay_len))) => {
                local_msgs += 1;
                local_bytes += pay_len as u64;
                if local_msgs.is_multiple_of(1024) {
                    msgs_recv.fetch_add(local_msgs, Ordering::Relaxed);
                    bytes_recv.fetch_add(local_bytes, Ordering::Relaxed);
                    local_msgs = 0;
                    local_bytes = 0;
                }
            }
            Ok(Some(_)) => {} // Pong or unknown, skip
            Ok(None) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                // Read timeout — loop back and check stop flag.
            }
            Err(_) => break,
        }
    }
    msgs_recv.fetch_add(local_msgs, Ordering::Relaxed);
    bytes_recv.fetch_add(local_bytes, Ordering::Relaxed);
}

// ── CLI ──────────────────────────────────────────────────────────────────────

struct Args {
    /// Address publishers connect to.
    pub_addr: String,
    /// Address subscribers connect to (may differ from pub_addr for cross-node scenarios).
    sub_addr: String,
    n_pub: usize,
    n_sub: usize,
    size: usize,
    duration: u64,
    subject: String,
}

impl Args {
    fn parse() -> Self {
        let mut args = std::env::args().skip(1);
        let mut addr = "127.0.0.1:4223".to_string();
        let mut pub_addr = String::new();
        let mut sub_addr = String::new();
        let mut n_pub = 1usize;
        let mut n_sub = 0usize;
        let mut size = 128usize;
        let mut duration = 5u64;
        let mut subject = "bench.test".to_string();

        while let Some(key) = args.next() {
            match key.as_str() {
                "--addr" => addr = args.next().expect("--addr requires a value"),
                "--pub-addr" => pub_addr = args.next().expect("--pub-addr requires a value"),
                "--sub-addr" => sub_addr = args.next().expect("--sub-addr requires a value"),
                "--pub" => {
                    n_pub = args
                        .next()
                        .expect("--pub requires a value")
                        .parse()
                        .expect("--pub must be a valid integer")
                }
                "--sub" => {
                    n_sub = args
                        .next()
                        .expect("--sub requires a value")
                        .parse()
                        .expect("--sub must be a valid integer")
                }
                "--size" => {
                    size = args
                        .next()
                        .expect("--size requires a value")
                        .parse()
                        .expect("--size must be a valid integer")
                }
                "--duration" => {
                    duration = args
                        .next()
                        .expect("--duration requires a value")
                        .parse()
                        .expect("--duration must be a valid integer")
                }
                "--subject" => subject = args.next().expect("--subject requires a value"),
                other => {
                    eprintln!("unknown option: {other}");
                    std::process::exit(1);
                }
            }
        }

        // --addr sets both unless overridden individually.
        if pub_addr.is_empty() {
            pub_addr = addr.clone();
        }
        if sub_addr.is_empty() {
            sub_addr = addr;
        }

        Args {
            pub_addr,
            sub_addr,
            n_pub,
            n_sub,
            size,
            duration,
            subject,
        }
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    let subject = Arc::new(args.subject.clone());
    let stop = Arc::new(AtomicBool::new(false));

    let pub_msgs = Arc::new(AtomicU64::new(0));
    let pub_bytes = Arc::new(AtomicU64::new(0));
    let sub_msgs = Arc::new(AtomicU64::new(0));
    let sub_bytes = Arc::new(AtomicU64::new(0));

    let addr_desc = if args.pub_addr == args.sub_addr {
        args.pub_addr.clone()
    } else {
        format!("pub={} sub={}", args.pub_addr, args.sub_addr)
    };
    println!(
        "Connecting to {} | pub={} sub={} size={}B duration={}s subject={}",
        addr_desc, args.n_pub, args.n_sub, args.size, args.duration, args.subject
    );

    let mut handles = Vec::new();

    // Start subscribers first so they are ready before publishers begin.
    for _ in 0..args.n_sub {
        let addr = args.sub_addr.clone();
        let subj = Arc::clone(&subject);
        let stop = Arc::clone(&stop);
        let msgs = Arc::clone(&sub_msgs);
        let bytes = Arc::clone(&sub_bytes);
        handles.push(std::thread::spawn(move || {
            run_subscriber(&addr, subj, stop, msgs, bytes);
        }));
    }

    // Brief pause to let subscribers register before publishers start.
    if args.n_sub > 0 {
        std::thread::sleep(Duration::from_millis(100));
    }

    for _ in 0..args.n_pub {
        let addr = args.pub_addr.clone();
        let subj = Arc::clone(&subject);
        let stop = Arc::clone(&stop);
        let msgs = Arc::clone(&pub_msgs);
        let bytes = Arc::clone(&pub_bytes);
        let size = args.size;
        handles.push(std::thread::spawn(move || {
            run_publisher(&addr, subj, size, stop, msgs, bytes);
        }));
    }

    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(args.duration));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let pm = pub_msgs.load(Ordering::Relaxed);
    let pb = pub_bytes.load(Ordering::Relaxed);
    let sm = sub_msgs.load(Ordering::Relaxed);
    let sb = sub_bytes.load(Ordering::Relaxed);

    println!();
    if args.n_pub > 0 {
        let rate = pm as f64 / elapsed;
        let mbps = pb as f64 / elapsed / 1_000_000.0;
        println!(
            "Pub: {:>12} msgs  {:>8.3} Mmsg/s  {:>8.2} MB/s  ({} conns)",
            pm,
            rate / 1_000_000.0,
            mbps,
            args.n_pub
        );
    }
    if args.n_sub > 0 {
        let rate = sm as f64 / elapsed;
        let mbps = sb as f64 / elapsed / 1_000_000.0;
        println!(
            "Sub: {:>12} msgs  {:>8.3} Mmsg/s  {:>8.2} MB/s  ({} conns)",
            sm,
            rate / 1_000_000.0,
            mbps,
            args.n_sub
        );
    }
    println!("Elapsed: {elapsed:.2}s");
}
