//! Sharded server: N single-worker instances with direct in-memory
//! cross-shard dispatch.
//!
//! Each shard is a `Server` with `workers = 1` and its own `SubList`.
//! Cross-shard delivery uses `mpsc` channels carrying `ShardMsg` — no
//! serialization, no route protocol framing, no kernel round-trip.
//!
//! External clients connect to ONE set of ports (the base config's port
//! and binary_port). The accept loop distributes connections round-robin
//! across all shard workers. WorkerInterest tracks which shards have
//! matching subscriptions so the publish path only pushes to interested
//! shards.

use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use tracing::info;

use super::server::{Server, ServerConfig, ShardDispatch};

/// Run N single-worker `Server` instances in one process, connected by
/// in-memory channels for cross-shard message delivery.
pub struct ShardedServer {
    shards: Vec<Server>,
    metrics_port: Option<u16>,
    /// Original host + ports for the external listener.
    host: String,
    port: u16,
    binary_port: Option<u16>,
}

impl ShardedServer {
    /// Create `n` shards from `base`. Each shard gets `workers = 1` and
    /// its own `SubList`. Ports are NOT per-shard — the ShardedServer
    /// runs one external listener and distributes connections.
    pub fn new(base: ServerConfig, n: usize) -> Self {
        let metrics_port = base.metrics_port;
        let host = base.host.clone();
        let port = base.port;
        let binary_port = base.binary_port;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let mut cfg = base.clone();
            cfg.workers = 1;
            cfg.server_name = format!("{}-shard-{}", base.server_name, i);
            cfg.metrics_port = None;
            cfg.monitoring_port = None;
            // No cluster seeds — shards are connected via in-memory channels.
            cfg.cluster.seeds.clear();
            cfg.cluster.port = None;
            shards.push(Server::new(cfg));
        }
        Self { shards, metrics_port, host, port, binary_port }
    }

    /// Start all shards + one external accept loop. Blocks forever.
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let n = self.shards.len();
        if n < 2 {
            return self.shards[0].run();
        }

        if let Some(port) = self.metrics_port {
            super::server::install_metrics_exporter(port)?;
            info!(port, "prometheus metrics endpoint listening (sharded)");
        }

        info!(shards = n, "starting sharded server");

        // Create per-shard inbox channels.
        let mut senders: Vec<crossbeam_channel::Sender<crate::handler::ShardMsg>> = Vec::with_capacity(n);
        let mut receivers: Vec<Option<crossbeam_channel::Receiver<crate::handler::ShardMsg>>> = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = crossbeam_channel::bounded(65536);
            senders.push(tx);
            receivers.push(Some(rx));
        }

        // Collect worker handles + eventfds via the hook, then wire up
        // shard_dispatch and shard_inbox.
        let barrier = Arc::new(std::sync::Barrier::new(n + 1));
        type HandleEntry = (
            Vec<mpsc::Sender<super::worker::WorkerCmd>>,
            Vec<Arc<std::os::fd::OwnedFd>>,
            Arc<super::server::ServerState>,
        );
        let registry: Arc<std::sync::Mutex<Vec<HandleEntry>>> =
            Arc::new(std::sync::Mutex::new(Vec::with_capacity(n)));

        let host = &self.host;
        let port = self.port;
        let binary_port = self.binary_port;

        thread::scope(|s| {
            // Phase 1: spawn shard threads. Each starts its worker but
            // does NOT start a listener (we'll run the listener below).
            for i in 0..n {
                let barrier = Arc::clone(&barrier);
                let registry = Arc::clone(&registry);
                let server = &self.shards[i];
                let rx = receivers[i].take().expect("receiver already taken");
                let senders_clone = senders.clone();

                s.spawn(move || {
                    server.run_workers_only(|workers, state| {
                        let wk_senders: Vec<_> = workers.iter().map(|w| w.tx.clone()).collect();
                        let wk_eventfds: Vec<_> = workers.iter().map(|w| Arc::clone(&w.event_fd)).collect();

                        // Wire the inbox receiver into the worker.
                        // The worker's shard_inbox is currently None; we
                        // can't set it directly because Worker is on
                        // another thread. Instead, send the receiver via
                        // a special command... actually, we need to set
                        // it before the worker's run() enters the epoll
                        // loop. Since the hook fires AFTER spawn_workers
                        // but the worker is already running its epoll
                        // loop, we can't inject the receiver into the
                        // worker struct.
                        //
                        // Workaround: store the receiver on ServerState
                        // so the worker can pick it up on its next
                        // eventfd wake. This is safe because only one
                        // worker thread reads it.
                        //
                        // Actually — let's use a simpler trick: the
                        // receiver goes into the worker via a Mutex on
                        // ServerState, and the worker takes it on its
                        // first eventfd wake.
                        *state.shard_inbox_slot.lock().expect("lock") = Some(rx);

                        {
                            let mut reg = registry.lock().expect("lock");
                            reg.push((wk_senders, wk_eventfds, Arc::clone(state)));
                        }
                        barrier.wait(); // "my workers are ready"
                        barrier.wait(); // "dispatch is wired"
                    });
                    // run_workers_only blocks forever (parks the thread).
                });
            }

            // Phase 2: wire shard_dispatch on each shard's ServerState.
            barrier.wait(); // all shards registered
            {
                let reg = registry.lock().expect("lock");
                let all_eventfds: Vec<Arc<std::os::fd::OwnedFd>> = reg
                    .iter()
                    .flat_map(|(_, fds, _)| fds.iter().cloned())
                    .collect();

                // Shared interest map — ALL shards read/write the same instance.
                let shared_interest = Arc::new(
                    crate::pubsub::worker_interest::WorkerInterest::new(),
                );

                for (i, (_, _, state)) in reg.iter().enumerate() {
                    let dispatch = ShardDispatch {
                        shard_index: i,
                        inboxes: senders.clone(),
                        eventfds: all_eventfds.clone(),
                        interest: Arc::clone(&shared_interest),
                    };
                    let _ = state.shard_dispatch.set(dispatch);
                }
            }

            info!("shard dispatch wired");
            barrier.wait(); // release shards to accept loops

            // Phase 3: run one external accept loop that distributes
            // connections across ALL shard workers.
            let reg = registry.lock().expect("lock");
            let all_senders: Vec<mpsc::Sender<super::worker::WorkerCmd>> = reg
                .iter()
                .flat_map(|(senders, _, _)| senders.iter().cloned())
                .collect();

            let any_state = Arc::clone(&reg[0].2);
            drop(reg);

            let bind_addr = format!("{host}:{port}");
            let listener = TcpListener::bind(&bind_addr).expect("bind main listener");
            info!(addr = %bind_addr, "sharded server listening (tcp)");

            let binary_listener = binary_port.map(|bin_port| {
                let bin_addr = format!("{host}:{bin_port}");
                let bl = TcpListener::bind(&bin_addr).expect("bind binary listener");
                info!(addr = %bin_addr, "sharded server listening (binary)");
                bl
            });

            // Simple accept loop — poll both listeners.
            listener.set_nonblocking(true).ok();
            if let Some(ref bl) = binary_listener {
                bl.set_nonblocking(true).ok();
            }

            let mut pfds = [
                libc::pollfd {
                    fd: listener.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: binary_listener.as_ref().map(|l| l.as_raw_fd()).unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];

            // Collect eventfds for waking workers after sending connections.
            let reg2 = registry.lock().expect("lock");
            let worker_eventfds: Vec<Arc<std::os::fd::OwnedFd>> = reg2
                .iter()
                .flat_map(|(_, fds, _)| fds.iter().cloned())
                .collect();
            drop(reg2);

            let mut next_worker = 0usize;
            let total_workers = all_senders.len();

            loop {
                pfds[0].revents = 0;
                pfds[1].revents = 0;
                let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
                if ret < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    break;
                }

                if pfds[0].revents & libc::POLLIN != 0 {
                    while let Ok((stream, addr)) = listener.accept() {
                        let cid = any_state.next_client_id();
                        any_state.active_connections.fetch_add(1, Ordering::Relaxed);
                        let idx = next_worker % total_workers;
                        next_worker = idx + 1;
                        let _ = all_senders[idx].send(super::worker::WorkerCmd::NewConn {
                            id: cid,
                            stream,
                            addr,
                            is_websocket: false,
                        });
                        wake_worker(&worker_eventfds[idx]);
                    }
                }

                if pfds[1].revents & libc::POLLIN != 0 {
                    if let Some(ref bl) = binary_listener {
                        while let Ok((stream, addr)) = bl.accept() {
                            let cid = any_state.next_client_id();
                            any_state.active_connections.fetch_add(1, Ordering::Relaxed);
                            let idx = next_worker % total_workers;
                            next_worker = idx + 1;
                            let _ = all_senders[idx].send(super::worker::WorkerCmd::NewBinaryConn {
                                id: cid,
                                stream,
                                addr,
                            });
                            wake_worker(&worker_eventfds[idx]);
                        }
                    }
                }
            }
        });
        Ok(())
    }
}

/// Write 1 to a worker's eventfd to wake its epoll loop so it picks up
/// pending commands from its mpsc channel.
fn wake_worker(fd: &std::os::fd::OwnedFd) {
    let val: u64 = 1;
    unsafe {
        libc::write(
            fd.as_raw_fd(),
            &val as *const u64 as *const libc::c_void,
            8,
        );
    }
}
