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
//!
//! The ShardedServer appears as ONE node to the mesh cluster. All
//! shards share shard 0's ClusterState (route_writers, local_sub_counts,
//! route_peers) via the `cluster_state()` helper so route connections
//! can be distributed across all shard workers. Shard 0's ServerState
//! drives the RouteConnManager for outbound route connections.

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
    host: String,
    port: u16,
    binary_port: Option<u16>,
    cluster_port: Option<u16>,
}

impl ShardedServer {
    /// Create `n` shards from `base`. Each shard gets `workers = 1` and
    /// its own `SubList`. Ports are NOT per-shard — the ShardedServer
    /// runs one external listener and distributes connections.
    ///
    /// Shard 0 retains the cluster config (seeds, name) so its
    /// `ServerState` can drive the `RouteConnManager`. All other shards
    /// have cluster config cleared.
    pub fn new(base: ServerConfig, n: usize) -> Self {
        let metrics_port = base.metrics_port;
        let host = base.host.clone();
        let port = base.port;
        let binary_port = base.binary_port;
        let cluster_port = base.cluster.port;
        // Single server_id for the whole sharded server. Route peer dedup
        // keys on server_id; per-shard random IDs would let one peer-pair
        // form one conn per (local-shard, remote-shard) combo, multiplying
        // route deliveries by N×M.
        let shared_server_id = base
            .server_id
            .clone()
            .unwrap_or_else(|| format!("LEAF_{}", rand::random::<u32>()));
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let mut cfg = base.clone();
            cfg.workers = 1;
            cfg.server_name = format!("{}-shard-{}", base.server_name, i);
            cfg.server_id = Some(shared_server_id.clone());
            cfg.metrics_port = None;
            cfg.monitoring_port = None;
            // All shards get the cluster port + name so route handshake
            // INFO is correct regardless of which shard the route lands
            // on. Only shard 0 keeps seeds (drives RouteConnManager).
            // ShardedServer owns the actual listener — no shard binds.
            if i > 0 {
                cfg.cluster.seeds.clear();
            }
            shards.push(Server::new(cfg));
        }
        Self {
            shards,
            metrics_port,
            host,
            port,
            binary_port,
            cluster_port,
        }
    }

    /// Number of mesh route peers currently connected, deduped by `server_id`.
    /// Reads the shared `route_peers` map (held on shard 0's cluster state) so
    /// the count reflects the cluster-wide unique-peer view, not per-shard
    /// route conn counts. Intended for tests + observability — a healthy full
    /// mesh has `route_peer_count() == hub_count - 1` on every node.
    pub fn route_peer_count(&self) -> usize {
        self.shards[0]
            .state()
            .cluster_state()
            .route_peers
            .lock()
            .map(|p| p.connected.len())
            .unwrap_or(0)
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

        // Bounded per-shard inbox channels. Capacity chosen to absorb
        // short bursts without blocking while providing backpressure
        // under sustained overload (~640 KB per channel at 80B/msg).
        const SHARD_CHANNEL_CAP: usize = 8192;
        let mut senders: Vec<mpsc::SyncSender<crate::handler::ShardMsg>> = Vec::with_capacity(n);
        let mut receivers: Vec<Option<mpsc::Receiver<crate::handler::ShardMsg>>> =
            Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::sync_channel(SHARD_CHANNEL_CAP);
            senders.push(tx);
            receivers.push(Some(rx));
        }

        let barrier = Arc::new(std::sync::Barrier::new(n + 1));
        type HandleEntry = (
            usize, // shard index
            Vec<mpsc::Sender<super::worker::WorkerCmd>>,
            Vec<Arc<std::os::fd::OwnedFd>>,
            Arc<super::server::ServerState>,
        );
        let registry: Arc<std::sync::Mutex<Vec<HandleEntry>>> =
            Arc::new(std::sync::Mutex::new(Vec::with_capacity(n)));

        let host = &self.host;
        let port = self.port;
        let binary_port = self.binary_port;
        let cluster_port = self.cluster_port;

        thread::scope(|s| {
            // Phase 1: spawn shard threads.
            for (i, rx_slot) in receivers.iter_mut().enumerate() {
                let barrier = Arc::clone(&barrier);
                let registry = Arc::clone(&registry);
                let server = &self.shards[i];
                let rx = rx_slot.take().expect("receiver already taken");

                s.spawn(move || {
                    server.run_workers_only(|workers, state| {
                        let wk_senders: Vec<_> = workers.iter().map(|w| w.tx.clone()).collect();
                        let wk_eventfds: Vec<_> =
                            workers.iter().map(|w| Arc::clone(&w.event_fd)).collect();

                        *state.shard_inbox_slot.lock().expect("lock") = Some(rx);

                        {
                            let mut reg = registry.lock().expect("lock");
                            reg.push((i, wk_senders, wk_eventfds, Arc::clone(state)));
                        }
                        barrier.wait(); // "my workers are ready"
                        barrier.wait(); // "dispatch is wired"
                    });
                });
            }

            // Phase 2: wire shard_dispatch on each shard's ServerState.
            barrier.wait();
            {
                let mut reg = registry.lock().expect("lock");
                reg.sort_by_key(|(idx, _, _, _)| *idx);

                let all_eventfds: Vec<Arc<std::os::fd::OwnedFd>> = reg
                    .iter()
                    .flat_map(|(_, _, fds, _)| fds.iter().cloned())
                    .collect();

                let shared_interest =
                    Arc::new(crate::pubsub::worker_interest::WorkerInterest::new());

                // reg[0] is now guaranteed to be shard 0.
                let route_authority = Arc::clone(&reg[0].3);

                for (i, (_, _, _, state)) in reg.iter().enumerate() {
                    let dispatch = ShardDispatch {
                        shard_index: i,
                        inboxes: senders.clone(),
                        eventfds: all_eventfds.clone(),
                        interest: Arc::clone(&shared_interest),
                        congested: std::sync::atomic::AtomicBool::new(false),
                        route_authority: Arc::clone(&route_authority),
                    };
                    let _ = state.shard_dispatch.set(dispatch);
                }
            }

            info!("shard dispatch wired");
            barrier.wait();

            // Phase 3: collect worker handles, start RouteConnManager,
            // bind listeners, run accept loop.
            let reg = registry.lock().expect("lock");
            let all_senders: Vec<mpsc::Sender<super::worker::WorkerCmd>> = reg
                .iter()
                .flat_map(|(_, senders, _, _)| senders.iter().cloned())
                .collect();
            let worker_eventfds: Vec<Arc<std::os::fd::OwnedFd>> = reg
                .iter()
                .flat_map(|(_, _, fds, _)| fds.iter().cloned())
                .collect();

            // reg[0] is shard 0 (sorted in Phase 2). Its ServerState
            // has the cluster seeds, port, and route_writers.
            let shard0_state = Arc::clone(&reg[0].3);
            let any_state = Arc::clone(&shard0_state);

            drop(reg);

            // Start outbound route connections using shard 0's state.
            let _route_mgr = if !shard0_state.cluster.seeds.is_empty() {
                info!(
                    seeds = ?shard0_state.cluster.seeds,
                    "connecting to route peers (sharded)"
                );
                Some(crate::connector::mesh::RouteConnManager::spawn(Arc::clone(
                    &shard0_state,
                )))
            } else {
                None
            };

            // Bind listeners.
            let bind_addr = format!("{host}:{port}");
            let listener = TcpListener::bind(&bind_addr).expect("bind main listener");
            unsafe {
                libc::listen(listener.as_raw_fd() as _, 4096);
            }
            info!(addr = %bind_addr, "sharded server listening (tcp)");

            let binary_listener = binary_port.map(|bin_port| {
                let bin_addr = format!("{host}:{bin_port}");
                let bl = TcpListener::bind(&bin_addr).expect("bind binary listener");
                unsafe {
                    libc::listen(bl.as_raw_fd() as _, 4096);
                }
                info!(addr = %bin_addr, "sharded server listening (binary)");
                bl
            });

            let cluster_listener = cluster_port.map(|cp| {
                let cl_addr = format!("{host}:{cp}");
                let cl = TcpListener::bind(&cl_addr).expect("bind cluster listener");
                unsafe {
                    libc::listen(cl.as_raw_fd() as _, 4096);
                }
                info!(addr = %cl_addr, "sharded server listening (cluster)");
                cl
            });

            listener.set_nonblocking(true).ok();
            if let Some(ref bl) = binary_listener {
                bl.set_nonblocking(true).ok();
            }
            if let Some(ref cl) = cluster_listener {
                cl.set_nonblocking(true).ok();
            }

            let mut pfds = [
                libc::pollfd {
                    fd: listener.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: binary_listener
                        .as_ref()
                        .map(|l| l.as_raw_fd())
                        .unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: cluster_listener
                        .as_ref()
                        .map(|l| l.as_raw_fd())
                        .unwrap_or(-1),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];

            let mut next_worker = 0usize;
            let total_workers = all_senders.len();

            loop {
                pfds[0].revents = 0;
                pfds[1].revents = 0;
                pfds[2].revents = 0;
                let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 3, -1) };
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
                            let _ =
                                all_senders[idx].send(super::worker::WorkerCmd::NewBinaryConn {
                                    id: cid,
                                    stream,
                                    addr,
                                });
                            wake_worker(&worker_eventfds[idx]);
                        }
                    }
                }

                // Route connections round-robin across all shard workers.
                // Shared cluster metadata (route_writers, local_sub_counts,
                // route_peers) accessed via cluster_state() helper.
                if pfds[2].revents & libc::POLLIN != 0 {
                    if let Some(ref cl) = cluster_listener {
                        while let Ok((stream, addr)) = cl.accept() {
                            let cid = any_state.next_client_id();
                            any_state.active_connections.fetch_add(1, Ordering::Relaxed);
                            let idx = next_worker % total_workers;
                            next_worker = idx + 1;
                            let _ = all_senders[idx].send(super::worker::WorkerCmd::NewRouteConn {
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

fn wake_worker(fd: &std::os::fd::OwnedFd) {
    let val: u64 = 1;
    unsafe {
        libc::write(fd.as_raw_fd(), &val as *const u64 as *const libc::c_void, 8);
    }
}
