// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! End-to-end integration tests for the leaf node gateway.
//!
//! These tests require the `nats-server` binary in PATH (or at the Go install
//! location). Install via: `go install github.com/nats-io/nats-server/v2@main`

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use open_wire::{LeafServer, LeafServerConfig};
use tokio::time::timeout;

/// Find a free TCP port by binding to :0.
fn free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Find the nats-server binary, checking common locations.
fn nats_server_bin() -> String {
    // Check PATH first
    if let Ok(output) = Command::new("which").arg("nats-server").output() {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }

    // Check Go install path
    if let Ok(output) = Command::new("go").arg("env").arg("GOPATH").output() {
        if output.status.success() {
            let gopath = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let bin = format!("{}/bin/nats-server", gopath);
            if std::path::Path::new(&bin).exists() {
                return bin;
            }
        }
    }

    panic!("nats-server binary not found. Install with: go install github.com/nats-io/nats-server/v2@main");
}

/// A running nats-server process for testing.
struct NatsServer {
    child: Child,
    port: u16,
}

impl NatsServer {
    /// Start a nats-server on the given port and wait until it accepts connections.
    fn start(port: u16) -> Self {
        let bin = nats_server_bin();
        let child = Command::new(&bin)
            .args(["-p", &port.to_string(), "-a", "127.0.0.1"])
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start nats-server at {}: {}", bin, e));

        let server = NatsServer { child, port };
        server.wait_ready();
        server
    }

    /// Poll until the server accepts a TCP connection (up to 5s).
    fn wait_ready(&self) {
        let addr = format!("127.0.0.1:{}", self.port);
        for _ in 0..50 {
            if std::net::TcpStream::connect(&addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("nats-server did not become ready on port {}", self.port);
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a LeafServer on the given port with optional hub_url, returning the
/// shutdown sender. The server runs in a background tokio task.
#[cfg(feature = "leaf")]
fn spawn_leaf(port: u16, hub_url: Option<String>) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = LeafServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        hub_url,
        server_name: format!("test-leaf-{}", port),
        ..Default::default()
    };
    let server = LeafServer::new(config);

    std::thread::spawn(move || {
        if let Err(e) = server.run_until_shutdown(shutdown_clone, reload, None) {
            eprintln!("leaf server error: {}", e);
        }
    });

    shutdown
}

/// Wait until the leaf server accepts a TCP connection.
async fn wait_for_leaf(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("leaf server did not become ready on port {}", port);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn local_pub_sub() {
    let leaf_port = free_port();
    let shutdown_tx = spawn_leaf(leaf_port, None);
    wait_for_leaf(leaf_port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", leaf_port))
        .await
        .expect("failed to connect to leaf server");

    let mut sub = client
        .subscribe("test.subject")
        .await
        .expect("subscribe failed");

    // Small delay to let subscription propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish("test.subject", "hello".into())
        .await
        .expect("publish failed");

    client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "test.subject");
    assert_eq!(&msg.payload[..], b"hello");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn upstream_forward() {
    // Start upstream nats-server
    let upstream_port = free_port();
    let _upstream = NatsServer::start(upstream_port);

    // Start leaf pointing at upstream
    let leaf_port = free_port();
    let shutdown_tx = spawn_leaf(
        leaf_port,
        Some(format!("nats://127.0.0.1:{}", upstream_port)),
    );
    wait_for_leaf(leaf_port).await;

    // Connect clients
    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_port))
        .await
        .expect("failed to connect to leaf");

    let upstream_client = async_nats::connect(format!("127.0.0.1:{}", upstream_port))
        .await
        .expect("failed to connect to upstream");

    // Leaf subscribes to wildcard
    let mut leaf_sub = leaf_client
        .subscribe("events.>")
        .await
        .expect("leaf subscribe failed");

    // Let subscription propagate to upstream via the leaf's hub connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Upstream publishes
    upstream_client
        .publish("events.hello", "from-upstream".into())
        .await
        .expect("upstream publish failed");

    upstream_client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), leaf_sub.next())
        .await
        .expect("timed out waiting for upstream message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "events.hello");
    assert_eq!(&msg.payload[..], b"from-upstream");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn leaf_to_upstream() {
    // Start upstream nats-server
    let upstream_port = free_port();
    let _upstream = NatsServer::start(upstream_port);

    // Start leaf pointing at upstream
    let leaf_port = free_port();
    let shutdown_tx = spawn_leaf(
        leaf_port,
        Some(format!("nats://127.0.0.1:{}", upstream_port)),
    );
    wait_for_leaf(leaf_port).await;

    // Connect clients
    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_port))
        .await
        .expect("failed to connect to leaf");

    let upstream_client = async_nats::connect(format!("127.0.0.1:{}", upstream_port))
        .await
        .expect("failed to connect to upstream");

    // Upstream subscribes
    let mut upstream_sub = upstream_client
        .subscribe("data.test")
        .await
        .expect("upstream subscribe failed");

    // Let subscription settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Leaf publishes
    leaf_client
        .publish("data.test", "from-leaf".into())
        .await
        .expect("leaf publish failed");

    leaf_client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), upstream_sub.next())
        .await
        .expect("timed out waiting for message on upstream")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "data.test");
    assert_eq!(&msg.payload[..], b"from-leaf");

    shutdown_tx.store(true, Ordering::Release);
}

// --- Hub mode helpers ---

/// Start a LeafServer in hub mode (with leafnode_port), returning the shutdown sender.
#[cfg(feature = "hub")]
fn spawn_hub(client_port: u16, leafnode_port: u16) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = LeafServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: format!("test-hub-{}", client_port),
        leafnode_port: Some(leafnode_port),
        ..Default::default()
    };
    let server = LeafServer::new(config);

    std::thread::spawn(move || {
        if let Err(e) = server.run_until_shutdown(shutdown_clone, reload, None) {
            eprintln!("hub server error: {}", e);
        }
    });

    shutdown
}

impl NatsServer {
    /// Start a Go nats-server configured as a leaf connecting to the given hub leafnode port.
    #[cfg(feature = "hub")]
    fn start_as_leaf(client_port: u16, hub_leafnode_port: u16) -> Self {
        let bin = nats_server_bin();

        // Write a temporary config file for leaf mode
        let config_path = format!("/tmp/nats_leaf_test_{}.conf", client_port);
        std::fs::write(
            &config_path,
            format!(
                "listen: 127.0.0.1:{client_port}\n\
                 leafnodes {{\n  remotes [{{\n    url: \"nats://127.0.0.1:{hub_leafnode_port}\"\n  }}]\n}}\n"
            ),
        )
        .unwrap();

        let child = Command::new(&bin)
            .args(["-c", &config_path])
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start nats-server leaf at {}: {}", bin, e));

        let server = NatsServer {
            child,
            port: client_port,
        };
        server.wait_ready();

        // Give the leaf connection time to establish
        std::thread::sleep(Duration::from_millis(500));

        server
    }
}

// --- Hub mode tests ---

#[tokio::test]
#[cfg(feature = "hub")]
async fn hub_mode_local_pub_sub() {
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", hub_client_port))
        .await
        .expect("failed to connect to hub");

    let mut sub = client
        .subscribe("hub.test")
        .await
        .expect("subscribe failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish("hub.test", "hello-hub".into())
        .await
        .expect("publish failed");

    client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "hub.test");
    assert_eq!(&msg.payload[..], b"hello-hub");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "hub")]
async fn hub_mode_leaf_to_hub() {
    // Start Rust hub
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    // Start Go nats-server as leaf connecting to Rust hub
    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on hub
    let hub_client = async_nats::connect(format!("127.0.0.1:{}", hub_client_port))
        .await
        .expect("connect to hub failed");

    let mut hub_sub = hub_client
        .subscribe("cross.test")
        .await
        .expect("hub subscribe failed");

    // Wait for LS+ to propagate from hub to leaf
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on leaf
    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_client_port))
        .await
        .expect("connect to leaf failed");

    leaf_client
        .publish("cross.test", "from-leaf".into())
        .await
        .expect("leaf publish failed");

    leaf_client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), hub_sub.next())
        .await
        .expect("timed out waiting for leaf→hub message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "cross.test");
    assert_eq!(&msg.payload[..], b"from-leaf");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "hub")]
async fn hub_mode_hub_to_leaf() {
    // Start Rust hub
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    // Start Go nats-server as leaf connecting to Rust hub
    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on leaf
    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_client_port))
        .await
        .expect("connect to leaf failed");

    let mut leaf_sub = leaf_client
        .subscribe("reverse.test")
        .await
        .expect("leaf subscribe failed");

    // Wait for LS+ from Go leaf to propagate to Rust hub
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on hub
    let hub_client = async_nats::connect(format!("127.0.0.1:{}", hub_client_port))
        .await
        .expect("connect to hub failed");

    hub_client
        .publish("reverse.test", "from-hub".into())
        .await
        .expect("hub publish failed");

    hub_client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), leaf_sub.next())
        .await
        .expect("timed out waiting for hub→leaf message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "reverse.test");
    assert_eq!(&msg.payload[..], b"from-hub");

    shutdown_tx.store(true, Ordering::Release);
}

// --- Cluster mode helpers ---

/// Start a LeafServer in cluster mode, returning the shutdown sender.
#[cfg(feature = "cluster")]
fn spawn_cluster_node(
    client_port: u16,
    cluster_port: u16,
    seeds: Vec<String>,
    name: &str,
) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = LeafServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: name.to_string(),
        cluster_port: Some(cluster_port),
        cluster_seeds: seeds,
        cluster_name: Some("test-cluster".to_string()),
        ..Default::default()
    };
    let server = LeafServer::new(config);
    std::thread::Builder::new()
        .name(format!("cluster-{}", name))
        .spawn(move || {
            if let Err(e) = server.run_until_shutdown(shutdown_clone, reload, None) {
                eprintln!("cluster node error: {}", e);
            }
        })
        .expect("failed to spawn cluster node thread");

    shutdown
}

// --- Cluster mode tests ---

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_two_node_pub_sub() {
    // Node A: cluster port, no seeds
    let port_a = free_port();
    let cluster_port_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cluster_port_a, vec![], "node-a");
    wait_for_leaf(port_a).await;

    // Node B: connects to Node A as seed
    let port_b = free_port();
    let cluster_port_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cluster_port_b,
        vec![format!("nats-route://127.0.0.1:{}", cluster_port_a)],
        "node-b",
    );
    wait_for_leaf(port_b).await;

    // Let route connection establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client on Node A subscribes
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect to node A failed");

    let mut sub_a = client_a
        .subscribe("cluster.test")
        .await
        .expect("subscribe on A failed");

    // Let subscription propagate via RS+ to Node B
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client on Node B publishes
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect to node B failed");

    client_b
        .publish("cluster.test", "hello-cluster".into())
        .await
        .expect("publish on B failed");

    client_b.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_a.next())
        .await
        .expect("timed out waiting for cluster message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "cluster.test");
    assert_eq!(&msg.payload[..], b"hello-cluster");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_reverse_direction() {
    // Test message flow: publish on A, subscribe on B
    let port_a = free_port();
    let cluster_port_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cluster_port_a, vec![], "node-a-rev");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cluster_port_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cluster_port_b,
        vec![format!("nats-route://127.0.0.1:{}", cluster_port_a)],
        "node-b-rev",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on Node B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect to B failed");
    let mut sub_b = client_b
        .subscribe("reverse.>")
        .await
        .expect("subscribe on B failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on Node A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect to A failed");
    client_a
        .publish("reverse.hello", "from-a".into())
        .await
        .expect("publish on A failed");
    client_a.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for reverse message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "reverse.hello");
    assert_eq!(&msg.payload[..], b"from-a");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_three_node() {
    // Three-node cluster: A ← B (seed A), A ← C (seed A)
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "tri-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "tri-b",
    );
    wait_for_leaf(port_b).await;

    let port_c = free_port();
    let cport_c = free_port();
    let shutdown_c = spawn_cluster_node(
        port_c,
        cport_c,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "tri-c",
    );
    wait_for_leaf(port_c).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on B and C
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect to B failed");
    let mut sub_b = client_b
        .subscribe("tri.test")
        .await
        .expect("subscribe on B failed");

    let client_c = async_nats::connect(format!("127.0.0.1:{}", port_c))
        .await
        .expect("connect to C failed");
    let mut sub_c = client_c
        .subscribe("tri.test")
        .await
        .expect("subscribe on C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect to A failed");
    client_a
        .publish("tri.test", "from-a".into())
        .await
        .expect("publish on A failed");
    client_a.flush().await.expect("flush failed");

    // Both B and C should receive
    let msg_b = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for message on B")
        .expect("sub B ended");
    assert_eq!(msg_b.subject.as_str(), "tri.test");
    assert_eq!(&msg_b.payload[..], b"from-a");

    let msg_c = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out waiting for message on C")
        .expect("sub C ended");
    assert_eq!(msg_c.subject.as_str(), "tri.test");
    assert_eq!(&msg_c.payload[..], b"from-a");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
    shutdown_c.store(true, Ordering::Release);
}
