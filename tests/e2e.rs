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
