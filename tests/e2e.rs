//! End-to-end integration tests for open-wire.
//!
//! These tests require the `nats-server` binary in PATH (or at the Go install
//! location). Install via: `go install github.com/nats-io/nats-server/v2@main`

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
#[cfg(feature = "gateway")]
use open_wire::GatewayRemote;
use open_wire::{LeafServer, LeafServerConfig};
use tokio::time::timeout;

/// Drain a subscriber, counting messages until a timeout gap with no messages.
async fn collect_msgs(sub: &mut async_nats::Subscriber, gap_ms: u64) -> u32 {
    let mut count = 0u32;
    while let Ok(Some(_)) = timeout(Duration::from_millis(gap_ms), sub.next()).await {
        count += 1;
    }
    count
}

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

// --- Wire protocol tests (no feature gate beyond "leaf") ---

#[tokio::test]
#[cfg(feature = "leaf")]
async fn queue_sub_distribution() {
    let port = free_port();
    let shutdown = spawn_leaf(port, None);
    wait_for_leaf(port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("connect failed");

    let mut sub1 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 1 failed");
    let mut sub2 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 2 failed");
    let mut sub3 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 3 failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..90u32 {
        client
            .publish("work.tasks", format!("msg-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let (c1, c2, c3) = tokio::join!(
        collect_msgs(&mut sub1, 500),
        collect_msgs(&mut sub2, 500),
        collect_msgs(&mut sub3, 500)
    );

    assert_eq!(c1 + c2 + c3, 90, "total should be 90, got {c1}+{c2}+{c3}");
    assert!(c1 >= 1, "sub1 should get at least 1, got {c1}");
    assert!(c2 >= 1, "sub2 should get at least 1, got {c2}");
    assert!(c3 >= 1, "sub3 should get at least 1, got {c3}");

    shutdown.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn multiple_queue_groups() {
    let port = free_port();
    let shutdown = spawn_leaf(port, None);
    wait_for_leaf(port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("connect failed");

    let mut ga1 = client
        .queue_subscribe("events.tick", "group-a".into())
        .await
        .expect("ga1 failed");
    let mut ga2 = client
        .queue_subscribe("events.tick", "group-a".into())
        .await
        .expect("ga2 failed");
    let mut gb1 = client
        .queue_subscribe("events.tick", "group-b".into())
        .await
        .expect("gb1 failed");
    let mut gb2 = client
        .queue_subscribe("events.tick", "group-b".into())
        .await
        .expect("gb2 failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..20u32 {
        client
            .publish("events.tick", format!("t-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let (a1, a2, b1, b2) = tokio::join!(
        collect_msgs(&mut ga1, 500),
        collect_msgs(&mut ga2, 500),
        collect_msgs(&mut gb1, 500),
        collect_msgs(&mut gb2, 500)
    );

    assert_eq!(a1 + a2, 20, "group-a total should be 20, got {a1}+{a2}");
    assert_eq!(b1 + b2, 20, "group-b total should be 20, got {b1}+{b2}");

    shutdown.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn unsub_max_auto_unsubscribe() {
    let port = free_port();
    let shutdown = spawn_leaf(port, None);
    wait_for_leaf(port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("connect failed");

    let mut sub = client
        .subscribe("unsub.test")
        .await
        .expect("subscribe failed");

    sub.unsubscribe_after(5)
        .await
        .expect("unsubscribe_after failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..10u32 {
        client
            .publish("unsub.test", format!("m-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let mut count = 0u32;
    while let Ok(Some(_)) = timeout(Duration::from_millis(500), sub.next()).await {
        count += 1;
    }

    assert_eq!(count, 5, "should receive exactly 5, got {count}");

    shutdown.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn wildcard_subscriptions() {
    let port = free_port();
    let shutdown = spawn_leaf(port, None);
    wait_for_leaf(port).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("connect failed");

    // Subscribe to wildcards and exact
    let mut star_sub = client.subscribe("events.*").await.expect("star sub failed");
    let mut gt_sub = client.subscribe("events.>").await.expect("gt sub failed");
    let mut exact_sub = client
        .subscribe("events.login")
        .await
        .expect("exact sub failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // events.login → matched by *, >, exact
    client
        .publish("events.login", "a".into())
        .await
        .expect("pub failed");
    // events.logout → matched by *, >
    client
        .publish("events.logout", "b".into())
        .await
        .expect("pub failed");
    // events.login.us → matched by > only (two tokens deep)
    client
        .publish("events.login.us", "c".into())
        .await
        .expect("pub failed");
    client.flush().await.expect("flush failed");

    let (star, gt, exact) = tokio::join!(
        collect_msgs(&mut star_sub, 500),
        collect_msgs(&mut gt_sub, 500),
        collect_msgs(&mut exact_sub, 500)
    );

    assert_eq!(star, 2, "events.* should match 2, got {star}");
    assert_eq!(gt, 3, "events.> should match 3, got {gt}");
    assert_eq!(exact, 1, "events.login exact should match 1, got {exact}");

    shutdown.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "leaf")]
async fn no_echo_publish() {
    let port = free_port();
    let shutdown = spawn_leaf(port, None);
    wait_for_leaf(port).await;

    // client_a with no_echo
    let client_a = async_nats::connect_with_options(
        format!("127.0.0.1:{}", port),
        async_nats::ConnectOptions::new().no_echo(),
    )
    .await
    .expect("connect a failed");

    // client_b normal
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("connect b failed");

    let mut sub_a = client_a.subscribe("echo.test").await.expect("sub a failed");
    let mut sub_b = client_b.subscribe("echo.test").await.expect("sub b failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // client_a publishes
    client_a
        .publish("echo.test", "hello".into())
        .await
        .expect("pub failed");
    client_a.flush().await.expect("flush failed");

    // client_b should receive
    let msg_b = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for msg on b")
        .expect("sub b ended");
    assert_eq!(&msg_b.payload[..], b"hello");

    // client_a should NOT receive (no_echo)
    let result_a = timeout(Duration::from_millis(500), sub_a.next()).await;
    assert!(
        result_a.is_err(),
        "client_a should not receive its own echo"
    );

    shutdown.store(true, Ordering::Release);
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

#[tokio::test]
#[cfg(feature = "hub")]
async fn leaf_subscription_propagation() {
    // Rust hub + Go leaf: subscribe on leaf, publish from hub → received
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on leaf
    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_client_port))
        .await
        .expect("connect to leaf failed");

    let mut leaf_sub = leaf_client
        .subscribe("leaf.prop.test")
        .await
        .expect("leaf subscribe failed");

    // Wait for LS+ to propagate from Go leaf through hub
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from hub
    let hub_client = async_nats::connect(format!("127.0.0.1:{}", hub_client_port))
        .await
        .expect("connect to hub failed");

    hub_client
        .publish("leaf.prop.test", "from-hub".into())
        .await
        .expect("hub publish failed");
    hub_client.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), leaf_sub.next())
        .await
        .expect("timed out waiting for leaf sub propagation")
        .expect("sub ended");

    assert_eq!(msg.subject.as_str(), "leaf.prop.test");
    assert_eq!(&msg.payload[..], b"from-hub");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "hub")]
async fn leaf_no_echo() {
    // Rust hub + Go leaf: sub + pub on same leaf → publisher should NOT get echo
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    let leaf_client = async_nats::connect(format!("127.0.0.1:{}", leaf_client_port))
        .await
        .expect("connect to leaf failed");

    let mut leaf_sub = leaf_client
        .subscribe("leaf.echo.test")
        .await
        .expect("subscribe failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from same leaf
    leaf_client
        .publish("leaf.echo.test", "echo-check".into())
        .await
        .expect("pub failed");
    leaf_client.flush().await.expect("flush failed");

    // The subscriber on the same leaf should get it directly from Go nats-server
    // but it should NOT come back through the hub as a duplicate
    let msg = timeout(Duration::from_secs(5), leaf_sub.next())
        .await
        .expect("timed out")
        .expect("sub ended");
    assert_eq!(&msg.payload[..], b"echo-check");

    // Should NOT get a second (echoed) copy from the hub
    let dup = timeout(Duration::from_millis(500), leaf_sub.next()).await;
    assert!(dup.is_err(), "should not get echo back through hub");

    shutdown_tx.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "hub")]
async fn leaf_queue_distribution() {
    // Rust hub + 2 Go leaves: queue sub on each + hub, publish 30 from hub
    let hub_client_port = free_port();
    let hub_leaf_port = free_port();
    let shutdown_tx = spawn_hub(hub_client_port, hub_leaf_port);
    wait_for_leaf(hub_client_port).await;

    let leaf1_port = free_port();
    let _leaf1 = NatsServer::start_as_leaf(leaf1_port, hub_leaf_port);

    let leaf2_port = free_port();
    let _leaf2 = NatsServer::start_as_leaf(leaf2_port, hub_leaf_port);

    let hub_client = async_nats::connect(format!("127.0.0.1:{}", hub_client_port))
        .await
        .expect("connect hub failed");
    let leaf1_client = async_nats::connect(format!("127.0.0.1:{}", leaf1_port))
        .await
        .expect("connect leaf1 failed");
    let leaf2_client = async_nats::connect(format!("127.0.0.1:{}", leaf2_port))
        .await
        .expect("connect leaf2 failed");

    // Queue subs
    let mut hub_sub = hub_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("hub queue sub failed");
    let mut leaf1_sub = leaf1_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("leaf1 queue sub failed");
    let mut leaf2_sub = leaf2_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("leaf2 queue sub failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 30 from hub
    for i in 0..30u32 {
        hub_client
            .publish("leaf.q.test", format!("q-{}", i).into())
            .await
            .expect("pub failed");
    }
    hub_client.flush().await.expect("flush failed");

    let (h, l1, l2) = tokio::join!(
        collect_msgs(&mut hub_sub, 1000),
        collect_msgs(&mut leaf1_sub, 1000),
        collect_msgs(&mut leaf2_sub, 1000)
    );

    let total = h + l1 + l2;
    assert_eq!(total, 30, "total should be 30, got {h}+{l1}+{l2}");
    assert!(h >= 1 || l1 >= 1 || l2 >= 1, "at least one should get msgs");

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
            let _ = server.run_until_shutdown(shutdown_clone, reload, None);
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

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_gossip_discovery() {
    // Three-node cluster where B and C only seed A.
    // B and C should discover each other via A's gossip and form a direct route.
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "gossip-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "gossip-b",
    );
    wait_for_leaf(port_b).await;

    let port_c = free_port();
    let cport_c = free_port();
    let shutdown_c = spawn_cluster_node(
        port_c,
        cport_c,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "gossip-c",
    );
    wait_for_leaf(port_c).await;

    // Allow time for gossip discovery and route formation between B↔C.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Now shut down node A to prove B↔C have a direct route.
    shutdown_a.store(true, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on C
    let client_c = async_nats::connect(format!("127.0.0.1:{}", port_c))
        .await
        .expect("connect to C failed");
    let mut sub_c = client_c
        .subscribe("gossip.test")
        .await
        .expect("subscribe on C failed");

    // Let subscription propagate via RS+ over B↔C route
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect to B failed");
    client_b
        .publish("gossip.test", "from-b-via-gossip".into())
        .await
        .expect("publish on B failed");
    client_b.flush().await.expect("flush failed");

    // C should receive the message via the B↔C direct route
    let msg = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out waiting for gossip-routed message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "gossip.test");
    assert_eq!(&msg.payload[..], b"from-b-via-gossip");

    shutdown_b.store(true, Ordering::Release);
    shutdown_c.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_partial_seed() {
    // Chain topology: A seeds B, B seeds C.
    // C should discover A via transitive gossip and form a full mesh.
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "chain-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "chain-b",
    );
    wait_for_leaf(port_b).await;

    // C only seeds B — should discover A via gossip.
    let port_c = free_port();
    let cport_c = free_port();
    let shutdown_c = spawn_cluster_node(
        port_c,
        cport_c,
        vec![format!("nats-route://127.0.0.1:{}", cport_b)],
        "chain-c",
    );
    wait_for_leaf(port_c).await;

    // Allow time for gossip discovery and full mesh formation.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Subscribe on C
    let client_c = async_nats::connect(format!("127.0.0.1:{}", port_c))
        .await
        .expect("connect to C failed");
    let mut sub_c = client_c
        .subscribe("chain.test")
        .await
        .expect("subscribe on C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A — should reach C via full mesh
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect to A failed");
    client_a
        .publish("chain.test", "from-a-chain".into())
        .await
        .expect("publish on A failed");
    client_a.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out waiting for chain-routed message")
        .expect("subscription stream ended");

    assert_eq!(msg.subject.as_str(), "chain.test");
    assert_eq!(&msg.payload[..], b"from-a-chain");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
    shutdown_c.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_queue_semantics() {
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "queue-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "queue-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");

    // 2 queue subs on each node
    let mut qa1 = client_a
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qa1 failed");
    let mut qa2 = client_a
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qa2 failed");
    let mut qb1 = client_b
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qb1 failed");
    let mut qb2 = client_b
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qb2 failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 100 from node-A
    for i in 0..100u32 {
        client_a
            .publish("work.item", format!("job-{}", i).into())
            .await
            .expect("publish failed");
    }
    client_a.flush().await.expect("flush failed");

    let (a1, a2, b1, b2) = tokio::join!(
        collect_msgs(&mut qa1, 1000),
        collect_msgs(&mut qa2, 1000),
        collect_msgs(&mut qb1, 1000),
        collect_msgs(&mut qb2, 1000)
    );

    let total = a1 + a2 + b1 + b2;
    assert_eq!(total, 100, "total should be 100, got {a1}+{a2}+{b1}+{b2}");
    assert!(b1 + b2 >= 1, "node-B should get at least 1, got {b1}+{b2}");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_one_hop_enforcement() {
    // 3-node cluster: A, B (seed A), C (seed A)
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "hop-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "hop-b",
    );
    wait_for_leaf(port_b).await;

    let port_c = free_port();
    let cport_c = free_port();
    let shutdown_c = spawn_cluster_node(
        port_c,
        cport_c,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "hop-c",
    );
    wait_for_leaf(port_c).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on C
    let client_c = async_nats::connect(format!("127.0.0.1:{}", port_c))
        .await
        .expect("connect C failed");
    let mut sub_c = client_c.subscribe("hop.test").await.expect("sub C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    client_a
        .publish("hop.test", "once".into())
        .await
        .expect("pub failed");
    client_a.flush().await.expect("flush failed");

    // Should get exactly 1 copy (no duplication from re-forwarding)
    let msg = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out")
        .expect("sub ended");
    assert_eq!(&msg.payload[..], b"once");

    // No second copy
    let dup = timeout(Duration::from_millis(500), sub_c.next()).await;
    assert!(
        dup.is_err(),
        "should not receive duplicate from re-forwarding"
    );

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
    shutdown_c.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn cluster_sub_unsub_propagation() {
    let port_a = free_port();
    let cport_a = free_port();
    let shutdown_a = spawn_cluster_node(port_a, cport_a, vec![], "prop-a");
    wait_for_leaf(port_a).await;

    let port_b = free_port();
    let cport_b = free_port();
    let shutdown_b = spawn_cluster_node(
        port_b,
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "prop-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    let mut sub_a = client_a.subscribe("prop.test").await.expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from B → should be received on A
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    client_b
        .publish("prop.test", "before".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_a.next())
        .await
        .expect("timed out")
        .expect("sub ended");
    assert_eq!(&msg.payload[..], b"before");

    // Unsubscribe on A
    sub_a.unsubscribe().await.expect("unsub failed");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish again from B → should NOT be received
    client_b
        .publish("prop.test", "after".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let result = timeout(Duration::from_millis(500), sub_a.next()).await;
    assert!(
        result.is_err() || result.unwrap().is_none(),
        "should not receive after unsub"
    );

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

// --- Gateway mode helpers ---

/// Start a LeafServer in gateway mode, returning the shutdown sender.
#[cfg(feature = "gateway")]
fn spawn_gateway_node(
    client_port: u16,
    _cluster_port: u16,
    gateway_port: u16,
    gateway_name: &str,
    remotes: Vec<(String, String)>,
    _cluster_seeds: Vec<String>,
    server_name: &str,
) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let gateway_remotes = remotes
        .into_iter()
        .map(|(name, url)| GatewayRemote {
            name,
            urls: vec![url],
        })
        .collect();

    let config = LeafServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: server_name.to_string(),
        #[cfg(feature = "cluster")]
        cluster_port: Some(_cluster_port),
        #[cfg(feature = "cluster")]
        cluster_seeds: _cluster_seeds,
        #[cfg(feature = "cluster")]
        cluster_name: Some(gateway_name.to_string()),
        gateway_port: Some(gateway_port),
        gateway_name: Some(gateway_name.to_string()),
        gateway_remotes,
        ..Default::default()
    };
    let server = LeafServer::new(config);
    std::thread::Builder::new()
        .name(format!("gw-{}", server_name))
        .spawn(move || {
            let _ = server.run_until_shutdown(shutdown_clone, reload, None);
        })
        .expect("failed to spawn gateway node thread");

    shutdown
}

// --- Gateway mode tests ---

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_basic_pub_sub() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-node-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-node-b",
    );
    wait_for_leaf(port_b).await;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    let mut sub_b = client_b
        .subscribe("gw.basic.test")
        .await
        .expect("sub B failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    client_a
        .publish("gw.basic.test", "hello-gw".into())
        .await
        .expect("pub failed");
    client_a.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for gateway message")
        .expect("sub ended");

    assert_eq!(msg.subject.as_str(), "gw.basic.test");
    assert_eq!(&msg.payload[..], b"hello-gw");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_reverse_direction() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-rev-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-rev-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    let mut sub_a = client_a
        .subscribe("gw.rev.test")
        .await
        .expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    client_b
        .publish("gw.rev.test", "from-b".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_a.next())
        .await
        .expect("timed out")
        .expect("sub ended");

    assert_eq!(msg.subject.as_str(), "gw.rev.test");
    assert_eq!(&msg.payload[..], b"from-b");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_request_reply() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-rr-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-rr-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Service on B: subscribe and reply
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    let mut service_sub = client_b
        .subscribe("service.echo")
        .await
        .expect("service sub failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn responder task
    let responder_client = client_b.clone();
    let responder = tokio::spawn(async move {
        if let Some(msg) = timeout(Duration::from_secs(5), service_sub.next())
            .await
            .ok()
            .flatten()
        {
            if let Some(reply) = msg.reply {
                responder_client
                    .publish(reply, "echo-reply".into())
                    .await
                    .expect("reply failed");
                responder_client.flush().await.expect("flush failed");
            }
        }
    });

    // Client on A: request
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");

    let reply = timeout(
        Duration::from_secs(10),
        client_a.request("service.echo", "ping".into()),
    )
    .await
    .expect("request timed out")
    .expect("request failed");

    assert_eq!(&reply.payload[..], b"echo-reply");

    responder.await.expect("responder task failed");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_queue_groups() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-qq-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-qq-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 2 queue subs on B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    let mut qb1 = client_b
        .queue_subscribe("gw.queue.test", "qworkers".into())
        .await
        .expect("qb1 failed");
    let mut qb2 = client_b
        .queue_subscribe("gw.queue.test", "qworkers".into())
        .await
        .expect("qb2 failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 20 from A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    for i in 0..20u32 {
        client_a
            .publish("gw.queue.test", format!("gq-{}", i).into())
            .await
            .expect("pub failed");
    }
    client_a.flush().await.expect("flush failed");

    let (b1, b2) = tokio::join!(collect_msgs(&mut qb1, 1000), collect_msgs(&mut qb2, 1000));

    assert_eq!(b1 + b2, 20, "total should be 20, got {b1}+{b2}");
    assert!(b1 >= 1, "qb1 should get at least 1, got {b1}");
    assert!(b2 >= 1, "qb2 should get at least 1, got {b2}");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_interest_mode_transition() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-im-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-im-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe to a known subject on A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    let mut sub_a = client_a
        .subscribe("interest.final")
        .await
        .expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 1100 unique no-interest subjects from B to trigger Optimistic → InterestOnly
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");
    for i in 0..1100u32 {
        client_b
            .publish(format!("nointerest.unique.{}", i), "x".into())
            .await
            .expect("pub failed");
    }
    client_b.flush().await.expect("flush failed");

    // Wait for interest mode transition
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish the subject A is subscribed to
    client_b
        .publish("interest.final", "after-transition".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(5), sub_a.next())
        .await
        .expect("timed out waiting for interest mode message")
        .expect("sub ended");

    assert_eq!(msg.subject.as_str(), "interest.final");
    assert_eq!(&msg.payload[..], b"after-transition");

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}

#[tokio::test]
#[cfg(feature = "gateway")]
async fn gateway_fan_out() {
    let port_a = free_port();
    let cport_a = free_port();
    let gport_a = free_port();

    let port_b = free_port();
    let cport_b = free_port();
    let gport_b = free_port();

    let shutdown_a = spawn_gateway_node(
        port_a,
        cport_a,
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-fan-a",
    );
    wait_for_leaf(port_a).await;

    let shutdown_b = spawn_gateway_node(
        port_b,
        cport_b,
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-fan-b",
    );
    wait_for_leaf(port_b).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe to 5 distinct subjects on B
    let client_b = async_nats::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("connect B failed");

    let subjects = [
        "fan.alpha",
        "fan.beta",
        "fan.gamma",
        "fan.delta",
        "fan.epsilon",
    ];
    let mut subs = Vec::new();
    for &subj in &subjects {
        let sub = client_b.subscribe(subj).await.expect("sub failed");
        subs.push(sub);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 1 to each from A
    let client_a = async_nats::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("connect A failed");
    for &subj in &subjects {
        client_a
            .publish(subj, format!("payload-{}", subj).into())
            .await
            .expect("pub failed");
    }
    client_a.flush().await.expect("flush failed");

    // All 5 should be received on correct subjects
    for (i, sub) in subs.iter_mut().enumerate() {
        let msg = timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap_or_else(|_| panic!("timed out on subject {}", subjects[i]))
            .unwrap_or_else(|| panic!("sub ended for {}", subjects[i]));

        assert_eq!(
            msg.subject.as_str(),
            subjects[i],
            "wrong subject for index {i}"
        );
        assert_eq!(
            &msg.payload[..],
            format!("payload-{}", subjects[i]).as_bytes()
        );
    }

    shutdown_a.store(true, Ordering::Release);
    shutdown_b.store(true, Ordering::Release);
}
