# Low-Level Design — open-wire

Interactive diagrams using Mermaid. Each diagram links types and methods
to source locations (`file:line`) so you can jump straight to the code.

---

## Table of Contents

1. [Type Relationships](#1-type-relationships)
2. [Connection Lifecycle State Machine](#2-connection-lifecycle-state-machine)
3. [Message Delivery Pipeline](#3-message-delivery-pipeline)
4. [Client SUB → Interest Propagation](#4-client-sub--interest-propagation)
5. [Upstream Leaf Connection Flow](#5-upstream-leaf-connection-flow)
6. [Worker Event Loop](#6-worker-event-loop)
7. [MsgWriter Cross-Worker Delivery](#7-msgwriter-cross-worker-delivery)
8. [Inbound Peer Handshake](#8-inbound-peer-handshake)
9. [Gateway Interest Modes](#9-gateway-interest-modes)

---

## 1. Type Relationships

Core structs and their ownership/reference relationships.

```mermaid
classDiagram
    class LeafServer {
        +config: LeafServerConfig
        +run()
        %%  server.rs:1432
    }

    class ServerState {
        +sub_list: SubscriptionManager
        +route_writers: RwLock~HashMap~
        +gateway_writers: RwLock~HashMap~
        +upstream_txs: Mutex~Vec~
        %%  server.rs:1094
    }

    class Worker {
        +state: Arc~ServerState~
        +clients: HashMap~u64, ClientState~
        +reactor: R
        +run()
        %%  worker.rs:275
    }

    class ClientState {
        +conn_id: u64
        +phase: ConnPhase
        +ext: ConnExt
        +write_buf: BytesMut
        +msg_writer: MsgWriter
        +subs: HashMap~u64, Subscription~
        %%  worker.rs:218
    }

    class ConnExt {
        <<enum>>
        Client
        Leaf(leaf_sid_counter, leaf_sids)
        Route(route_sid_counter, route_sids)
        Gateway(gateway_sid_counter, gateway_sids)
        +kind_tag() ConnKind
        +is_leaf() bool
        +is_route() bool
        +is_gateway() bool
        %%  handler/conn.rs:67
    }

    class ConnCtx {
        +conn_id: u64
        +write_buf: &mut BytesMut
        +direct_writer: &MsgWriter
        +echo: bool
        +no_responders: bool
        +permissions: &Option~Permissions~
        +ext: &mut ConnExt
        +draining: bool
        %%  handler/conn.rs:46
    }

    class MessageDeliveryHub {
        +state: &Arc~ServerState~
        +dirty_eventfds: &mut Vec~RawFd~
        +publish()
        +record_delivery()
        +queue_notify()
        %%  handler/delivery.rs:31
    }

    class SubscriptionManager {
        +exact: HashMap~Bytes, Vec~Subscription~~
        +wildcards: WildTrie
        +insert()
        +remove()
        +for_each_match()
        %%  sub_list.rs:375
    }

    class Subscription {
        +conn_id: u64
        +sid: u64
        +subject: Bytes
        +queue: Option~Bytes~
        +writer: MsgWriter
        +is_leaf: bool
        +is_route: bool
        +is_gateway: bool
        %%  sub_list.rs:10
    }

    class MsgWriter {
        +buf: Arc~Mutex~BytesMut~~
        +has_pending: Arc~AtomicBool~
        +event_fd: Arc~OwnedFd~
        +write_msg()
        +write_lmsg()
        +write_rmsg()
        +drain()
        %%  msg_writer.rs:37
    }

    class Msg {
        +subject: &Bytes
        +reply: Option~&Bytes~
        +header: Option~&[u8]~
        +payload: &[u8]
        %%  handler/delivery.rs:75
    }

    class DeliveryScope {
        +skip_routes: bool
        +skip_gateways: bool
        +skip_leafs: bool
        +skip_echo: bool
        +local()
        +from_route()
        +from_gateway()
        %%  handler/delivery.rs:104
    }

    LeafServer --> ServerState : creates
    ServerState --> SubscriptionManager : owns
    Worker --> ServerState : Arc ref
    Worker --> ClientState : owns many
    ClientState --> ConnExt : owns
    ClientState --> MsgWriter : owns
    ConnCtx --> ConnExt : borrows
    ConnCtx --> MsgWriter : borrows
    MessageDeliveryHub --> ServerState : borrows
    Subscription --> MsgWriter : clones
    SubscriptionManager --> Subscription : stores
```

---

## 2. Connection Lifecycle State Machine

Every inbound connection (client, leaf, route, gateway) follows this
state machine in the worker event loop.

> [`ConnPhase`](../src/worker.rs#L178) — `worker.rs:178`

```mermaid
stateDiagram-v2
    [*] --> SendInfo : accept() → register fd
    SendInfo --> WaitConnect : INFO written to write_buf
    WaitConnect --> Active : valid CONNECT received

    state Active {
        [*] --> Processing
        Processing --> Processing : parse_op → handle_op
        Processing --> Flushing : HandleResult Flush
        Flushing --> Processing : write_buf flushed
    }

    Active --> Draining : server shutdown / drain cmd
    Draining --> [*] : write_buf empty, close fd

    WaitConnect --> [*] : invalid CONNECT / timeout
    Active --> [*] : protocol error (HandleResult Disconnect)
    Active --> [*] : I/O error / EOF

    note right of SendInfo
        worker.rs — handle_writable()
        Writes INFO JSON to socket
    end note

    note right of WaitConnect
        worker.rs — process_wait_connect()
        Parses CONNECT, validates auth
        Sets echo, no_responders, etc.
    end note

    note right of Active
        worker.rs — process_active()
        Calls H.parse_op() + H.handle_op()
        via ConnectionHandler trait
        handler/conn.rs:25
    end note
```

---

## 3. Message Delivery Pipeline

All message sources (client PUB, route RMSG, gateway RMSG, leaf LMSG)
share the same delivery pipeline. The common flow is shown once below,
with per-source differences captured in the scope variants table.

```mermaid
sequenceDiagram
    participant Src as Source Connection
    participant W as Worker<br/>worker.rs:275
    participant H as Handler<br/>(ClientHandler / RouteHandler / etc.)
    participant MDH as MessageDeliveryHub<br/>handler/delivery.rs:31
    participant SM as SubscriptionManager<br/>sub_list.rs:375
    participant MW as MsgWriter<br/>msg_writer.rs:37
    participant S as Subscriber Socket

    Src->>W: PUB / RMSG / LMSG
    W->>W: process_active[Handler]()
    W->>H: parse_op(buf) → Op variant
    W->>H: handle_op(conn, mdh, op)

    H->>H: Msg::new(subject, reply, hdr, payload)<br/>handler/delivery.rs:75
    H->>MDH: publish(&msg, scope, conn_id, ...)<br/>handler/delivery.rs:162
    MDH->>MDH: deliver_to_subs(mdh, &msg, ...)<br/>handler/delivery.rs:331

    MDH->>SM: for_each_match(subject, callback)<br/>sub_list.rs:495
    SM-->>MDH: matching Subscription[]

    loop Each matching sub
        MDH->>MDH: deliver_to_sub_inner(sub, &msg)<br/>handler/delivery.rs:210
        alt scope skips this sub type
            MDH->>MDH: SKIP (scope filter)
        else deliver
            MDH->>MW: sub.writer.write_msg/write_lmsg/write_rmsg<br/>msg_writer.rs:74
            MW->>MW: lock buf, append wire bytes, set has_pending=true
            MDH->>MDH: record_delivery(len), queue_notify(event_fd)
        end
    end

    Note over MDH: Also calls:<br/>forward_to_optimistic_gateways()<br/>deliver_cross_account()<br/>handler/delivery.rs:590, 642

    H->>H: forward_to_upstream(&msg)<br/>handler/delivery.rs:448
    Note over H: mpsc::Sender → upstream thread

    W->>W: batch: deduplicate dirty_eventfds
    W->>MW: eventfd write(1) per unique worker
    MW-->>S: Worker wakes → drain() → socket write
```

### Scope Variants

How `DeliveryScope` differs per message origin:

| Origin | Handler | DeliveryScope | Skips |
|--------|---------|---------------|-------|
| Client PUB | `ClientHandler` | `local(echo)` | echo if `!echo` |
| Route RMSG | `RouteHandler` | `from_route()` | route subs (one-hop rule) |
| Gateway RMSG | `GatewayHandler` | `from_gateway()` | route + gateway subs |
| Leaf LMSG | `LeafHandler` | `local(false)` | originating leaf |

---

## 4. Client SUB → Interest Propagation

When a client subscribes, interest is propagated to upstream, routes,
leafs, and gateways.

```mermaid
sequenceDiagram
    participant C as Client Socket
    participant CH as ClientHandler<br/>client_handler.rs:20
    participant SM as SubscriptionManager<br/>sub_list.rs:375
    participant US as Upstream<br/>upstream.rs:76
    participant P as Propagation<br/>propagation.rs
    participant LW as Leaf Writers
    participant RW as Route Writers
    participant GW as Gateway Writers

    C->>CH: SUB subject [queue] sid
    CH->>CH: handle_sub()<br/>client_handler.rs:72
    CH->>CH: create Subscription<br/>sub_list.rs:10
    CH->>SM: insert(subscription)<br/>sub_list.rs:389

    CH->>US: UpstreamCmd::AddInterest(subject, queue)<br/>upstream.rs:25
    Note over US: InterestPipeline applies<br/>subject-mapping + collapse<br/>interest.rs:131

    CH->>P: propagate_all_interest(subject, queue, is_sub=true)<br/>propagation.rs:279

    par Leaf propagation
        P->>LW: propagate_leaf_interest()<br/>propagation.rs:20
        Note over LW: LS+ subject [queue]\r\n
    and Route propagation
        P->>RW: propagate_route_interest()<br/>propagation.rs:107
        Note over RW: RS+ $G subject [queue]\r\n
    and Gateway propagation
        P->>GW: propagate_gateway_interest()
        Note over GW: RS+ $G subject [queue]\r\n
    end
```

---

## 5. Upstream Leaf Connection Flow

The upstream module connects to a hub server using the leaf node protocol.

> [`Upstream`](../src/upstream.rs#L76) — `upstream.rs:76`
> [`LeafConn`](../src/leaf_conn.rs#L98) — `leaf_conn.rs:98`

```mermaid
sequenceDiagram
    participant W as Workers
    participant U as Upstream<br/>upstream.rs:76
    participant LC as LeafConn<br/>leaf_conn.rs:98
    participant LR as LeafReader<br/>leaf_conn.rs:261
    participant LW as LeafWriter<br/>leaf_conn.rs:294
    participant Hub as Hub Server

    U->>LC: LeafConn::connect(addr, tls)<br/>leaf_conn.rs:98
    LC->>Hub: TCP connect + optional TLS
    Hub-->>LC: INFO {...}
    LC->>Hub: CONNECT {...}
    LC->>Hub: PING
    Hub-->>LC: PONG
    Note over LC: Connection established

    U->>LR: spawn reader thread
    U->>LW: spawn writer thread

    par Reader loop
        Hub->>LR: LMSG / PING / -ERR
        LR->>LR: parse leaf ops<br/>nats_proto.rs:546 (LeafOp)
        LR->>W: deliver_to_subs_upstream()<br/>handler/delivery.rs
        LR->>LW: PONG (via mpsc)
    and Writer loop
        W->>U: UpstreamCmd via mpsc<br/>upstream.rs:25
        U->>LW: SUB/UNSUB/PUB commands
        LW->>Hub: LS+ / LS- / LMSG
    end

    Note over LR: On disconnect:<br/>UpstreamCmd::Shutdown<br/>Upstream reconnects
```

---

## 6. Worker Event Loop

Each worker thread runs a tight epoll loop processing socket events
and cross-worker notifications.

> [`Worker::run()`](../src/worker.rs#L275) — `worker.rs:275`

```mermaid
stateDiagram-v2
    [*] --> EpollWait

    EpollWait --> HandleEvent : event ready

    state HandleEvent {
        [*] --> CheckFd
        CheckFd --> EventFd : fd == eventfd
        CheckFd --> ListenerFd : fd == listener
        CheckFd --> ClientFd : fd == client socket

        state EventFd {
            [*] --> ReadEventFd
            ReadEventFd --> ScanPending
            ScanPending --> DrainMsgBuf : has_pending == true
            DrainMsgBuf --> WriteSocket
        }
        note right of EventFd
            handle_eventfd()
            worker.rs:476
            Scans ALL conns for pending
            MsgWriter data
        end note

        state ClientFd {
            [*] --> CheckPhase
            CheckPhase --> ProcessSendInfo : SendInfo
            CheckPhase --> ProcessWaitConnect : WaitConnect
            CheckPhase --> ProcessActive : Active

            state ProcessActive {
                [*] --> ReadLoop
                ReadLoop --> ParseOp : data available
                ParseOp --> HandleOp : H.parse_op()
                HandleOp --> ReadLoop : HandleResult Ok
                HandleOp --> FlushAndRead : HandleResult Flush
                HandleOp --> Disconnect : HandleResult Disconnect
                ReadLoop --> [*] : WouldBlock
            }
        }
        note right of ClientFd
            process_active[H]()
            worker.rs:2152
            Generic over ConnectionHandler
            handler/conn.rs:25
        end note
    }

    HandleEvent --> FlushPending : all events processed
    note right of FlushPending
        flush_pending()
        worker.rs:809
        Same-worker delivery bypass:
        skip eventfd round-trip
    end note
    FlushPending --> EpollWait
```

---

## 7. MsgWriter Cross-Worker Delivery

How messages cross worker boundaries using shared buffers and eventfd.

> [`MsgWriter`](../src/msg_writer.rs#L37) — `msg_writer.rs:37`

```mermaid
sequenceDiagram
    participant PW as Publisher Worker
    participant MW as MsgWriter<br/>msg_writer.rs:37
    participant EFD as eventfd (kernel)
    participant SW as Subscriber Worker
    participant SS as Subscriber Socket

    Note over PW: Processing PUB from client

    PW->>MW: write_msg(sid, subject, reply, hdr, payload)<br/>msg_writer.rs:74
    MW->>MW: lock buf (Arc Mutex BytesMut)
    MW->>MW: append MSG wire bytes
    MW->>MW: set has_pending = true (AtomicBool)
    MW->>MW: unlock buf

    PW->>PW: queue_notify(event_fd)<br/>handler/delivery.rs — MessageDeliveryHub
    Note over PW: Dedup: collect unique eventfds<br/>across all deliveries in batch

    PW->>EFD: write(1) — one per unique worker
    Note over EFD: Only 1 syscall even if<br/>100 subs on same worker

    EFD-->>SW: epoll_wait returns eventfd ready
    SW->>SW: handle_eventfd()<br/>worker.rs:476

    loop Each conn on this worker
        SW->>MW: check has_pending (AtomicBool)
        alt has_pending == true
            SW->>MW: drain()<br/>msg_writer.rs:157
            MW->>MW: lock buf, split_to(len)
            MW-->>SW: Bytes chunk
            SW->>SS: write(chunk) to socket
        end
    end
```

---

## 8. Inbound Peer Handshake

All inbound peer connections (leaf, route, gateway) follow a common
handshake template. The worker accepts the connection, exchanges
INFO/CONNECT, creates the appropriate `ConnExt` variant, syncs existing
subscriptions, then enters the active protocol loop.

```mermaid
sequenceDiagram
    participant Peer as Peer Node
    participant A as Acceptor<br/>server.rs:1699
    participant W as Worker<br/>worker.rs:275

    Peer->>A: TCP connect to peer port
    A->>W: WorkerCmd::NewPeerConn(fd)
    W->>W: register fd, phase = SendInfo

    W->>Peer: INFO (server_id, cluster/leafnode metadata)
    W->>W: phase = WaitConnect

    opt Route/Gateway peers send INFO first
        Peer->>W: INFO (peer metadata)
        Note over W: Store or skip peer INFO
    end

    Peer->>W: CONNECT (verbose, auth, ...)
    W->>W: validate auth, create ConnExt variant

    alt Leaf peer
        W->>Peer: PING
        Peer->>W: PONG
    else Route/Gateway peer
        W->>Peer: PONG
    end

    W->>W: phase = Active

    Note over W: Sync existing subscriptions<br/>to new peer (see variants table)

    loop Active
        Peer->>W: protocol ops (LS+/RS+/LMSG/RMSG/PING)
        W->>W: process_active[Handler]()
    end
```

### Handshake Variants

| Peer Type | Port | ConnExt | Interest Sync | Handler | Protocol Ops |
|-----------|------|---------|---------------|---------|--------------|
| Leaf | `leafnode_port` | `Leaf { leaf_sid_counter, leaf_sids }` | `send_existing_subs()` (LS+) | `LeafHandler` | LS+/LS-/LMSG |
| Route | `cluster_port` | `Route { route_sid_counter, route_sids }` | `send_existing_route_subs()` (RS+) | `RouteHandler` | RS+/RS-/RMSG |
| Gateway | `gateway_port` | `Gateway { gateway_sid_counter, gateway_sids }` | `send_existing_route_subs()` (RS+) | `GatewayHandler` | RS+/RS-/RMSG |

---

## 9. Gateway Interest Modes

Gateways use two interest modes to optimize cross-cluster traffic.

```mermaid
stateDiagram-v2
    [*] --> Optimistic : initial state

    state Optimistic {
        [*] --> ForwardAll
        ForwardAll : Forward all messages to peer gateway
        ForwardAll : No per-subject filtering
        ForwardAll --> TrackNoInterest : peer sends RS- (no interest)
        TrackNoInterest : Build deny list of subjects
        TrackNoInterest : Skip subjects in deny list
    }

    Optimistic --> InterestOnly : too many RS- received
    note right of Optimistic
        Gateway starts optimistic:
        sends all messages, learns
        what peer doesn't want.
        gateway_conn.rs / gateway_handler.rs
    end note

    state InterestOnly {
        [*] --> WaitForInterest
        WaitForInterest : Only forward subjects with RS+
        WaitForInterest : Peer sends RS+ for wanted subjects
    }

    note right of InterestOnly
        After threshold, switch to
        interest-only: only forward
        messages peer explicitly wants.
    end note
```

---

## Source File Index

Quick reference linking diagram elements to source code.

| Type / Function | File | Line |
|---|---|---|
| `LeafServer` | `server.rs` | 1432 |
| `ServerState` | `server.rs` | 1094 |
| `Worker` | `worker.rs` | 275 |
| `ClientState` | `worker.rs` | 218 |
| `ConnPhase` | `worker.rs` | 178 |
| `process_active()` | `worker.rs` | 2152 |
| `flush_pending()` | `worker.rs` | 809 |
| `handle_eventfd()` | `worker.rs` | 476 |
| `ConnectionHandler` | `handler/conn.rs` | 25 |
| `ConnCtx` | `handler/conn.rs` | 46 |
| `ConnExt` | `handler/conn.rs` | 67 |
| `ConnKind` | `handler/conn.rs` | 97 |
| `HandleResult` | `handler/conn.rs` | 179 |
| `MessageDeliveryHub` | `handler/delivery.rs` | 31 |
| `Msg` | `handler/delivery.rs` | 75 |
| `DeliveryScope` | `handler/delivery.rs` | 104 |
| `MessageDeliveryHub::publish()` | `handler/delivery.rs` | 162 |
| `deliver_to_sub_inner()` | `handler/delivery.rs` | 210 |
| `deliver_to_subs_core()` | `handler/delivery.rs` | 258 |
| `deliver_to_subs()` | `handler/delivery.rs` | 331 |
| `forward_to_upstream()` | `handler/delivery.rs` | 448 |
| `handle_expired_subs()` | `handler/delivery.rs` | 503 |
| `forward_to_optimistic_gateways()` | `handler/delivery.rs` | 590 |
| `deliver_cross_account()` | `handler/delivery.rs` | 642 |
| `SubscriptionManager` | `sub_list.rs` | 375 |
| `Subscription` | `sub_list.rs` | 10 |
| `SubscriptionManager::insert()` | `sub_list.rs` | 389 |
| `SubscriptionManager::remove()` | `sub_list.rs` | 397 |
| `SubscriptionManager::for_each_match()` | `sub_list.rs` | 495 |
| `MsgWriter` | `msg_writer.rs` | 37 |
| `MsgWriter::write_msg()` | `msg_writer.rs` | 74 |
| `MsgWriter::write_lmsg()` | `msg_writer.rs` | 92 |
| `MsgWriter::write_rmsg()` | `msg_writer.rs` | 109 |
| `MsgWriter::drain()` | `msg_writer.rs` | 157 |
| `ClientOp` | `nats_proto.rs` | 153 |
| `LeafOp` | `nats_proto.rs` | 546 |
| `RouteOp` | `nats_proto.rs` | 838 |
| `MsgBuilder` | `nats_proto.rs` | 1194 |
| `try_parse_client_op()` | `nats_proto.rs` | 182 |
| `ClientHandler` | `client_handler.rs` | 20 |
| `handle_sub()` | `client_handler.rs` | 72 |
| `handle_pub()` | `client_handler.rs` | 281 |
| `LeafHandler` | `leaf_handler.rs` | 22 |
| `RouteHandler` | `route_handler.rs` | 21 |
| `GatewayHandler` | `gateway_handler.rs` | 23 |
| `Upstream` | `upstream.rs` | 76 |
| `UpstreamCmd` | `upstream.rs` | 25 |
| `LeafConn` | `leaf_conn.rs` | 98 |
| `LeafReader` | `leaf_conn.rs` | 261 |
| `LeafWriter` | `leaf_conn.rs` | 294 |
| `InterestPipeline` | `interest.rs` | 131 |
| `propagate_all_interest()` | `propagation.rs` | 279 |
| `propagate_leaf_interest()` | `propagation.rs` | 20 |
| `propagate_route_interest()` | `propagation.rs` | 107 |
