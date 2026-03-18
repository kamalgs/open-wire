//! Shared handler types and delivery functions for protocol command dispatch.
//!
//! Defines per-connection (`ConnCtx`) and per-worker (`WorkerCtx`) context views,
//! the `ConnExt` discriminator that replaces `ConnectionKind`, and common delivery
//! helpers used by both `ClientHandler` and `LeafHandler`.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
#[cfg(feature = "leaf")]
use std::sync::mpsc;
use std::sync::Arc;

#[cfg(feature = "gateway")]
use std::cell::RefCell;

use bytes::{Bytes, BytesMut};
use metrics::gauge;
use tracing::debug;

#[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
use crate::nats_proto;
use crate::server::{Permissions, ServerState};
use crate::sub_list::DirectWriter;
use crate::types::HeaderMap;
#[cfg(feature = "leaf")]
use crate::upstream::UpstreamCmd;

/// Per-connection state view. Borrows disjointly from `ClientState` fields
/// so the handler can read/write connection-specific data without holding
/// a mutable borrow on the entire `Worker`.
pub(crate) struct ConnCtx<'a> {
    pub conn_id: u64,
    pub write_buf: &'a mut BytesMut,
    pub direct_writer: &'a DirectWriter,
    pub echo: bool,
    pub sub_count: &'a mut usize,
    #[cfg(feature = "leaf")]
    pub upstream_tx: &'a mut Option<mpsc::Sender<UpstreamCmd>>,
    pub permissions: &'a Option<Permissions>,
    pub ext: &'a mut ConnExt,
    /// Whether the connection is in Draining phase.
    pub draining: bool,
}

/// Per-connection extension state, replacing the flat `ConnectionKind` enum
/// plus the `leaf_sid_counter` / `leaf_sids` fields on `ClientState`.
pub(crate) enum ConnExt {
    /// Normal NATS client connection (uses MSG/HMSG for delivery).
    Client,
    /// Inbound leaf node connection (uses LMSG for delivery, LS+/LS- for interest).
    #[cfg(feature = "hub")]
    Leaf {
        leaf_sid_counter: u64,
        leaf_sids: HashMap<(Bytes, Option<Bytes>), u64>,
    },
    /// Inbound route connection (uses RMSG for delivery, RS+/RS- for interest).
    #[cfg(feature = "cluster")]
    Route {
        route_sid_counter: u64,
        route_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Peer's server_id, stored for cleanup deregistration from RoutePeerRegistry.
        peer_server_id: Option<String>,
    },
    /// Inbound gateway connection (uses RMSG for delivery, RS+/RS- for interest).
    #[cfg(feature = "gateway")]
    Gateway {
        gateway_sid_counter: u64,
        gateway_sids: HashMap<(Bytes, Option<Bytes>), u64>,
        /// Secondary index: subject → list of (queue, sid) for O(1) unsub lookup.
        gateway_sids_by_subject: HashMap<Bytes, Vec<(Option<Bytes>, u64)>>,
        /// Remote cluster's gateway name.
        peer_gateway_name: Option<String>,
    },
}

impl ConnExt {
    /// Returns `true` for inbound leaf connections.
    #[cfg(feature = "hub")]
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
    }

    /// Returns `true` for inbound leaf connections.
    #[cfg(not(feature = "hub"))]
    pub fn is_leaf(&self) -> bool {
        false
    }

    /// Returns `true` for inbound route connections.
    #[cfg(feature = "cluster")]
    pub fn is_route(&self) -> bool {
        matches!(self, Self::Route { .. })
    }

    /// Returns `true` for inbound route connections.
    #[cfg(not(feature = "cluster"))]
    #[allow(dead_code)]
    pub fn is_route(&self) -> bool {
        false
    }

    /// Returns `true` for inbound gateway connections.
    #[cfg(feature = "gateway")]
    pub fn is_gateway(&self) -> bool {
        matches!(self, Self::Gateway { .. })
    }

    /// Returns `true` for inbound gateway connections.
    #[cfg(not(feature = "gateway"))]
    #[allow(dead_code)]
    pub fn is_gateway(&self) -> bool {
        false
    }
}

/// Worker-level context for delivery and notification.
pub(crate) struct WorkerCtx<'a> {
    pub state: &'a Arc<ServerState>,
    pub event_fd: RawFd,
    pub pending_notify: &'a mut [RawFd; 16],
    pub pending_notify_count: &'a mut usize,
    pub msgs_received: &'a mut u64,
    pub msgs_received_bytes: &'a mut u64,
    pub msgs_delivered: &'a mut u64,
    pub msgs_delivered_bytes: &'a mut u64,
    pub worker_label: &'a str,
}

/// Result of handling a single protocol operation.
pub(crate) enum HandleResult {
    /// Operation handled successfully, continue processing.
    Ok,
    /// Operation handled, connection should be flushed.
    Flush,
    /// Protocol error — disconnect the connection.
    Disconnect,
}

/// Inner dispatch for writing a message to a single subscription based on its type.
#[inline]
fn deliver_to_sub_inner(
    sub: &crate::sub_list::Subscription,
    subject: &[u8],
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
) {
    #[cfg(feature = "cluster")]
    if sub.is_route {
        sub.writer.write_rmsg(subject, reply, headers, payload);
        return;
    }
    #[cfg(feature = "hub")]
    if sub.is_leaf {
        sub.writer.write_lmsg(subject, reply, headers, payload);
    } else {
        sub.writer
            .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
    }
    #[cfg(not(feature = "hub"))]
    sub.writer
        .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
}

/// Deliver a message to all matching subscriptions in the global sub list.
///
/// Writes MSG (for client subs) or LMSG (for leaf subs) directly to each
/// matching subscription's `DirectWriter`. Accumulates eventfd notifications
/// for remote workers (deduplicating within the batch).
///
/// Returns `(delivered_count, expired_subs)` where `delivered_count` is the
/// number of subscriptions that actually received the message (excluding skipped
/// subs), and expired_subs contains `(conn_id, sid)` pairs for subscriptions
/// that reached their delivery limit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_to_subs(
    wctx: &mut WorkerCtx<'_>,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    skip_conn_id: u64,
    skip_echo: bool,
    #[cfg(feature = "cluster")] skip_routes: bool,
    #[cfg(feature = "gateway")] skip_gateways: bool,
) -> (usize, Vec<(u64, u64)>) {
    let payload_len = payload.len() as u64;
    let mut delivered: usize = 0;

    let subs = wctx.state.subs.read().unwrap();
    let (_match_count, expired) = subs.for_each_match(subject_str, |sub| {
        // Suppress echo: don't deliver to the publisher itself
        // when the client set echo: false in CONNECT, or suppress
        // delivery back to the originating leaf connection.
        if skip_echo && sub.conn_id == skip_conn_id {
            return;
        }
        // One-hop rule: messages from routes are never re-forwarded to other routes.
        #[cfg(feature = "cluster")]
        if skip_routes && sub.is_route {
            return;
        }
        // One-hop rule: messages from gateways are never re-forwarded to other gateways.
        #[cfg(feature = "gateway")]
        if skip_gateways && sub.is_gateway {
            return;
        }
        #[cfg(feature = "gateway")]
        if sub.is_gateway {
            // Rewrite reply with _GR_ prefix before forwarding across gateway
            let gw_reply = rewrite_gateway_reply(reply, wctx.state);
            sub.writer
                .write_rmsg(subject, gw_reply.as_deref(), headers, payload);
        } else {
            deliver_to_sub_inner(sub, subject, reply, headers, payload);
        }
        #[cfg(not(feature = "gateway"))]
        {
            deliver_to_sub_inner(sub, subject, reply, headers, payload);
        }
        delivered += 1;
        *wctx.msgs_delivered += 1;
        *wctx.msgs_delivered_bytes += payload_len;
        // Skip notification for our own worker — flush_pending
        // runs after the event loop iteration.
        let fd = sub.writer.event_raw_fd();
        if fd == wctx.event_fd {
            return;
        }
        // Accumulate notification (deduplicated across entire batch).
        if !wctx.pending_notify[..*wctx.pending_notify_count].contains(&fd)
            && *wctx.pending_notify_count < wctx.pending_notify.len()
        {
            wctx.pending_notify[*wctx.pending_notify_count] = fd;
            *wctx.pending_notify_count += 1;
        }
    });
    drop(subs);

    (delivered, expired)
}

/// Deliver a message to all matching subscriptions from the upstream reader thread.
///
/// Unlike `deliver_to_subs`, this variant collects dirty writers for batch
/// notification (since the upstream reader runs outside the worker event loop)
/// and does not track worker-level metrics.
#[cfg(feature = "leaf")]
pub(crate) fn deliver_to_subs_upstream(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
) -> (usize, Vec<(u64, u64)>) {
    deliver_to_subs_upstream_inner(
        state,
        subject,
        subject_str,
        reply,
        headers,
        payload,
        dirty_writers,
        #[cfg(feature = "cluster")]
        false,
        #[cfg(feature = "gateway")]
        false,
    )
}

/// Deliver to local subs from an upstream-like source (hub or route reader thread).
/// When `skip_routes` is true (cluster feature), route subs are skipped (one-hop rule).
///
/// Returns `(delivered_count, expired_subs)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_to_subs_upstream_inner(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
    #[cfg(feature = "cluster")] skip_routes: bool,
    #[cfg(feature = "gateway")] skip_gateways: bool,
) -> (usize, Vec<(u64, u64)>) {
    let mut delivered: usize = 0;
    let subs = state.subs.read().unwrap();
    let (_count, expired) = subs.for_each_match(subject_str, |sub| {
        #[cfg(feature = "cluster")]
        if sub.is_route {
            if skip_routes {
                return;
            }
            sub.writer.write_rmsg(subject, reply, headers, payload);
            dirty_writers.push(sub.writer.clone());
            delivered += 1;
            return;
        }
        #[cfg(feature = "gateway")]
        if sub.is_gateway {
            if skip_gateways {
                return;
            }
            let gw_reply = rewrite_gateway_reply(reply, state);
            sub.writer
                .write_rmsg(subject, gw_reply.as_deref(), headers, payload);
            dirty_writers.push(sub.writer.clone());
            delivered += 1;
            return;
        }
        #[cfg(feature = "hub")]
        if sub.is_leaf {
            sub.writer.write_lmsg(subject, reply, headers, payload);
        } else {
            sub.writer
                .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
        }
        #[cfg(not(feature = "hub"))]
        sub.writer
            .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
        dirty_writers.push(sub.writer.clone());
        delivered += 1;
    });
    drop(subs);

    (delivered, expired)
}

/// Handle expired subscriptions after delivery.
///
/// Removes expired subs from the global sub list, decrements `sub_count` on
/// their owning connections, removes upstream interest, and updates metrics.
pub(crate) fn handle_expired_subs(
    expired: &[(u64, u64)],
    state: &ServerState,
    conns: &mut HashMap<u64, crate::worker::ClientState>,
    worker_label: &str,
) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state.subs.write().unwrap();
    for (exp_conn_id, exp_sid) in expired {
        if let Some(removed) = subs.remove(*exp_conn_id, *exp_sid) {
            if let Some(client) = conns.get_mut(exp_conn_id) {
                client.sub_count = client.sub_count.saturating_sub(1);
            }
            #[cfg(feature = "leaf")]
            {
                let mut upstream = state.upstream.write().unwrap();
                if let Some(ref mut up) = *upstream {
                    up.remove_interest(&removed.subject, removed.queue.as_deref());
                }
            }
            let _ = &removed;
            gauge!(
                "subscriptions_active",
                "worker" => worker_label.to_string()
            )
            .decrement(1.0);
            debug!(
                conn_id = exp_conn_id,
                sid = exp_sid,
                subject = %removed.subject,
                "auto-unsubscribed (max reached)"
            );
        }
    }
    state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
}

/// Handle expired subscriptions from the upstream reader thread.
///
/// Similar to `handle_expired_subs` but without access to worker connections
/// (upstream runs in its own thread).
#[cfg(feature = "leaf")]
pub(crate) fn handle_expired_subs_upstream(expired: &[(u64, u64)], state: &ServerState) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state.subs.write().unwrap();
    for (conn_id, sid) in expired {
        if let Some(removed) = subs.remove(*conn_id, *sid) {
            let mut upstream = state.upstream.write().unwrap();
            if let Some(ref mut up) = *upstream {
                up.remove_interest(&removed.subject, removed.queue.as_deref());
            }
        }
    }
    state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
}

/// Propagate LS+ to all inbound leaf connections (interest advertisement).
#[cfg(feature = "hub")]
pub(crate) fn propagate_leaf_sub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    let writers = state.leaf_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_leaf_sub_queue(subject, q)
    } else {
        builder.build_leaf_sub(subject)
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Propagate LS- to all inbound leaf connections (interest removal).
#[cfg(feature = "hub")]
pub(crate) fn propagate_leaf_unsub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    let writers = state.leaf_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_leaf_unsub_queue(subject, q)
    } else {
        builder.build_leaf_unsub(subject)
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send LS+ for all existing client subscriptions to a given leaf's DirectWriter.
#[cfg(feature = "hub")]
pub(crate) fn send_existing_subs(state: &ServerState, writer: &DirectWriter) {
    let subs = state.subs.read().unwrap();
    let mut builder = nats_proto::MsgBuilder::new();
    for (subject, queue) in subs.client_interests() {
        let data = if let Some(q) = queue {
            builder.build_leaf_sub_queue(subject.as_bytes(), q.as_bytes())
        } else {
            builder.build_leaf_sub(subject.as_bytes())
        };
        writer.write_raw(data);
    }
    drop(subs);
    writer.notify();
}

/// Propagate RS+ to all inbound route connections (interest advertisement).
#[cfg(feature = "cluster")]
pub(crate) fn propagate_route_sub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    let writers = state.route_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_route_sub_queue(subject, q)
    } else {
        builder.build_route_sub(subject)
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Propagate RS- to all inbound route connections (interest removal).
#[cfg(feature = "cluster")]
pub(crate) fn propagate_route_unsub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    let writers = state.route_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_route_unsub_queue(subject, q)
    } else {
        builder.build_route_unsub(subject)
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send RS+ for all existing local subscriptions to a given route's DirectWriter.
#[cfg(feature = "cluster")]
pub(crate) fn send_existing_subs_to_route(state: &ServerState, writer: &DirectWriter) {
    let subs = state.subs.read().unwrap();
    let mut builder = nats_proto::MsgBuilder::new();
    for (subject, queue) in subs.local_interests() {
        let data = if let Some(q) = queue {
            builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
        } else {
            builder.build_route_sub(subject.as_bytes())
        };
        writer.write_raw(data);
    }
    drop(subs);
    writer.notify();
}

#[cfg(feature = "gateway")]
thread_local! {
    static GW_BUILDER: RefCell<nats_proto::MsgBuilder> = RefCell::new(nats_proto::MsgBuilder::new());
}

/// Propagate RS+ to all gateway connections (interest advertisement).
///
/// In optimistic mode, RS+ is skipped for outbound gateways (the remote already
/// receives everything). Only inbound gateways (tracked in `gateway_writers`)
/// and outbound gateways in InterestOnly mode receive RS+.
#[cfg(feature = "gateway")]
pub(crate) fn propagate_gateway_sub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    use crate::server::GatewayInterestMode;

    let writers = state.gateway_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }

    // Determine which outbound gateways are in InterestOnly mode.
    let gi = state.gateway_interest.read().unwrap();

    GW_BUILDER.with(|cell| {
        let mut builder = cell.borrow_mut();
        let data = if let Some(q) = queue {
            builder.build_route_sub_queue(subject, q)
        } else {
            builder.build_route_sub(subject)
        };
        for (conn_id, writer) in writers.iter() {
            // Skip outbound gateways in Optimistic mode (they forward everything already).
            if let Some(gis) = gi.get(conn_id) {
                if gis.mode == GatewayInterestMode::Optimistic
                    || gis.mode == GatewayInterestMode::Transitioning
                {
                    continue;
                }
            }
            writer.write_raw(data);
            writer.notify();
        }
    });
}

/// Propagate RS- to all gateway connections (interest removal).
///
/// Only sent to outbound gateways in InterestOnly mode. Optimistic gateways
/// don't need RS- (they use negative interest in the opposite direction).
#[cfg(feature = "gateway")]
pub(crate) fn propagate_gateway_unsub(state: &ServerState, subject: &[u8], queue: Option<&[u8]>) {
    use crate::server::GatewayInterestMode;

    let writers = state.gateway_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }

    let gi = state.gateway_interest.read().unwrap();

    GW_BUILDER.with(|cell| {
        let mut builder = cell.borrow_mut();
        let data = if let Some(q) = queue {
            builder.build_route_unsub_queue(subject, q)
        } else {
            builder.build_route_unsub(subject)
        };
        for (conn_id, writer) in writers.iter() {
            // Skip outbound gateways in Optimistic mode.
            if let Some(gis) = gi.get(conn_id) {
                if gis.mode == GatewayInterestMode::Optimistic
                    || gis.mode == GatewayInterestMode::Transitioning
                {
                    continue;
                }
            }
            writer.write_raw(data);
            writer.notify();
        }
    });
}

/// Send RS+ for all existing local subscriptions to a given gateway's DirectWriter.
#[cfg(feature = "gateway")]
pub(crate) fn send_existing_subs_to_gateway(state: &ServerState, writer: &DirectWriter) {
    let subs = state.subs.read().unwrap();
    let mut builder = nats_proto::MsgBuilder::new();
    for (subject, queue) in subs.local_interests() {
        let data = if let Some(q) = queue {
            builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
        } else {
            builder.build_route_sub(subject.as_bytes())
        };
        writer.write_raw(data);
    }
    drop(subs);
    writer.notify();
}

/// Forward a message to outbound gateways in optimistic mode.
///
/// Called after `deliver_to_subs` when gateway subs may not exist in SubList
/// (because the remote hasn't sent RS+ yet). In optimistic mode, we forward
/// unless the subject is in the gateway's negative interest set.
#[cfg(feature = "gateway")]
pub(crate) fn forward_to_optimistic_gateways(
    wctx: &mut WorkerCtx<'_>,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
) {
    use crate::server::GatewayInterestMode;

    let gi = wctx.state.gateway_interest.read().unwrap();
    if gi.is_empty() {
        return;
    }

    let payload_len = payload.len() as u64;

    for gis in gi.values() {
        if gis.mode != GatewayInterestMode::Optimistic {
            continue;
        }
        // Skip if subject is in the negative interest set.
        if gis.ni.contains(subject_str) {
            continue;
        }
        // Check if a gateway sub already matched (delivered via SubList).
        // If the SubList already has a gateway sub for this subject, deliver_to_subs
        // already wrote to the writer, so skip to avoid duplicate delivery.
        // We check has_local_interest inverted — if a gateway sub exists in SubList
        // for this conn_id, the message was already delivered.
        // Actually, in optimistic mode we don't insert gateway subs into SubList,
        // so there's no duplication risk. Forward unconditionally (unless ni'd).

        // Rewrite reply with _GR_ prefix before forwarding across gateway.
        let gw_reply = rewrite_gateway_reply(reply, wctx.state);
        gis.writer
            .write_rmsg(subject, gw_reply.as_deref(), headers, payload);

        // Batch-accumulate notification (same as deliver_to_subs) instead of
        // per-message notify() to avoid one eventfd write syscall per PUB.
        let fd = gis.writer.event_raw_fd();
        if !wctx.pending_notify[..*wctx.pending_notify_count].contains(&fd)
            && *wctx.pending_notify_count < wctx.pending_notify.len()
        {
            wctx.pending_notify[*wctx.pending_notify_count] = fd;
            *wctx.pending_notify_count += 1;
        }

        *wctx.msgs_delivered += 1;
        *wctx.msgs_delivered_bytes += payload_len;
    }
}

/// Rewrite outbound reply: `reply` → `_GR_.<cluster_hash>.<server_hash>.reply`.
/// Returns `None` if the input reply is `None`.
#[cfg(feature = "gateway")]
fn rewrite_gateway_reply(reply: Option<&[u8]>, state: &ServerState) -> Option<bytes::Bytes> {
    let reply = reply?;
    // Don't double-rewrite
    if reply.starts_with(b"_GR_.") {
        return Some(bytes::Bytes::copy_from_slice(reply));
    }
    let prefix = &state.gateway_reply_prefix;
    let mut buf = Vec::with_capacity(prefix.len() + reply.len());
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(reply);
    Some(bytes::Bytes::from(buf))
}

/// Unwrap inbound reply as a zero-copy `Bytes` sub-slice.
/// Returns a `Bytes::slice()` into the original buffer — no heap allocation.
#[cfg(feature = "gateway")]
pub(crate) fn unwrap_gateway_reply_bytes(reply: &Bytes) -> Bytes {
    if !reply.starts_with(b"_GR_.") {
        return reply.clone();
    }
    let after_prefix = &reply[5..];
    let dot1 = match memchr::memchr(b'.', after_prefix) {
        Some(i) => i,
        None => return reply.clone(),
    };
    let rest = &after_prefix[dot1 + 1..];
    let dot2 = match memchr::memchr(b'.', rest) {
        Some(i) => i,
        None => return reply.clone(),
    };
    let start = 5 + dot1 + 1 + dot2 + 1;
    reply.slice(start..)
}

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
pub(crate) fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}

/// Forward a publish to the upstream hub. If the writer thread has gone,
/// refresh the sender from global state.
#[cfg(feature = "leaf")]
pub(crate) fn forward_to_upstream(
    upstream_tx: &mut Option<mpsc::Sender<UpstreamCmd>>,
    state: &ServerState,
    subject: Bytes,
    reply: Option<Bytes>,
    headers: Option<HeaderMap>,
    payload: Bytes,
) {
    if let Some(ref tx) = upstream_tx {
        if tx
            .send(UpstreamCmd::Publish {
                subject,
                reply,
                headers,
                payload,
            })
            .is_err()
        {
            // Writer thread gone — refresh from global state (may have reconnected)
            *upstream_tx = state.upstream_tx.read().unwrap().clone();
        }
    }
}
