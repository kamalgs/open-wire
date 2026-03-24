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
    /// When true, send 503 no-responders status for request-reply with zero subscribers.
    pub no_responders: bool,
    pub sub_count: &'a mut usize,
    #[cfg(feature = "leaf")]
    pub upstream_txs: &'a mut Vec<mpsc::Sender<UpstreamCmd>>,
    pub permissions: &'a Option<Permissions>,
    pub ext: &'a mut ConnExt,
    /// Whether the connection is in Draining phase.
    pub draining: bool,
    /// Account this connection belongs to. 0 = `$G` (global/default).
    #[cfg(feature = "accounts")]
    pub account_id: crate::server::AccountId,
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
///
/// Returns `true` if the message was delivered, `false` if it was filtered
/// (e.g., by leaf subscribe permissions).
#[inline]
#[allow(unused_variables)]
fn deliver_to_sub_inner(
    sub: &crate::sub_list::Subscription,
    subject: &[u8],
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    #[cfg(feature = "accounts")] account_name: &[u8],
) -> bool {
    #[cfg(feature = "cluster")]
    if sub.is_route {
        sub.writer.write_rmsg(
            subject,
            reply,
            headers,
            payload,
            #[cfg(feature = "accounts")]
            account_name,
        );
        return true;
    }
    #[cfg(feature = "hub")]
    if sub.is_leaf {
        // Check subscribe permissions: don't deliver to leaf if the subject
        // is not in the leaf's allowed subscribe set.
        if let Some(ref perms) = sub.leaf_perms {
            let subject_str = std::str::from_utf8(subject).unwrap_or("");
            if !perms.subscribe.is_allowed(subject_str) {
                return false;
            }
        }
        sub.writer.write_lmsg(subject, reply, headers, payload);
        return true;
    }
    #[cfg(not(feature = "hub"))]
    {
        sub.writer
            .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
    }
    #[cfg(feature = "hub")]
    {
        sub.writer
            .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
    }
    true
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
    #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
) -> (usize, Vec<(u64, u64)>) {
    let payload_len = payload.len() as u64;
    let mut delivered: usize = 0;

    #[cfg(feature = "accounts")]
    let acct_name_str = wctx.state.account_name(account_id);
    #[cfg(feature = "accounts")]
    let acct_name = acct_name_str.as_bytes();

    let subs = wctx
        .state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .read()
        .unwrap();
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
            sub.writer.write_rmsg(
                subject,
                gw_reply.as_deref(),
                headers,
                payload,
                #[cfg(feature = "accounts")]
                acct_name,
            );
        } else {
            let did_deliver = deliver_to_sub_inner(
                sub,
                subject,
                reply,
                headers,
                payload,
                #[cfg(feature = "accounts")]
                acct_name,
            );
            if !did_deliver {
                return;
            }
        }
        #[cfg(not(feature = "gateway"))]
        {
            let did_deliver = deliver_to_sub_inner(
                sub,
                subject,
                reply,
                headers,
                payload,
                #[cfg(feature = "accounts")]
                acct_name,
            );
            if !did_deliver {
                return;
            }
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_to_subs_upstream(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
    #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
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
        #[cfg(feature = "accounts")]
        account_id,
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
    #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
) -> (usize, Vec<(u64, u64)>) {
    let mut delivered: usize = 0;

    #[cfg(feature = "accounts")]
    #[allow(unused)]
    let acct_name_str = state.account_name(account_id);
    #[cfg(feature = "accounts")]
    #[allow(unused)]
    let acct_name = acct_name_str.as_bytes();

    let subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .read()
        .unwrap();
    let (_count, expired) = subs.for_each_match(subject_str, |sub| {
        #[cfg(feature = "cluster")]
        if sub.is_route {
            if skip_routes {
                return;
            }
            sub.writer.write_rmsg(
                subject,
                reply,
                headers,
                payload,
                #[cfg(feature = "accounts")]
                acct_name,
            );
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
            sub.writer.write_rmsg(
                subject,
                gw_reply.as_deref(),
                headers,
                payload,
                #[cfg(feature = "accounts")]
                acct_name,
            );
            dirty_writers.push(sub.writer.clone());
            delivered += 1;
            return;
        }
        #[cfg(feature = "hub")]
        if sub.is_leaf {
            // Check subscribe permissions: don't deliver to leaf if filtered.
            if let Some(ref perms) = sub.leaf_perms {
                let subject_str_inner = std::str::from_utf8(subject).unwrap_or("");
                if !perms.subscribe.is_allowed(subject_str_inner) {
                    return;
                }
            }
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

/// Deliver a message to cross-account subscribers (worker context).
///
/// Called after same-account `deliver_to_subs()`. For each cross-account route
/// whose export pattern matches the published subject, delivers to the
/// destination account's SubList (optionally remapping the subject).
///
/// Single-hop: cross-account delivery does NOT recursively trigger more
/// cross-account forwarding.
#[cfg(feature = "accounts")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_cross_account(
    wctx: &mut WorkerCtx<'_>,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    src_account_id: crate::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match wctx.state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let payload_len = payload.len() as u64;
    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, subject_str) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes);
        match &route.remap {
            Some(r) => {
                dst_subject_str =
                    crate::sub_list::remap_subject(&r.from_pattern, &r.to_pattern, subject_str);
                dst_subject_bytes = dst_subject_str.as_bytes();
            }
            None => {
                dst_subject_str = subject_str.to_string();
                dst_subject_bytes = subject;
            }
        };

        let dst_acct_name = wctx.state.account_name(route.dst_account_id).as_bytes();

        let subs = wctx.state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(&dst_subject_str, |sub| {
            let did_deliver = deliver_to_sub_inner(
                sub,
                dst_subject_bytes,
                reply,
                headers,
                payload,
                dst_acct_name,
            );
            if !did_deliver {
                return;
            }
            *wctx.msgs_delivered += 1;
            *wctx.msgs_delivered_bytes += payload_len;
            let fd = sub.writer.event_raw_fd();
            if fd != wctx.event_fd
                && !wctx.pending_notify[..*wctx.pending_notify_count].contains(&fd)
                && *wctx.pending_notify_count < wctx.pending_notify.len()
            {
                wctx.pending_notify[*wctx.pending_notify_count] = fd;
                *wctx.pending_notify_count += 1;
            }
        });
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
}

/// Deliver a message to cross-account subscribers (upstream/route reader thread).
///
/// Same as `deliver_cross_account` but for contexts outside the worker event loop.
/// Accumulates dirty writers for batch notification.
#[cfg(feature = "accounts")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_cross_account_upstream(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
    src_account_id: crate::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, subject_str) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes_owned);
        match &route.remap {
            Some(r) => {
                dst_subject_str =
                    crate::sub_list::remap_subject(&r.from_pattern, &r.to_pattern, subject_str);
                dst_subject_bytes_owned = Some(dst_subject_str.as_bytes().to_vec());
            }
            None => {
                dst_subject_str = subject_str.to_string();
                dst_subject_bytes_owned = None;
            }
        };
        let dst_subject_bytes = dst_subject_bytes_owned.as_deref().unwrap_or(subject);

        #[allow(unused)]
        let dst_acct_name = state.account_name(route.dst_account_id).as_bytes();

        let subs = state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(&dst_subject_str, |sub| {
            #[cfg(feature = "cluster")]
            if sub.is_route {
                sub.writer
                    .write_rmsg(dst_subject_bytes, reply, headers, payload, dst_acct_name);
                dirty_writers.push(sub.writer.clone());
                return;
            }
            #[cfg(feature = "hub")]
            if sub.is_leaf {
                // Check subscribe permissions for leaf delivery.
                if let Some(ref perms) = sub.leaf_perms {
                    let subj = std::str::from_utf8(dst_subject_bytes).unwrap_or("");
                    if !perms.subscribe.is_allowed(subj) {
                        return;
                    }
                }
                sub.writer
                    .write_lmsg(dst_subject_bytes, reply, headers, payload);
            } else {
                sub.writer
                    .write_msg(dst_subject_bytes, &sub.sid_bytes, reply, headers, payload);
            }
            #[cfg(not(feature = "hub"))]
            sub.writer
                .write_msg(dst_subject_bytes, &sub.sid_bytes, reply, headers, payload);
            dirty_writers.push(sub.writer.clone());
        });
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
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
    #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .write()
        .unwrap();
    for (exp_conn_id, exp_sid) in expired {
        if let Some(removed) = subs.remove(*exp_conn_id, *exp_sid) {
            if let Some(client) = conns.get_mut(exp_conn_id) {
                client.sub_count = client.sub_count.saturating_sub(1);
            }
            #[cfg(feature = "leaf")]
            {
                let mut upstreams = state.upstreams.write().unwrap();
                for up in upstreams.iter_mut() {
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
pub(crate) fn handle_expired_subs_upstream(
    expired: &[(u64, u64)],
    state: &ServerState,
    #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
) {
    if expired.is_empty() {
        return;
    }
    let mut subs = state
        .get_subs(
            #[cfg(feature = "accounts")]
            account_id,
        )
        .write()
        .unwrap();
    for (conn_id, sid) in expired {
        if let Some(removed) = subs.remove(*conn_id, *sid) {
            let mut upstreams = state.upstreams.write().unwrap();
            for up in upstreams.iter_mut() {
                up.remove_interest(&removed.subject, removed.queue.as_deref());
            }
        }
    }
    state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
}

/// Propagate LS+ to all inbound leaf connections (interest advertisement).
///
/// Checks each leaf's publish permissions before sending — LS+ is only sent
/// to leafs that are allowed to publish on the subject (controls what the
/// leaf can export back).
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
    let subject_str = std::str::from_utf8(subject).unwrap_or("");
    for (_conn_id, (writer, perms)) in writers.iter() {
        if let Some(ref p) = perms {
            if !p.publish.is_allowed(subject_str) {
                continue;
            }
        }
        writer.write_raw(data);
        writer.notify();
    }
}

/// Propagate LS- to all inbound leaf connections (interest removal).
///
/// Checks each leaf's publish permissions before sending — LS- is only sent
/// to leafs that are allowed to publish on the subject.
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
    let subject_str = std::str::from_utf8(subject).unwrap_or("");
    for (_conn_id, (writer, perms)) in writers.iter() {
        if let Some(ref p) = perms {
            if !p.publish.is_allowed(subject_str) {
                continue;
            }
        }
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send LS+ for all existing client subscriptions to a given leaf's DirectWriter.
///
/// Filters by the leaf's publish permissions — only sends LS+ for subjects that
/// the leaf is allowed to publish on (controls what the leaf can export).
#[cfg(feature = "hub")]
pub(crate) fn send_existing_subs(
    state: &ServerState,
    writer: &DirectWriter,
    leaf_perms: &Option<Arc<crate::server::Permissions>>,
) {
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for account_sub in &state.account_subs {
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.client_interests() {
                if let Some(ref p) = leaf_perms {
                    if !p.publish.is_allowed(subject) {
                        continue;
                    }
                }
                let data = if let Some(q) = queue {
                    builder.build_leaf_sub_queue(subject.as_bytes(), q.as_bytes())
                } else {
                    builder.build_leaf_sub(subject.as_bytes())
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.client_interests() {
            if let Some(ref p) = leaf_perms {
                if !p.publish.is_allowed(subject) {
                    continue;
                }
            }
            let data = if let Some(q) = queue {
                builder.build_leaf_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_leaf_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
    writer.notify();
}

/// Propagate RS+ to all inbound route connections (interest advertisement).
#[cfg(feature = "cluster")]
pub(crate) fn propagate_route_sub(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    let writers = state.route_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_route_sub_queue(
            subject,
            q,
            #[cfg(feature = "accounts")]
            account,
        )
    } else {
        builder.build_route_sub(
            subject,
            #[cfg(feature = "accounts")]
            account,
        )
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Propagate RS- to all inbound route connections (interest removal).
#[cfg(feature = "cluster")]
pub(crate) fn propagate_route_unsub(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    let writers = state.route_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if let Some(q) = queue {
        builder.build_route_unsub_queue(
            subject,
            q,
            #[cfg(feature = "accounts")]
            account,
        )
    } else {
        builder.build_route_unsub(
            subject,
            #[cfg(feature = "accounts")]
            account,
        )
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send RS+ for all existing local subscriptions to a given route's DirectWriter.
#[cfg(feature = "cluster")]
pub(crate) fn send_existing_subs_to_route(state: &ServerState, writer: &DirectWriter) {
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for (idx, account_sub) in state.account_subs.iter().enumerate() {
            let acct = state
                .account_name(idx as crate::server::AccountId)
                .as_bytes();
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.local_interests() {
                let data = if let Some(q) = queue {
                    builder.build_route_sub_queue(
                        subject.as_bytes(),
                        q.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                } else {
                    builder.build_route_sub(
                        subject.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
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
pub(crate) fn propagate_gateway_sub(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    #[cfg(feature = "accounts")] account: &[u8],
) {
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
            builder.build_route_sub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_sub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
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
pub(crate) fn propagate_gateway_unsub(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    use crate::server::GatewayInterestMode;

    let writers = state.gateway_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }

    let gi = state.gateway_interest.read().unwrap();

    GW_BUILDER.with(|cell| {
        let mut builder = cell.borrow_mut();
        let data = if let Some(q) = queue {
            builder.build_route_unsub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_unsub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
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
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for (idx, account_sub) in state.account_subs.iter().enumerate() {
            let acct = state
                .account_name(idx as crate::server::AccountId)
                .as_bytes();
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.local_interests() {
                let data = if let Some(q) = queue {
                    builder.build_route_sub_queue(
                        subject.as_bytes(),
                        q.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                } else {
                    builder.build_route_sub(
                        subject.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
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
    #[cfg(feature = "accounts")] account: &[u8],
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
        gis.writer.write_rmsg(
            subject,
            gw_reply.as_deref(),
            headers,
            payload,
            #[cfg(feature = "accounts")]
            account,
        );

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

/// Forward a publish to all upstream hubs. If a writer thread has gone,
/// refresh the senders from global state.
#[cfg(feature = "leaf")]
pub(crate) fn forward_to_upstream(
    upstream_txs: &mut Vec<mpsc::Sender<UpstreamCmd>>,
    state: &ServerState,
    subject: Bytes,
    reply: Option<Bytes>,
    headers: Option<HeaderMap>,
    payload: Bytes,
) {
    if upstream_txs.is_empty() {
        return;
    }
    let mut any_failed = false;
    for tx in upstream_txs.iter() {
        if tx
            .send(UpstreamCmd::Publish {
                subject: subject.clone(),
                reply: reply.clone(),
                headers: headers.clone(),
                payload: payload.clone(),
            })
            .is_err()
        {
            any_failed = true;
        }
    }
    if any_failed {
        // At least one writer thread gone — refresh from global state (may have reconnected)
        *upstream_txs = state.upstream_txs.read().unwrap().clone();
    }
}
