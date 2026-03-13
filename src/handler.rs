// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Shared handler types and delivery functions for protocol command dispatch.
//!
//! Defines per-connection (`ConnCtx`) and per-worker (`WorkerCtx`) context views,
//! the `ConnExt` discriminator that replaces `ConnectionKind`, and common delivery
//! helpers used by both `ClientHandler` and `LeafHandler`.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use metrics::gauge;
use tracing::debug;

use crate::nats_proto;
use crate::server::{Permissions, ServerState};
use crate::sub_list::DirectWriter;
use crate::types::HeaderMap;
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
    Leaf {
        leaf_sid_counter: u64,
        leaf_sids: HashMap<(Bytes, Option<Bytes>), u64>,
    },
}

impl ConnExt {
    /// Returns `true` for inbound leaf connections.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
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

/// Deliver a message to all matching subscriptions in the global sub list.
///
/// Writes MSG (for client subs) or LMSG (for leaf subs) directly to each
/// matching subscription's `DirectWriter`. Accumulates eventfd notifications
/// for remote workers (deduplicating within the batch).
///
/// Returns `(match_count, expired_subs)` where expired_subs contains
/// `(conn_id, sid)` pairs for subscriptions that reached their delivery limit.
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
) -> Vec<(u64, u64)> {
    let payload_len = payload.len() as u64;

    let subs = wctx.state.subs.read().unwrap();
    let (_match_count, expired) = subs.for_each_match(subject_str, |sub| {
        // Suppress echo: don't deliver to the publisher itself
        // when the client set echo: false in CONNECT, or suppress
        // delivery back to the originating leaf connection.
        if skip_echo && sub.conn_id == skip_conn_id {
            return;
        }
        if sub.is_leaf {
            sub.writer.write_lmsg(subject, reply, headers, payload);
        } else {
            sub.writer
                .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
        }
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

    expired
}

/// Deliver a message to all matching subscriptions from the upstream reader thread.
///
/// Unlike `deliver_to_subs`, this variant collects dirty writers for batch
/// notification (since the upstream reader runs outside the worker event loop)
/// and does not track worker-level metrics.
pub(crate) fn deliver_to_subs_upstream(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
) -> Vec<(u64, u64)> {
    let subs = state.subs.read().unwrap();
    let (_count, expired) = subs.for_each_match(subject_str, |sub| {
        if sub.is_leaf {
            sub.writer.write_lmsg(subject, reply, headers, payload);
        } else {
            sub.writer
                .write_msg(subject, &sub.sid_bytes, reply, headers, payload);
        }
        dirty_writers.push(sub.writer.clone());
    });
    drop(subs);

    expired
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
            let mut upstream = state.upstream.write().unwrap();
            if let Some(ref mut up) = *upstream {
                up.remove_interest(&removed.subject, removed.queue.as_deref());
            }
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

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
pub(crate) fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}

/// Forward a publish to the upstream hub. If the writer thread has gone,
/// refresh the sender from global state.
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
