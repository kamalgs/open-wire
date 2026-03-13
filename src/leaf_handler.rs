// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Leaf node protocol handler: LS+, LS-, LMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Leaf`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::{debug, warn};

use crate::handler::{
    bytes_to_str, deliver_to_subs, forward_to_upstream, ConnCtx, ConnExt, HandleResult, WorkerCtx,
};
use crate::nats_proto;
use crate::protocol::LeafOp;
use crate::sub_list::Subscription;

/// Handles leaf node protocol operations (LS+, LS-, LMSG, PING, PONG).
pub(crate) struct LeafHandler;

impl LeafHandler {
    /// Dispatch a parsed leaf protocol operation.
    ///
    /// Returns `(HandleResult, expired_subs)`. Expired subs must be cleaned up
    /// by the worker after regaining `&mut self` access to the connections map.
    pub(crate) fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        op: LeafOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            LeafOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            LeafOp::Pong => (HandleResult::Ok, Vec::new()),
            LeafOp::LeafSub { subject, queue } => {
                let result = Self::handle_leaf_sub(conn, wctx, subject, queue);
                (result, Vec::new())
            }
            LeafOp::LeafUnsub { subject, queue } => {
                let result = Self::handle_leaf_unsub(conn, wctx, subject, queue);
                (result, Vec::new())
            }
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => Self::handle_lmsg(conn, wctx, subject, reply, headers, payload),
            LeafOp::Info(_) | LeafOp::Ok | LeafOp::Err(_) => {
                // Ignore INFO/+OK/-ERR from inbound leaf in Active phase.
                (HandleResult::Ok, Vec::new())
            }
        }
    }

    fn handle_leaf_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        queue: Option<Bytes>,
    ) -> HandleResult {
        let subject_str = bytes_to_str(&subject);
        let queue_str = queue.as_ref().map(|q| bytes_to_str(q).to_string());

        // Generate synthetic SID for this leaf subscription.
        let sid = match conn.ext {
            ConnExt::Leaf {
                ref mut leaf_sid_counter,
                ref mut leaf_sids,
            } => {
                *leaf_sid_counter += 1;
                let sid = *leaf_sid_counter;
                leaf_sids.insert((subject.clone(), queue.clone()), sid);
                sid
            }
            ConnExt::Client => unreachable!("leaf op on client connection"),
        };

        let upstream_queue = queue_str.clone();

        let sub = Subscription {
            conn_id: conn.conn_id,
            sid,
            sid_bytes: nats_proto::sid_to_bytes(sid),
            subject: subject_str.to_string(),
            queue: queue_str,
            writer: conn.direct_writer.clone(),
            max_msgs: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            is_leaf: true,
        };

        {
            let mut subs = wctx.state.subs.write().unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        {
            let mut upstream = wctx.state.upstream.write().unwrap();
            if let Some(ref mut up) = *upstream {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue) {
                    warn!(error = %e, "failed to add upstream interest for leaf sub");
                }
            }
        }

        *conn.sub_count += 1;

        gauge!(
            "subscriptions_active",
            "worker" => wctx.worker_label.to_string()
        )
        .increment(1.0);
        debug!(conn_id = conn.conn_id, sid, subject = %subject_str, "leaf subscribed");

        HandleResult::Ok
    }

    fn handle_leaf_unsub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        queue: Option<Bytes>,
    ) -> HandleResult {
        let sid = match conn.ext {
            ConnExt::Leaf {
                ref mut leaf_sids, ..
            } => match leaf_sids.remove(&(subject.clone(), queue)) {
                Some(s) => s,
                None => return HandleResult::Ok,
            },
            ConnExt::Client => unreachable!("leaf op on client connection"),
        };

        let removed = {
            let mut subs = wctx.state.subs.write().unwrap();
            let r = subs.remove(conn.conn_id, sid);
            wctx.state
                .has_subs
                .store(!subs.is_empty(), Ordering::Relaxed);
            r
        };

        if let Some(ref removed) = removed {
            *conn.sub_count = conn.sub_count.saturating_sub(1);
            let mut upstream = wctx.state.upstream.write().unwrap();
            if let Some(ref mut up) = *upstream {
                up.remove_interest(&removed.subject, removed.queue.as_deref());
            }
            gauge!(
                "subscriptions_active",
                "worker" => wctx.worker_label.to_string()
            )
            .decrement(1.0);
            debug!(
                conn_id = conn.conn_id,
                sid,
                subject = %removed.subject,
                "leaf unsubscribed"
            );
        }

        HandleResult::Ok
    }

    fn handle_lmsg(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
        payload: Bytes,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        let subject_str = bytes_to_str(&subject);
        // Echo suppression: always skip delivery back to the originating leaf.
        let expired = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            reply.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            true, // always suppress echo for leaf connections
        );

        forward_to_upstream(
            conn.upstream_tx,
            wctx.state,
            subject,
            reply,
            headers,
            payload,
        );

        (HandleResult::Ok, expired)
    }
}
