// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Route protocol handler: RS+, RS-, RMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Route`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

#[cfg(feature = "leaf")]
use crate::handler::forward_to_upstream;
use crate::handler::{bytes_to_str, deliver_to_subs, ConnCtx, ConnExt, HandleResult, WorkerCtx};
use crate::nats_proto;
use crate::protocol::RouteOp;
use crate::sub_list::Subscription;

/// Handles route protocol operations (RS+, RS-, RMSG, PING, PONG).
pub(crate) struct RouteHandler;

impl RouteHandler {
    /// Dispatch a parsed route protocol operation.
    ///
    /// Returns `(HandleResult, expired_subs)`. Expired subs must be cleaned up
    /// by the worker after regaining `&mut self` access to the connections map.
    pub(crate) fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        op: RouteOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            RouteOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            RouteOp::Pong => (HandleResult::Ok, Vec::new()),
            RouteOp::RouteSub { subject, queue } => {
                let result = Self::handle_route_sub(conn, wctx, subject, queue);
                (result, Vec::new())
            }
            RouteOp::RouteUnsub { subject } => {
                let result = Self::handle_route_unsub(conn, wctx, subject);
                (result, Vec::new())
            }
            RouteOp::RouteMsg {
                subject,
                reply,
                headers,
                payload,
            } => Self::handle_rmsg(conn, wctx, subject, reply, headers, payload),
            RouteOp::Info(info) => {
                // Gossip: process connect_urls from active-phase INFO updates.
                if !info.connect_urls.is_empty() {
                    let mut peers = wctx.state.route_peers.lock().unwrap();
                    let tx = wctx.state.route_connect_tx.lock().unwrap();
                    for url in &info.connect_urls {
                        let normalized = crate::route_conn::normalize_route_url(url);
                        if peers.known_urls.insert(normalized.clone()) {
                            if let Some(ref sender) = *tx {
                                let _ = sender.send(normalized);
                            }
                        }
                    }
                }
                (HandleResult::Ok, Vec::new())
            }
            RouteOp::Connect(_) => {
                // Ignore CONNECT from inbound route in Active phase.
                (HandleResult::Ok, Vec::new())
            }
        }
    }

    fn handle_route_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        queue: Option<Bytes>,
    ) -> HandleResult {
        let subject_str = bytes_to_str(&subject);
        let queue_str = queue.as_ref().map(|q| bytes_to_str(q).to_string());

        // Generate synthetic SID for this route subscription.
        let sid = match conn.ext {
            ConnExt::Route {
                ref mut route_sid_counter,
                ref mut route_sids,
                ..
            } => {
                *route_sid_counter += 1;
                let sid = *route_sid_counter;
                route_sids.insert((subject.clone(), queue.clone()), sid);
                sid
            }
            _ => unreachable!("route op on non-route connection"),
        };

        let sub = Subscription {
            conn_id: conn.conn_id,
            sid,
            sid_bytes: nats_proto::sid_to_bytes(sid),
            subject: subject_str.to_string(),
            queue: queue_str,
            writer: conn.direct_writer.clone(),
            max_msgs: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            is_leaf: false,
            is_route: true,
        };

        {
            let mut subs = wctx.state.subs.write().unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        *conn.sub_count += 1;

        gauge!(
            "subscriptions_active",
            "worker" => wctx.worker_label.to_string()
        )
        .increment(1.0);
        debug!(conn_id = conn.conn_id, sid, subject = %subject_str, "route subscribed");

        HandleResult::Ok
    }

    fn handle_route_unsub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
    ) -> HandleResult {
        // RS- doesn't include queue in the wire format, so look up by subject only.
        // We try to find the SID for this subject with any queue value.
        let sid = match conn.ext {
            ConnExt::Route {
                ref mut route_sids, ..
            } => {
                // First try exact match with no queue
                if let Some(s) = route_sids.remove(&(subject.clone(), None)) {
                    s
                } else {
                    // Try to find any entry with this subject
                    let key = route_sids.keys().find(|(s, _)| *s == subject).cloned();
                    match key {
                        Some(k) => route_sids.remove(&k).unwrap(),
                        None => return HandleResult::Ok,
                    }
                }
            }
            _ => unreachable!("route op on non-route connection"),
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
            gauge!(
                "subscriptions_active",
                "worker" => wctx.worker_label.to_string()
            )
            .decrement(1.0);
            debug!(
                conn_id = conn.conn_id,
                sid,
                subject = %removed.subject,
                "route unsubscribed"
            );
        }

        HandleResult::Ok
    }

    fn handle_rmsg(
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
        // One-hop rule: skip_routes = true — messages from routes are never re-forwarded
        // to other routes. Only deliver to local client subs and leaf subs.
        let expired = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            reply.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            true, // suppress echo back to originating route
            true, // skip_routes — one-hop enforcement
        );

        // Also forward to upstream hub if configured
        #[cfg(feature = "leaf")]
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
