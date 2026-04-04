//! Gateway protocol handler: RS+, RS-, RMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Gateway`.
//! Gateways reuse the route wire format (RS+/RS-/RMSG) but with different
//! interest semantics: one outbound connection per remote cluster, and
//! reply subject rewriting for cross-cluster request-reply.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

use crate::handler::propagation::unwrap_gateway_reply_bytes;
use crate::handler::{
    bytes_to_str, ConnCtx, ConnExt, ConnectionHandler, DeliveryScope, HandleResult,
    MessageDeliveryHub, Msg,
};
use crate::nats_proto;
use crate::nats_proto::GatewayOp;
use crate::sub_list::Subscription;

/// Handles gateway protocol operations (RS+, RS-, RMSG, PING, PONG).
pub(crate) struct GatewayHandler;

impl ConnectionHandler for GatewayHandler {
    type Op = GatewayOp;

    fn parse_op(buf: &mut bytes::BytesMut) -> std::io::Result<Option<GatewayOp>> {
        nats_proto::try_parse_gateway_op(buf)
    }

    fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        op: GatewayOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            GatewayOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            GatewayOp::Pong => (HandleResult::Ok, Vec::new()),
            GatewayOp::RouteSub { subject, queue, .. } => {
                let result = Self::handle_gateway_sub(conn, wctx, subject, queue);
                (result, Vec::new())
            }
            GatewayOp::RouteUnsub { subject, .. } => {
                let result = Self::handle_gateway_unsub(conn, wctx, subject);
                (result, Vec::new())
            }
            GatewayOp::RouteMsg {
                subject,
                reply,
                headers,
                payload,
                ..
            } => Self::handle_gmsg(conn, wctx, subject, reply, headers, payload),
            GatewayOp::Info(info) => {
                // Process gateway_urls for gossip discovery.
                #[cfg(feature = "gateway")]
                if let Some(ref urls) = info.gateway_urls {
                    if !urls.is_empty() {
                        let tx = wctx.state.gateway_connect_tx.lock().unwrap();
                        let mut peers = wctx.state.gateway_peers.lock().unwrap();
                        let mut changed = false;
                        for url in urls {
                            if peers.known_urls.insert(url.clone()) {
                                changed = true;
                                if let Some(ref sender) = *tx {
                                    let _ = sender.send(url.clone());
                                }
                            }
                        }
                        drop(peers);
                        drop(tx);
                        if changed {
                            crate::connector::gateway::rebuild_gateway_info(wctx.state);
                        }
                    }
                }
                let _ = info;
                (HandleResult::Ok, Vec::new())
            }
            GatewayOp::Connect(_) => {
                // Ignore CONNECT from inbound gateway in Active phase.
                (HandleResult::Ok, Vec::new())
            }
        }
    }
}

impl GatewayHandler {
    fn handle_gateway_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        queue: Option<Bytes>,
    ) -> HandleResult {
        let subject_str = bytes_to_str(&subject);
        let queue_str = queue.as_ref().map(|q| bytes_to_str(q).to_string());

        // Generate synthetic SID for this gateway subscription.
        let sid = match conn.ext {
            ConnExt::Gateway {
                ref mut gateway_sid_counter,
                ref mut gateway_sids,
                ref mut gateway_sids_by_subject,
                ..
            } => {
                *gateway_sid_counter += 1;
                let sid = *gateway_sid_counter;
                gateway_sids.insert((subject.clone(), queue.clone()), sid);
                gateway_sids_by_subject
                    .entry(subject.clone())
                    .or_default()
                    .push((queue.clone(), sid));
                sid
            }
            _ => unreachable!("gateway op on non-gateway connection"),
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
            #[cfg(feature = "mesh")]
            is_route: false,
            is_gateway: true,
            #[cfg(feature = "accounts")]
            account_id: 0,
            #[cfg(feature = "hub")]
            leaf_perms: None,
        };

        {
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .write()
                .unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        *conn.sub_count += 1;

        gauge!(
            "subscriptions_active",
            "worker" => wctx.worker_label.to_string()
        )
        .increment(1.0);
        debug!(conn_id = conn.conn_id, sid, subject = %subject_str, "gateway subscribed");

        HandleResult::Ok
    }

    fn handle_gateway_unsub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
    ) -> HandleResult {
        // RS- doesn't include queue in the wire format, so look up by subject via index.
        let sid = match conn.ext {
            ConnExt::Gateway {
                ref mut gateway_sids,
                ref mut gateway_sids_by_subject,
                ..
            } => {
                if let Some(entries) = gateway_sids_by_subject.get_mut(&subject) {
                    if let Some((queue, sid)) = entries.pop() {
                        gateway_sids.remove(&(subject.clone(), queue));
                        if entries.is_empty() {
                            gateway_sids_by_subject.remove(&subject);
                        }
                        sid
                    } else {
                        gateway_sids_by_subject.remove(&subject);
                        return HandleResult::Ok;
                    }
                } else {
                    return HandleResult::Ok;
                }
            }
            _ => unreachable!("gateway op on non-gateway connection"),
        };

        let removed = {
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .write()
                .unwrap();
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
                "gateway unsubscribed"
            );
        }

        HandleResult::Ok
    }

    fn handle_gmsg(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
        payload: Bytes,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        // Unwrap _GR_ reply prefix if present (cross-cluster reply rewriting).
        // Uses Bytes::slice() for zero-copy sub-slicing.
        let unwrapped_reply = reply.as_ref().map(unwrap_gateway_reply_bytes);

        let msg = Msg::new(
            subject.clone(),
            unwrapped_reply.clone(),
            headers.as_ref(),
            payload.clone(),
        );
        // One-hop: skip_routes = true and skip_gateways = true — messages from a gateway
        // are never re-forwarded to other routes or gateways.
        let (delivered, expired) = wctx.publish(
            &msg,
            conn.conn_id,
            &DeliveryScope::from_gateway(),
            #[cfg(feature = "accounts")]
            conn.account_id,
        );

        // Send RS- back when no local subs matched (negative interest signal).
        if delivered == 0 {
            let mut builder = nats_proto::MsgBuilder::new();
            let rs_minus = builder.build_route_unsub(
                &subject,
                #[cfg(feature = "accounts")]
                b"$G".as_slice(),
            );
            conn.write_buf.extend_from_slice(rs_minus);
        }

        // Also forward to upstream hub if configured
        #[cfg(feature = "leaf")]
        conn.forward_to_upstream(wctx.state, subject, unwrapped_reply, headers, payload);

        let result = if delivered == 0 {
            HandleResult::Flush
        } else {
            HandleResult::Ok
        };
        (result, expired)
    }
}
