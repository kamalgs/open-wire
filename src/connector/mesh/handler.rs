//! Route protocol handler: RS+, RS-, RMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Route`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

use crate::buf::RouteOp;
#[cfg(feature = "gateway")]
use crate::handler::propagation::propagate_gateway_interest;
use crate::handler::{
    bytes_to_str, ConnCtx, ConnExt, ConnectionHandler, DeliveryScope, HandleResult,
    MessageDeliveryHub, Msg,
};
use crate::nats_proto;
use crate::sub_list::Subscription;

/// Handles route protocol operations (RS+, RS-, RMSG, PING, PONG).
pub(crate) struct RouteHandler;

impl ConnectionHandler for RouteHandler {
    type Op = RouteOp;

    fn parse_op(buf: &mut bytes::BytesMut) -> std::io::Result<Option<RouteOp>> {
        nats_proto::try_parse_route_op(buf)
    }

    fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        op: RouteOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            RouteOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            RouteOp::Pong => (HandleResult::Ok, Vec::new()),
            RouteOp::RouteSub {
                #[cfg(feature = "accounts")]
                account,
                subject,
                queue,
                ..
            } => {
                #[cfg(feature = "accounts")]
                let account_id = {
                    let acct_str = crate::handler::bytes_to_str(&account);
                    wctx.state.resolve_account(acct_str)
                };
                let result = Self::handle_route_sub(
                    conn,
                    wctx,
                    subject,
                    queue,
                    #[cfg(feature = "accounts")]
                    account_id,
                );
                (result, Vec::new())
            }
            RouteOp::RouteUnsub {
                #[cfg(feature = "accounts")]
                account,
                subject,
                ..
            } => {
                #[cfg(feature = "accounts")]
                let account_id = {
                    let acct_str = crate::handler::bytes_to_str(&account);
                    wctx.state.resolve_account(acct_str)
                };
                let result = Self::handle_route_unsub(
                    conn,
                    wctx,
                    subject,
                    #[cfg(feature = "accounts")]
                    account_id,
                );
                (result, Vec::new())
            }
            RouteOp::RouteMsg {
                #[cfg(feature = "accounts")]
                account,
                subject,
                reply,
                headers,
                payload,
                ..
            } => {
                #[cfg(feature = "accounts")]
                let account_id = {
                    let acct_str = crate::handler::bytes_to_str(&account);
                    wctx.state.resolve_account(acct_str)
                };
                Self::handle_rmsg(
                    conn,
                    wctx,
                    subject,
                    reply,
                    headers,
                    payload,
                    #[cfg(feature = "accounts")]
                    account_id,
                )
            }
            RouteOp::Info(info) => {
                if !info.connect_urls.is_empty() {
                    let mut peers = wctx.state.route_peers.lock().unwrap();
                    let tx = wctx.state.route_connect_tx.lock().unwrap();
                    for url in &info.connect_urls {
                        let normalized = crate::connector::mesh::normalize_route_url(url);
                        if peers.known_urls.insert(normalized.clone()) {
                            if let Some(ref sender) = *tx {
                                let _ = sender.send(normalized);
                            }
                        }
                    }
                }
                (HandleResult::Ok, Vec::new())
            }
            RouteOp::Connect(_) => (HandleResult::Ok, Vec::new()),
        }
    }
}

impl RouteHandler {
    fn handle_route_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        queue: Option<Bytes>,
        #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
    ) -> HandleResult {
        let subject_str = bytes_to_str(&subject);
        let queue_str = queue.as_ref().map(|q| bytes_to_str(q).to_string());

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
            #[cfg(feature = "gateway")]
            is_gateway: false,
            #[cfg(feature = "binary-client")]
            is_binary_client: false,
            #[cfg(feature = "accounts")]
            account_id,
            #[cfg(feature = "hub")]
            leaf_perms: None,
        };

        {
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    account_id,
                )
                .write()
                .unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        *conn.sub_count += 1;

        #[cfg(feature = "gateway")]
        propagate_gateway_interest(
            wctx.state,
            subject_str.as_bytes(),
            queue.as_deref(),
            true,
            #[cfg(feature = "accounts")]
            wctx.state.account_name(account_id).as_bytes(),
        );

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
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
    ) -> HandleResult {
        // RS- doesn't include queue in the wire format, so look up by subject only.
        // We try to find the SID for this subject with any queue value.
        let sid = match conn.ext {
            ConnExt::Route {
                ref mut route_sids, ..
            } => {
                if let Some(s) = route_sids.remove(&(subject.clone(), None)) {
                    s
                } else {
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
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    account_id,
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
            #[cfg(feature = "gateway")]
            propagate_gateway_interest(
                wctx.state,
                removed.subject.as_bytes(),
                removed.queue.as_deref().map(|q| q.as_bytes()),
                false,
                #[cfg(feature = "accounts")]
                wctx.state.account_name(account_id).as_bytes(),
            );
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
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
        payload: Bytes,
        #[cfg(feature = "accounts")] account_id: crate::core::server::AccountId,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        let msg = Msg::new(
            subject.clone(),
            reply.clone(),
            headers.as_ref(),
            payload.clone(),
        );
        // One-hop rule: skip_routes = true — messages from routes are never re-forwarded
        // to other routes. Only deliver to local client subs and leaf subs.
        let (_delivered, expired) = wctx.publish(
            &msg,
            conn.conn_id,
            &DeliveryScope::from_route(),
            #[cfg(feature = "accounts")]
            account_id,
        );

        #[cfg(feature = "leaf")]
        conn.forward_to_upstream(wctx.state, subject, reply, headers, payload);

        (HandleResult::Ok, expired)
    }
}
