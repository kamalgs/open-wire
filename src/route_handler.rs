//! Route protocol handler: RS+, RS-, RMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Route`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

#[cfg(feature = "accounts")]
use crate::handler::deliver_cross_account;
#[cfg(feature = "leaf")]
use crate::handler::forward_to_upstream;
use crate::handler::{bytes_to_str, deliver_to_subs, ConnCtx, ConnExt, HandleResult, WorkerCtx};
#[cfg(feature = "gateway")]
use crate::handler::{propagate_gateway_sub, propagate_gateway_unsub};
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
        #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
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
            #[cfg(feature = "gateway")]
            is_gateway: false,
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

        // Propagate RS+ to gateway peers.
        #[cfg(feature = "gateway")]
        propagate_gateway_sub(
            wctx.state,
            subject_str.as_bytes(),
            queue.as_deref(),
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
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
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
            propagate_gateway_unsub(
                wctx.state,
                removed.subject.as_bytes(),
                removed.queue.as_deref().map(|q| q.as_bytes()),
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
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
        payload: Bytes,
        #[cfg(feature = "accounts")] account_id: crate::server::AccountId,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        let subject_str = bytes_to_str(&subject);
        // One-hop rule: skip_routes = true — messages from routes are never re-forwarded
        // to other routes. Only deliver to local client subs and leaf subs.
        let (_delivered, expired) = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            reply.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            true, // suppress echo back to originating route
            true, // skip_routes — one-hop enforcement
            #[cfg(feature = "gateway")]
            false, // don't skip gateways — route msgs forward to gateway peers
            #[cfg(feature = "accounts")]
            account_id,
        );

        // Forward to optimistic gateways when no gateway sub matched.
        #[cfg(feature = "gateway")]
        crate::handler::forward_to_optimistic_gateways(
            wctx,
            &subject,
            subject_str,
            reply.as_deref(),
            headers.as_ref(),
            &payload,
            #[cfg(feature = "accounts")]
            wctx.state.account_name(account_id).as_bytes(),
        );

        // Cross-account forwarding: deliver to destination accounts' SubLists.
        #[cfg(feature = "accounts")]
        let expired = {
            let mut expired = expired;
            let cross_expired = deliver_cross_account(
                wctx,
                &subject,
                subject_str,
                reply.as_deref(),
                headers.as_ref(),
                &payload,
                account_id,
            );
            expired.extend(cross_expired);
            expired
        };

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
