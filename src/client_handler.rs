//! Client protocol handler: PUB/HPUB, SUB, UNSUB, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Client`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::{counter, gauge};
use tracing::{debug, warn};

#[cfg(feature = "leaf")]
use crate::handler::forward_to_upstream;
use crate::handler::{bytes_to_str, deliver_to_subs, ConnCtx, HandleResult, WorkerCtx};
#[cfg(feature = "gateway")]
use crate::handler::{propagate_gateway_sub, propagate_gateway_unsub};
#[cfg(feature = "hub")]
use crate::handler::{propagate_leaf_sub, propagate_leaf_unsub};
#[cfg(feature = "cluster")]
use crate::handler::{propagate_route_sub, propagate_route_unsub};
use crate::nats_proto::{self, ClientOp};
use crate::sub_list::Subscription;

/// Handles client protocol operations (PUB, SUB, UNSUB, PING, PONG).
pub(crate) struct ClientHandler;

impl ClientHandler {
    /// Dispatch a parsed client operation.
    ///
    /// Returns `(HandleResult, expired_subs)`. Expired subs must be cleaned up
    /// by the worker after regaining `&mut self` access to the connections map.
    pub(crate) fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        op: ClientOp,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        match op {
            ClientOp::Ping => {
                conn.write_buf.extend_from_slice(b"PONG\r\n");
                (HandleResult::Flush, Vec::new())
            }
            ClientOp::Pong => {
                // Pings outstanding is reset by the worker after handler returns.
                // We signal Ok here — the worker handles the pong bookkeeping.
                (HandleResult::Ok, Vec::new())
            }
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                let result = Self::handle_sub(conn, wctx, sid, subject, queue_group);
                (result, Vec::new())
            }
            ClientOp::Unsubscribe { sid, max } => {
                let result = Self::handle_unsub(conn, wctx, sid, max);
                (result, Vec::new())
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => Self::handle_pub(conn, wctx, subject, payload, respond, headers),
            ClientOp::Connect(_) => {
                // Duplicate CONNECT in active phase, ignore.
                (HandleResult::Ok, Vec::new())
            }
        }
    }

    fn handle_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        sid: u64,
        subject: Bytes,
        queue_group: Option<Bytes>,
    ) -> HandleResult {
        // Silently reject SUB during drain
        if conn.draining {
            return HandleResult::Ok;
        }

        // Check subscribe permissions
        if let Some(ref perms) = conn.permissions {
            let subj = bytes_to_str(&subject);
            if !perms.subscribe.is_allowed(subj) {
                counter!("subscriptions_rejected_total", "reason" => "permissions").increment(1);
                conn.write_buf
                    .extend_from_slice(b"-ERR 'Permissions Violation for Subscription'\r\n");
                return HandleResult::Flush;
            }
        }

        let max_subs = wctx.state.max_subscriptions.load(Ordering::Relaxed);
        if max_subs > 0 && *conn.sub_count >= max_subs {
            warn!(
                conn_id = conn.conn_id,
                max_subscriptions = max_subs,
                "maximum subscriptions exceeded"
            );
            counter!("subscriptions_rejected_total").increment(1);
            conn.write_buf
                .extend_from_slice(b"-ERR 'Maximum Subscriptions Exceeded'\r\n");
            return HandleResult::Flush;
        }

        let subject_str = bytes_to_str(&subject);
        let queue_str = queue_group.as_ref().map(|q| bytes_to_str(q).to_string());

        // Clone for upstream + leaf propagation before moving into Subscription
        #[cfg(feature = "leaf")]
        let upstream_queue = queue_str.clone();
        #[cfg(feature = "hub")]
        let leaf_queue = queue_str.clone();

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
            #[cfg(feature = "cluster")]
            is_route: false,
            #[cfg(feature = "gateway")]
            is_gateway: false,
        };

        {
            let mut subs = wctx.state.subs.write().unwrap();
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        #[cfg(feature = "leaf")]
        {
            let mut upstream = wctx.state.upstream.write().unwrap();
            if let Some(ref mut up) = *upstream {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue) {
                    warn!(error = %e, "failed to add upstream interest");
                }
            }
        }

        *conn.sub_count += 1;

        // Propagate LS+ to inbound leaf connections.
        #[cfg(feature = "hub")]
        propagate_leaf_sub(
            wctx.state,
            subject_str.as_bytes(),
            leaf_queue.as_deref().map(|q| q.as_bytes()),
        );

        // Propagate RS+ to inbound route connections.
        #[cfg(feature = "cluster")]
        propagate_route_sub(wctx.state, subject_str.as_bytes(), queue_group.as_deref());

        // Propagate RS+ to gateway connections.
        #[cfg(feature = "gateway")]
        propagate_gateway_sub(wctx.state, subject_str.as_bytes(), queue_group.as_deref());

        gauge!(
            "subscriptions_active",
            "worker" => wctx.worker_label.to_string()
        )
        .increment(1.0);
        debug!(conn_id = conn.conn_id, sid, subject = %subject_str, "client subscribed");

        HandleResult::Ok
    }

    fn handle_unsub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        sid: u64,
        max: Option<u64>,
    ) -> HandleResult {
        if let Some(n) = max {
            // UNSUB with max: set delivery limit, auto-remove when reached.
            let subs = wctx.state.subs.read().unwrap();
            let found = subs.set_unsub_max(conn.conn_id, sid, n);
            let already_expired = found && subs.is_expired(conn.conn_id, sid);
            drop(subs);

            if already_expired {
                let mut subs = wctx.state.subs.write().unwrap();
                if let Some(removed) = subs.remove(conn.conn_id, sid) {
                    wctx.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Relaxed);
                    *conn.sub_count = conn.sub_count.saturating_sub(1);
                    #[cfg(feature = "leaf")]
                    {
                        let mut upstream = wctx.state.upstream.write().unwrap();
                        if let Some(ref mut up) = *upstream {
                            up.remove_interest(&removed.subject, removed.queue.as_deref());
                        }
                    }
                    gauge!(
                        "subscriptions_active",
                        "worker" => wctx.worker_label.to_string()
                    )
                    .decrement(1.0);
                    #[cfg(feature = "hub")]
                    propagate_leaf_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                    );
                    #[cfg(feature = "cluster")]
                    propagate_route_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                    );
                    #[cfg(feature = "gateway")]
                    propagate_gateway_unsub(
                        wctx.state,
                        removed.subject.as_bytes(),
                        removed.queue.as_deref().map(|q| q.as_bytes()),
                    );
                    debug!(
                        conn_id = conn.conn_id,
                        sid,
                        subject = %removed.subject,
                        "auto-unsubscribed (max already reached)"
                    );
                }
            }
        } else {
            // Immediate unsubscribe
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
                #[cfg(feature = "leaf")]
                {
                    let mut upstream = wctx.state.upstream.write().unwrap();
                    if let Some(ref mut up) = *upstream {
                        up.remove_interest(&removed.subject, removed.queue.as_deref());
                    }
                }
                gauge!(
                    "subscriptions_active",
                    "worker" => wctx.worker_label.to_string()
                )
                .decrement(1.0);
                #[cfg(feature = "hub")]
                propagate_leaf_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                );
                #[cfg(feature = "cluster")]
                propagate_route_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                );
                #[cfg(feature = "gateway")]
                propagate_gateway_unsub(
                    wctx.state,
                    removed.subject.as_bytes(),
                    removed.queue.as_deref().map(|q| q.as_bytes()),
                );
                debug!(
                    conn_id = conn.conn_id,
                    sid,
                    subject = %removed.subject,
                    "client unsubscribed"
                );
            }
        }

        HandleResult::Ok
    }

    fn handle_pub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut WorkerCtx<'_>,
        subject: Bytes,
        payload: Bytes,
        respond: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
    ) -> (HandleResult, Vec<(u64, u64)>) {
        // Check publish permissions
        if let Some(ref perms) = conn.permissions {
            let subj = bytes_to_str(&subject);
            if !perms.publish.is_allowed(subj) {
                counter!("messages_rejected_total", "reason" => "permissions").increment(1);
                conn.write_buf
                    .extend_from_slice(b"-ERR 'Permissions Violation for Publish'\r\n");
                return (HandleResult::Disconnect, Vec::new());
            }
        }

        let max_payload = wctx.state.max_payload.load(Ordering::Relaxed);
        if max_payload > 0 && payload.len() > max_payload {
            warn!(
                conn_id = conn.conn_id,
                payload_len = payload.len(),
                max_payload,
                "maximum payload violation"
            );
            counter!("messages_rejected_total", "reason" => "max_payload").increment(1);
            conn.write_buf
                .extend_from_slice(b"-ERR 'Maximum Payload Violation'\r\n");
            return (HandleResult::Disconnect, Vec::new());
        }

        let payload_len = payload.len() as u64;
        *wctx.msgs_received += 1;
        *wctx.msgs_received_bytes += payload_len;

        let subject_str = bytes_to_str(&subject);
        let (_delivered, expired) = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            respond.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            !conn.echo,
            #[cfg(feature = "cluster")]
            false, // don't skip routes — client pubs forward to route peers
            #[cfg(feature = "gateway")]
            false, // don't skip gateways — client pubs forward to gateway peers
        );

        // Forward to optimistic gateways when no gateway sub matched.
        #[cfg(feature = "gateway")]
        crate::handler::forward_to_optimistic_gateways(
            wctx,
            &subject,
            subject_str,
            respond.as_deref(),
            headers.as_ref(),
            &payload,
        );

        #[cfg(feature = "leaf")]
        forward_to_upstream(
            conn.upstream_tx,
            wctx.state,
            subject,
            respond,
            headers,
            payload,
        );

        (HandleResult::Ok, expired)
    }
}
