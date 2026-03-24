//! Leaf node protocol handler: LS+, LS-, LMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Leaf`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;
#[cfg(feature = "leaf")]
use tracing::warn;

#[cfg(feature = "accounts")]
use crate::handler::deliver_cross_account;
#[cfg(feature = "leaf")]
use crate::handler::forward_to_upstream;
use crate::handler::{bytes_to_str, deliver_to_subs, ConnCtx, ConnExt, HandleResult, WorkerCtx};
#[cfg(feature = "gateway")]
use crate::handler::{propagate_gateway_sub, propagate_gateway_unsub};
#[cfg(feature = "cluster")]
use crate::handler::{propagate_route_sub, propagate_route_unsub};
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

        // Check subscribe permissions: ignore LS+ if the leaf is not allowed
        // to subscribe on this subject (controls what the leaf can import).
        if let Some(ref perms) = conn.permissions {
            if !perms.subscribe.is_allowed(subject_str) {
                debug!(
                    conn_id = conn.conn_id,
                    subject = %subject_str,
                    "leaf sub denied by subscribe permissions"
                );
                return HandleResult::Ok;
            }
        }

        let queue_str = queue.as_ref().map(|q| bytes_to_str(q).to_string());

        // Capture permissions for later delivery-path filtering.
        let leaf_perms_arc = conn
            .permissions
            .as_ref()
            .map(|p| std::sync::Arc::new(p.clone()));

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
            #[cfg(feature = "cluster")]
            ConnExt::Route { .. } => unreachable!("leaf op on route connection"),
            #[cfg(feature = "gateway")]
            ConnExt::Gateway { .. } => unreachable!("leaf op on gateway connection"),
        };

        #[cfg(feature = "leaf")]
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
            #[cfg(feature = "cluster")]
            is_route: false,
            #[cfg(feature = "gateway")]
            is_gateway: false,
            #[cfg(feature = "accounts")]
            account_id: 0,
            leaf_perms: leaf_perms_arc,
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

        #[cfg(feature = "leaf")]
        {
            let mut upstreams = wctx.state.upstreams.write().unwrap();
            for up in upstreams.iter_mut() {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue.clone()) {
                    warn!(error = %e, "failed to add upstream interest for leaf sub");
                }
            }
        }

        *conn.sub_count += 1;

        // Propagate RS+ to route peers.
        #[cfg(feature = "cluster")]
        propagate_route_sub(
            wctx.state,
            subject.as_ref(),
            queue.as_deref(),
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

        // Propagate RS+ to gateway peers.
        #[cfg(feature = "gateway")]
        propagate_gateway_sub(
            wctx.state,
            subject.as_ref(),
            queue.as_deref(),
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

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
            #[cfg(feature = "cluster")]
            ConnExt::Route { .. } => unreachable!("leaf op on route connection"),
            #[cfg(feature = "gateway")]
            ConnExt::Gateway { .. } => unreachable!("leaf op on gateway connection"),
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
            #[cfg(feature = "leaf")]
            {
                let mut upstreams = wctx.state.upstreams.write().unwrap();
                for up in upstreams.iter_mut() {
                    up.remove_interest(&removed.subject, removed.queue.as_deref());
                }
            }
            #[cfg(feature = "cluster")]
            propagate_route_unsub(
                wctx.state,
                removed.subject.as_bytes(),
                removed.queue.as_deref().map(|q| q.as_bytes()),
                #[cfg(feature = "accounts")]
                wctx.state.account_name(conn.account_id).as_bytes(),
            );
            #[cfg(feature = "gateway")]
            propagate_gateway_unsub(
                wctx.state,
                removed.subject.as_bytes(),
                removed.queue.as_deref().map(|q| q.as_bytes()),
                #[cfg(feature = "accounts")]
                wctx.state.account_name(conn.account_id).as_bytes(),
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

        // Check publish permissions: drop LMSG if the leaf is not allowed
        // to publish on this subject.
        if let Some(ref perms) = conn.permissions {
            if !perms.publish.is_allowed(subject_str) {
                return (HandleResult::Ok, Vec::new());
            }
        }

        // Echo suppression: always skip delivery back to the originating leaf.
        let (_delivered, expired) = deliver_to_subs(
            wctx,
            &subject,
            subject_str,
            reply.as_deref(),
            headers.as_ref(),
            &payload,
            conn.conn_id,
            true, // always suppress echo for leaf connections
            #[cfg(feature = "cluster")]
            false, // don't skip routes — leaf msgs forward to route peers
            #[cfg(feature = "gateway")]
            false, // don't skip gateways — leaf msgs forward to gateway peers
            #[cfg(feature = "accounts")]
            conn.account_id,
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
            wctx.state.account_name(conn.account_id).as_bytes(),
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
                conn.account_id,
            );
            expired.extend(cross_expired);
            expired
        };

        #[cfg(feature = "leaf")]
        forward_to_upstream(
            conn.upstream_txs,
            wctx.state,
            subject,
            reply,
            headers,
            payload,
        );

        (HandleResult::Ok, expired)
    }
}
