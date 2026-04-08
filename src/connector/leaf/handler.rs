//! Leaf node protocol handler: LS+, LS-, LMSG, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Leaf`.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use metrics::gauge;
use tracing::debug;

use tracing::warn;

use crate::buf::LeafOp;
use crate::handler::propagation::propagate_route_gateway_interest;
use crate::handler::{
    bytes_to_str, ConnCtx, ConnExt, ConnectionHandler, DeliveryScope, HandleResult,
    MessageDeliveryHub, Msg,
};
use crate::nats_proto;
use crate::sub_list::Subscription;

/// Handles leaf node protocol operations (LS+, LS-, LMSG, PING, PONG).
pub(crate) struct LeafHandler;

impl ConnectionHandler for LeafHandler {
    type Op = LeafOp;

    fn parse_op(buf: &mut bytes::BytesMut) -> std::io::Result<Option<LeafOp>> {
        nats_proto::try_parse_leaf_op(buf)
    }

    fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
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
            LeafOp::Info(_) | LeafOp::Ok | LeafOp::Err(_) => (HandleResult::Ok, Vec::new()),
        }
    }
}

impl LeafHandler {
    fn handle_leaf_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
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

        let leaf_perms_arc = conn
            .permissions
            .as_ref()
            .map(|p| std::sync::Arc::new(p.clone()));

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

            ConnExt::Route { .. } => unreachable!("leaf op on route connection"),

            ConnExt::Gateway { .. } => unreachable!("leaf op on gateway connection"),

            ConnExt::BinaryClient => unreachable!("leaf op on binary client connection"),
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

            is_route: false,

            is_gateway: false,

            is_binary_client: false,
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

        {
            let mut upstreams = wctx.state.upstreams.write().unwrap();
            for up in upstreams.iter_mut() {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue.clone()) {
                    warn!(error = %e, "failed to add upstream interest for leaf sub");
                }
            }
        }

        *conn.sub_count += 1;

        propagate_route_gateway_interest(
            wctx.state,
            subject.as_ref(),
            queue.as_deref(),
            true,
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
        wctx: &mut MessageDeliveryHub<'_>,
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

            ConnExt::Route { .. } => unreachable!("leaf op on route connection"),

            ConnExt::Gateway { .. } => unreachable!("leaf op on gateway connection"),

            ConnExt::BinaryClient => unreachable!("leaf op on binary client connection"),
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

            {
                let mut upstreams = wctx.state.upstreams.write().unwrap();
                for up in upstreams.iter_mut() {
                    up.remove_interest(&removed.subject, removed.queue.as_deref());
                }
            }
            propagate_route_gateway_interest(
                wctx.state,
                removed.subject.as_bytes(),
                removed.queue.as_deref().map(|q| q.as_bytes()),
                false,
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
        wctx: &mut MessageDeliveryHub<'_>,
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

        let msg = Msg::new(
            subject.clone(),
            reply.clone(),
            headers.as_ref(),
            payload.clone(),
        );
        let (_delivered, expired) = wctx.publish(
            &msg,
            conn.conn_id,
            &DeliveryScope::local(true),
            #[cfg(feature = "accounts")]
            conn.account_id,
        );

        conn.forward_to_upstream(wctx.state, subject, reply, headers, payload);

        (HandleResult::Ok, expired)
    }
}
