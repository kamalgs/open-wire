//! Client protocol handler: PUB/HPUB, SUB, UNSUB, PING, PONG.
//!
//! Dispatched by the worker for connections with `ConnExt::Client`.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use metrics::{counter, gauge};
use tracing::{debug, warn};

use crate::handler::propagation::propagate_all_interest;
use crate::handler::{
    bytes_to_str, deliver_to_subs, ConnCtx, ConnectionHandler, DeliveryScope, HandleResult,
    MessageDeliveryHub, Msg,
};
use crate::nats_proto::{self, ClientOp};
use crate::sub_list::{SubKind, Subscription};

/// Handles client protocol operations (PUB, SUB, UNSUB, PING, PONG).
pub(crate) struct ClientHandler;

impl ConnectionHandler for ClientHandler {
    type Op = ClientOp;

    fn parse_op(buf: &mut bytes::BytesMut) -> std::io::Result<Option<ClientOp>> {
        nats_proto::try_parse_client_op(buf)
    }

    fn handle_op(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
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
            ClientOp::Connect(_) => (HandleResult::Ok, Vec::new()),
        }
    }
}

impl ClientHandler {
    fn handle_sub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        sid: u64,
        subject: Bytes,
        queue_group: Option<Bytes>,
    ) -> HandleResult {
        if conn.draining {
            return HandleResult::Ok;
        }

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

        // Clone for upstream propagation before moving into Subscription
        let upstream_queue = queue_str.clone();

        let sub = Subscription::new(
            conn.conn_id,
            sid,
            subject_str.to_string(),
            queue_str,
            conn.direct_writer.clone(),
            SubKind::Client,
            #[cfg(feature = "accounts")]
            conn.account_id,
        );

        {
            let mut subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .write()
                .expect("subs write lock");
            subs.insert(sub);
            wctx.state.has_subs.store(true, Ordering::Relaxed);
        }

        #[cfg(feature = "worker-affinity")]
        wctx.state
            .affinity
            .record_sub(subject_str, wctx.worker_index);

        {
            let mut upstreams = wctx
                .state
                .leaf
                .upstreams
                .write()
                .expect("upstreams write lock");
            for up in upstreams.iter_mut() {
                if let Err(e) = up.add_interest(subject_str.to_string(), upstream_queue.clone()) {
                    warn!(error = %e, "failed to add upstream interest");
                }
            }
        }

        *conn.sub_count += 1;

        propagate_all_interest(
            wctx.state,
            subject_str.as_bytes(),
            queue_group.as_deref(),
            true,
            #[cfg(feature = "accounts")]
            wctx.state.account_name(conn.account_id).as_bytes(),
        );

        // Reverse interest: if this subscription matches an import's local pattern,
        // propagate interest in the source account's namespace so remote peers
        // forward messages to us.
        #[cfg(feature = "accounts")]
        {
            if let Some(reverses) = wctx.state.reverse_imports.get(conn.account_id as usize) {
                for ri in reverses {
                    if crate::sub_list::subject_matches(&ri.local_pattern, subject_str) {
                        let src_acct_name = wctx.state.account_name(ri.src_account_id).as_bytes();
                        {
                            let mut upstreams = wctx
                                .state
                                .leaf
                                .upstreams
                                .write()
                                .expect("upstreams write lock");
                            for up in upstreams.iter_mut() {
                                let _ = up.add_interest(ri.src_pattern.clone(), None);
                            }
                        }
                        propagate_all_interest(
                            wctx.state,
                            ri.src_pattern.as_bytes(),
                            None,
                            true,
                            src_acct_name,
                        );
                    }
                }
            }
        }

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
        wctx: &mut MessageDeliveryHub<'_>,
        sid: u64,
        max: Option<u64>,
    ) -> HandleResult {
        if let Some(n) = max {
            let subs = wctx
                .state
                .get_subs(
                    #[cfg(feature = "accounts")]
                    conn.account_id,
                )
                .read()
                .expect("subs read lock");
            let found = subs.set_unsub_max(conn.conn_id, sid, n);
            let already_expired = found && subs.is_expired(conn.conn_id, sid);
            drop(subs);

            if already_expired {
                let mut subs = wctx
                    .state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        conn.account_id,
                    )
                    .write()
                    .expect("subs write lock");
                if let Some(removed) = subs.remove(conn.conn_id, sid) {
                    wctx.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Relaxed);
                    cleanup_removed_sub(
                        conn,
                        wctx,
                        sid,
                        &removed,
                        "auto-unsubscribed (max reached)",
                    );
                }
            }
        } else {
            let removed = {
                let mut subs = wctx
                    .state
                    .get_subs(
                        #[cfg(feature = "accounts")]
                        conn.account_id,
                    )
                    .write()
                    .expect("subs write lock");
                let r = subs.remove(conn.conn_id, sid);
                wctx.state
                    .has_subs
                    .store(!subs.is_empty(), Ordering::Relaxed);
                r
            };

            if let Some(ref removed) = removed {
                cleanup_removed_sub(conn, wctx, sid, removed, "client unsubscribed");
            }
        }

        HandleResult::Ok
    }

    fn handle_pub(
        conn: &mut ConnCtx<'_>,
        wctx: &mut MessageDeliveryHub<'_>,
        subject: Bytes,
        payload: Bytes,
        respond: Option<Bytes>,
        headers: Option<crate::types::HeaderMap>,
    ) -> (HandleResult, Vec<(u64, u64)>) {
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

        // No-responders: if the client opted in (no_responders && headers) and
        // this PUB has a reply-to subject, check whether any subscriber exists.
        // If not, send a 503 status HMSG back on the reply subject.
        // Skip this check when connected to an upstream hub — let the hub handle it.
        if let Some(ref reply_bytes) = respond {
            if conn.no_responders {
                let has_upstream = { !conn.upstream_txs.is_empty() };
                if !has_upstream {
                    let subs = wctx
                        .state
                        .get_subs(
                            #[cfg(feature = "accounts")]
                            conn.account_id,
                        )
                        .read()
                        .expect("subs read lock");
                    let has_sub = subs.has_any_subscriber(subject_str);
                    drop(subs);

                    let has_gw = wctx.state.gateway.has_interest.load(Ordering::Relaxed);

                    if !has_sub && !has_gw {
                        let mut hdr = crate::types::HeaderMap::new();
                        hdr.set_status(503, None);
                        let no_resp_msg =
                            Msg::new(reply_bytes.clone(), None, Some(&hdr), Bytes::new());
                        deliver_to_subs(
                            wctx,
                            &no_resp_msg,
                            conn.conn_id,
                            &DeliveryScope::local(false),
                            #[cfg(feature = "accounts")]
                            conn.account_id,
                        );
                        return (HandleResult::Ok, Vec::new());
                    }
                }
            }
        }

        let msg = Msg::new(
            subject.clone(),
            respond.clone(),
            headers.as_ref(),
            payload.clone(),
        );
        let (_delivered, expired) = wctx.publish(
            &msg,
            conn.conn_id,
            &DeliveryScope::local(!conn.echo),
            #[cfg(feature = "accounts")]
            conn.account_id,
        );

        conn.forward_to_upstream(wctx.state, subject, respond, headers, payload);

        (HandleResult::Ok, expired)
    }
}

/// Cleanup after removing a subscription: update counters, upstream interest,
/// metrics, propagation, and reverse imports.
fn cleanup_removed_sub(
    conn: &mut ConnCtx<'_>,
    wctx: &mut MessageDeliveryHub<'_>,
    sid: u64,
    removed: &Subscription,
    reason: &str,
) {
    *conn.sub_count = conn.sub_count.saturating_sub(1);
    #[cfg(feature = "worker-affinity")]
    wctx.state
        .affinity
        .record_unsub(&removed.subject, wctx.worker_index);
    {
        let mut upstreams = wctx
            .state
            .leaf
            .upstreams
            .write()
            .expect("upstreams write lock");
        for up in upstreams.iter_mut() {
            up.remove_interest(&removed.subject, removed.queue.as_deref());
        }
    }
    gauge!(
        "subscriptions_active",
        "worker" => wctx.worker_label.to_string()
    )
    .decrement(1.0);
    propagate_all_interest(
        wctx.state,
        removed.subject.as_bytes(),
        removed.queue.as_deref().map(|q| q.as_bytes()),
        false,
        #[cfg(feature = "accounts")]
        wctx.state.account_name(conn.account_id).as_bytes(),
    );
    #[cfg(feature = "accounts")]
    propagate_reverse_unsub(wctx, conn.account_id, &removed.subject);
    debug!(conn_id = conn.conn_id, sid, subject = %removed.subject, reason);
}

/// Propagate reverse interest removal for cross-account imports.
///
/// When a client unsubscribes from a subject that matches an import's local pattern,
/// remove the corresponding interest in the source account's namespace from
/// upstream/leaf/route/gateway peers.
#[cfg(feature = "accounts")]
fn propagate_reverse_unsub(
    wctx: &mut MessageDeliveryHub<'_>,
    account_id: crate::core::server::AccountId,
    subject: &str,
) {
    if let Some(reverses) = wctx.state.reverse_imports.get(account_id as usize) {
        for ri in reverses {
            if crate::sub_list::subject_matches(&ri.local_pattern, subject) {
                let src_acct_name = wctx.state.account_name(ri.src_account_id).as_bytes();
                {
                    let mut upstreams = wctx
                        .state
                        .leaf
                        .upstreams
                        .write()
                        .expect("upstreams write lock");
                    for up in upstreams.iter_mut() {
                        up.remove_interest(&ri.src_pattern, None);
                    }
                }
                propagate_all_interest(
                    wctx.state,
                    ri.src_pattern.as_bytes(),
                    None,
                    false,
                    src_acct_name,
                );
            }
        }
    }
}
