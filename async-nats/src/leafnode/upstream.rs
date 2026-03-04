// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use tracing::debug;

use crate::Client;

use super::server::ServerState;

/// Manages connection to an upstream NATS hub server.
/// Mirrors local subscriptions to the hub and routes hub messages
/// back to matching local clients.
pub(crate) struct Upstream {
    client: Client,
    /// subject → (JoinHandle for the forwarder task, refcount)
    hub_subs: HashMap<String, (tokio::task::JoinHandle<()>, u32)>,
    state: Arc<ServerState>,
}

impl Upstream {
    pub(crate) fn new(client: Client, state: Arc<ServerState>) -> Self {
        Self {
            client,
            hub_subs: HashMap::new(),
            state,
        }
    }

    /// Add a subscription interest for the given subject.
    /// If this is the first interest, subscribe on the hub.
    pub(crate) async fn add_interest(&mut self, subject: String) -> Result<(), crate::Error> {
        if let Some(entry) = self.hub_subs.get_mut(&subject) {
            entry.1 += 1;
            return Ok(());
        }

        let mut subscriber = self.client.subscribe(subject.clone()).await?;
        let state = Arc::clone(&self.state);
        let sub_subject = subject.clone();

        let handle = tokio::spawn(async move {
            while let Some(msg) = subscriber.next().await {
                let subs = state.subs.read().await;
                let matches = subs.match_subject(msg.subject.as_str());
                if matches.is_empty() {
                    continue;
                }

                let conns = state.conns.read().await;
                for sub in &matches {
                    if let Some(handle) = conns.get(&sub.conn_id) {
                        let _ = handle.msg_tx.send(ClientMsg {
                            subject: msg.subject.to_string(),
                            sid: sub.sid,
                            reply: msg.reply.as_ref().map(|r| r.to_string()),
                            headers: msg.headers.clone(),
                            payload: msg.payload.clone(),
                        }).await;
                    }
                }
            }
            debug!(subject = %sub_subject, "hub subscriber ended");
        });

        self.hub_subs.insert(subject.to_string(), (handle, 1));
        Ok(())
    }

    /// Remove a subscription interest. If refcount reaches zero,
    /// drop the hub subscriber (which auto-unsubscribes).
    pub(crate) fn remove_interest(&mut self, subject: &str) {
        if let Some(entry) = self.hub_subs.get_mut(subject) {
            entry.1 -= 1;
            if entry.1 == 0 {
                let (handle, _) = self.hub_subs.remove(subject).unwrap();
                handle.abort();
            }
        }
    }

    /// Forward a publish from a local client to the hub.
    pub(crate) async fn publish(
        &self,
        subject: String,
        reply: Option<String>,
        headers: Option<crate::HeaderMap>,
        payload: Bytes,
    ) -> Result<(), crate::Error> {
        match (reply, headers) {
            (Some(reply), Some(headers)) if !headers.is_empty() => {
                self.client
                    .publish_with_reply_and_headers(subject, reply, headers, payload)
                    .await?;
            }
            (Some(reply), _) => {
                self.client
                    .publish_with_reply(subject, reply, payload)
                    .await?;
            }
            (None, Some(headers)) if !headers.is_empty() => {
                self.client
                    .publish_with_headers(subject, headers, payload)
                    .await?;
            }
            _ => {
                self.client.publish(subject, payload).await?;
            }
        }
        Ok(())
    }

    /// Re-subscribe for all currently tracked subjects.
    /// Called after reconnection.
    #[allow(dead_code)]
    pub(crate) async fn resync_interests(&mut self) -> Result<(), crate::Error> {
        let subjects: Vec<String> = {
            let subs = self.state.subs.read().await;
            subs.unique_subjects().into_iter().map(String::from).collect()
        };

        // Abort old forwarding tasks
        for (_, (handle, _)) in self.hub_subs.drain() {
            handle.abort();
        }

        for subject in subjects {
            // Force refcount to 0 so add_interest creates fresh subscription
            self.add_interest(subject).await?;
        }

        Ok(())
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        for (_, (handle, _)) in self.hub_subs.drain() {
            handle.abort();
        }
    }
}

/// A message to be delivered to a local client connection.
#[derive(Debug)]
pub(crate) struct ClientMsg {
    pub subject: String,
    pub sid: u64,
    pub reply: Option<String>,
    pub headers: Option<crate::HeaderMap>,
    pub payload: Bytes,
}
