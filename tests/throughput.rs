use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use open_wire::sub_list::{subject_matches, SubList};

fn bench_subject_matches(c: &mut Criterion) {
    let mut group = c.benchmark_group("subject_matches");

    group.bench_function("exact", |b| {
        b.iter(|| subject_matches("foo.bar.baz", "foo.bar.baz"));
    });

    group.bench_function("star_wildcard", |b| {
        b.iter(|| subject_matches("foo.*.baz", "foo.bar.baz"));
    });

    group.bench_function("gt_wildcard", |b| {
        b.iter(|| subject_matches("foo.>", "foo.bar.baz.qux"));
    });

    group.bench_function("no_match", |b| {
        b.iter(|| subject_matches("foo.bar.baz", "foo.bar.qux"));
    });

    group.finish();
}

fn bench_sublist_match(c: &mut Criterion) {
    use open_wire::sub_list::Subscription;

    let mut group = c.benchmark_group("sublist_match");

    for count in [10, 100, 1000] {
        let mut sl = SubList::new();
        for i in 0..count {
            sl.insert(Subscription::new_dummy(
                i,
                1,
                format!("test.subject.{i}"),
                None,
            ));
        }
        // Add a wildcard that matches
        sl.insert(Subscription::new_dummy(
            9999,
            1,
            "test.subject.*".to_string(),
            None,
        ));

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{count}_subs"), |b| {
            b.iter(|| sl.match_subject("test.subject.42"));
        });
    }

    group.finish();
}

fn bench_publish_local(c: &mut Criterion) {
    use open_wire::sub_list::Subscription;
    use std::sync::{Arc, RwLock};

    let mut group = c.benchmark_group("publish_local");
    group.throughput(Throughput::Elements(1));

    // Simulate the publish hot path: lock subs, match
    let subs = Arc::new(RwLock::new({
        let mut sl = SubList::new();
        sl.insert(Subscription::new_dummy(1, 1, "test.>".to_string(), None));
        sl
    }));

    group.bench_function("lock_and_match", |b| {
        let subs = subs.clone();
        b.iter(|| {
            let subs = subs.read().unwrap();
            let _matches = subs.match_subject("test.subject.foo");
        });
    });

    group.finish();
}

/// Benchmark wildcard subscription scaling: measures for_each_match with
/// increasing numbers of wildcard subscriptions. This exposes the O(N) linear
/// scan bottleneck in the current Vec-based wildcard storage.
fn bench_wildcard_scaling(c: &mut Criterion) {
    use open_wire::sub_list::Subscription;

    let mut group = c.benchmark_group("wildcard_scaling");
    group.throughput(Throughput::Elements(1));

    // Many wildcard subs across different namespaces.
    // Only 1 matches "app.orders.created" — the rest are in other namespaces.
    // This stresses the linear scan: every non-matching wildcard must be checked.
    for count in [10, 100, 1000, 5000] {
        let mut sl = SubList::new();
        // Add `count` wildcard subs that DON'T match our publish subject
        for i in 0..count {
            sl.insert(Subscription::new_dummy(i, 1, format!("svc{}.>", i), None));
        }
        // Add 1 wildcard sub that DOES match
        sl.insert(Subscription::new_dummy(
            count + 1,
            1,
            "app.>".to_string(),
            None,
        ));

        group.bench_function(format!("{count}_wild_subs"), |b| {
            b.iter(|| {
                let mut matched = 0usize;
                sl.for_each_match("app.orders.created", |_sub| {
                    matched += 1;
                });
                assert_eq!(matched, 1);
            });
        });
    }

    group.finish();
}

/// Benchmark for_each_match with many wildcard subs that ALL match.
/// This measures the cost of the matching itself plus delivery fan-out.
fn bench_wildcard_fanout(c: &mut Criterion) {
    use open_wire::sub_list::Subscription;

    let mut group = c.benchmark_group("wildcard_fanout");
    group.throughput(Throughput::Elements(1));

    for count in [10, 100, 1000] {
        let mut sl = SubList::new();
        // All wildcards match "app.events.x"
        for i in 0..count {
            sl.insert(Subscription::new_dummy(
                i,
                1,
                "app.events.*".to_string(),
                None,
            ));
        }
        // Also add some exact subs
        for i in 0..10 {
            sl.insert(Subscription::new_dummy(
                count + i,
                1,
                "app.events.x".to_string(),
                None,
            ));
        }

        group.bench_function(format!("{count}_matching_wild"), |b| {
            b.iter(|| {
                let mut matched = 0usize;
                sl.for_each_match("app.events.x", |_sub| {
                    matched += 1;
                });
            });
        });
    }

    group.finish();
}

/// Benchmark mixed exact + wildcard with realistic NATS subject hierarchy.
/// Simulates a microservices deployment: many services with exact subs,
/// plus monitoring/logging wildcards that catch everything.
fn bench_mixed_realistic(c: &mut Criterion) {
    use open_wire::sub_list::Subscription;

    let mut group = c.benchmark_group("mixed_realistic");
    group.throughput(Throughput::Elements(1));

    let services = ["orders", "users", "payments", "inventory", "shipping"];
    let actions = ["created", "updated", "deleted", "queried"];

    let mut sl = SubList::new();
    let mut sid = 0u64;

    // 100 exact subscriptions (service-specific)
    for svc in &services {
        for action in &actions {
            for i in 0..5 {
                sl.insert(Subscription::new_dummy(
                    sid,
                    1,
                    format!("svc.{svc}.{action}"),
                    None,
                ));
                sid += 1;
                let _ = i;
            }
        }
    }

    // Wildcard monitoring subs
    let wild_patterns = [
        "svc.>",                // catch-all
        "svc.orders.>",         // order events
        "svc.*.created",        // all creation events
        "svc.*.deleted",        // all deletion events
        "audit.>",              // audit trail (no match)
        "metrics.>",            // metrics (no match)
        "logs.>",               // logs (no match)
        "internal.*.heartbeat", // heartbeats (no match)
    ];
    for pat in &wild_patterns {
        for i in 0..10 {
            sl.insert(Subscription::new_dummy(sid, 1, pat.to_string(), None));
            sid += 1;
            let _ = i;
        }
    }
    // Total: 100 exact + 80 wildcard

    group.bench_function("publish_to_orders", |b| {
        b.iter(|| {
            let mut matched = 0usize;
            sl.for_each_match("svc.orders.created", |_sub| {
                matched += 1;
            });
        });
    });

    group.bench_function("publish_no_match", |b| {
        b.iter(|| {
            let mut matched = 0usize;
            sl.for_each_match("unknown.topic.here", |_sub| {
                matched += 1;
            });
            assert_eq!(matched, 0);
        });
    });

    group.finish();
}

// ── Protocol parsing benchmarks ─────────────────────────────────────────────

mod proto_bench {
    use bytes::BytesMut;
    use criterion::{Criterion, Throughput};
    use open_wire::nats_proto;

    /// Bench parsing a single PUB + payload from a BytesMut buffer.
    pub fn bench_parse_pub(c: &mut Criterion) {
        let mut group = c.benchmark_group("parse");

        // PUB with 128-byte payload (the default bench size)
        let payload = vec![0u8; 128];
        let mut raw = Vec::new();
        raw.extend_from_slice(b"PUB test.subject 128\r\n");
        raw.extend_from_slice(&payload);
        raw.extend_from_slice(b"\r\n");
        let template = raw.clone();

        group.throughput(Throughput::Bytes(template.len() as u64));
        group.bench_function("pub_128b", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(&template[..]);
                let _ = nats_proto::try_parse_client_op(&mut buf).unwrap();
            });
        });

        // PUB with reply-to
        let mut raw_reply = Vec::new();
        raw_reply.extend_from_slice(b"PUB test.subject _INBOX.abc123 128\r\n");
        raw_reply.extend_from_slice(&payload);
        raw_reply.extend_from_slice(b"\r\n");
        let template_reply = raw_reply.clone();

        group.throughput(Throughput::Bytes(template_reply.len() as u64));
        group.bench_function("pub_128b_reply", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(&template_reply[..]);
                let _ = nats_proto::try_parse_client_op(&mut buf).unwrap();
            });
        });

        // PING (minimal op)
        group.throughput(Throughput::Bytes(6));
        group.bench_function("ping", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from("PING\r\n");
                let _ = nats_proto::try_parse_client_op(&mut buf).unwrap();
            });
        });

        // SUB
        let sub_raw = b"SUB test.subject.foo 1\r\n";
        group.throughput(Throughput::Bytes(sub_raw.len() as u64));
        group.bench_function("sub", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(&sub_raw[..]);
                let _ = nats_proto::try_parse_client_op(&mut buf).unwrap();
            });
        });

        // Batch: 10 PUBs in one buffer
        let mut batch_raw = Vec::new();
        for _ in 0..10 {
            batch_raw.extend_from_slice(b"PUB test.subject 128\r\n");
            batch_raw.extend_from_slice(&payload);
            batch_raw.extend_from_slice(b"\r\n");
        }
        let batch_template = batch_raw.clone();
        group.throughput(Throughput::Elements(10));
        group.bench_function("pub_128b_batch10", |b| {
            b.iter(|| {
                let mut buf = BytesMut::from(&batch_template[..]);
                while nats_proto::try_parse_client_op(&mut buf).unwrap().is_some() {}
            });
        });

        group.finish();
    }

    /// Bench building outgoing MSG lines with MsgBuilder.
    pub fn bench_build_msg(c: &mut Criterion) {
        let mut group = c.benchmark_group("build_msg");

        let subject = b"test.subject";
        let sid = nats_proto::sid_to_bytes(1);
        let payload = vec![0u8; 128];

        group.throughput(Throughput::Elements(1));

        group.bench_function("msg_128b", |b| {
            let mut builder = nats_proto::MsgBuilder::new();
            b.iter(|| {
                let _ = builder.build_msg(subject, &sid, None, None, &payload);
            });
        });

        group.bench_function("msg_128b_reply", |b| {
            let mut builder = nats_proto::MsgBuilder::new();
            let reply = b"_INBOX.abc123";
            b.iter(|| {
                let _ = builder.build_msg(subject, &sid, Some(reply.as_slice()), None, &payload);
            });
        });

        #[cfg(any(feature = "leaf", feature = "hub"))]
        group.bench_function("lmsg_128b", |b| {
            let mut builder = nats_proto::MsgBuilder::new();
            b.iter(|| {
                let _ = builder.build_lmsg(subject, None, None, &payload);
            });
        });

        group.finish();
    }

    /// Bench LMSG parsing (hub → leaf path).
    pub fn bench_parse_lmsg(_c: &mut Criterion) {
        #[cfg(any(feature = "leaf", feature = "hub"))]
        {
            let mut group = _c.benchmark_group("parse_leaf");

            let payload = vec![0u8; 128];
            let mut raw = Vec::new();
            raw.extend_from_slice(b"LMSG test.subject 128\r\n");
            raw.extend_from_slice(&payload);
            raw.extend_from_slice(b"\r\n");
            let template = raw.clone();

            group.throughput(Throughput::Bytes(template.len() as u64));
            group.bench_function("lmsg_128b", |b| {
                b.iter(|| {
                    let mut buf = BytesMut::from(&template[..]);
                    let _ = nats_proto::try_parse_leaf_op(&mut buf).unwrap();
                });
            });

            // LMSG with reply
            let mut raw_reply = Vec::new();
            raw_reply.extend_from_slice(b"LMSG test.subject reply.to 128\r\n");
            raw_reply.extend_from_slice(&payload);
            raw_reply.extend_from_slice(b"\r\n");
            let template_reply = raw_reply.clone();

            group.throughput(Throughput::Bytes(template_reply.len() as u64));
            group.bench_function("lmsg_128b_reply", |b| {
                b.iter(|| {
                    let mut buf = BytesMut::from(&template_reply[..]);
                    let _ = nats_proto::try_parse_leaf_op(&mut buf).unwrap();
                });
            });

            group.finish();
        }
    }
}

criterion_group!(
    benches,
    bench_subject_matches,
    bench_sublist_match,
    bench_publish_local,
    bench_wildcard_scaling,
    bench_wildcard_fanout,
    bench_mixed_realistic,
    proto_bench::bench_parse_pub,
    proto_bench::bench_build_msg,
    proto_bench::bench_parse_lmsg,
);
criterion_main!(benches);
