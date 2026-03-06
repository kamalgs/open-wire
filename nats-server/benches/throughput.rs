// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use nats_server::sub_list::{subject_matches, SubList, Subscription};

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
    let mut group = c.benchmark_group("sublist_match");

    for count in [10, 100, 1000] {
        let mut sl = SubList::new();
        for i in 0..count {
            sl.insert(Subscription {
                conn_id: i,
                sid: 1,
                subject: format!("test.subject.{i}"),
                queue: None,
            });
        }
        // Add a wildcard that matches
        sl.insert(Subscription {
            conn_id: 9999,
            sid: 1,
            subject: "test.subject.*".to_string(),
            queue: None,
        });

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{count}_subs"), |b| {
            b.iter(|| sl.match_subject("test.subject.42"));
        });
    }

    group.finish();
}

fn bench_publish_local(c: &mut Criterion) {
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    use tokio::sync::RwLock;

    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("publish_local");
    group.throughput(Throughput::Elements(1));

    // Simulate the publish hot path: lock subs, match, lock conns, send
    let subs = Arc::new(RwLock::new({
        let mut sl = SubList::new();
        sl.insert(Subscription {
            conn_id: 1,
            sid: 1,
            subject: "test.>".to_string(),
            queue: None,
        });
        sl
    }));

    group.bench_function("lock_and_match", |b| {
        b.to_async(&rt).iter(|| {
            let subs = subs.clone();
            async move {
                let subs = subs.read().await;
                let _matches = subs.match_subject("test.subject.foo");
            }
        });
    });

    group.finish();
}

// ── Protocol parsing benchmarks ─────────────────────────────────────────────

mod proto_bench {
    use bytes::BytesMut;
    use criterion::{Criterion, Throughput};
    use nats_server::nats_proto;

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
                while nats_proto::try_parse_client_op(&mut buf)
                    .unwrap()
                    .is_some()
                {}
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

        group.bench_function("lmsg_128b", |b| {
            let mut builder = nats_proto::MsgBuilder::new();
            b.iter(|| {
                let _ = builder.build_lmsg(subject, None, None, &payload);
            });
        });

        group.finish();
    }

    /// Bench LMSG parsing (hub → leaf path).
    pub fn bench_parse_lmsg(c: &mut Criterion) {
        let mut group = c.benchmark_group("parse_leaf");

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

criterion_group!(
    benches,
    bench_subject_matches,
    bench_sublist_match,
    bench_publish_local,
    proto_bench::bench_parse_pub,
    proto_bench::bench_build_msg,
    proto_bench::bench_parse_lmsg,
);
criterion_main!(benches);
