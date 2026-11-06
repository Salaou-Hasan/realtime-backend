// Task 13: End-to-end integration (spec §Testing).
//
// Full stack: TCP PUT -> Core::apply -> HTTP GET -> WS PUB/SUB,
// plus a kill-9 replay simulated by re-applying records decoded
// from raw WAL bytes into a fresh `Core`.

use realtime_backend::adapters::http::parse_http;
use realtime_backend::adapters::tcp::{decode_frame, frame_put};
use realtime_backend::adapters::ws::decode_ws_json;
use realtime_backend::core::Core;
use realtime_backend::op::Op;
use realtime_backend::wal::{decode_all, encode_record, WAL_FLUSH_INTERVAL};
use std::time::Duration;

fn integration_put_get_pubsub() -> String {
    let mut core = Core::new_in_memory();

    // --- TCP PUT via tcp::frame_put -> Core::apply ---
    let frame = frame_put("k", b"hello");
    let op = decode_frame(&frame).expect("decode tcp frame");
    assert!(matches!(&op, Op::Put { key, .. } if key == "k"));
    let ack = core.apply(op).expect("apply tcp put");
    assert_eq!(ack, Some(b"OK".to_vec()));

    // --- HTTP GET via parse_http -> Core::apply ---
    let http_op = parse_http("GET /kv/k HTTP/1.1\r\nHost: x\r\n\r\n").expect("parse http get");
    assert!(matches!(&http_op, Op::Get { key } if key == "k"));
    let reply = core.apply(http_op).expect("apply http get");
    assert_eq!(reply, Some(b"hello".to_vec()));

    // --- WS SUB + PUB via decode_ws_json -> Core::apply ---
    let sub_op = decode_ws_json(r#"{"op":"sub","topic":"chat"}"#).expect("ws sub");
    assert!(matches!(&sub_op, Op::Sub { topic } if topic == "chat"));
    core.apply(sub_op).expect("apply ws sub");

    // "hello" base64 = aGVsbG8=
    let pub_op =
        decode_ws_json(r#"{"op":"pub","topic":"chat","v":"aGVsbG8=","sync":false}"#)
            .expect("ws pub");
    assert!(matches!(
        &pub_op,
        Op::Pub { topic, value, .. } if topic == "chat" && value == b"hello"
    ));
    core.apply(pub_op).expect("apply ws pub");

    let topic = core.topic("chat").expect("chat topic");
    assert_eq!(topic.recent_len(), 1);
    assert_eq!(topic.recent()[0], b"hello".to_vec());

    "OK".to_string()
}

#[test]
fn integration_tcp_put_http_get_ws_pubsub() {
    let r1 = integration_put_get_pubsub();
    assert_eq!(r1, "OK");
}

#[test]
fn kill9_replay_from_wal_bytes() {
    let mut core = Core::new_in_memory();

    // TCP PUT path.
    let frame = frame_put("k", b"hello");
    let op = decode_frame(&frame).expect("decode tcp frame");
    core.apply(op).expect("apply put");

    // WS PUB path (topic survives via WAL record).
    let pub_op =
        decode_ws_json(r#"{"op":"pub","topic":"chat","v":"aGVsbG8=","sync":false}"#)
            .expect("ws pub");
    core.apply(pub_op).expect("apply pub");

    // Persist group-commit buffer, then replay into fresh bytes.
    // This simulates kill -9 + restart: the old Core memory is
    // dropped and state is rebuilt solely from WAL bytes.
    core.wal.flush().expect("flush wal");
    let replayed = core.wal.replay().expect("replay");
    assert_eq!(replayed.len(), 2);

    // Re-encode every replayed record into raw WAL bytes, then
    // decode that byte buffer (the kill-9 artifact).
    let mut wal_bytes = Vec::new();
    for rec in &replayed {
        wal_bytes.extend_from_slice(&encode_record(rec).expect("encode record"));
    }
    assert!(!wal_bytes.is_empty());
    let from_bytes = decode_all(&wal_bytes).expect("decode wal bytes");
    assert_eq!(from_bytes.len(), 2);

    // Rebuild a fresh Core from the byte-level replay.
    drop(core);
    let mut core2 = Core::new_in_memory();
    for rec in from_bytes {
        core2.apply(rec.op).expect("re-apply replayed op");
    }

    // HTTP GET against the rebuilt core sees the pre-crash PUT.
    let http_op = parse_http("GET /kv/k HTTP/1.1\r\nHost: x\r\n\r\n").expect("parse http get");
    let reply = core2.apply(http_op).expect("apply http get");
    assert_eq!(reply, Some(b"hello".to_vec()));

    // The published topic message survives replay.
    let topic = core2.topic("chat").expect("chat topic");
    assert_eq!(topic.recent_len(), 1);
    assert_eq!(topic.recent()[0], b"hello".to_vec());
}

#[test]
fn group_commit_window_exists() {
    // Spec §Durability / §WAL: async ack may lose at most the 0.5ms
    // group-commit window. The window must exist and be <= 0.5ms.
    assert_eq!(WAL_FLUSH_INTERVAL, Duration::from_micros(500));
    assert!(WAL_FLUSH_INTERVAL <= Duration::from_micros(500));
}
