//! Task 9: TCP binary adapter framing.
//!
//! Wire format (all integers little-endian, zero JSON):
//!
//! ```text
//! +-----------+----------+--------------------------------------+
//! | len: u32  | opcode:u8 | payload (len - 1 bytes)              |
//! +-----------+----------+--------------------------------------+
//! ```
//!
//! `len` counts everything after itself: the 1-byte opcode plus the
//! payload. Opcodes: `1=Put, 2=Get, 3=Pub, 4=Sub, 5=Unsub`.
//!
//! Payload layouts (key/topic is length-prefixed):
//! - Put:   `key_len:u32 + key + value` (value = rest of payload)
//! - Get:   `key_len:u32 + key`
//! - Pub:   `topic_len:u32 + topic + value` (value = rest of payload)
//! - Sub:   `topic_len:u32 + topic`
//! - Unsub: `topic_len:u32 + topic`
//!
//! `Put`/`Pub` decode with `sync = false` (the binary frame has no sync
//! flag; `frame_put`/`frame_pub` take no sync argument either).
//!
//! Errors are values: truncated/malformed frames return
//! [`Error::BadFrame`], frames exceeding [`MAX_KEY`] / [`MAX_VALUE`] /
//! [`MAX_FRAME_PAYLOAD`] return [`Error::OpTooLarge`]. No `unwrap` on the
//! decode hot path.

use crate::op::{Error, Op, MAX_KEY, MAX_VALUE};

/// Length prefix size (`len: u32 LE`).
pub const LEN_PREFIX: usize = 4;
/// Opcode size (1 byte).
pub const OPCODE_LEN: usize = 1;
/// Smallest valid frame: len prefix + opcode.
pub const MIN_FRAME: usize = LEN_PREFIX + OPCODE_LEN;

/// Maximum payload length (`len` field value): opcode + key-len +
/// max key/topic + max value. Any frame claiming more is oversized.
pub const MAX_FRAME_PAYLOAD: usize = OPCODE_LEN + 4 + MAX_KEY + MAX_VALUE;
/// Maximum total frame size on the wire.
pub const MAX_FRAME_LEN: usize = LEN_PREFIX + MAX_FRAME_PAYLOAD;

pub const OP_PUT: u8 = 1;
pub const OP_GET: u8 = 2;
pub const OP_PUB: u8 = 3;
pub const OP_SUB: u8 = 4;
pub const OP_UNSUB: u8 = 5;

fn write_str_prefix(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn wrap_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    // len = opcode (1) + payload. Checked arithmetic: no panic on
    // overflow; saturating is impossible here for in-memory inputs
    // below MAX_FRAME_LEN, and callers pass bounded key/value slices.
    let len = OPCODE_LEN
        .checked_add(payload.len())
        .unwrap_or(MAX_FRAME_PAYLOAD);
    let mut buf = Vec::with_capacity(LEN_PREFIX + len);
    buf.extend_from_slice(&(len as u32).to_le_bytes());
    buf.push(opcode);
    buf.extend_from_slice(payload);
    buf
}

fn payload_str(payload: &[u8]) -> Result<(String, usize), Error> {
    if payload.len() < 4 {
        return Err(Error::BadFrame);
    }
    let key_len =
        u32::from_le_bytes(payload[0..4].try_into().map_err(|_| Error::BadFrame)?) as usize;
    if key_len > MAX_KEY {
        return Err(Error::OpTooLarge);
    }
    let end = 4usize.checked_add(key_len).ok_or(Error::BadFrame)?;
    if payload.len() < end {
        return Err(Error::BadFrame);
    }
    let s = String::from_utf8(payload[4..end].to_vec()).map_err(|_| Error::BadFrame)?;
    Ok((s, end))
}

/// Frame a `Put` op: `len(4B LE) + opcode(1=Put) + key_len(4B LE) + key + value`.
pub fn frame_put(key: &str, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + key.len() + value.len());
    write_str_prefix(&mut payload, key);
    payload.extend_from_slice(value);
    wrap_frame(OP_PUT, &payload)
}

/// Frame a `Get` op: `len + opcode(2=Get) + key_len + key`.
pub fn frame_get(key: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + key.len());
    write_str_prefix(&mut payload, key);
    wrap_frame(OP_GET, &payload)
}

/// Frame a `Pub` op: `len + opcode(3=Pub) + topic_len + topic + value`.
pub fn frame_pub(topic: &str, value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + topic.len() + value.len());
    write_str_prefix(&mut payload, topic);
    payload.extend_from_slice(value);
    wrap_frame(OP_PUB, &payload)
}

/// Frame a `Sub` op: `len + opcode(4=Sub) + topic_len + topic`.
pub fn frame_sub(topic: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + topic.len());
    write_str_prefix(&mut payload, topic);
    wrap_frame(OP_SUB, &payload)
}

/// Frame an `Unsub` op: `len + opcode(5=Unsub) + topic_len + topic`.
pub fn frame_unsub(topic: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + topic.len());
    write_str_prefix(&mut payload, topic);
    wrap_frame(OP_UNSUB, &payload)
}

/// Frame any [`Op`] using its opcode. `sync` is not carried on the wire
/// and decodes as `false`.
pub fn frame_op(op: &Op) -> Vec<u8> {
    match op {
        Op::Put { key, value, .. } => frame_put(key, value),
        Op::Get { key } => frame_get(key),
        Op::Pub { topic, value, .. } => frame_pub(topic, value),
        Op::Sub { topic } => frame_sub(topic),
        Op::Unsub { topic } => frame_unsub(topic),
    }
}

/// Decode one complete frame (`len + opcode + payload`) into an [`Op`].
///
/// Returns [`Error::BadFrame`] for truncated/malformed frames and
/// [`Error::OpTooLarge`] for frames exceeding the size limits.
pub fn decode_frame(buf: &[u8]) -> Result<Op, Error> {
    if buf.len() < MIN_FRAME {
        return Err(Error::BadFrame);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().map_err(|_| Error::BadFrame)?) as usize;
    // Oversized check comes before the truncation check so a lying
    // length prefix is reported as oversized, not merely truncated.
    if len == 0 || len > MAX_FRAME_PAYLOAD {
        return Err(Error::OpTooLarge);
    }
    let total = LEN_PREFIX.checked_add(len).ok_or(Error::BadFrame)?;
    if buf.len() < total {
        return Err(Error::BadFrame);
    }
    let payload = &buf[LEN_PREFIX..total];
    let opcode = payload[0];
    let body = &payload[OPCODE_LEN..];

    match opcode {
        OP_PUT => {
            let (key, consumed) = payload_str(body)?;
            let value = &body[consumed..];
            if value.len() > MAX_VALUE {
                return Err(Error::OpTooLarge);
            }
            Ok(Op::Put {
                key,
                value: value.to_vec(),
                sync: false,
            })
        }
        OP_GET => {
            let (key, consumed) = payload_str(body)?;
            if consumed != body.len() {
                return Err(Error::BadFrame);
            }
            Ok(Op::Get { key })
        }
        OP_PUB => {
            let (topic, consumed) = payload_str(body)?;
            let value = &body[consumed..];
            if value.len() > MAX_VALUE {
                return Err(Error::OpTooLarge);
            }
            Ok(Op::Pub {
                topic,
                value: value.to_vec(),
                sync: false,
            })
        }
        OP_SUB => {
            let (topic, consumed) = payload_str(body)?;
            if consumed != body.len() {
                return Err(Error::BadFrame);
            }
            Ok(Op::Sub { topic })
        }
        OP_UNSUB => {
            let (topic, consumed) = payload_str(body)?;
            if consumed != body.len() {
                return Err(Error::BadFrame);
            }
            Ok(Op::Unsub { topic })
        }
        _ => Err(Error::BadFrame),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_frame_put_then_get() {
        // Put roundtrip (plan's Task 9 test).
        let bytes = frame_put("k", &[1u8]);
        let op = decode_frame(&bytes).unwrap();
        assert!(matches!(
            &op,
            Op::Put { key, value, sync } if key == "k" && value == &vec![1u8] && !sync
        ));

        // Get roundtrip: frame a Get, decode it back.
        let get_bytes = frame_get("k");
        let get_op = decode_frame(&get_bytes).unwrap();
        assert!(matches!(&get_op, Op::Get { key } if key == "k"));

        // Framing header sanity: len prefix == opcode + payload,
        // opcode byte is 1 for Put.
        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, bytes.len() - 4);
        assert_eq!(bytes[4], OP_PUT);
        assert_eq!(get_bytes[4], OP_GET);
    }

    #[test]
    fn bad_frame_truncated() {
        // Empty / shorter than len+opcode.
        assert!(matches!(decode_frame(&[]), Err(Error::BadFrame)));
        assert!(matches!(decode_frame(&[1, 0, 0]), Err(Error::BadFrame)));

        // Valid frame chopped mid-payload.
        let bytes = frame_put("hello", &[1u8, 2, 3, 4]);
        assert!(matches!(
            decode_frame(&bytes[..bytes.len() - 1]),
            Err(Error::BadFrame)
        ));
        // Only the length prefix, no opcode/payload yet.
        assert!(matches!(decode_frame(&bytes[..3]), Err(Error::BadFrame)));

        // Length prefix claims more than the buffer holds.
        let mut lying = frame_get("k");
        lying[0] = 0xFF;
        lying[1] = 0x00;
        lying[2] = 0x00;
        lying[3] = 0x00;
        assert!(matches!(decode_frame(&lying), Err(Error::BadFrame)));

        // Unknown opcode.
        let mut unknown = frame_get("k");
        unknown[4] = 99;
        assert!(matches!(decode_frame(&unknown), Err(Error::BadFrame)));

        // Key-length prefix claims more than the payload holds.
        let mut bad_key = frame_get("k");
        let last = bad_key.len() - 1;
        bad_key[last] = 0xFF;
        assert!(matches!(decode_frame(&bad_key), Err(Error::BadFrame)));
    }

    #[test]
    fn oversized_rejected() {
        // Value over MAX_VALUE.
        let big_value = vec![0u8; MAX_VALUE + 1];
        let frame = frame_put("k", &big_value);
        assert!(matches!(decode_frame(&frame), Err(Error::OpTooLarge)));

        // Key over MAX_KEY.
        let big_key = "k".repeat(MAX_KEY + 1);
        let frame = frame_put(&big_key, &[1u8]);
        assert!(matches!(decode_frame(&frame), Err(Error::OpTooLarge)));

        // Lying length prefix far over MAX_FRAME_PAYLOAD.
        let mut huge = Vec::new();
        huge.extend_from_slice(&((MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes());
        huge.push(OP_PUT);
        huge.extend_from_slice(&[0u8; 8]);
        assert!(matches!(decode_frame(&huge), Err(Error::OpTooLarge)));

        // Pub value over MAX_VALUE is rejected too.
        let frame = frame_pub("t", &big_value);
        assert!(matches!(decode_frame(&frame), Err(Error::OpTooLarge)));
    }

    #[test]
    fn tcp_roundtrip_all_opcodes() {
        let cases = [
            (
                frame_put("a", &[9u8, 8]),
                Op::Put {
                    key: "a".into(),
                    value: vec![9, 8],
                    sync: false,
                },
            ),
            (frame_get("a"), Op::Get { key: "a".into() }),
            (
                frame_pub("chat", &[1u8]),
                Op::Pub {
                    topic: "chat".into(),
                    value: vec![1],
                    sync: false,
                },
            ),
            (
                frame_sub("chat"),
                Op::Sub {
                    topic: "chat".into(),
                },
            ),
            (
                frame_unsub("chat"),
                Op::Unsub {
                    topic: "chat".into(),
                },
            ),
        ];
        for (bytes, expected) in cases {
            let decoded = decode_frame(&bytes).unwrap();
            match (&decoded, &expected) {
                (
                    Op::Put { key, value, sync },
                    Op::Put {
                        key: k2,
                        value: v2,
                        sync: s2,
                    },
                ) => {
                    assert_eq!(key, k2);
                    assert_eq!(value, v2);
                    assert_eq!(sync, s2);
                }
                (Op::Get { key }, Op::Get { key: k2 }) => assert_eq!(key, k2),
                (
                    Op::Pub { topic, value, sync },
                    Op::Pub {
                        topic: t2,
                        value: v2,
                        sync: s2,
                    },
                ) => {
                    assert_eq!(topic, t2);
                    assert_eq!(value, v2);
                    assert_eq!(sync, s2);
                }
                (Op::Sub { topic }, Op::Sub { topic: t2 }) => assert_eq!(topic, t2),
                (Op::Unsub { topic }, Op::Unsub { topic: t2 }) => assert_eq!(topic, t2),
                _ => panic!("variant mismatch: {decoded:?} vs {expected:?}"),
            }
        }
    }
}
