// WS JSON adapter: parse text frames into `Op`.
// Wire format: JSON object `{"op":"put|get|pub|sub|unsub", ...}`.
// - put:  {"op":"put","k":"<key>","v":"<base64>","sync":bool?}
// - get:  {"op":"get","k":"<key>"}
// - pub:  {"op":"pub","topic":"<topic>","v":"<base64>","sync":bool?}
// - sub:  {"op":"sub","topic":"<topic>"}
// - unsub:{"op":"unsub","topic":"<topic>"}
// Bad JSON / missing fields / bad base64 / unknown op => Error::BadFrame.

use crate::op::{validate_key_len, validate_value_len, Error, Op};
use serde_json::Value;

pub fn decode_ws_json(s: &str) -> Result<Op, Error> {
    let v: Value = serde_json::from_str(s).map_err(|_| Error::BadFrame)?;
    let op = v.get("op").and_then(Value::as_str).ok_or(Error::BadFrame)?;
    match op {
        "put" => {
            let k = required_str(&v, "k")?;
            let v_b64 = v.get("v").and_then(Value::as_str).ok_or(Error::BadFrame)?;
            let value = decode_base64(v_b64).ok_or(Error::BadFrame)?;
            validate_key_len(k.len())?;
            validate_value_len(value.len())?;
            Ok(Op::Put {
                key: k.to_owned(),
                value,
                sync: bool_field(&v),
            })
        }
        "get" => {
            let k = required_str(&v, "k")?;
            validate_key_len(k.len())?;
            Ok(Op::Get { key: k.to_owned() })
        }
        "pub" => {
            let topic = required_str(&v, "topic")?;
            let v_b64 = v.get("v").and_then(Value::as_str).ok_or(Error::BadFrame)?;
            let value = decode_base64(v_b64).ok_or(Error::BadFrame)?;
            validate_key_len(topic.len())?;
            validate_value_len(value.len())?;
            Ok(Op::Pub {
                topic: topic.to_owned(),
                value,
                sync: bool_field(&v),
            })
        }
        "sub" => {
            let topic = required_str(&v, "topic")?;
            validate_key_len(topic.len())?;
            Ok(Op::Sub {
                topic: topic.to_owned(),
            })
        }
        "unsub" => {
            let topic = required_str(&v, "topic")?;
            validate_key_len(topic.len())?;
            Ok(Op::Unsub {
                topic: topic.to_owned(),
            })
        }
        _ => Err(Error::BadFrame),
    }
}

fn required_str<'a>(v: &'a Value, field: &str) -> Result<&'a str, Error> {
    match v.get(field).and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Err(Error::BadFrame),
    }
}

fn bool_field(v: &Value) -> bool {
    v.get("sync").and_then(Value::as_bool).unwrap_or(false)
}

// Minimal standard base64 decoder (A-Za-z0-9+/ with = padding).
// Returns None on any invalid input. Empty input decodes to empty vec.
fn decode_base64(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    fn val(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let chunks = bytes.len() / 4;
    for i in 0..chunks {
        let chunk = &bytes[i * 4..i * 4 + 4];
        let is_last = i + 1 == chunks;
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        if pad > 0 && !is_last {
            return None;
        }
        if pad > 2 {
            return None;
        }
        // '=' may only appear as the trailing 1 or 2 bytes of the input.
        if pad == 1 && chunk[3] != b'=' {
            return None;
        }
        if pad == 2 && (chunk[2] != b'=' || chunk[3] != b'=') {
            return None;
        }
        let mut vals = [0u8; 4];
        for (j, &c) in chunk.iter().enumerate() {
            if c == b'=' {
                vals[j] = 0;
            } else {
                vals[j] = val(c)?;
            }
        }
        let n = ((vals[0] as u32) << 18)
            | ((vals[1] as u32) << 12)
            | ((vals[2] as u32) << 6)
            | vals[3] as u32;
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_json_put() {
        let msg = r#"{"op":"put","k":"a","v":"aGk=","sync":false}"#;
        let op = decode_ws_json(msg).unwrap();
        assert!(
            matches!(&op, Op::Put { key, value, sync } if key == "a" && value == b"hi" && !sync)
        );
    }

    #[test]
    fn ws_json_get() {
        let msg = r#"{"op":"get","k":"a"}"#;
        let op = decode_ws_json(msg).unwrap();
        assert!(matches!(&op, Op::Get { key } if key == "a"));
    }

    #[test]
    fn bad_json() {
        assert_eq!(decode_ws_json("not json").unwrap_err(), Error::BadFrame);
        assert_eq!(decode_ws_json("{}").unwrap_err(), Error::BadFrame);
        assert_eq!(
            decode_ws_json(r#"{"op":"nope"}"#).unwrap_err(),
            Error::BadFrame
        );
        // put missing v
        assert_eq!(
            decode_ws_json(r#"{"op":"put","k":"a"}"#).unwrap_err(),
            Error::BadFrame
        );
        // put missing k
        assert_eq!(
            decode_ws_json(r#"{"op":"put","v":"aGk="}"#).unwrap_err(),
            Error::BadFrame
        );
        // pub missing topic
        assert_eq!(
            decode_ws_json(r#"{"op":"pub","v":"aGk="}"#).unwrap_err(),
            Error::BadFrame
        );
        // invalid base64
        assert_eq!(
            decode_ws_json(r#"{"op":"put","k":"a","v":"!!!"}"#).unwrap_err(),
            Error::BadFrame
        );
    }

    #[test]
    fn ws_json_pub_sub_unsub() {
        let op =
            decode_ws_json(r#"{"op":"pub","topic":"chat","v":"aGVsbG8=","sync":true}"#).unwrap();
        assert!(
            matches!(&op, Op::Pub { topic, value, sync } if topic == "chat" && value == b"hello" && *sync)
        );
        let op = decode_ws_json(r#"{"op":"sub","topic":"chat"}"#).unwrap();
        assert!(matches!(&op, Op::Sub { topic } if topic == "chat"));
        let op = decode_ws_json(r#"{"op":"unsub","topic":"chat"}"#).unwrap();
        assert!(matches!(&op, Op::Unsub { topic } if topic == "chat"));
    }
}
