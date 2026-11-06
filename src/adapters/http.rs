//! Task 11: Minimal HTTP/1.1 adapter.
//!
//! Adapters only parse / serialize; all three protocols share
//! `Core::apply(op)`. This module turns a raw HTTP request string
//! into an [`Op`]:
//!
//! - `GET  /kv/{key}`   -> [`Op::Get`]
//! - `POST /kv/{key}`   -> [`Op::Put`]  (body is the value)
//! - `POST /pub/{topic}` -> [`Op::Pub`] (body is the value)
//! - anything else      -> [`Error::BadFrame`]
//!
//! Keys/topics are validated with [`validate_key_len`] and value
//! bodies with [`validate_value_len`], so oversized input surfaces
//! as [`Error::OpTooLarge`] exactly as it would at the core layer.
//! An empty key/topic (e.g. `GET /kv/`) or an unknown method/path
//! is a [`Error::BadFrame`].

use crate::op::{validate_key_len, validate_value_len, Error, Op};

/// Parse a minimal HTTP/1.1 request into an [`Op`].
///
/// The request line is the first line (`METHOD SP PATH SP HTTP/x.x`);
/// the body, when present, is everything after the first blank line
/// (`\r\n\r\n` or `\n\n`). Headers are ignored. A query string or
/// fragment on the path is stripped before routing.
pub fn parse_http(s: &str) -> Result<Op, Error> {
    // Request line: first line, CRLF or LF terminated.
    let line = s.lines().next().ok_or(Error::BadFrame)?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(Error::BadFrame);
    }
    // If a version token is present it must look like HTTP/x.y.
    if parts.len() == 3 && !parts[2].starts_with("HTTP/") {
        return Err(Error::BadFrame);
    }
    let method = parts[0];
    // Strip query/fragment so `/kv/a?x=1` routes to key `a`.
    let path = parts[1].split(['?', '#']).next().unwrap_or("");
    if path.is_empty() {
        return Err(Error::BadFrame);
    }

    let body = request_body(s);

    match method {
        "GET" => {
            let key = path.strip_prefix("/kv/").ok_or(Error::BadFrame)?;
            if key.is_empty() {
                return Err(Error::BadFrame);
            }
            validate_key_len(key.len())?;
            Ok(Op::Get { key: key.into() })
        }
        "POST" => {
            if let Some(key) = path.strip_prefix("/kv/") {
                if key.is_empty() {
                    return Err(Error::BadFrame);
                }
                validate_key_len(key.len())?;
                validate_value_len(body.len())?;
                Ok(Op::Put {
                    key: key.into(),
                    value: body.to_vec(),
                    sync: false,
                })
            } else if let Some(topic) = path.strip_prefix("/pub/") {
                if topic.is_empty() {
                    return Err(Error::BadFrame);
                }
                validate_key_len(topic.len())?;
                validate_value_len(body.len())?;
                Ok(Op::Pub {
                    topic: topic.into(),
                    value: body.to_vec(),
                    sync: false,
                })
            } else {
                Err(Error::BadFrame)
            }
        }
        _ => Err(Error::BadFrame),
    }
}

/// Everything after the first blank line, or empty when there is none.
fn request_body(s: &str) -> &[u8] {
    if let Some(idx) = s.find("\r\n\r\n") {
        &s.as_bytes()[idx + 4..]
    } else if let Some(idx) = s.find("\n\n") {
        &s.as_bytes()[idx + 2..]
    } else {
        b""
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_get_kv() {
        let req = "GET /kv/a HTTP/1.1\r\nHost: x\r\n\r\n";
        let op = parse_http(req).unwrap();
        assert!(matches!(op, Op::Get { key } if key == "a"));
    }

    #[test]
    fn http_post_kv() {
        let req = "POST /kv/a HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";
        let op = parse_http(req).unwrap();
        assert!(matches!(
            op,
            Op::Put { ref key, ref value, sync: false }
                if key == "a" && value == b"hello"
        ));
    }

    #[test]
    fn http_post_pub() {
        let req = "POST /pub/chat HTTP/1.1\r\nHost: x\r\n\r\nworld";
        let op = parse_http(req).unwrap();
        assert!(matches!(
            op,
            Op::Pub { ref topic, ref value, sync: false }
                if topic == "chat" && value == b"world"
        ));
    }

    #[test]
    fn bad_path() {
        // Unknown path / method / malformed request -> BadFrame.
        // (Op has no PartialEq, so compare .err().)
        assert_eq!(
            parse_http("GET /unknown HTTP/1.1\r\n\r\n").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(
            parse_http("POST /bad/x HTTP/1.1\r\n\r\nbody").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(
            parse_http("DELETE /kv/a HTTP/1.1\r\n\r\n").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(
            parse_http("GET /kv/ HTTP/1.1\r\n\r\n").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(
            parse_http("POST /pub/ HTTP/1.1\r\n\r\nx").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(
            parse_http("GET /kv HTTP/1.1\r\n\r\n").err(),
            Some(Error::BadFrame)
        );
        assert_eq!(parse_http("").err(), Some(Error::BadFrame));
        assert_eq!(parse_http("GET").err(), Some(Error::BadFrame));
        assert_eq!(parse_http("garbage in here").err(), Some(Error::BadFrame));
    }

    #[test]
    fn oversized_key_or_value_rejected() {
        let big_key = "x".repeat(crate::op::MAX_KEY + 1);
        let req = format!("GET /kv/{big_key} HTTP/1.1\r\n\r\n");
        assert_eq!(parse_http(&req).err(), Some(Error::OpTooLarge));

        let big_topic = "t".repeat(crate::op::MAX_KEY + 1);
        let req = format!("POST /pub/{big_topic} HTTP/1.1\r\n\r\nx");
        assert_eq!(parse_http(&req).err(), Some(Error::OpTooLarge));

        let big_body = "v".repeat(crate::op::MAX_VALUE + 1);
        let req = format!("POST /kv/a HTTP/1.1\r\n\r\n{big_body}");
        assert_eq!(parse_http(&req).err(), Some(Error::OpTooLarge));
    }
}
