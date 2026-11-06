#![no_main]

use libfuzzer_sys::fuzz_target;
use realtime_backend::adapters::{http::parse_http, ws::decode_ws_json};

fuzz_target!(|data: &[u8]| {
    // Adapters take `&str`; non-UTF8 input is not a valid frame here.
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = decode_ws_json(text);
    let _ = parse_http(text);
});
