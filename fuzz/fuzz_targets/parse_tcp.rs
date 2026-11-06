#![no_main]

use libfuzzer_sys::fuzz_target;
use realtime_backend::adapters::tcp::decode_frame;

fuzz_target!(|data: &[u8]| {
    let _ = decode_frame(data);
});
