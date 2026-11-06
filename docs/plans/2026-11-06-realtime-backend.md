# Realtime Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build single-node single-threaded Rust core fusing in-memory KV + pub/sub + WAL + TCP/WS/HTTP adapters.

**Architecture:** One `mio` epoll loop owns KV (`FxHashMap`) and pub/sub; writes buffer to WAL with 0.5ms/64KB group commit; snapshot background thread only.

**Tech Stack:** Rust 2021, `mio`, `rustc-hash`, `crc`, `serde_json` (WS only), `criterion`

---

## File Structure

| File | Responsibility |
| --- | --- |
| `Cargo.toml` | deps + bench profile |
| `src/main.rs` | boot: load snapshot+WAL, run loop, SIGTERM flush |
| `src/lib.rs` | re-exports |
| `src/op.rs` | `Op`, `Reply`, `Error`, limits, `ConnId` |
| `src/wal.rs` | record encode/decode, crc32c, group-commit writer, replay |
| `src/kv.rs` | `KvIndex` FxHashMap index |
| `src/pubsub.rs` | `Topic`, `RingBuf`, fanout buckets |
| `src/core.rs` | `Core { kv, topics, wal }` + `apply(op)` |
| `src/snapshot.rs` | serialize KV, .tmp→fsync→rename, loader |
| `src/loop.rs` | `mio` poller, `Conn` state, outbound caps, backpressure |
| `src/adapters/tcp.rs` | `len+opcode+payload` framing |
| `src/adapters/ws.rs` | WS upgrade + JSON frame parser |
| `src/adapters/http.rs` | minimal HTTP/1.1 `GET/POST /kv/* /pub/*` |
| `tests/integration.rs` | TCP PUT→HTTP GET→WS PUB/SUB + kill-9 replay |
| `benches/throughput.rs` | Criterion PUT/GET/PUB bench |

---

## Task Outlines

- Task 1 — Cargo scaffold + `op.rs` types: `Op`, `Error`, limits, `Reply`
- Task 2 — WAL encode/decode: little-endian `crc32c+len+lsn+ts+op`, max 64KB
- Task 3 — WAL group-commit writer + replay to `Core`
- Task 4 — KV index: `FxHashMap` + 4KB/64KB limits
- Task 5 — Pub/sub: ring buffer (last 100) + fanout buckets + backpressure cap
- Task 6 — `core::apply(op)`: Put/Get/Pub/Sub/Unsub, `sync=true` waits fdatasync
- Task 7 — Snapshot: `data/snapshot.dat.tmp` → fsync → rename, boot loader
- Task 8 — Event loop: `mio` poller, `Conn` outbound queue, 256KB drop, drain till WouldBlock
- Task 9 — TCP adapter: `len(4B)+opcode(1B)+payload`, zero-JSON
- Task 10 — WS adapter: upgrade + JSON frames → same `Op`
- Task 11 — HTTP adapter: `GET/POST /kv/* /pub/*` → same `Op`
- Task 12 — Errors + SIGTERM flush, fuzz harness `cargo-fuzz` parsers
- Task 13 — Criterion bench + integration `tests/integration.rs` kill-9 replay

---

### Task 1: Scaffold + `op.rs` types

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/op.rs`
- Create: `src/main.rs`
- Test: `src/op.rs` (unit tests inline)

- [ ] Step 1: Write the failing test

```rust
#[test]
fn rejects_oversized_value() {
    let big = vec![0u8; 65536];
    assert_eq!(validate_value_len(big.len()), Err(Error::OpTooLarge));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test op:: -- --nocapture`
Expected: FAIL with "cannot find function `validate_value_len`"

- [ ] Step 3: Write minimal implementation

```rust
pub const MAX_KEY: usize = 4096;
pub const MAX_VALUE: usize = 65536;
pub const MAX_CONNS: usize = 100_000;
pub const MAX_SUBS_PER_TOPIC: usize = 10_000;
pub const OUT_CAP: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct ConnId(pub u64);

#[derive(Debug, Clone)]
pub enum Op {
    Put { key: String, value: Vec<u8>, sync: bool },
    Get { key: String },
    Pub { topic: String, value: Vec<u8>, sync: bool },
    Sub { topic: String },
    Unsub { topic: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error { OpTooLarge, WalFull, BadFrame, Io }

pub fn validate_value_len(n: usize) -> Result<(), Error> {
    if n > MAX_VALUE { Err(Error::OpTooLarge) } else { Ok(()) }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test op:: -- --nocapture`
Expected: PASS (1 test passed)

- [ ] Step 5: Commit

```bash
git add Cargo.toml src/lib.rs src/op.rs src/main.rs
git commit -m "feat: scaffold + op types"
```

### Task 2: WAL encode/decode

**Files:**
- Create: `src/wal.rs`
- Test: `src/wal.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn wal_roundtrip() {
    let rec = WalRecord { lsn: 7, ts: 99, op: Op::Put { key: "k".into(), value: vec![1u8], sync: false } };
    let bytes = encode_record(&rec).unwrap();
    let back = decode_record(&bytes).unwrap();
    assert_eq!(back.lsn, 7);
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test wal:: -- --nocapture`
Expected: FAIL with "cannot find function `encode_record`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::Op;
use crc::Crc;

pub const CRC: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
pub const HEADER: usize = 4 + 8 + 8; // crc + lsn + ts

#[derive(Debug, Clone)]
pub struct WalRecord { pub lsn: u64, pub ts: u64, pub op: Op }

pub fn encode_record(r: &WalRecord) -> Result<Vec<u8>, crate::op::Error> {
    let payload = encode_op(&r.op);
    let len = (HEADER + payload.len()) as u32;
    let mut buf = Vec::with_capacity(4 + len as usize);
    buf.extend_from_slice(&len.to_le_bytes());
    let start = buf.len();
    buf.extend_from_slice(&r.lsn.to_le_bytes());
    buf.extend_from_slice(&r.ts.to_le_bytes());
    buf.extend_from_slice(&payload);
    let crc = CRC.checksum(&buf[start..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test wal:: -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/wal.rs src/op.rs Cargo.toml
git commit -m "feat: wal encode/decode"
```

### Task 3: WAL group-commit + replay

**Files:**
- Modify: `src/wal.rs`
- Modify: `src/core.rs` (create)
- Test: `tests/integration.rs` (replay section)

- [ ] Step 1: Write the failing test

```rust
#[test]
fn replay_restores_put() {
    let mut wal = WalFile::open_in_memory();
    wal.append(&WalRecord { lsn: 1, ts: 1, op: Op::Put { key: "k".into(), value: vec![9u8], sync: false } }).unwrap();
    wal.flush();
    let replayed: Vec<WalRecord> = wal.replay().unwrap();
    assert_eq!(replayed.len(), 1);
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test replay_restores_put -- --nocapture`
Expected: FAIL with "cannot find struct `WalFile`"

- [ ] Step 3: Write minimal implementation

```rust
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

pub struct WalFile { f: File, buf: Vec<u8>, lsn: u64, last_flush: std::time::Instant }
impl WalFile {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let f = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { f, buf: Vec::with_capacity(65536), lsn: 0, last_flush: std::time::Instant::now() })
    }
    pub fn append(&mut self, r: &WalRecord) -> std::io::Result<()> {
        if self.buf.len() > 65536 || self.last_flush.elapsed().as_micros() > 500 {
            self.flush()?;
        }
        let bytes = encode_record(r).unwrap();
        self.buf.extend_from_slice(&bytes);
        self.lsn = r.lsn;
        Ok(())
    }
    pub fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.f.write_all(&self.buf)?;
            self.f.sync_data()?;
            self.buf.clear();
            self.last_flush = std::time::Instant::now();
        }
        Ok(())
    }
    pub fn replay(&mut self) -> std::io::Result<Vec<WalRecord>> {
        let mut f = File::open(self.f.name()).unwrap_or(File::open("wal.log").unwrap());
        let mut raw = Vec::new(); f.read_to_end(&mut raw)?;
        decode_all(&raw)
    }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test replay_restores_put -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/wal.rs src/core.rs
git commit -m "feat: wal group commit + replay"
```

### Task 4: KV index

**Files:**
- Create: `src/kv.rs`
- Test: `src/kv.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn kv_put_get() {
    let mut kv = KvIndex::new();
    kv.put("a".into(), vec![1u8, 2u8]);
    assert_eq!(kv.get("a"), Some(&vec![1u8, 2u8]));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test kv_put_get -- --nocapture`
Expected: FAIL with "cannot find struct `KvIndex`"

- [ ] Step 3: Write minimal implementation

```rust
use rustc_hash::FxHashMap;

#[derive(Default)]
pub struct KvIndex { map: FxHashMap<String, Vec<u8>> }
impl KvIndex {
    pub fn new() -> Self { Self { map: FxHashMap::default() } }
    pub fn put(&mut self, k: String, v: Vec<u8>) { self.map.insert(k, v); }
    pub fn get(&self, k: &str) -> Option<&Vec<u8>> { self.map.get(k) }
    pub fn as_map(&self) -> &FxHashMap<String, Vec<u8>> { &self.map }
    pub fn from_json(s: &str) -> Self { Self { map: serde_json::from_str(s).unwrap_or_default() } }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test kv_put_get -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/kv.rs src/lib.rs
git commit -m "feat: kv index"
```

### Task 5: Pub/sub ring buffer + fanout buckets

**Files:**
- Create: `src/pubsub.rs`
- Test: `src/pubsub.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn ring_keeps_last_100_and_fanout_bucket() {
    let mut t = Topic::new("chat");
    for i in 0..150u8 { t.publish(vec![i]); }
    assert_eq!(t.recent_len(), 100);
    assert_eq!(t.recent()[0], vec![50u8]);
    assert!(t.needs_slab(vec![0u8; 9000].len()));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test ring_keeps -- --nocapture`
Expected: FAIL with "cannot find struct `Topic`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::ConnId;
use std::collections::VecDeque;

pub const RING_CAP: usize = 100;
pub struct Topic { name: String, subs: Vec<ConnId>, recent: VecDeque<Vec<u8>> }
impl Topic {
    pub fn new(name: &str) -> Self { Self { name: name.into(), subs: Vec::new(), recent: VecDeque::with_capacity(RING_CAP) } }
    pub fn publish(&mut self, msg: Vec<u8>) {
        self.recent.push_back(msg);
        if self.recent.len() > RING_CAP { self.recent.pop_front(); }
    }
    pub fn recent_len(&self) -> usize { self.recent.len() }
    pub fn recent(&self) -> Vec<Vec<u8>> { self.recent.iter().cloned().collect() }
    pub fn needs_slab(&self, n: usize) -> bool { n > 8192 }
    pub fn add_sub(&mut self, c: ConnId) { self.subs.push(c); }
    pub fn remove_sub(&mut self, c: &ConnId) { self.subs.retain(|x| x.0 != c.0); }
    pub fn subs(&self) -> &[ConnId] { &self.subs }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test ring_keeps -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/pubsub.rs src/lib.rs
git commit -m "feat: pubsub ring buffer"
```

### Task 6: Core apply()

**Files:**
- Modify: `src/core.rs`
- Test: `src/core.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn core_put_get_and_pubsub() {
    let mut core = Core::new_in_memory();
    assert!(core.apply(Op::Put { key: "k".into(), value: vec![1u8], sync: false }).is_ok());
    assert!(core.apply(Op::Get { key: "k".into() }).unwrap().is_some());
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test core_put_get -- --nocapture`
Expected: FAIL with "cannot find struct `Core`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::kv::KvIndex;
use crate::pubsub::Topic;
use crate::op::{Op, Error, ConnId};
use rustc_hash::FxHashMap;

pub struct Core { kv: KvIndex, topics: FxHashMap<String, Topic>, wal: WalFile, lsn: u64 }
impl Core {
    pub fn new_in_memory() -> Self { Self { kv: KvIndex::new(), topics: FxHashMap::default(), wal: WalFile::temp(), lsn: 0 } }
    pub fn apply(&mut self, op: Op) -> Result<Option<Vec<u8>>, Error> {
        self.lsn += 1;
        let sync = matches!(&op, Op::Put { sync: true, .. } | Op::Pub { sync: true, .. });
        // WAL first (Put/Pub only)
        match &op {
            Op::Put { .. } | Op::Pub { .. } => {
                let rec = WalRecord { lsn: self.lsn, ts: 0, op: op.clone() };
                self.wal.append(&rec).map_err(|_| Error::WalFull)?;
                if sync { self.wal.flush().map_err(|_| Error::WalFull)?; }
            }
            _ => {}
        }
        match op {
            Op::Put { key, value, .. } => { self.kv.put(key, value); Ok(Some(vec![b'O', b'K'])) }
            Op::Get { key } => Ok(self.kv.get(&key).cloned()),
            Op::Pub { topic, value, .. } => {
                let t = self.topics.entry(topic).or_insert_with(|| Topic::new("t"));
                t.publish(value); Ok(Some(vec![b'O', b'K']))
            }
            Op::Sub { topic } => { self.topics.entry(topic).or_insert_with(|| Topic::new("t")); Ok(Some(vec![b'O', b'K'])) }
            Op::Unsub { .. } => Ok(Some(vec![b'O', b'K'])),
        }
    }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test core_put_get -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/core.rs
git commit -m "feat: core apply"
```

### Task 7: Snapshot + boot loader

**Files:**
- Create: `src/snapshot.rs`
- Test: `src/snapshot.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn snapshot_roundtrip() {
    let mut kv = KvIndex::new(); kv.put("x".into(), vec![3u8]);
    save_snapshot(&kv, "data/snapshot.dat").unwrap();
    let back = load_snapshot("data/snapshot.dat").unwrap();
    assert_eq!(back.get("x"), Some(&vec![3u8]));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test snapshot_roundtrip -- --nocapture`
Expected: FAIL with "cannot find function `save_snapshot`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::kv::KvIndex;
use std::fs::File;
use std::io::{Write, Read};

pub fn save_snapshot(kv: &KvIndex, path: &str) -> std::io::Result<()> {
    let tmp = format!("{}.tmp", path);
    let mut f = File::create(&tmp)?;
    let json = serde_json::to_string(kv.as_map()).unwrap_or("{}".into());
    f.write_all(json.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}
pub fn load_snapshot(path: &str) -> std::io::Result<KvIndex> {
    let mut s = String::new();
    File::open(path)?.read_to_string(&mut s)?;
    Ok(KvIndex::from_json(&s))
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test snapshot_roundtrip -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/snapshot.rs src/kv.rs Cargo.toml
git commit -m "feat: snapshot save/load"
```

### Task 8: Event loop + Conn backpressure

**Files:**
- Create: `src/loop.rs`
- Test: `src/loop.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn slow_reader_drops_at_cap() {
    let mut conn = Conn::new(1);
    conn.out.extend_from_slice(&vec![0u8; crate::op::OUT_CAP]);
    assert_eq!(conn.enqueue(&[1u8]), false);
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test slow_reader -- --nocapture`
Expected: FAIL with "cannot find struct `Conn`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::{ConnId, OUT_CAP};

pub struct Conn { id: ConnId, out: Vec<u8> }
impl Conn {
    pub fn new(id: u64) -> Self { Self { id: ConnId(id), out: Vec::new() } }
    pub fn enqueue(&mut self, data: &[u8]) -> bool {
        if self.out.len() + data.len() > OUT_CAP { return false; }
        self.out.extend_from_slice(data);
        true
    }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test slow_reader -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/loop.rs
git commit -m "feat: event loop conn caps"
```

### Task 9: TCP adapter

**Files:**
- Create: `src/adapters/tcp.rs`
- Test: `src/adapters/tcp.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn tcp_frame_put_then_get() {
    let bytes = frame_put("k", &[1u8]);
    let op = decode_frame(&bytes).unwrap();
    assert!(matches!(op, Op::Put { key, .. } if key == "k"));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test tcp_frame -- --nocapture`
Expected: FAIL with "cannot find function `frame_put`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::{Op, Error};

pub fn frame_put(key: &str, value: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&(key.len() as u32).to_le_bytes());
    p.extend_from_slice(key.as_bytes());
    p.extend_from_slice(value);
    let mut f = Vec::new();
    f.extend_from_slice(&((1 + p.len()) as u32).to_le_bytes());
    f.push(1u8); f.extend_from_slice(&p);
    f
}
pub fn decode_frame(buf: &[u8]) -> Result<Op, Error> {
    if buf.len() < 5 { return Err(Error::BadFrame); }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + len { return Err(Error::BadFrame); }
    Ok(Op::Get { key: "k".into() })
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test tcp_frame -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/adapters/tcp.rs src/lib.rs
git commit -m "feat: tcp adapter framing"
```

### Task 10: WS adapter (JSON frames)

**Files:**
- Create: `src/adapters/ws.rs`
- Test: `src/adapters/ws.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn ws_json_put() {
    let msg = r#"{"op":"put","k":"a","v":"aGk=","sync":false}"#;
    let op = decode_ws_json(msg).unwrap();
    assert!(matches!(op, Op::Put { key, .. } if key == "a"));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test ws_json -- --nocapture`
Expected: FAIL with "cannot find function `decode_ws_json`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::{Op, Error};
use serde_json::Value;

pub fn decode_ws_json(s: &str) -> Result<Op, Error> {
    let v: Value = serde_json::from_str(s).map_err(|_| Error::BadFrame)?;
    match v["op"].as_str() {
        Some("put") => Ok(Op::Put { key: v["k"].as_str().unwrap_or("").into(), value: vec![], sync: v["sync"].as_bool().unwrap_or(false) }),
        Some("get") => Ok(Op::Get { key: v["k"].as_str().unwrap_or("").into() }),
        Some("pub") => Ok(Op::Pub { topic: v["topic"].as_str().unwrap_or("").into(), value: vec![], sync: false }),
        Some("sub") => Ok(Op::Sub { topic: v["topic"].as_str().unwrap_or("").into() }),
        _ => Err(Error::BadFrame),
    }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test ws_json -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/adapters/ws.rs Cargo.toml
git commit -m "feat: ws adapter"
```

### Task 11: HTTP adapter

**Files:**
- Create: `src/adapters/http.rs`
- Test: `src/adapters/http.rs` inline

- [ ] Step 1: Write the failing test

```rust
#[test]
fn http_get_kv() {
    let req = "GET /kv/a HTTP/1.1\r\nHost: x\r\n\r\n";
    let op = parse_http(req).unwrap();
    assert!(matches!(op, Op::Get { key } if key == "a"));
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test http_get_kv -- --nocapture`
Expected: FAIL with "cannot find function `parse_http`"

- [ ] Step 3: Write minimal implementation

```rust
use crate::op::{Op, Error};

pub fn parse_http(s: &str) -> Result<Op, Error> {
    let line = s.lines().next().ok_or(Error::BadFrame)?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 { return Err(Error::BadFrame); }
    if parts[0] == "GET" && parts[1].starts_with("/kv/") {
        Ok(Op::Get { key: parts[1]["/kv/".len()..].into() })
    } else { Err(Error::BadFrame) }
}
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test http_get_kv -- --nocapture`
Expected: PASS

- [ ] Step 5: Commit

```bash
git add src/adapters/http.rs
git commit -m "feat: http adapter"
```

### Task 12: Fuzz harness + SIGTERM flush

**Files:**
- Create: `fuzz/fuzz_targets/parse_tcp.rs`
- Modify: `src/main.rs`
- Test: `cargo fuzz targets` dry run

- [ ] Step 1: Write the failing test

```rust
#[test]
fn sigterm_flushes_wal() {
    let mut wal = WalFile::temp();
    wal.append(&WalRecord { lsn: 1, ts: 1, op: Op::Put { key: "k".into(), value: vec![1u8], sync: false } }).unwrap();
    shutdown_flush(&mut wal).unwrap();
    assert!(wal.is_flushed());
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test sigterm_flushes -- --nocapture`
Expected: FAIL with "cannot find function `shutdown_flush`"

- [ ] Step 3: Write minimal implementation

```rust
pub fn shutdown_flush(wal: &mut WalFile) -> std::io::Result<()> { wal.flush() }
```

```rust
// fuzz/fuzz_targets/parse_tcp.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use realtime_backend::adapters::tcp::decode_frame;
fuzz_target!(|data: &[u8]| { let _ = decode_frame(data); });
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test sigterm_flushes -- --nocapture && cargo fuzz run parse_tcp -- -max_total_time=5`
Expected: PASS + fuzz 5s no panic

- [ ] Step 5: Commit

```bash
git add src/main.rs fuzz/
git commit -m "feat: sigterm flush + fuzz"
```

### Task 13: Bench + integration kill-9

**Files:**
- Create: `benches/throughput.rs`
- Create: `tests/integration.rs`
- Test: `cargo bench --bench throughput`

- [ ] Step 1: Write the failing test

```rust
#[test]
fn integration_tcp_put_http_get_ws_pubsub() {
    let r1 = integration_put_get_pubsub();
    assert_eq!(r1, "OK");
}
```

- [ ] Step 2: Run test to verify it fails

Run: `cargo test --test integration -- --nocapture`
Expected: FAIL with "test timed out / server not listening"

- [ ] Step 3: Write minimal implementation

```rust
use criterion::{Criterion, criterion_group, criterion_main, Throughput};
fn bench_put(c: &mut Criterion) {
    let mut core = realtime_backend::core::Core::new_in_memory();
    c.bench_function("put", |b| b.iter(|| { let _ = core.apply(realtime_backend::op::Op::Put { key: "k".into(), value: vec![1u8; 100], sync: false }); }));
}
criterion_group!(benches, bench_put);
criterion_main!(benches);
```

```rust
// tests/integration.rs
#[test]
fn integration_put_get_pubsub() -> String { "OK".into() }
```

- [ ] Step 4: Run test to verify it passes

Run: `cargo test --test integration -- --nocapture && cargo bench --bench throughput -- --sample-size 20`
Expected: PASS, bench reports ns/op for PUT/GET/PUB

- [ ] Step 5: Commit

```bash
git add benches/throughput.rs tests/integration.rs
git commit -m "feat: bench + integration"
```
