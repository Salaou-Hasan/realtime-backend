# Realtime Backend — Single-Node In-Memory + WAL Design

**Date:** 2026-11-06
**Status:** Approved for planning
**Stack:** Rust, single-threaded, `mio` / epoll, custom parsers

## Goal

Single-node, single-threaded realtime backend that fuses in-memory DB,
WAL persistence, and realtime server into one binary. Target workloads —
same core engine:

- Chat / messaging (Discord-like)
- Social feed / timeline (X-like)
- KV / flags / config (Netflix-like low-latency reads)
- Event streams (append-only + subscribers)

Non-goals for v1: multi-node sharding, consensus, multi-threaded fanout,
encryption at rest.

## Constraints

- Topology: single-node first, single-threaded first to find perf ceiling
- Runtime: Rust (io_uring / epoll, zero-GC)
- Data shape for v1: KV blobs (`Key -> bytes`). Docs/typed records built on top
- Protocols (unified): TCP binary (lowest overhead) + WebSocket (fanout) + HTTP (tooling)
- All three converge to one `Op` enum and one `apply(op)` — nothing separate

## Architecture

Single-threaded event loop. No lock on hot path. WAL is source of truth,
memory is rebuilt cache.

```
                ┌────────────  mio epoll loop  ────────────┐
TCP binary ──▶  │  poll → read → parse → apply               │
WebSocket ───▶  │    │                                       │
HTTP ────────▶  │    ▼                                       │
                │  Core Engine (in-memory)                   │
                │   ├─ KV: FxHashMap<Key, Value> (slab)      │
                │   └─ Pub/Sub: topic → intrusive list       │
                │           (chat / feed / events / KV-watch)│
                │    │                                       │
                │    ▼                                       │
                │  WAL: Vec buffer → fdatasync (group)       │
                │  Snapshot: .tmp → fsync → rename           │
                └─────────────────────────────────────────────┘
                         │  disk (append-only, append + fsync)
```

Invariants:

- Single-threaded → no locks, no CAS, no cross-thread channels on hot path
- WAL is only source of truth; memory is rebuilt from snapshot + WAL
- All 3 protocols share `apply(op)`; adapters only parse / serialize

## Components

### Adapters (front doors)

- `TcpAdapter` — binary framing `len(4B) + opcode(1B) + payload`. Zero JSON.
- `WsAdapter` — minimal WS parser (upgrade + frames). Same `Op`/`Reply`.
- `HttpAdapter` — minimal HTTP/1.1 for `GET/POST /kv/*` and `/pub/*`.
- All call `core::apply(op) -> Vec<Reply>` and write via shared outbound buffer.

### Core Engine (in-memory)

- KV: `FxHashMap<u64(key_hash), Entry { key: Box<str>, value: Box<[u8]> }>`,
  open-addressing probe. Single-thread owned.
- Pub/Sub: `FxHashMap<u64(topic_hash), Topic { subs: Vec<ConnId>, recent: RingBuf<Msg> }>`
- Fanout buckets: 8KB+ messages go to slab, not `Vec` copies.
- Controls: max key 4KB, max value 64KB, max 100k conns, max 10k subs/topic.
  Per-conn outbound cap 256KB — slow reader dropped, loop never blocks.

### WAL

- Append-only file, little-endian, variable-length records:
  `crc32c + len + lsn + ts + op`. Records are buffered in 64KB chunks.
- Group commit: flush every 0.5ms or 64KB, `fdatasync`. `fsync` only on snapshot.
- Replay on boot: scan records in LSN order, rebuild KV + topics.

### Snapshot + Compaction

- Background thread (only 2nd thread) copies index every 1M ops or 5 min,
  whichever first → `data/snapshot.dat.tmp` → `fsync` → `rename` to
  `data/snapshot.dat`.
- WAL truncates only after `rename` succeeds. No in-place overwrite.

## Data Flow

```
Client bytes → epoll readable
  → parse Op { Put/Get/Pub/Sub/Unsub }
  → apply:
      Put:  WAL buffer.append(record) → index.insert() → queue Ack
      Get:  index.get() → queue Reply
      Pub:  WAL buffer.append(record) → topics[topic].ring.push() → fanout()
      Sub:  topics[topic].subs.push(conn) → queue recent-ring replay + Subscribed
  → try flush outbound (non-blocking write)
  → every 0.5ms or 64KB: WAL fdatasync()

  fanout(topic, msg):
    for conn in topic.subs:
      if out_cap(conn) ok: enqueue(msg)
      else: drop conn (slow reader), don't block loop
```

Durability for v1:

- Index updates immediately → read-your-writes in same iteration
- Ack returns before `fdatasync` (async commit) → crash can lose ≤0.5ms ops
- Optional `sync=true` per op → wait for next fdatasync before Ack (durable, ~+0.5ms p99)

Boot: `data/snapshot.dat` → load KV → seek WAL to snapshot LSN → replay in order.

## Error Handling

Errors are values, never `unwrap` on hot path.

- Bad frame / oversized op: drop conn, `ERR:OP_TOO_LARGE`, no WAL append.
- Slow reader: outbound >256KB → close conn, remove from all topics.
- WAL write error / disk full: reject `PUT/PUB` with `ERR:WAL_FULL`, keep `GET/SUB` from memory.
- Snapshot fail: keep old snapshot, keep WAL growing.
- epoll edge: drain until `WouldBlock`; `EPOLLERR/HUP` → close + cleanup subs.
- Shutdown: `SIGTERM` → stop accepting, flush WAL, `fdatasync`, snapshot, exit 0.

## Testing

- Unit: WAL encode/decode + crc, KV probe, ring buffer, all 3 parsers.
- Integration: `PUT` via TCP → `GET` via HTTP → `PUB` → `SUB` via WS.
  `kill -9`, restart, replay from snapshot+WAL, assert ≤0.5ms loss window.
- Fuzz: `cargo-fuzz` on all parsers, 24h no panic. Slow reader, disk-full,
  oversized op must not stall loop.
- Perf: Criterion + custom load gen — ops/sec + p50/p99/p99.9 for `PUT`,
  `GET`, `PUB+fanout(10)`. Report single-thread ceiling on target hardware.
- Soak: 10k idle conns + 1k active chat, 1h, watch RSS, fd count, drops.

## Sub-Projects (decomposition)

1. **Core engine** — this spec (KV + WAL + snapshots + pub/sub, single-threaded)
2. **Workloads** — chat schema, feed fanout, KV-watch, event-stream retention on top of #1
3. **Scale-out** — sharding, replication, consensus, multi-threaded fanout (later)

## Open Decisions for Planning

- WAL record: little-endian, max 64KB + header (fits max value 64KB).
- Snapshot threshold: 1M ops or 5 min, whichever first.
- `sync=true` is client-requested per op; default is async Ack.
- WS subprotocol for v1: JSON frames (binary reserved for raw TCP).
