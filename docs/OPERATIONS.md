# Operations — Realtime Backend

Single-node, single-threaded core (`mio` epoll). Data lives in `data/`
(`snapshot.dat` + `wal.log`). See `docs/specs/2026-11-06-realtime-backend-design.md`
for design; this page is field tuning only.

## Enforced limits (mirror `src/op.rs` — do not retune here)

| Knob | Value | Source |
| --- | --- | --- |
| `MAX_CONNS` | 100,000 | `src/op.rs` |
| `MAX_SUBS` / `MAX_SUBS_PER_TOPIC` | 10,000 | `src/op.rs` |
| `OUT_CAP` (per-conn outbound) | 256 KiB | `src/op.rs` |
| `MAX_KEY` | 4 KiB | `src/op.rs` |
| `MAX_VALUE` | 64 KiB | `src/op.rs` |
| Pub/sub ring | last 100 msgs/topic | spec §Pub/Sub |
| Fanout slab threshold | >8 KiB | spec §Fanout buckets |

Over-limit ops are rejected (`ERR:OP_TOO_LARGE`), never appended to WAL.
Slow readers (>256 KiB queued) are dropped — the loop never blocks.

## WAL tuning

- Record: little-endian `crc32c + len + lsn + ts + op`, buffered in **64 KiB** chunks.
- **Group commit: flush every 0.5 ms or 64 KiB, `fdatasync`**; `fsync` only on snapshot.
- Durability: async `Ack` loses ≤0.5 ms of ops on crash. `sync=true` per op waits
  for the next `fdatasync` (durable, ~+0.5 ms p99).
- On `WAL write error / disk full`: `PUT`/`PUB` get `ERR:WAL_FULL`; `GET`/`SUB` still
  serve from memory. Alert on `wal.log` growth + `ENOSPC` before rejecting.
- Boot: `snapshot.dat` → load KV → seek WAL to snapshot LSN → replay in order.
  `kill -9` + restart must recover with only the ≤0.5 ms async-Ack window lost.

## Snapshot intervals

- Trigger: **every 1M ops or 5 min, whichever first** (background thread only).
- Write `data/snapshot.dat.tmp` → `fsync` → `rename` to `data/snapshot.dat`.
- **WAL truncates only after `rename` succeeds.** Never overwrite in place.
- On snapshot failure keep the old snapshot; WAL keeps growing. Page ops if
  `.tmp` lingers >2× the interval.

## Soak test

Spec: 10k idle conns + 1k active chat, 1 h; watch RSS, fd count, drops.

```bash
# Dry-run (CI-safe, no sockets):
./tools/soak.sh --dry-run

# Full run against a local server (or let the script boot one):
./tools/soak.sh --binary ./target/release/realtime-backend --duration 3600
```

Pass criteria: no RSS/fd leak after warmup, no panics, chat p99 within bench
budget, slow-reader drops only (see `benches/throughput.rs` for throughput).

## Shutdown

`SIGTERM` → stop accepting → flush WAL (`fdatasync`) → snapshot → exit 0.
