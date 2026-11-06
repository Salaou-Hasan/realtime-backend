use crate::op::Error;
use crate::op::Op;
use crc::Crc;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const CRC: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
/// Record header: crc32 (4 bytes) + lsn (8 bytes) + ts (8 bytes).
pub const HEADER: usize = 4 + 8 + 8;

/// Maximum payload (op bytes) per record. Fits max value 64KB per spec.
pub const MAX_PAYLOAD: usize = 64 * 1024;
/// Maximum total encoded record size (header + payload).
pub const MAX_RECORD_SIZE: usize = HEADER + MAX_PAYLOAD;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: u64,
    pub ts: u64,
    pub op: Op,
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32, Error> {
    if buf.len() < offset + 4 {
        return Err(Error::BadFrame);
    }
    let bytes: [u8; 4] = buf[offset..offset + 4]
        .try_into()
        .map_err(|_| Error::BadFrame)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64, Error> {
    if buf.len() < offset + 8 {
        return Err(Error::BadFrame);
    }
    let bytes: [u8; 8] = buf[offset..offset + 8]
        .try_into()
        .map_err(|_| Error::BadFrame)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_string(buf: &[u8], offset: &mut usize) -> Result<String, Error> {
    let len = read_u32_le(buf, *offset)? as usize;
    *offset += 4;
    if buf.len() < *offset + len {
        return Err(Error::BadFrame);
    }
    let raw = &buf[*offset..*offset + len];
    let s = String::from_utf8(raw.to_vec()).map_err(|_| Error::BadFrame)?;
    *offset += len;
    Ok(s)
}

fn read_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>, Error> {
    let len = read_u32_le(buf, *offset)? as usize;
    *offset += 4;
    if buf.len() < *offset + len {
        return Err(Error::BadFrame);
    }
    let out = buf[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(out)
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

/// Encode an [`Op`] into a self-delimiting binary payload (little-endian).
///
/// Layout:
/// - tag: u8 (0=Put, 1=Get, 2=Pub, 3=Sub, 4=Unsub)
/// - Put: sync: u8, key_len: u32 LE + key, value_len: u32 LE + value
/// - Get: key_len: u32 LE + key
/// - Pub: sync: u8, topic_len: u32 LE + topic, value_len: u32 LE + value
/// - Sub/Unsub: topic_len: u32 LE + topic
pub fn encode_op(op: &Op) -> Vec<u8> {
    let mut buf = Vec::new();
    match op {
        Op::Put { key, value, sync } => {
            buf.push(0u8);
            buf.push(u8::from(*sync));
            write_string(&mut buf, key);
            write_bytes(&mut buf, value);
        }
        Op::Get { key } => {
            buf.push(1u8);
            write_string(&mut buf, key);
        }
        Op::Pub { topic, value, sync } => {
            buf.push(2u8);
            buf.push(u8::from(*sync));
            write_string(&mut buf, topic);
            write_bytes(&mut buf, value);
        }
        Op::Sub { topic } => {
            buf.push(3u8);
            write_string(&mut buf, topic);
        }
        Op::Unsub { topic } => {
            buf.push(4u8);
            write_string(&mut buf, topic);
        }
    }
    buf
}

/// Decode an [`Op`] from `buf`, returning the op and bytes consumed.
pub fn decode_op(buf: &[u8]) -> Result<(Op, usize), Error> {
    let mut offset = 0usize;
    if buf.is_empty() {
        return Err(Error::BadFrame);
    }
    let tag = buf[0];
    offset += 1;
    let op = match tag {
        0 => {
            if buf.len() < offset + 1 {
                return Err(Error::BadFrame);
            }
            let sync = buf[offset] != 0;
            offset += 1;
            let key = read_string(buf, &mut offset)?;
            let value = read_bytes(buf, &mut offset)?;
            Op::Put { key, value, sync }
        }
        1 => {
            let key = read_string(buf, &mut offset)?;
            Op::Get { key }
        }
        2 => {
            if buf.len() < offset + 1 {
                return Err(Error::BadFrame);
            }
            let sync = buf[offset] != 0;
            offset += 1;
            let topic = read_string(buf, &mut offset)?;
            let value = read_bytes(buf, &mut offset)?;
            Op::Pub { topic, value, sync }
        }
        3 => {
            let topic = read_string(buf, &mut offset)?;
            Op::Sub { topic }
        }
        4 => {
            let topic = read_string(buf, &mut offset)?;
            Op::Unsub { topic }
        }
        _ => return Err(Error::BadFrame),
    };
    Ok((op, offset))
}

/// Encode a [`WalRecord`] into `crc(4) + lsn(8 LE) + ts(8 LE) + op_payload`.
///
/// The CRC (CRC-32-ISCSI / CRC-32C, Castagnoli) is computed over
/// `lsn + ts + payload` (everything after the 4-byte CRC field).
///
/// Returns [`Error::OpTooLarge`] if the payload exceeds 64KB.
pub fn encode_record(r: &WalRecord) -> Result<Vec<u8>, Error> {
    let payload = encode_op(&r.op);
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::OpTooLarge);
    }
    let mut buf = Vec::with_capacity(4 + HEADER - 4 + payload.len());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&r.lsn.to_le_bytes());
    buf.extend_from_slice(&r.ts.to_le_bytes());
    buf.extend_from_slice(&payload);
    let crc = CRC.checksum(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Decode a single [`WalRecord`] from its full encoded buffer.
///
/// Returns [`Error::BadFrame`] on truncation, CRC mismatch, or
/// malformed/oversized payload.
pub fn decode_record(buf: &[u8]) -> Result<WalRecord, Error> {
    if buf.len() < HEADER {
        return Err(Error::BadFrame);
    }
    if buf.len() > MAX_RECORD_SIZE {
        return Err(Error::BadFrame);
    }
    let stored = read_u32_le(buf, 0)?;
    let actual = CRC.checksum(&buf[4..]);
    if stored != actual {
        return Err(Error::BadFrame);
    }
    let lsn = read_u64_le(buf, 4)?;
    let ts = read_u64_le(buf, 12)?;
    let (op, consumed) = decode_op(&buf[HEADER..])?;
    if HEADER + consumed != buf.len() {
        return Err(Error::BadFrame);
    }
    Ok(WalRecord { lsn, ts, op })
}

/// Decode a buffer containing concatenated [`WalRecord`]s.
///
/// Self-delimiting op payloads determine each record's boundary; every
/// record is CRC-verified. Returns [`Error::BadFrame`] on any bad frame.
pub fn decode_all(buf: &[u8]) -> Result<Vec<WalRecord>, Error> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < buf.len() {
        if buf.len() - offset < HEADER {
            return Err(Error::BadFrame);
        }
        // Payload length is discovered via the self-delimiting op encoding.
        let (_, consumed) = decode_op(&buf[offset + HEADER..])?;
        let total = HEADER + consumed;
        if total > MAX_RECORD_SIZE {
            return Err(Error::BadFrame);
        }
        if buf.len() - offset < total {
            return Err(Error::BadFrame);
        }
        out.push(decode_record(&buf[offset..offset + total])?);
        offset += total;
    }
    Ok(out)
}

/// Group-commit WAL writer (Task 3/6).
///
/// Buffers encoded records and flushes via `fdatasync` on group commit
/// (64KB or 0.5ms). `sync=true` ops force an immediate [`flush`](WalFile::flush)
/// before ack. WAL errors map to [`Error::WalFull`] so callers reject
/// Put/Pub but keep serving Get/Sub from memory (spec Error Handling).
/// Group-commit buffer threshold: flush when the buffer exceeds 64KB.
pub const GROUP_COMMIT_BYTES: usize = 64 * 1024;

/// Alias for [`GROUP_COMMIT_BYTES`] (64KB buffer, spec §WAL).
pub const WAL_BUF_CAP: usize = GROUP_COMMIT_BYTES;

/// Group-commit interval (spec §WAL): flush every 0.5ms or 64KB.
pub const WAL_FLUSH_INTERVAL: Duration = Duration::from_micros(500);

/// Decode the valid prefix of `raw`, stopping at the first
/// torn/corrupt frame (crash mid-write). Records are returned in
/// LSN order. Unlike [`decode_all`] (strict), this never errors on
/// a torn tail so boot-time replay can ignore a partial final
/// record.
fn decode_prefix(raw: &[u8]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while raw.len() - offset >= HEADER {
        let Ok((_, consumed)) = decode_op(&raw[offset + HEADER..]) else {
            break;
        };
        let total = HEADER + consumed;
        if total > MAX_RECORD_SIZE || raw.len() - offset < total {
            break;
        }
        let Ok(rec) = decode_record(&raw[offset..offset + total]) else {
            break;
        };
        out.push(rec);
        offset += total;
    }
    out.sort_by_key(|r| r.lsn);
    out
}

#[derive(Debug)]
pub struct WalFile {
    file: Option<File>,
    path: Option<String>,
    buf: Vec<u8>,
    persisted: Vec<u8>,
    lsn: u64,
    last_flush: Instant,
    flush_count: u64,
    fail_next: bool,
    cleanup_on_drop: bool,
}

impl Default for WalFile {
    fn default() -> Self {
        Self::open_in_memory()
    }
}

impl WalFile {
    /// In-memory WAL (no file). Used by `Core::new_in_memory` and tests.
    pub fn open_in_memory() -> Self {
        Self {
            file: None,
            path: None,
            buf: Vec::with_capacity(GROUP_COMMIT_BYTES),
            persisted: Vec::new(),
            lsn: 0,
            last_flush: Instant::now(),
            flush_count: 0,
            fail_next: false,
            cleanup_on_drop: false,
        }
    }

    /// Alias for [`open_in_memory`](WalFile::open_in_memory).
    #[allow(clippy::new_without_default)]
    pub fn new_in_memory() -> Self {
        Self::open_in_memory()
    }

    /// Temp-file WAL for tests. The file is created under the system
    /// temp dir with a unique name and removed on drop. Unlike
    /// [`open_in_memory`](WalFile::open_in_memory) this exercises the
    /// real `sync_data` path and [`replay`](WalFile::replay) file I/O.
    pub fn temp() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "realtime-wal-{}-{}-{}.log",
            std::process::id(),
            seq,
            nanos
        ));
        let mut wal =
            Self::open(path.to_string_lossy().into_owned()).expect("failed to create temp WAL");
        wal.cleanup_on_drop = true;
        wal
    }

    /// Open (or create) a file-backed WAL at `path`.
    pub fn open(path: impl Into<String>) -> Result<Self, Error> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|_| Error::WalFull)?;
        Ok(Self {
            file: Some(file),
            path: Some(path),
            buf: Vec::with_capacity(GROUP_COMMIT_BYTES),
            persisted: Vec::new(),
            lsn: 0,
            last_flush: Instant::now(),
            flush_count: 0,
            fail_next: false,
            cleanup_on_drop: false,
        })
    }

    /// Path of the underlying WAL file, or an empty path for the
    /// pure in-memory WAL. Useful for tests (`WalFile::temp`) and
    /// diagnostics.
    pub fn name(&self) -> &Path {
        self.path
            .as_deref()
            .map(Path::new)
            .unwrap_or_else(|| Path::new(""))
    }

    /// Alias for [`name`](WalFile::name).
    pub fn path(&self) -> &Path {
        self.name()
    }

    /// Test hook: next `append`/`flush` returns `WalFull` (disk-full mock).
    pub fn set_fail_next(&mut self, v: bool) {
        self.fail_next = v;
    }

    pub fn fail_next(&self) -> bool {
        self.fail_next
    }

    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }

    /// True when there is nothing buffered (all appended bytes flushed).
    pub fn is_flushed(&self) -> bool {
        self.buf.is_empty()
    }

    /// Append a record to the group-commit buffer. Returns
    /// [`Error::WalFull`] on (mocked) I/O failure and [`Error::OpTooLarge`]
    /// when the encoded record exceeds the payload cap. Does not
    /// write-through itself; `sync=true` callers must call
    /// [`flush`](WalFile::flush) explicitly.
    ///
    /// Group commit (spec §WAL): flushes pending data first if the
    /// buffer has been waiting >=0.5ms, and flushes before this
    /// record would overflow the 64KB buffer. WAL I/O errors surface
    /// as [`Error::WalFull`] (spec §Error Handling) so `Core` can
    /// reject Put/Pub while keeping Get/Sub servable.
    pub fn append(&mut self, r: &WalRecord) -> Result<(), Error> {
        if self.fail_next {
            self.fail_next = false;
            return Err(Error::WalFull);
        }
        // Time half of group commit: flush if pending bytes have
        // been waiting >=0.5ms since the last fdatasync.
        if !self.buf.is_empty() && self.last_flush.elapsed() >= WAL_FLUSH_INTERVAL {
            self.flush()?;
        }
        let bytes = encode_record(r)?;
        // Size half of group commit: flush if this record would
        // overflow the 64KB buffer.
        if self.buf.len() + bytes.len() > GROUP_COMMIT_BYTES && !self.buf.is_empty() {
            self.flush()?;
        }
        self.buf.extend_from_slice(&bytes);
        self.lsn = r.lsn;
        Ok(())
    }

    /// Write buffered bytes and `fdatasync` (group commit). No-op when
    /// the buffer is empty. In-memory WALs move bytes to `persisted`.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.fail_next {
            self.fail_next = false;
            return Err(Error::WalFull);
        }
        if let Some(file) = self.file.as_mut() {
            file.write_all(&self.buf).map_err(|_| Error::WalFull)?;
            file.sync_data().map_err(|_| Error::WalFull)?;
        }
        self.persisted.extend_from_slice(&self.buf);
        self.buf.clear();
        self.last_flush = Instant::now();
        self.flush_count += 1;
        Ok(())
    }

    /// Replay all records in LSN order. Used on boot to rebuild
    /// KV + topics. Flushes pending buffered bytes first, then
    /// reads the full file (file-backed) or the persisted bytes
    /// (in-memory). A torn tail from a crash mid-write terminates
    /// the scan; the valid prefix is returned.
    pub fn replay(&mut self) -> Result<Vec<WalRecord>, Error> {
        self.flush()?;
        let raw = if let Some(path) = self.path.clone() {
            let mut raw = Vec::new();
            File::open(&path)
                .map_err(|_| Error::WalFull)?
                .read_to_end(&mut raw)
                .map_err(|_| Error::WalFull)?;
            raw
        } else {
            let mut raw = self.persisted.clone();
            raw.extend_from_slice(&self.buf);
            raw
        };
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(decode_prefix(&raw))
    }

    /// All bytes still buffered (pending group-commit flush).
    pub fn buffered_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for WalFile {
    fn drop(&mut self) {
        // `WalFile::temp` shares the file with `open` but owns its
        // temp filename; remove it best-effort. A never-created file
        // (in-memory WAL) has no path to remove.
        if self.cleanup_on_drop {
            if let Some(path) = &self.path {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_roundtrip() {
        let rec = WalRecord {
            lsn: 7,
            ts: 99,
            op: Op::Put {
                key: "k".into(),
                value: vec![1u8],
                sync: false,
            },
        };
        let bytes = encode_record(&rec).unwrap();
        let back = decode_record(&bytes).unwrap();
        assert_eq!(back.lsn, 7);
        assert_eq!(back.ts, 99);
        match back.op {
            Op::Put { key, value, sync } => {
                assert_eq!(key, "k");
                assert_eq!(value, vec![1u8]);
                assert!(sync == false);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn crc_mismatch() {
        let rec = WalRecord {
            lsn: 3,
            ts: 42,
            op: Op::Put {
                key: "k".into(),
                value: vec![9u8],
                sync: false,
            },
        };
        let mut bytes = encode_record(&rec).unwrap();
        // Corrupt the last payload byte (value). Length prefixes are
        // untouched, so framing stays intact and only CRC catches it.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(decode_record(&bytes), Err(Error::BadFrame)));
    }

    #[test]
    fn op_roundtrip_all_variants() {
        let ops = [
            Op::Put {
                key: "k".into(),
                value: vec![1u8, 2, 3],
                sync: true,
            },
            Op::Get { key: "g".into() },
            Op::Pub {
                topic: "chat".into(),
                value: vec![7u8],
                sync: false,
            },
            Op::Sub {
                topic: "chat".into(),
            },
            Op::Unsub {
                topic: "chat".into(),
            },
        ];
        for (i, op) in ops.iter().enumerate() {
            let enc = encode_op(op);
            let (dec, consumed) = decode_op(&enc).unwrap();
            assert_eq!(consumed, enc.len());
            match (&dec, op) {
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
                _ => panic!("variant mismatch at index {i}"),
            }
        }
    }

    #[test]
    fn decode_all_concatenated() {
        let recs = vec![
            WalRecord {
                lsn: 1,
                ts: 10,
                op: Op::Put {
                    key: "a".into(),
                    value: vec![1u8],
                    sync: false,
                },
            },
            WalRecord {
                lsn: 2,
                ts: 11,
                op: Op::Pub {
                    topic: "t".into(),
                    value: vec![2u8, 3],
                    sync: true,
                },
            },
            WalRecord {
                lsn: 3,
                ts: 12,
                op: Op::Get { key: "a".into() },
            },
        ];
        let mut all = Vec::new();
        for r in &recs {
            all.extend_from_slice(&encode_record(r).unwrap());
        }
        let back = decode_all(&all).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back[0].lsn, 1);
        assert_eq!(back[1].lsn, 2);
        assert_eq!(back[2].lsn, 3);
        assert_eq!(back[1].ts, 11);
    }

    #[test]
    fn replay_restores_put() {
        let mut wal = WalFile::temp();
        wal.append(&WalRecord {
            lsn: 1,
            ts: 1,
            op: Op::Put {
                key: "k".into(),
                value: vec![9u8],
                sync: false,
            },
        })
        .unwrap();
        // Small append stays in the group-commit buffer until flushed.
        assert!(!wal.is_flushed());
        wal.flush().unwrap();
        assert!(wal.is_flushed());
        let replayed: Vec<WalRecord> = wal.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].lsn, 1);
        assert_eq!(replayed[0].ts, 1);
        match &replayed[0].op {
            Op::Put { key, value, sync } => {
                assert_eq!(key, "k");
                assert_eq!(value, &vec![9u8]);
                assert!(!*sync);
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn flush_size_threshold() {
        let mut wal = WalFile::temp();
        // Each record: HEADER(20) + 1(tag) + 1(sync) + 4 + 1(k) + 4 + 8192
        // = 8223 bytes. 20 records ~= 164KB >> 64KB buffer cap.
        // Group commit must auto-flush without any explicit flush().
        // A single record must stay buffered (not flushed yet).
        wal.append(&WalRecord {
            lsn: 1,
            ts: 0,
            op: Op::Put {
                key: "k".into(),
                value: vec![0u8; 8192],
                sync: false,
            },
        })
        .unwrap();
        assert!(!wal.is_flushed());
        assert!(wal.buffered_len() < WAL_BUF_CAP);

        for i in 1..20u64 {
            wal.append(&WalRecord {
                lsn: i + 1,
                ts: i,
                op: Op::Put {
                    key: "k".into(),
                    value: vec![i as u8; 8192],
                    sync: false,
                },
            })
            .unwrap();
        }

        // No explicit flush() was called: size/time group-commit
        // must have written at least one full 64KB chunk to disk,
        // and the buffer can never hold more than one chunk.
        let on_disk = std::fs::metadata(wal.name()).unwrap().len();
        assert!(on_disk >= WAL_BUF_CAP as u64, "on_disk={on_disk}");
        assert!(
            wal.buffered_len() < WAL_BUF_CAP,
            "buffered={}",
            wal.buffered_len()
        );
        assert_eq!(wal.replay().unwrap().len(), 20);
    }
}
