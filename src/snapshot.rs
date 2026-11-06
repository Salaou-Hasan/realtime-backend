//! Task 7: Snapshot save/load.
//!
//! Spec §Snapshot + Compaction: a background snapshot copies the KV index
//! every 1M ops or 5 min, whichever first, via
//! `data/snapshot.dat.tmp` -> `fsync` -> `rename` to `data/snapshot.dat`.
//! WAL truncates only after `rename` succeeds; no in-place overwrite.
//! On snapshot failure the old snapshot is kept and WAL keeps growing.
//!
//! HIGH fix: errors are propagated as `Err`, never swallowed with
//! `unwrap_or_default()` / `unwrap_or("{}")`. In particular
//! [`KvIndex::from_json`] uses `unwrap_or_default`, so [`load_snapshot`]
//! deliberately does *not* call it: corrupt JSON must surface as
//! `Err(ErrorKind::InvalidData)`, not as a silently-empty index that
//! would lose every key on boot.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rustc_hash::FxHashMap;

use crate::kv::KvIndex;

/// Snapshot threshold: 1M ops or 5 min, whichever first (spec §Snapshot).
pub const SNAPSHOT_OP_THRESHOLD: u64 = 1_000_000;
pub const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(300);

/// Ops counter (threshold hook): ops applied since the last successful
/// snapshot. Callers increment via [`record_op`]; [`save_snapshot`]
/// resets it to 0 only after a successful rename.
static OPS_SINCE_SNAPSHOT: AtomicU64 = AtomicU64::new(0);

/// LSN of the last snapshot known to this process. The on-disk JSON
/// format stores only the KV map (no LSN field), so this stays 0 until
/// a caller records one via [`set_latest_snapshot_lsn`]; boot code
/// falls back to 0 (replay the whole WAL) when it is unset.
static LATEST_SNAPSHOT_LSN: AtomicU64 = AtomicU64::new(0);

/// Record one applied op against the snapshot threshold counter.
pub fn record_op() {
    OPS_SINCE_SNAPSHOT.fetch_add(1, Ordering::Relaxed);
}

/// Ops applied since the last successful snapshot.
pub fn ops_since_snapshot() -> u64 {
    OPS_SINCE_SNAPSHOT.load(Ordering::Relaxed)
}

/// Reset the ops counter (called by [`save_snapshot`] after success).
pub fn reset_ops_counter() {
    OPS_SINCE_SNAPSHOT.store(0, Ordering::Relaxed);
}

/// LSN of the last snapshot recorded in this process, 0 if none yet.
pub fn last_snapshot_lsn() -> u64 {
    LATEST_SNAPSHOT_LSN.load(Ordering::Relaxed)
}

/// Record the LSN a snapshot was taken at.
pub fn set_latest_snapshot_lsn(lsn: u64) {
    LATEST_SNAPSHOT_LSN.store(lsn, Ordering::Relaxed);
}

/// Threshold hook: should a background snapshot run now?
/// True when either 1M ops have accumulated or 5 min have elapsed.
pub fn should_snapshot(ops_since_last: u64, elapsed: Duration) -> bool {
    ops_since_last >= SNAPSHOT_OP_THRESHOLD || elapsed >= SNAPSHOT_INTERVAL
}

/// LSN to resume WAL replay from for the snapshot at `path`.
///
/// Fallback 0: a missing, unreadable, or corrupt snapshot means
/// "no snapshot" — replay from the start of the WAL. Never panics.
///
/// The current JSON format stores only the KV map, so a valid snapshot
/// that a caller has tagged via [`set_latest_snapshot_lsn`] reports
/// that LSN; an untagged valid snapshot also falls back to 0, which
/// is always safe (replaying already-snapshotted records is idempotent
/// for `Put`).
pub fn latest_snapshot_lsn(path: &str) -> u64 {
    match load_snapshot(path) {
        Ok(_) => last_snapshot_lsn(),
        Err(_) => 0,
    }
}

/// Save a snapshot atomically: write `{path}.tmp`, `sync_all`,
/// `rename` to `path`, then fsync the parent directory.
///
/// On any error the old snapshot (if any) is left untouched and the
/// ops counter is *not* reset, so the threshold hook will retry.
pub fn save_snapshot(kv: &KvIndex, path: &str) -> io::Result<()> {
    let target = Path::new(path);
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp = format!("{path}.tmp");
    // Propagate serialization errors as Err (never unwrap_or("{}")).
    let json = serde_json::to_string(kv.as_map())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    {
        let mut f = File::create(&tmp)?;
        f.write_all(json.as_bytes())?;
        f.sync_all()?;
        // File is dropped here, before rename (required on Windows).
    }

    fs::rename(&tmp, path)?;

    // Fsync the parent directory so the rename itself is durable.
    // Best-effort on non-Unix (opening a dir as a File fails on
    // Windows); propagate on Unix where it is meaningful.
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
            #[cfg(not(unix))]
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }

    reset_ops_counter();
    eprintln!(
        "snapshot: wrote {} entries to {}",
        kv.as_map().len(),
        path
    );
    Ok(())
}

/// Load a snapshot previously written by [`save_snapshot`].
///
/// Parse errors propagate as `Err(ErrorKind::InvalidData)` — this must
/// never fall back to an empty index via `unwrap_or_default`
/// (see [`KvIndex::from_json`]). A corrupt snapshot therefore fails
/// boot loudly instead of silently discarding all keys; callers that
/// want the "no snapshot" fallback can use [`latest_snapshot_lsn`]
/// or map `Err` to an empty index at the call site.
pub fn load_snapshot(path: &str) -> io::Result<KvIndex> {
    let s = fs::read_to_string(path)?;
    // Propagate parse errors as Err (never unwrap_or_default).
    let map: FxHashMap<String, Vec<u8>> = serde_json::from_str(&s)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Rebuild via the validated public API instead of from_json, so
    // oversized keys/values in a hand-crafted snapshot are rejected
    // as InvalidData rather than silently accepted or dropped.
    let mut kv = KvIndex::new();
    for (k, v) in map {
        kv.put(k, v).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid snapshot entry: {e}"))
        })?;
    }
    Ok(kv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64 as TestCounter;

    static TEST_SEQ: TestCounter = TestCounter::new(0);

    fn temp_path(name: &str) -> PathBuf {
        let n = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "realtime-snapshot-{}-{}-{}.dat",
            std::process::id(),
            name,
            n
        ))
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.tmp", path.display()));
    }

    #[test]
    fn snapshot_roundtrip() {
        let path = temp_path("roundtrip");
        cleanup(&path);
        let path_str = path.to_str().unwrap().to_string();

        let mut kv = KvIndex::new();
        kv.put("x".into(), vec![3u8]).unwrap();
        kv.put("y".into(), vec![1u8, 2, 3, 4]).unwrap();
        kv.put("empty".into(), Vec::new()).unwrap();

        save_snapshot(&kv, &path_str).unwrap();
        // No leftover tmp file after a successful rename.
        assert!(!Path::new(&format!("{path_str}.tmp")).exists());

        let back = load_snapshot(&path_str).unwrap();
        assert_eq!(back.get("x"), Some(&vec![3u8]));
        assert_eq!(back.get("y"), Some(&vec![1u8, 2, 3, 4]));
        assert_eq!(back.get("empty"), Some(&Vec::new()));
        assert_eq!(back.as_map().len(), 3);

        cleanup(&path);
    }

    #[test]
    fn corrupt_fails() {
        let path = temp_path("corrupt");
        cleanup(&path);
        let path_str = path.to_str().unwrap().to_string();

        fs::write(&path, "{ not valid json !!!").unwrap();
        let err = load_snapshot(&path_str).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // Truncated / wrong-shaped JSON must also fail, not yield
        // an empty index via unwrap_or_default.
        fs::write(&path, r#"{"a": "not-a-byte-array"}"#).unwrap();
        assert!(load_snapshot(&path_str).is_err());

        fs::write(&path, "").unwrap();
        assert!(load_snapshot(&path_str).is_err());

        cleanup(&path);
    }

    #[test]
    fn latest_snapshot_lsn_fallback_zero() {
        let path = temp_path("lsn");
        cleanup(&path);
        let path_str = path.to_str().unwrap().to_string();

        // Missing snapshot -> 0, never panics.
        assert_eq!(latest_snapshot_lsn(&path_str), 0);
        // Corrupt snapshot -> 0.
        fs::write(&path, "garbage").unwrap();
        assert_eq!(latest_snapshot_lsn(&path_str), 0);

        cleanup(&path);
    }

    #[test]
    fn snapshot_threshold_hook() {
        assert!(should_snapshot(SNAPSHOT_OP_THRESHOLD, Duration::ZERO));
        assert!(should_snapshot(0, SNAPSHOT_INTERVAL));
        assert!(!should_snapshot(SNAPSHOT_OP_THRESHOLD - 1, Duration::from_secs(1)));
    }
}
