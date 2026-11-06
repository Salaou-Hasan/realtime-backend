//! `realtime-backend` binary: boot + SIGTERM shutdown (Task 12).
//!
//! Shutdown (spec §Error Handling): `SIGTERM` → stop accepting → flush
//! WAL (`fdatasync`) → snapshot → exit 0. The flush goes through
//! [`shutdown_flush`]; the snapshot is taken only when a snapshot file
//! already exists at [`SNAPSHOT_PATH`].

use realtime_backend::core::Core;
use realtime_backend::snapshot;
use realtime_backend::wal::WalFile;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Set by the SIGTERM handler once SIGTERM is received (spec §Shutdown).
pub static TERMINATED: AtomicBool = AtomicBool::new(false);

/// On-disk snapshot path (spec §Snapshot + Compaction).
pub const SNAPSHOT_PATH: &str = "data/snapshot.dat";

/// SIGTERM installation (Unix `signal(2)`; no-op elsewhere).
///
/// The handler body is a single atomic store, which is
/// async-signal-safe — no I/O, no allocation, no locking in the
/// handler. All flush/snapshot work happens on the parked thread.
#[cfg(unix)]
mod sigterm {
    use super::TERMINATED;
    use std::sync::atomic::Ordering;

    /// SIGTERM on Linux/macOS.
    const SIGTERM: i32 = 15;

    extern "C" fn on_sigterm(_sig: i32) {
        TERMINATED.store(true, Ordering::SeqCst);
    }

    extern "C" {
        /// Standard C library `signal(2)` — returns the previous handler.
        fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
    }

    /// Install the SIGTERM handler. Failure to install is non-fatal:
    /// the process still exits on default SIGTERM termination.
    pub fn install() {
        // SAFETY: `on_sigterm` only stores to an `AtomicBool`, which is
        // async-signal-safe and needs no cleanup; `signal` never fails
        // for a valid signum/handler pair, and its return value (the
        // previous handler) is intentionally ignored here.
        unsafe {
            signal(SIGTERM, on_sigterm);
        }
    }
}

#[cfg(not(unix))]
mod sigterm {
    /// No-op on non-Unix targets (Windows has no SIGTERM; use Ctrl-Break).
    pub fn install() {}
}

/// `true` once SIGTERM has been received.
pub fn terminated() -> bool {
    TERMINATED.load(Ordering::SeqCst)
}

/// Flush the WAL as part of shutdown (spec §Shutdown: `fdatasync`).
///
/// Maps the WAL's [`realtime_backend::op::Error`] to an [`io::Error`]
/// so shutdown reports I/O failures uniformly.
pub fn shutdown_flush(wal: &mut WalFile) -> io::Result<()> {
    wal.flush()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("wal flush failed: {e}")))
}

/// Full shutdown (spec §Shutdown): flush the WAL, then take a fresh
/// snapshot when one already exists at [`SNAPSHOT_PATH`].
pub fn shutdown(core: &mut Core) -> io::Result<()> {
    shutdown_flush(&mut core.wal)?;
    if Path::new(SNAPSHOT_PATH).exists() {
        snapshot::save_snapshot(&core.kv, SNAPSHOT_PATH)?;
    }
    Ok(())
}

fn main() {
    println!("realtime-backend");
    sigterm::install();

    let mut core = Core::new_in_memory();

    // Park until SIGTERM. The mio run loop is not wired yet (later task);
    // this loop is the only wait so `shutdown` can run on signal.
    while !terminated() {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    match shutdown(&mut core) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("realtime-backend: shutdown failed: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realtime_backend::adapters::{http::parse_http, tcp::decode_frame, ws::decode_ws_json};
    use realtime_backend::op::Op;
    use realtime_backend::wal::WalRecord;

    #[test]
    fn sigterm_flushes_wal() {
        let mut wal = WalFile::temp();
        wal.append(&WalRecord {
            lsn: 1,
            ts: 1,
            op: Op::Put {
                key: "k".into(),
                value: vec![1u8],
                sync: false,
            },
        })
        .unwrap();
        // Group-commit buffer holds the record until flush.
        assert!(!wal.is_flushed());
        shutdown_flush(&mut wal).unwrap();
        assert!(wal.is_flushed());
    }

    #[test]
    fn empty_input_ok() {
        // Empty input is a value error, never a panic, for every parser.
        assert!(decode_frame(&[]).is_err());
        assert!(decode_ws_json("").is_err());
        assert!(parse_http("").is_err());
    }
}
