// Task 8: mio event loop + Conn backpressure (Errors/Backpressure spec).
// Per-conn outbound cap 256KB: slow reader dropped, loop never blocks.
// Errors are values — no `unwrap` on the hot path.

use crate::op::{ConnId, OUT_CAP};

/// Outbound queue for a single connection.
///
/// `out` holds bytes pending flush. If appending a message would exceed
/// [`OUT_CAP`], [`Conn::enqueue`] returns `false` so the loop can drop
/// the slow reader without blocking.
#[derive(Debug, Clone, PartialEq)]
pub struct Conn {
    pub id: ConnId,
    pub out: Vec<u8>,
}

impl Conn {
    /// Creates an empty connection state for `id`.
    pub fn new(id: u64) -> Self {
        Self {
            id: ConnId(id),
            out: Vec::new(),
        }
    }

    /// Attempts to append `data` to the outbound queue.
    ///
    /// Returns `true` if the bytes were queued. Returns `false` (and
    /// queues nothing, never partial) if the addition would exceed
    /// [`OUT_CAP`] — the caller should drop/close the connection.
    pub fn enqueue(&mut self, data: &[u8]) -> bool {
        if data.is_empty() {
            return true;
        }
        // Checked arithmetic: no unwrap/panic on hot path.
        let Some(total) = self.out.len().checked_add(data.len()) else {
            return false;
        };
        if total > OUT_CAP {
            return false;
        }
        self.out.extend_from_slice(data);
        true
    }

    /// Drains the pending outbound buffer.
    ///
    /// Returns the number of bytes that were pending. The buffer is
    /// cleared but its capacity is retained for reuse.
    pub fn drain(&mut self) -> usize {
        let n = self.out.len();
        self.out.clear();
        n
    }

    /// Returns `true` if the outbound queue has reached [`OUT_CAP`].
    pub fn is_full(&self) -> bool {
        self.out.len() >= OUT_CAP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::OUT_CAP;

    #[test]
    fn slow_reader_drops_at_cap() {
        let mut conn = Conn::new(1);
        conn.out.extend_from_slice(&vec![0u8; OUT_CAP]);
        assert!(conn.is_full());
        assert!(!conn.enqueue(&[1u8]));
        // No partial append: length unchanged after rejected enqueue.
        assert_eq!(conn.out.len(), OUT_CAP);
    }

    #[test]
    fn enqueue_ok() {
        let mut conn = Conn::new(2);
        assert_eq!(conn.id, ConnId(2));
        assert!(conn.enqueue(b"hello"));
        assert!(conn.enqueue(b" world"));
        assert_eq!(conn.out, b"hello world");
        assert!(!conn.is_full());
        // Empty slice is a no-op success.
        assert!(conn.enqueue(&[]));
    }

    #[test]
    fn drain() {
        let mut conn = Conn::new(3);
        assert!(conn.enqueue(&[1u8, 2, 3, 4]));
        assert!(conn.enqueue(&[5u8, 6]));
        assert_eq!(conn.drain(), 6);
        assert!(conn.out.is_empty());
        assert_eq!(conn.drain(), 0);
        // Buffer reusable after drain.
        assert!(conn.enqueue(&[7u8]));
        assert_eq!(conn.out, vec![7u8]);
    }
}
