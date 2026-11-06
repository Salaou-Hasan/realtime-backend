use crate::op::{ConnId, Error, MAX_SUBS};
use std::collections::VecDeque;

/// Maximum number of recent messages retained per topic.
pub const RING_CAP: usize = 100;

/// Messages larger than this are routed to slab storage instead of
/// inline `Vec` copies for fanout (8 KiB fanout bucket threshold).
pub const SLAB_THRESHOLD: usize = 8192;

/// A pub/sub topic: subscriber fanout list plus a bounded ring of
/// recent messages replayed to new subscribers.
#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub subs: Vec<ConnId>,
    pub recent: VecDeque<Vec<u8>>,
}

impl Topic {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            subs: Vec::new(),
            recent: VecDeque::with_capacity(RING_CAP),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Append `msg` to the ring, evicting the oldest message once the
    /// ring exceeds [`RING_CAP`].
    pub fn publish(&mut self, msg: Vec<u8>) {
        self.recent.push_back(msg);
        if self.recent.len() > RING_CAP {
            self.recent.pop_front();
        }
    }

    pub fn recent_len(&self) -> usize {
        self.recent.len()
    }

    /// Snapshot of recent messages, oldest first.
    pub fn recent(&self) -> Vec<Vec<u8>> {
        self.recent.iter().cloned().collect()
    }

    /// Fanout bucket: messages over 8 KiB go to slab, not `Vec` copies.
    pub fn needs_slab(&self, n: usize) -> bool {
        n > SLAB_THRESHOLD
    }

    /// Add a subscriber. Idempotent for an already-subscribed connection.
    /// Returns [`Error::OpTooLarge`] when the topic already has
    /// [`MAX_SUBS`] subscribers.
    pub fn add_sub(&mut self, c: ConnId) -> Result<(), Error> {
        if self.subs.contains(&c) {
            return Ok(());
        }
        if self.subs.len() >= MAX_SUBS {
            return Err(Error::OpTooLarge);
        }
        self.subs.push(c);
        Ok(())
    }

    pub fn remove_sub(&mut self, c: &ConnId) {
        self.subs.retain(|x| x.0 != c.0);
    }

    pub fn subs(&self) -> &[ConnId] {
        &self.subs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_last_100_and_fanout_bucket() {
        let mut t = Topic::new("chat");
        for i in 0..150u8 {
            t.publish(vec![i]);
        }
        assert_eq!(t.recent_len(), 100);
        assert_eq!(t.recent()[0], vec![50u8]);
        assert_eq!(t.recent()[99], vec![149u8]);
        assert!(t.needs_slab(vec![0u8; 9000].len()));
        assert!(!t.needs_slab(8192));
        assert!(t.needs_slab(8193));
        // Fanout bucket: subscribers are tracked for fanout.
        assert!(t.add_sub(ConnId(1)).is_ok());
        assert!(t.add_sub(ConnId(2)).is_ok());
        assert_eq!(t.subs().len(), 2);
        assert_eq!(t.subs(), &[ConnId(1), ConnId(2)]);
    }

    #[test]
    fn sub_limit() {
        let mut t = Topic::new("chat");
        for i in 0..MAX_SUBS as u64 {
            assert!(t.add_sub(ConnId(i)).is_ok());
        }
        assert_eq!(t.subs().len(), MAX_SUBS);
        // Overflow: adding a new subscriber beyond MAX_SUBS is rejected.
        assert_eq!(t.add_sub(ConnId(MAX_SUBS as u64)), Err(Error::OpTooLarge));
        assert_eq!(t.subs().len(), MAX_SUBS);
        // Re-adding an existing subscriber at the cap is idempotent.
        assert!(t.add_sub(ConnId(0)).is_ok());
        assert_eq!(t.subs().len(), MAX_SUBS);
        // Removing frees a slot.
        t.remove_sub(&ConnId(0));
        assert_eq!(t.subs().len(), MAX_SUBS - 1);
        assert!(t.add_sub(ConnId(MAX_SUBS as u64)).is_ok());
        assert_eq!(t.subs().len(), MAX_SUBS);
    }
}
