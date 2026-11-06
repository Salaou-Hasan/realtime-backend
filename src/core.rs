// Task 6: Core { kv, topics, wal } + apply(op).
// Data Flow (spec §Data Flow): WAL first for Put/Pub, then in-memory
// index/topic update. sync=true forces an immediate fdatasync before Ack.
// WAL errors reject Put/Pub with WalFull and keep Get/Sub servable.

use crate::kv::KvIndex;
use crate::op::{validate_key_len, validate_value_len, Error, Op};
use crate::pubsub::Topic;
use crate::wal::{WalFile, WalRecord};
use rustc_hash::FxHashMap;

pub struct Core {
    pub kv: KvIndex,
    pub topics: FxHashMap<String, Topic>,
    pub wal: WalFile,
    pub lsn: u64,
}

impl Core {
    pub fn new_in_memory() -> Self {
        Self {
            kv: KvIndex::new(),
            topics: FxHashMap::default(),
            wal: WalFile::open_in_memory(),
            lsn: 0,
        }
    }

    /// Current LSN (number of WAL-appended ops so far).
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Borrow the topic if present.
    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.topics.get(name)
    }

    /// Apply a single op. Returns `Some(b"OK")` for ack'd Put/Pub/Sub/Unsub,
    /// the value for Get (`None` on miss), or an error.
    pub fn apply(&mut self, op: Op) -> Result<Option<Vec<u8>>, Error> {
        // Validate before WAL append: oversized ops must not be logged.
        match &op {
            Op::Put { key, value, .. } => {
                validate_key_len(key.len())?;
                validate_value_len(value.len())?;
            }
            Op::Pub { value, .. } => {
                validate_value_len(value.len())?;
            }
            Op::Get { key } => {
                validate_key_len(key.len())?;
            }
            Op::Sub { topic } | Op::Unsub { topic } => {
                validate_key_len(topic.len())?;
            }
        }

        let sync = matches!(&op, Op::Put { sync: true, .. } | Op::Pub { sync: true, .. });

        // WAL first (Put/Pub only). On WalFull, reject without touching
        // in-memory state.
        if matches!(&op, Op::Put { .. } | Op::Pub { .. }) {
            let lsn = self.lsn.checked_add(1).ok_or(Error::WalFull)?;
            self.lsn = lsn;
            let rec = WalRecord {
                lsn,
                ts: 0,
                op: op.clone(),
            };
            self.wal.append(&rec)?;
            if sync {
                self.wal.flush()?;
            }
        }

        match op {
            Op::Put { key, value, .. } => {
                self.kv.put(key, value)?;
                Ok(Some(b"OK".to_vec()))
            }
            Op::Get { key } => Ok(self.kv.get(&key).cloned()),
            Op::Pub { topic, value, .. } => {
                self.topics
                    .entry(topic.clone())
                    .or_insert_with(|| Topic::new(topic.clone()))
                    .publish(value);
                Ok(Some(b"OK".to_vec()))
            }
            Op::Sub { topic } => {
                self.topics
                    .entry(topic.clone())
                    .or_insert_with(|| Topic::new(topic.clone()));
                Ok(Some(b"OK".to_vec()))
            }
            Op::Unsub { .. } => Ok(Some(b"OK".to_vec())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::ConnId;

    #[test]
    fn core_put_get_and_pubsub() {
        let mut core = Core::new_in_memory();
        // Put -> Get roundtrip (async, sync=false)
        assert_eq!(
            core.apply(Op::Put {
                key: "k".into(),
                value: vec![1u8, 2, 3],
                sync: false
            })
            .unwrap(),
            Some(b"OK".to_vec())
        );
        assert_eq!(
            core.apply(Op::Get { key: "k".into() }).unwrap(),
            Some(vec![1u8, 2, 3])
        );
        // Miss returns None
        assert_eq!(
            core.apply(Op::Get {
                key: "missing".into()
            })
            .unwrap(),
            None
        );
        // Overwrite
        core.apply(Op::Put {
            key: "k".into(),
            value: vec![9u8],
            sync: false,
        })
        .unwrap();
        assert_eq!(
            core.apply(Op::Get { key: "k".into() }).unwrap(),
            Some(vec![9u8])
        );
        // PubSub: Sub creates topic, Pub appends to ring
        assert_eq!(
            core.apply(Op::Sub {
                topic: "chat".into()
            })
            .unwrap(),
            Some(b"OK".to_vec())
        );
        assert_eq!(
            core.apply(Op::Pub {
                topic: "chat".into(),
                value: vec![42u8],
                sync: false
            })
            .unwrap(),
            Some(b"OK".to_vec())
        );
        let t = core.topic("chat").expect("chat topic");
        assert_eq!(t.recent_len(), 1);
        assert_eq!(t.recent()[0], vec![42u8]);
        // Unsub acks OK
        assert_eq!(
            core.apply(Op::Unsub {
                topic: "chat".into()
            })
            .unwrap(),
            Some(b"OK".to_vec())
        );
        // Sub tracking still available via Topic::add_sub (needs ConnId)
        let t2 = core.topics.get_mut("chat").unwrap();
        t2.add_sub(ConnId(1)).unwrap();
        assert_eq!(t2.subs().len(), 1);
    }

    #[test]
    fn sync_flush() {
        let mut core = Core::new_in_memory();
        // Async put: buffered, not flushed
        core.apply(Op::Put {
            key: "a".into(),
            value: vec![1u8],
            sync: false,
        })
        .unwrap();
        assert!(!core.wal.is_flushed());
        assert!(core.wal.buffered_len() > 0);
        let before = core.wal.flush_count();
        // Sync put: forces flush before ack
        core.apply(Op::Put {
            key: "b".into(),
            value: vec![2u8],
            sync: true,
        })
        .unwrap();
        assert!(core.wal.is_flushed());
        assert!(core.wal.flush_count() > before);
        // Sync pub: forces flush before ack
        core.apply(Op::Pub {
            topic: "t".into(),
            value: vec![3u8],
            sync: false,
        })
        .unwrap();
        assert!(!core.wal.is_flushed());
        core.apply(Op::Pub {
            topic: "t".into(),
            value: vec![4u8],
            sync: true,
        })
        .unwrap();
        assert!(core.wal.is_flushed());
        // Get/Sub never touch WAL flush state beyond prior sync
        let count = core.wal.flush_count();
        core.apply(Op::Get { key: "a".into() }).unwrap();
        core.apply(Op::Sub { topic: "t".into() }).unwrap();
        assert_eq!(core.wal.flush_count(), count);
    }

    #[test]
    fn wal_full_propagates() {
        let mut core = Core::new_in_memory();
        // Mock disk-full: next WAL append fails
        core.wal.set_fail_next(true);
        let r = core.apply(Op::Put {
            key: "k".into(),
            value: vec![7u8],
            sync: false,
        });
        assert_eq!(r, Err(Error::WalFull));
        // KV untouched because WAL rejected first
        assert_eq!(core.kv.get("k"), None);
        // Pub also propagates and does not create/append to topic
        core.wal.set_fail_next(true);
        assert_eq!(
            core.apply(Op::Pub {
                topic: "chat".into(),
                value: vec![1u8],
                sync: false
            }),
            Err(Error::WalFull)
        );
        assert!(core.topic("chat").is_none());
        // Get/Sub still served from memory after WAL failure
        assert_eq!(core.apply(Op::Get { key: "k".into() }).unwrap(), None);
        assert!(core
            .apply(Op::Sub {
                topic: "chat".into()
            })
            .is_ok());
        // Mock consumed: next write succeeds again
        assert!(core
            .apply(Op::Put {
                key: "k".into(),
                value: vec![8u8],
                sync: false
            })
            .is_ok());
        assert_eq!(
            core.apply(Op::Get { key: "k".into() }).unwrap(),
            Some(vec![8u8])
        );
    }
}
