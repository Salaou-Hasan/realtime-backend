use crate::op::{validate_key_len, validate_value_len, Error};
use rustc_hash::FxHashMap;

#[derive(Default, Debug)]
pub struct KvIndex {
    map: FxHashMap<String, Vec<u8>>,
}

impl KvIndex {
    pub fn new() -> Self {
        Self {
            map: FxHashMap::default(),
        }
    }

    pub fn put(&mut self, k: String, v: Vec<u8>) -> Result<(), Error> {
        validate_key_len(k.len())?;
        validate_value_len(v.len())?;
        self.map.insert(k, v);
        Ok(())
    }

    pub fn get(&self, k: &str) -> Option<&Vec<u8>> {
        self.map.get(k)
    }

    pub fn as_map(&self) -> &FxHashMap<String, Vec<u8>> {
        &self.map
    }

    pub fn from_json(s: &str) -> Self {
        Self {
            map: serde_json::from_str(s).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_put_get() {
        let mut kv = KvIndex::new();
        kv.put("a".into(), vec![1u8, 2u8]).unwrap();
        assert_eq!(kv.get("a"), Some(&vec![1u8, 2u8]));
    }

    #[test]
    fn kv_overwrites() {
        let mut kv = KvIndex::new();
        kv.put("a".into(), vec![1u8]).unwrap();
        kv.put("a".into(), vec![2u8, 3u8]).unwrap();
        assert_eq!(kv.get("a"), Some(&vec![2u8, 3u8]));
        assert_eq!(kv.as_map().len(), 1);
    }

    #[test]
    fn kv_oversized_rejected() {
        let mut kv = KvIndex::new();
        // Oversized value (> MAX_VALUE = 65536) rejected, not stored.
        let big_value = vec![0u8; crate::op::MAX_VALUE + 1];
        assert_eq!(
            kv.put("k".into(), big_value),
            Err(Error::OpTooLarge)
        );
        assert_eq!(kv.get("k"), None);

        // Oversized key (> MAX_KEY = 4096) rejected, not stored.
        let big_key = "x".repeat(crate::op::MAX_KEY + 1);
        assert_eq!(
            kv.put(big_key.clone(), vec![1u8]),
            Err(Error::OpTooLarge)
        );
        assert_eq!(kv.get(&big_key), None);

        // Boundary: exactly max sizes accepted.
        let max_key = "y".repeat(crate::op::MAX_KEY);
        let max_value = vec![7u8; crate::op::MAX_VALUE];
        assert!(kv.put(max_key.clone(), max_value.clone()).is_ok());
        assert_eq!(kv.get(&max_key), Some(&max_value));
    }
}
