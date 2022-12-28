use std::collections::HashMap;

use crate::storage::{ApplyStorage, PersistentStateStorage};
use crate::util::read_le_u64;

pub struct MockInMemoryStorage {
    data: HashMap<String, Vec<u8>>,
}

impl MockInMemoryStorage {
    pub fn new() -> Self {
        let data = HashMap::new();
        Self {
            data
        }
    }
}

impl PersistentStateStorage for MockInMemoryStorage {
    fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    fn get(&self, key: String) -> Option<&Vec<u8>> {
        self.data.get(key.as_str())
    }

    fn set(&mut self, key: String, value: Vec<u8>) -> crate::storage::Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    fn has_data(&self) -> bool {
        self.data.keys().len() > 0
    }
}

#[derive(Clone)]
pub struct MockInMemoryKeyValueStore {
    pub data: HashMap<String, u64>,
}

impl MockInMemoryKeyValueStore {
    pub fn new() -> Self {
        let data = HashMap::new();
        Self {
            data
        }
    }
}

impl ApplyStorage for MockInMemoryKeyValueStore {
    fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    // expected structure
    // key_size: 8 bytes
    // value_size: 8 bytes
    // key
    // value
    fn apply(&mut self, payload: Vec<u8>) -> crate::storage::Result<()> {
        let bytes = payload.as_slice();
        let key_size = read_le_u64(&mut &bytes[0..8]);
        let value_size = read_le_u64(&mut &bytes[8..16]);

        let key_start = 16 as usize;
        let key_end = (16 + key_size) as usize;
        let value_start = (16 + key_size) as usize;
        let value_end = (16 + key_size + value_size) as usize;
        let key = &bytes[key_start..key_end];
        let mut value = &bytes[value_start..value_end];

        let key = String::from_utf8(key.to_vec()).unwrap();
        let value = read_le_u64(&mut value);
        self.data.insert(key, value);

        Ok(())
    }
}