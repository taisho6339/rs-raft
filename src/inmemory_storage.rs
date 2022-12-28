use std::collections::HashMap;

use crate::storage::{ApplyStorage, PersistentStateStorage};

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

impl ApplyStorage for MockInMemoryStorage {
    fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    fn apply(&mut self, payload: Vec<u8>) -> crate::storage::Result<()> {
        // TODO:
        // self.data.insert(key, value);
        Ok(())
    }
}