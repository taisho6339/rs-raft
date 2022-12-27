use std::collections::HashMap;
use crate::storage::PersistentStateStorage;

pub struct InMemoryStorage {
    data: HashMap<String, Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let data = HashMap::new();
        Self {
            data
        }
    }
}

impl PersistentStateStorage<String, Vec<u8>> for InMemoryStorage {
    fn get(&self, key: String) -> Option<&Vec<u8>> {
        self.data.get(key.as_str())
    }

    fn set(&mut self, key: String, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    fn has_data(&self) -> bool {
        self.data.keys().len() > 0
    }
}