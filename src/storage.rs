pub trait PersistentStateStorage<K, V>: Send {
    fn get(&self, key: K) -> Option<&V>;
    fn set(&mut self, key: K, value: V);
}