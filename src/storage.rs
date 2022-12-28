use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("raft: cannot access the storage")]
    SomeError,
}

/// A result type that wraps up the raft errors.
pub type Result<T> = std::result::Result<T, Error>;

pub trait PersistentStateStorage: Send + Sync + 'static {
    fn new() -> Self;
    fn get(&self, key: String) -> Option<&Vec<u8>>;
    fn set(&mut self, key: String, value: Vec<u8>) -> Result<()>;
    fn has_data(&self) -> bool;
}

pub trait ApplyStorage: Send + Sync + 'static {
    fn new() -> Self;
    fn apply(&mut self, payload: Vec<u8>) -> Result<()>;
}