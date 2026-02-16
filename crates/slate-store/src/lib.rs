mod error;
mod store;

pub use error::StoreError;
pub use store::{Store, Transaction};

#[cfg(feature = "rocksdb")]
mod rocks;

#[cfg(feature = "rocksdb")]
pub use rocks::RocksStore;

#[cfg(feature = "memory")]
mod memory;

#[cfg(feature = "memory")]
pub use memory::MemoryStore;
