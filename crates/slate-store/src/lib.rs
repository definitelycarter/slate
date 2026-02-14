mod error;
mod record;
mod store;

pub use error::StoreError;
pub use record::{Record, Value};
pub use store::{Store, Transaction};

#[cfg(feature = "rocksdb")]
mod rocks;

#[cfg(feature = "rocksdb")]
pub use rocks::RocksStore;
