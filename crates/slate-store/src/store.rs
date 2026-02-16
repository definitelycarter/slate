use std::ops::RangeBounds;

use crate::error::StoreError;

pub trait Store {
    type Txn<'a>: Transaction
    where
        Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;
    fn create_cf(&self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&self, name: &str) -> Result<(), StoreError>;
    /// Deletes all keys in the given range within a column family.
    ///
    /// This operates outside of transactions — concurrent transaction iterators
    /// won't see the deletes (they hold a snapshot), but they also won't conflict
    /// on commit, meaning a transaction could re-insert keys that were just wiped.
    ///
    /// Best used for user-level pruning (e.g. clearing a single user's cache),
    /// not global operations while transactions are in flight.
    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError>;
}

pub trait Transaction {
    // Reads
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError>;
    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>, StoreError>;
    fn scan_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), StoreError>> + '_>, StoreError>;

    // Writes
    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError>;

    // Schema
    fn create_cf(&mut self, name: &str) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
