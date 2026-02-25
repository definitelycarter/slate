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
    /// Backend-specific column family handle.
    /// Must be cheaply cloneable (all backends use Arc-based handles).
    type Cf: Clone;

    /// Resolve a column family by name. Must be called before any reads on that CF.
    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError>;

    // Reads
    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    fn multi_get(
        &self,
        cf: &Self::Cf,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, StoreError>;
    fn scan_prefix<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>,
        StoreError,
    >;
    fn scan_prefix_rev<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>,
        StoreError,
    >;

    // Writes
    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError>;

    // Schema
    fn create_cf(&mut self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError>;

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}
