use std::ops::RangeBounds;
use std::path::Path;

use crate::error::StoreError;

/// The durability guarantee a committed transaction makes about surviving a
/// crash. Chosen at the [`Store`] level (a builder default) and optionally
/// overridden per-transaction via [`Transaction::set_durability`].
///
/// Each level maps onto every persistent backend's native control; the
/// guarantee column is what `Ok(commit())` promises about data on disk if the
/// power dies one instruction later:
///
/// | Level      | RocksDB             | redb                  | Guarantee on `Ok(commit)`              |
/// |------------|---------------------|-----------------------|----------------------------------------|
/// | `Strict`   | `WriteOptions` sync | `Durability::Immediate` | fsync'd; survives power loss          |
/// | `Buffered` | default WAL, no sync | `Durability::Eventual`  | survives process crash, not power loss |
/// | `Relaxed`  | `disable_wal`       | `Durability::None`      | survives neither; fastest             |
///
/// `MemoryStore` is ephemeral, so the level is inert there (every commit is
/// equally non-durable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// fsync the commit before returning. Survives power loss. Slowest.
    Strict,
    /// Write through the backend's normal path (WAL on, no fsync). Survives a
    /// process crash but not power loss. The default — the balance a typical
    /// embedded workload wants.
    #[default]
    Buffered,
    /// Skip the durability machinery entirely (no WAL / no flush). Survives
    /// neither a process crash nor power loss. Fastest; for rebuildable data.
    Relaxed,
}

/// Increment a prefix byte-string to produce an exclusive upper bound.
///
/// Returns `None` when the entire prefix is `0xFF` (no upper bound exists).
pub(crate) fn increment_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for byte in upper.iter_mut().rev() {
        match byte.checked_add(1) {
            Some(incremented) => {
                *byte = incremented;
                return Some(upper);
            }
            None => *byte = 0x00, // carry
        }
    }
    None // all 0xFF — no upper bound exists
}

pub trait Store {
    type Txn<'a>: Transaction
    where
        Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError>;

    /// Begin a write transaction with an explicit durability level, overriding
    /// the store's default for this one transaction.
    ///
    /// The default implementation begins a normal transaction and sets the level
    /// via [`Transaction::set_durability`]; backends that resolve durability at
    /// `begin` time (rather than `commit`) may override this. `read_only` is
    /// always `false` here — durability only concerns writes.
    fn begin_with_durability(&self, durability: Durability) -> Result<Self::Txn<'_>, StoreError> {
        let mut txn = self.begin(false)?;
        txn.set_durability(durability);
        Ok(txn)
    }

    /// The store-wide default durability applied to every [`begin`](Self::begin)
    /// write transaction. Backends with a configurable default override this;
    /// the trait default is [`Durability::Buffered`].
    fn default_durability(&self) -> Durability {
        Durability::Buffered
    }

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

#[allow(clippy::type_complexity)]
pub trait Transaction {
    /// Backend-specific column family handle.
    /// Must be cheaply cloneable (all backends use Arc-based handles).
    type Cf: Clone;

    /// Resolve a column family by name. Must be called before any reads on that CF.
    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError>;

    // Reads
    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StoreError>;
    fn scan_prefix<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;
    fn scan_prefix_rev<'a>(
        &'a self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;
    /// Scan the key `range` within a column family, in the given direction.
    ///
    /// Yields every `(key, value)` whose key falls in `range`, ascending when
    /// `reverse` is false and descending when true — `reverse` only flips order
    /// over the same set. Bound style mirrors `Store::delete_range`.
    fn scan_range<'a, R: RangeBounds<Vec<u8>>>(
        &'a self,
        cf: &Self::Cf,
        range: R,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'a>, StoreError>;

    // Writes
    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError>;
    fn delete_batch(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<(), StoreError>;

    // Schema
    fn create_cf(&self, name: &str) -> Result<(), StoreError>;
    fn drop_cf(&self, name: &str) -> Result<(), StoreError>;

    // Durability

    /// Set this transaction's durability level, overriding the store default for
    /// this transaction's [`commit`](Self::commit). Has effect only before
    /// commit; calling it on a read-only transaction is a harmless no-op (a read
    /// makes no durability promise).
    ///
    /// The default implementation does nothing — appropriate for ephemeral
    /// backends (`MemoryStore`) where every commit is equally non-durable.
    /// Persistent backends override it to drive their native flush control.
    fn set_durability(&mut self, _durability: Durability) {}

    // Lifecycle
    fn commit(self) -> Result<(), StoreError>;
    fn rollback(self) -> Result<(), StoreError>;
}

/// Optional trait for stores that support physical backup.
///
/// Physical backup copies the store's native files to a destination path.
/// The backup can be opened with the same backend as a new database.
/// Safe to call while the database is live (online backup).
pub trait BackupStore: Store {
    fn backup(&self, dest: &Path) -> Result<(), StoreError>;
}
