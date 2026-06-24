use std::ops::{Bound, RangeBounds};
use std::path::Path;

use rocksdb::{MultiThreaded, OptimisticTransactionDB, Options, checkpoint::Checkpoint};

use crate::error::StoreError;
use crate::store::{BackupStore, Durability, Store};

use super::transaction::RocksTransaction;

type DB = OptimisticTransactionDB<MultiThreaded>;

pub struct RocksStore {
    db: DB,
    default_durability: Durability,
}

impl RocksStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::open_with_durability(path, Durability::default())
    }

    /// Open a store with an explicit default durability applied to every write
    /// transaction (overridable per-transaction via
    /// [`Store::begin_with_durability`]).
    pub fn open_with_durability(
        path: &Path,
        default_durability: Durability,
    ) -> Result<Self, StoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = DB::list_cf(&opts, path).unwrap_or_default();

        let db = if cfs.is_empty() {
            DB::open(&opts, path)
        } else {
            DB::open_cf(&opts, path, &cfs)
        }
        .map_err(|e| StoreError::Storage(e.to_string()))?;

        Ok(Self {
            db,
            default_durability,
        })
    }

    pub fn db(&self) -> &DB {
        &self.db
    }
}

impl Store for RocksStore {
    type Txn<'a> = RocksTransaction<'a>;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError> {
        RocksTransaction::new(&self.db, read_only, self.default_durability)
    }

    fn begin_with_durability(&self, durability: Durability) -> Result<Self::Txn<'_>, StoreError> {
        // RocksDB bakes the flush policy into the transaction's WriteOptions at
        // creation, so resolve it up front rather than via the post-begin
        // `set_durability` default (which would recreate the inner txn).
        RocksTransaction::new(&self.db, false, durability)
    }

    fn default_durability(&self) -> Durability {
        self.default_durability
    }

    fn create_cf(&self, name: &str) -> Result<(), StoreError> {
        if self.db.cf_handle(name).is_some() {
            return Ok(());
        }
        let opts = Options::default();
        self.db
            .create_cf(name, &opts)
            .map_err(|e| StoreError::Storage(e.to_string()))
    }

    fn drop_cf(&self, name: &str) -> Result<(), StoreError> {
        self.db
            .drop_cf(name)
            .map_err(|e| StoreError::Storage(e.to_string()))
    }

    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;

        let from = match range.start_bound() {
            Bound::Included(b) => b.clone(),
            Bound::Excluded(b) => {
                let mut v = b.clone();
                v.push(0);
                v
            }
            Bound::Unbounded => vec![],
        };

        let to = match range.end_bound() {
            Bound::Included(b) => {
                let mut v = b.clone();
                v.push(0);
                v
            }
            Bound::Excluded(b) => b.clone(),
            Bound::Unbounded => vec![0xFF],
        };

        self.db
            .delete_range_cf(&cf_handle, &from, &to)
            .map_err(|e| StoreError::Storage(e.to_string()))
    }
}

impl BackupStore for RocksStore {
    fn backup(&self, dest: &Path) -> Result<(), StoreError> {
        let cp = Checkpoint::new(&self.db).map_err(|e| StoreError::Storage(e.to_string()))?;
        cp.create_checkpoint(dest)
            .map_err(|e| StoreError::Storage(e.to_string()))
    }
}
