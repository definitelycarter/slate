use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::StoreError;
use crate::store::{BackupStore, Durability, Store};

use super::transaction::RedbTransaction;

pub struct RedbStore {
    db: Database,
    path: PathBuf,
    default_durability: Durability,
}

impl RedbStore {
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
        let db = Database::create(path).map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(Self {
            db,
            path: path.to_path_buf(),
            default_durability,
        })
    }
}

impl Store for RedbStore {
    type Txn<'a> = RedbTransaction<'a>;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError> {
        RedbTransaction::new(&self.db, read_only, self.default_durability)
    }

    fn default_durability(&self) -> Durability {
        self.default_durability
    }

    fn create_cf(&self, name: &str) -> Result<(), StoreError> {
        let name = name.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        txn.open_table(def)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        txn.commit()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn drop_cf(&self, name: &str) -> Result<(), StoreError> {
        let name = name.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        txn.delete_table(def)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        txn.commit()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError> {
        let cf = cf.to_string();
        let def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&cf);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(def)
                .map_err(|e| StoreError::Storage(e.to_string()))?;

            let start = match range.start_bound() {
                Bound::Included(b) => Bound::Included(b.clone()),
                Bound::Excluded(b) => Bound::Excluded(b.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let end = match range.end_bound() {
                Bound::Included(b) => Bound::Included(b.clone()),
                Bound::Excluded(b) => Bound::Excluded(b.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            // Collect keys first to avoid borrow conflict with remove()
            let keys: Vec<Vec<u8>> = table
                .range::<&[u8]>((
                    start.as_ref().map(|v| v.as_slice()),
                    end.as_ref().map(|v| v.as_slice()),
                ))
                .map_err(|e| StoreError::Storage(e.to_string()))?
                .map(|entry| {
                    let (k, _) = entry.unwrap();
                    k.value().to_vec()
                })
                .collect();

            for key in &keys {
                table
                    .remove(key.as_slice())
                    .map_err(|e| StoreError::Storage(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl BackupStore for RedbStore {
    fn backup(&self, dest: &Path) -> Result<(), StoreError> {
        std::fs::copy(&self.path, dest)
            .map_err(|e| StoreError::Storage(format!("backup failed: {e}")))?;
        Ok(())
    }
}
