use std::path::Path;

use rocksdb::{OptimisticTransactionDB, Options};

use crate::error::StoreError;
use crate::store::Store;

use super::transaction::RocksTransaction;

pub struct RocksStore {
    db: OptimisticTransactionDB,
}

impl RocksStore {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = OptimisticTransactionDB::open(&opts, path)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(Self { db })
    }
}

impl Store for RocksStore {
    type Txn<'a> = RocksTransaction<'a>;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError> {
        RocksTransaction::new(&self.db, read_only)
    }
}
