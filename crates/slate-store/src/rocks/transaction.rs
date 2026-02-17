use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, Direction, IteratorMode, MultiThreaded, OptimisticTransactionDB, Options,
};

use crate::error::StoreError;
use crate::store::Transaction;

type DB = OptimisticTransactionDB<MultiThreaded>;

pub struct RocksTransaction<'db> {
    txn: Option<rocksdb::Transaction<'db, DB>>,
    db: &'db DB,
    read_only: bool,
    cf_cache: HashMap<String, Arc<BoundColumnFamily<'db>>>,
}

impl<'db> RocksTransaction<'db> {
    pub fn new(db: &'db DB, read_only: bool) -> Result<Self, StoreError> {
        let txn = db.transaction();
        Ok(Self {
            txn: Some(txn),
            db,
            read_only,
            cf_cache: HashMap::new(),
        })
    }

    fn txn(&self) -> Result<&rocksdb::Transaction<'db, DB>, StoreError> {
        self.txn.as_ref().ok_or(StoreError::TransactionConsumed)
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    fn cf_handle(&mut self, cf: &str) -> Result<Arc<BoundColumnFamily<'db>>, StoreError> {
        if let Some(handle) = self.cf_cache.get(cf) {
            return Ok(Arc::clone(handle));
        }
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        self.cf_cache.insert(cf.to_string(), Arc::clone(&handle));
        Ok(handle)
    }
}

impl<'db> Transaction for RocksTransaction<'db> {
    fn get(&mut self, cf: &str, key: &[u8]) -> Result<Option<Cow<'_, [u8]>>, StoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let data = self
            .txn()?
            .get_cf(&cf_handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(data.map(|v| Cow::Owned(v)))
    }

    fn multi_get(
        &mut self,
        cf: &str,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'_, [u8]>>>, StoreError> {
        let cf_handle = self.cf_handle(cf)?;
        let txn = self.txn()?;
        let cf_keys: Vec<_> = keys.iter().map(|k| (&cf_handle, *k)).collect();
        let results = txn.multi_get_cf(cf_keys);
        results
            .into_iter()
            .map(|r| {
                r.map(|opt| opt.map(|v| Cow::Owned(v)))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            })
            .collect()
    }

    fn scan_prefix(
        &mut self,
        cf: &str,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'_, [u8]>, Cow<'_, [u8]>), StoreError>> + '_>,
        StoreError,
    > {
        let cf_handle = self.cf_handle(cf)?;
        let prefix_owned = prefix.to_vec();
        let iter = self
            .txn()?
            .iterator_cf(&cf_handle, IteratorMode::From(prefix, Direction::Forward));
        Ok(Box::new(
            iter.take_while(move |item| match item {
                Ok((key, _)) => key.starts_with(&prefix_owned),
                Err(_) => true,
            })
            .map(|item| {
                item.map(|(k, v)| (Cow::Owned(k.into_vec()), Cow::Owned(v.into_vec())))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            }),
        ))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self.cf_handle(cf)?;
        self.txn()?
            .put_cf(&cf_handle, key, value)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self.cf_handle(cf)?;
        let txn = self.txn()?;
        for (key, value) in entries {
            txn.put_cf(&cf_handle, key, value)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self.cf_handle(cf)?;
        self.txn()?
            .delete_cf(&cf_handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn create_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        if self.db.cf_handle(name).is_none() {
            let opts = Options::default();
            self.db
                .create_cf(name, &opts)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        // Pre-warm cache for the newly created CF
        if let Some(handle) = self.db.cf_handle(name) {
            self.cf_cache.insert(name.to_string(), handle);
        }
        Ok(())
    }

    fn commit(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.commit()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), StoreError> {
        let txn = self.txn.take().ok_or(StoreError::TransactionConsumed)?;
        txn.rollback()
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }
}
