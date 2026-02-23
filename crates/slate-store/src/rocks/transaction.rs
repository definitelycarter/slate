use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, Direction, IteratorMode, MultiThreaded, OptimisticTransactionDB, Options,
};

use crate::error::StoreError;
use crate::store::Transaction;

type DB = OptimisticTransactionDB<MultiThreaded>;

/// Pre-resolved column family handle for reads.
pub struct RocksCf<'db> {
    handle: Arc<BoundColumnFamily<'db>>,
}

pub struct RocksTransaction<'db> {
    txn: Option<rocksdb::Transaction<'db, DB>>,
    db: &'db DB,
    read_only: bool,
    cf_cache: RefCell<HashMap<String, Arc<BoundColumnFamily<'db>>>>,
}

impl<'db> RocksTransaction<'db> {
    pub fn new(db: &'db DB, read_only: bool) -> Result<Self, StoreError> {
        let txn = db.transaction();
        Ok(Self {
            txn: Some(txn),
            db,
            read_only,
            cf_cache: RefCell::new(HashMap::new()),
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

    /// Resolve a CF handle, caching it for reuse.
    fn cf_handle(&self, cf: &str) -> Result<Arc<BoundColumnFamily<'db>>, StoreError> {
        if let Some(handle) = self.cf_cache.borrow().get(cf) {
            return Ok(Arc::clone(handle));
        }
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        self.cf_cache
            .borrow_mut()
            .insert(cf.to_string(), Arc::clone(&handle));
        Ok(handle)
    }
}

impl<'db> Transaction for RocksTransaction<'db> {
    type Cf = RocksCf<'db>;

    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError> {
        let handle = self.cf_handle(name)?;
        Ok(RocksCf { handle })
    }

    fn get<'c>(&self, cf: &'c Self::Cf, key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        let data = self
            .txn()?
            .get_cf(&cf.handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(data.map(Cow::Owned))
    }

    fn multi_get<'c>(
        &self,
        cf: &'c Self::Cf,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        let txn = self.txn()?;
        let cf_keys: Vec<_> = keys.iter().map(|k| (&cf.handle, *k)).collect();
        let results = txn.multi_get_cf(cf_keys);
        results
            .into_iter()
            .map(|r| {
                r.map(|opt| opt.map(Cow::Owned))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            })
            .collect()
    }

    fn scan_prefix<'c>(
        &'c self,
        cf: &'c Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        let prefix_owned = prefix.to_vec();
        let iter = self
            .txn()?
            .iterator_cf(&cf.handle, IteratorMode::From(prefix, Direction::Forward));
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

    fn scan_prefix_rev<'c>(
        &'c self,
        cf: &'c Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        let prefix_owned = prefix.to_vec();
        let mut upper = prefix.to_vec();
        if let Some(last) = upper.last_mut() {
            *last = last.wrapping_add(1);
        }
        let iter = self
            .txn()?
            .iterator_cf(&cf.handle, IteratorMode::From(&upper, Direction::Reverse));
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

    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.txn()?
            .put_cf(&cf.handle, key, value)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let txn = self.txn()?;
        for (key, value) in entries {
            txn.put_cf(&cf.handle, key, value)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.txn()?
            .delete_cf(&cf.handle, key)
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
            self.cf_cache.borrow_mut().insert(name.to_string(), handle);
        }
        Ok(())
    }

    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        self.cf_cache.borrow_mut().remove(name);
        self.db
            .drop_cf(name)
            .map_err(|e| StoreError::Storage(e.to_string()))
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
