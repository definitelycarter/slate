use rocksdb::{Direction, IteratorMode, MultiThreaded, OptimisticTransactionDB};

use crate::error::StoreError;
use crate::store::Transaction;

type DB = OptimisticTransactionDB<MultiThreaded>;

pub struct RocksTransaction<'db> {
    txn: Option<rocksdb::Transaction<'db, DB>>,
    db: &'db DB,
    read_only: bool,
}

impl<'db> RocksTransaction<'db> {
    pub fn new(db: &'db DB, read_only: bool) -> Result<Self, StoreError> {
        let txn = db.transaction();
        Ok(Self {
            txn: Some(txn),
            db,
            read_only,
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
}

impl<'db> Transaction for RocksTransaction<'db> {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        let data = self
            .txn()?
            .get_cf(&cf_handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(data.map(|v| v.into_boxed_slice()))
    }

    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>, StoreError> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        let txn = self.txn()?;
        let cf_keys: Vec<_> = keys.iter().map(|k| (&cf_handle, *k)).collect();
        let results = txn.multi_get_cf(cf_keys);
        results
            .into_iter()
            .map(|r| {
                r.map(|opt| opt.map(|v| v.into_boxed_slice()))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            })
            .collect()
    }

    fn scan_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), StoreError>> + '_>, StoreError>
    {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        let prefix_owned = prefix.to_vec();
        let iter = self
            .txn()?
            .iterator_cf(&cf_handle, IteratorMode::From(prefix, Direction::Forward));
        Ok(Box::new(
            iter.take_while(move |item| match item {
                Ok((key, _)) => key.starts_with(&prefix_owned),
                Err(_) => true, // propagate errors
            })
            .map(|item| {
                item.map(|(k, v)| (k, v))
                    .map_err(|e| StoreError::Storage(e.to_string()))
            }),
        ))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        self.txn()?
            .put_cf(&cf_handle, key, value)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
        Ok(())
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        let txn = self.txn()?;
        for (key, value) in entries {
            txn.put_cf(&cf_handle, key, value)
                .map_err(|e| StoreError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        self.txn()?
            .delete_cf(&cf_handle, key)
            .map_err(|e| StoreError::Storage(e.to_string()))?;
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
