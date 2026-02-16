use std::collections::HashMap;
use std::sync::MutexGuard;

use crate::error::StoreError;
use crate::store::Transaction;

use super::store::{ColumnFamily, MemoryStore};

pub struct MemoryTransaction<'a> {
    snapshot: Option<HashMap<String, ColumnFamily>>,
    store: &'a MemoryStore,
    #[allow(dead_code)]
    write_guard: Option<MutexGuard<'a, ()>>,
    read_only: bool,
}

impl<'a> MemoryTransaction<'a> {
    pub(crate) fn new(
        store: &'a MemoryStore,
        snapshot: HashMap<String, ColumnFamily>,
        write_guard: Option<MutexGuard<'a, ()>>,
        read_only: bool,
    ) -> Self {
        Self {
            snapshot: Some(snapshot),
            store,
            write_guard,
            read_only,
        }
    }

    fn snapshot(&self) -> Result<&HashMap<String, ColumnFamily>, StoreError> {
        self.snapshot
            .as_ref()
            .ok_or(StoreError::TransactionConsumed)
    }

    fn snapshot_mut(&mut self) -> Result<&mut HashMap<String, ColumnFamily>, StoreError> {
        self.snapshot
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    fn get_cf(&self, cf: &str) -> Result<&ColumnFamily, StoreError> {
        self.snapshot()?
            .get(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))
    }

    fn get_cf_mut(&mut self, cf: &str) -> Result<&mut ColumnFamily, StoreError> {
        self.snapshot_mut()?
            .get_mut(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))
    }
}

impl<'a> Transaction for MemoryTransaction<'a> {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError> {
        let data = self.get_cf(cf)?;
        Ok(data.get(key).map(|v| v.clone().into_boxed_slice()))
    }

    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>, StoreError> {
        let data = self.get_cf(cf)?;
        Ok(keys
            .iter()
            .map(|k| data.get(*k).map(|v| v.clone().into_boxed_slice()))
            .collect())
    }

    fn scan_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), StoreError>> + '_>, StoreError>
    {
        let data = self.get_cf(cf)?;
        let prefix_owned = prefix.to_vec();

        let iter = data
            .range(prefix_owned.clone()..)
            .take_while(move |(k, _)| k.starts_with(&prefix_owned))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())));

        Ok(Box::new(iter))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let data = self.get_cf_mut(cf)?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        let data = self.get_cf_mut(cf)?;
        for (key, value) in entries {
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        let data = self.get_cf_mut(cf)?;
        data.remove(key);
        Ok(())
    }

    fn commit(mut self) -> Result<(), StoreError> {
        let snapshot = self
            .snapshot
            .take()
            .ok_or(StoreError::TransactionConsumed)?;

        if self.read_only {
            return Err(StoreError::ReadOnly);
        }

        for (name, data) in &snapshot {
            if self.store.cf_exists(name) {
                self.store.swap_cf(name, data.clone());
            }
        }

        Ok(())
    }

    fn rollback(mut self) -> Result<(), StoreError> {
        self.snapshot
            .take()
            .ok_or(StoreError::TransactionConsumed)?;
        Ok(())
    }
}
