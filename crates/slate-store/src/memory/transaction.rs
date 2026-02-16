use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::MutexGuard;

use crate::error::StoreError;
use crate::store::{Store, Transaction};

use super::store::{ColumnFamily, MemoryStore};

/// Lazily-loaded snapshot of column families.
struct Snapshot {
    data: HashMap<String, ColumnFamily>,
}

impl Snapshot {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Ensure a CF is loaded into the snapshot.
    fn ensure(&mut self, store: &MemoryStore, cf: &str) -> Result<(), StoreError> {
        if !self.data.contains_key(cf) {
            match store.snapshot_cf(cf) {
                Some(data) => {
                    self.data.insert(cf.to_string(), data);
                }
                None => {
                    return Err(StoreError::Storage(format!(
                        "column family not found: {cf}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn get_cf(&self, cf: &str) -> Result<&ColumnFamily, StoreError> {
        self.data
            .get(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))
    }

    fn get_cf_mut(&mut self, cf: &str) -> Result<&mut ColumnFamily, StoreError> {
        self.data
            .get_mut(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))
    }
}

pub struct MemoryTransaction<'a> {
    /// RefCell allows lazy loading from &self reads.
    snapshot: RefCell<Option<Snapshot>>,
    /// CFs that have been written to.
    dirty: HashSet<String>,
    store: &'a MemoryStore,
    read_only: bool,
    /// Write lock held for the duration of a write transaction.
    /// Dropped when the transaction is dropped (commit or rollback).
    _write_guard: Option<MutexGuard<'a, ()>>,
}

impl<'a> MemoryTransaction<'a> {
    pub(crate) fn new_read_only(store: &'a MemoryStore) -> Self {
        Self {
            snapshot: RefCell::new(Some(Snapshot::new())),
            dirty: HashSet::new(),
            store,
            read_only: true,
            _write_guard: None,
        }
    }

    pub(crate) fn new_writable(store: &'a MemoryStore, guard: MutexGuard<'a, ()>) -> Self {
        Self {
            snapshot: RefCell::new(Some(Snapshot::new())),
            dirty: HashSet::new(),
            store,
            read_only: false,
            _write_guard: Some(guard),
        }
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    /// Ensure a CF is loaded, then call a closure with a reference to it.
    fn with_cf<F, R>(&self, cf: &str, f: F) -> Result<R, StoreError>
    where
        F: FnOnce(&ColumnFamily) -> R,
    {
        let mut snap_opt = self.snapshot.borrow_mut();
        let snap = snap_opt.as_mut().ok_or(StoreError::TransactionConsumed)?;
        snap.ensure(self.store, cf)?;
        let data = snap.get_cf(cf)?;
        Ok(f(data))
    }

    /// Ensure a CF is loaded, then call a closure with a mutable reference.
    fn with_cf_mut<F, R>(&mut self, cf: &str, f: F) -> Result<R, StoreError>
    where
        F: FnOnce(&mut ColumnFamily) -> R,
    {
        let snap = self
            .snapshot
            .get_mut()
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.ensure(self.store, cf)?;
        let data = snap.get_cf_mut(cf)?;
        Ok(f(data))
    }
}

impl<'a> Transaction for MemoryTransaction<'a> {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError> {
        self.with_cf(cf, |data| {
            data.get(key).map(|v| v.clone().into_boxed_slice())
        })
    }

    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>, StoreError> {
        self.with_cf(cf, |data| {
            keys.iter()
                .map(|k| data.get(*k).map(|v| v.clone().into_boxed_slice()))
                .collect()
        })
    }

    fn scan_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), StoreError>> + '_>, StoreError>
    {
        // Ensure the CF is loaded before borrowing for the iterator.
        {
            let mut snap_opt = self.snapshot.borrow_mut();
            let snap = snap_opt.as_mut().ok_or(StoreError::TransactionConsumed)?;
            snap.ensure(self.store, cf)?;
        }

        // Now borrow immutably for the iterator lifetime.
        let snap_ref = self.snapshot.borrow();
        let prefix_owned = prefix.to_vec();

        // Collect because the borrow guard can't outlive this scope. The data
        // is already in memory (imbl clone is cheap).
        let snap = snap_ref.as_ref().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf(cf)?;
        let items: Vec<_> = data
            .range(prefix_owned.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix_owned))
            .map(|(k, v)| Ok((k.clone().into_boxed_slice(), v.clone().into_boxed_slice())))
            .collect();

        Ok(Box::new(items.into_iter()))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        self.with_cf_mut(cf, |data| {
            data.insert(key.to_vec(), value.to_vec());
        })
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        self.with_cf_mut(cf, |data| {
            for (key, value) in entries {
                data.insert(key.to_vec(), value.to_vec());
            }
        })
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        self.with_cf_mut(cf, |data| {
            data.remove(key);
        })
    }

    fn create_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        // Create on the store so it persists
        let _ = self.store.create_cf(name);
        // Add to snapshot so this transaction can use it immediately
        let snap = self
            .snapshot
            .get_mut()
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.data
            .entry(name.to_string())
            .or_insert_with(ColumnFamily::new);
        self.dirty.insert(name.to_string());
        Ok(())
    }

    fn commit(self) -> Result<(), StoreError> {
        let snapshot = self
            .snapshot
            .into_inner()
            .ok_or(StoreError::TransactionConsumed)?;

        if self.read_only {
            return Err(StoreError::ReadOnly);
        }

        // Extract only dirty CFs from the snapshot.
        let dirty: HashMap<String, ColumnFamily> = snapshot
            .data
            .into_iter()
            .filter(|(name, _)| self.dirty.contains(name))
            .collect();

        if dirty.is_empty() {
            return Ok(());
        }

        // Write lock is already held — just swap the dirty CFs.
        self.store.commit(dirty);
        // _write_guard drops here, releasing the write lock.
        Ok(())
    }

    fn rollback(self) -> Result<(), StoreError> {
        let _ = self
            .snapshot
            .into_inner()
            .ok_or(StoreError::TransactionConsumed)?;
        // _write_guard drops here, releasing the write lock.
        Ok(())
    }
}
