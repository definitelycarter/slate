use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, MutexGuard};

use crate::error::StoreError;
use crate::store::{Store, Transaction};

use super::store::{ColumnFamily, MemoryStore};

/// Column family handle for the memory backend.
///
/// This is a lightweight name token. All reads go through the transaction's
/// snapshot so that writes within the same transaction are visible.
#[derive(Clone)]
pub struct MemoryCf {
    pub(crate) name: String,
}

/// Lazily-loaded snapshot of column families.
struct Snapshot {
    data: HashMap<String, Arc<ColumnFamily>>,
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

    fn get_cf(&self, cf: &str) -> Result<&Arc<ColumnFamily>, StoreError> {
        self.data
            .get(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))
    }

    fn get_cf_mut(&mut self, cf: &str) -> Result<&mut ColumnFamily, StoreError> {
        let arc = self
            .data
            .get_mut(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;
        Ok(Arc::make_mut(arc))
    }
}

pub struct MemoryTransaction<'a> {
    snapshot: RefCell<Option<Snapshot>>,
    /// CFs that have been written to.
    dirty: RefCell<HashSet<String>>,
    store: &'a MemoryStore,
    read_only: bool,
    /// Write lock held for the duration of a write transaction.
    _write_guard: Option<MutexGuard<'a, ()>>,
}

impl<'a> MemoryTransaction<'a> {
    pub(crate) fn new_read_only(store: &'a MemoryStore) -> Self {
        Self {
            snapshot: RefCell::new(Some(Snapshot::new())),
            dirty: RefCell::new(HashSet::new()),
            store,
            read_only: true,
            _write_guard: None,
        }
    }

    pub(crate) fn new_writable(store: &'a MemoryStore, guard: MutexGuard<'a, ()>) -> Self {
        Self {
            snapshot: RefCell::new(Some(Snapshot::new())),
            dirty: RefCell::new(HashSet::new()),
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
}

impl<'a> Transaction for MemoryTransaction<'a> {
    type Cf = MemoryCf;

    fn cf(&self, name: &str) -> Result<Self::Cf, StoreError> {
        let mut snap = self.snapshot.borrow_mut();
        let snap = snap.as_mut().ok_or(StoreError::TransactionConsumed)?;
        snap.ensure(self.store, name)?;
        Ok(MemoryCf {
            name: name.to_string(),
        })
    }

    fn get(&self, cf: &Self::Cf, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.snapshot.borrow();
        let snap = snap.as_ref().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf(&cf.name)?;
        Ok(data.get(key).cloned())
    }

    fn multi_get(&self, cf: &Self::Cf, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
        let snap = self.snapshot.borrow();
        let snap = snap.as_ref().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf(&cf.name)?;
        Ok(keys.iter().map(|k| data.get(*k).cloned()).collect())
    }

    fn scan_prefix<'b>(
        &'b self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'b>, StoreError>
    {
        let snap = self.snapshot.borrow();
        let snap_ref = snap.as_ref().ok_or(StoreError::TransactionConsumed)?;
        let data = snap_ref.get_cf(&cf.name)?;
        let prefix_vec = prefix.to_vec();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = data
            .range(prefix_vec.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix_vec))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Box::new(entries.into_iter().map(Ok)))
    }

    fn scan_prefix_rev<'b>(
        &'b self,
        cf: &Self::Cf,
        prefix: &[u8],
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'b>, StoreError>
    {
        let snap = self.snapshot.borrow();
        let snap_ref = snap.as_ref().ok_or(StoreError::TransactionConsumed)?;
        let data = snap_ref.get_cf(&cf.name)?;
        let lower = prefix.to_vec();
        let mut upper = lower.clone();
        if let Some(last) = upper.last_mut() {
            *last = last.wrapping_add(1);
        }
        let entries: Vec<(Vec<u8>, Vec<u8>)> = data
            .range(lower..upper)
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Box::new(entries.into_iter().map(Ok)))
    }

    fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.borrow_mut().insert(cf.name.clone());
        let mut snap = self.snapshot.borrow_mut();
        let snap = snap.as_mut().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf_mut(&cf.name)?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn put_batch(&self, cf: &Self::Cf, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.borrow_mut().insert(cf.name.clone());
        let mut snap = self.snapshot.borrow_mut();
        let snap = snap.as_mut().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf_mut(&cf.name)?;
        for (key, value) in entries {
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(())
    }

    fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.borrow_mut().insert(cf.name.clone());
        let mut snap = self.snapshot.borrow_mut();
        let snap = snap.as_mut().ok_or(StoreError::TransactionConsumed)?;
        let data = snap.get_cf_mut(&cf.name)?;
        data.remove(key);
        Ok(())
    }

    fn create_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        let _ = self.store.create_cf(name);
        let snap = self
            .snapshot
            .get_mut()
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.data
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(ColumnFamily::new()));
        self.dirty.get_mut().insert(name.to_string());
        Ok(())
    }

    fn drop_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        self.store.drop_cf(name)?;
        let snap = self
            .snapshot
            .get_mut()
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.data.remove(name);
        self.dirty.get_mut().remove(name);
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

        let dirty_set = self.dirty.into_inner();
        let dirty: HashMap<String, Arc<ColumnFamily>> = snapshot
            .data
            .into_iter()
            .filter(|(name, _)| dirty_set.contains(name))
            .collect();

        if dirty.is_empty() {
            return Ok(());
        }

        self.store.commit(dirty);
        Ok(())
    }

    fn rollback(self) -> Result<(), StoreError> {
        if self.snapshot.into_inner().is_none() {
            return Err(StoreError::TransactionConsumed);
        }
        Ok(())
    }
}
