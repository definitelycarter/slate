use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, MutexGuard};

use crate::error::StoreError;
use crate::store::{Store, Transaction};

use super::store::{ColumnFamily, MemoryStore};

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
    snapshot: Option<Snapshot>,
    /// CFs that have been written to.
    dirty: HashSet<String>,
    store: &'a MemoryStore,
    read_only: bool,
    /// Write lock held for the duration of a write transaction.
    _write_guard: Option<MutexGuard<'a, ()>>,
}

impl<'a> MemoryTransaction<'a> {
    pub(crate) fn new_read_only(store: &'a MemoryStore) -> Self {
        Self {
            snapshot: Some(Snapshot::new()),
            dirty: HashSet::new(),
            store,
            read_only: true,
            _write_guard: None,
        }
    }

    pub(crate) fn new_writable(store: &'a MemoryStore, guard: MutexGuard<'a, ()>) -> Self {
        Self {
            snapshot: Some(Snapshot::new()),
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

    /// Ensure a CF is loaded, then return a mutable reference to it (for writes).
    fn ensure_cf_mut(&mut self, cf: &str) -> Result<&mut ColumnFamily, StoreError> {
        let snap = self
            .snapshot
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.ensure(self.store, cf)?;
        snap.get_cf_mut(cf)
    }
}

impl<'a> Transaction for MemoryTransaction<'a> {
    type Cf = Arc<ColumnFamily>;

    fn cf(&mut self, name: &str) -> Result<Self::Cf, StoreError> {
        let snap = self
            .snapshot
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.ensure(self.store, name)?;
        Ok(Arc::clone(snap.get_cf(name)?))
    }

    fn get<'c>(&self, cf: &'c Self::Cf, key: &[u8]) -> Result<Option<Cow<'c, [u8]>>, StoreError> {
        Ok(cf.get(key).map(|v| Cow::Borrowed(v.as_slice())))
    }

    fn multi_get<'c>(
        &self,
        cf: &'c Self::Cf,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Cow<'c, [u8]>>>, StoreError> {
        Ok(keys
            .iter()
            .map(|k| cf.get(*k).map(|v| Cow::Borrowed(v.as_slice())))
            .collect())
    }

    fn scan_prefix<'c>(
        &'c self,
        cf: &'c Self::Cf,
        prefix: &[u8],
    ) -> Result<
        Box<dyn Iterator<Item = Result<(Cow<'c, [u8]>, Cow<'c, [u8]>), StoreError>> + 'c>,
        StoreError,
    > {
        let prefix_vec = prefix.to_vec();

        Ok(Box::new(
            cf.range(prefix_vec.clone()..)
                .take_while(move |(k, _)| k.starts_with(&prefix_vec))
                .map(|(k, v)| Ok((Cow::Borrowed(k.as_slice()), Cow::Borrowed(v.as_slice())))),
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
        let lower = prefix.to_vec();
        let mut upper = lower.clone();
        if let Some(last) = upper.last_mut() {
            *last = last.wrapping_add(1);
        }

        Ok(Box::new(cf.range(lower..upper).rev().map(|(k, v)| {
            Ok((Cow::Borrowed(k.as_slice()), Cow::Borrowed(v.as_slice())))
        })))
    }

    fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        let data = self.ensure_cf_mut(cf)?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn put_batch(&mut self, cf: &str, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        let data = self.ensure_cf_mut(cf)?;
        for (key, value) in entries {
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(())
    }

    fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.dirty.insert(cf.to_string());
        let data = self.ensure_cf_mut(cf)?;
        data.remove(key);
        Ok(())
    }

    fn create_cf(&mut self, name: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        let _ = self.store.create_cf(name);
        let snap = self
            .snapshot
            .as_mut()
            .ok_or(StoreError::TransactionConsumed)?;
        snap.data
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(ColumnFamily::new()));
        self.dirty.insert(name.to_string());
        Ok(())
    }

    fn commit(self) -> Result<(), StoreError> {
        let snapshot = self.snapshot.ok_or(StoreError::TransactionConsumed)?;

        if self.read_only {
            return Err(StoreError::ReadOnly);
        }

        let dirty: HashMap<String, Arc<ColumnFamily>> = snapshot
            .data
            .into_iter()
            .filter(|(name, _)| self.dirty.contains(name))
            .collect();

        if dirty.is_empty() {
            return Ok(());
        }

        self.store.commit(dirty);
        Ok(())
    }

    fn rollback(self) -> Result<(), StoreError> {
        if self.snapshot.is_none() {
            return Err(StoreError::TransactionConsumed);
        }
        Ok(())
    }
}
