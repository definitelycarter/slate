use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use arc_swap::ArcSwap;
use imbl::OrdMap;

use crate::error::StoreError;
use crate::store::Store;

use super::transaction::MemoryTransaction;

pub(crate) type ColumnFamily = OrdMap<Vec<u8>, Vec<u8>>;

pub struct MemoryStore {
    cfs: RwLock<HashMap<String, Arc<ArcSwap<ColumnFamily>>>>,
    write_lock: Mutex<()>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            cfs: RwLock::new(HashMap::new()),
            write_lock: Mutex::new(()),
        }
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the write lock. Only one write transaction can exist at a time.
    pub(crate) fn acquire_write_lock(&self) -> Result<MutexGuard<'_, ()>, StoreError> {
        self.write_lock
            .lock()
            .map_err(|e| StoreError::Storage(format!("write lock poisoned: {e}")))
    }

    /// Snapshot a single column family (lazy — called on first access).
    pub(crate) fn snapshot_cf(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        let cfs = self.cfs.read().unwrap();
        let arc_swap = cfs.get(name)?;
        Some(arc_swap.load_full())
    }

    /// Commit dirty CFs back to the store. The caller must already hold the
    /// write lock, so no conflict detection is needed.
    pub(crate) fn commit(&self, dirty: HashMap<String, Arc<ColumnFamily>>) {
        let cfs = self.cfs.read().unwrap();
        for (name, data) in dirty {
            if let Some(arc_swap) = cfs.get(&name) {
                arc_swap.store(data);
            }
        }
    }
}

impl Store for MemoryStore {
    type Txn<'a> = MemoryTransaction<'a>;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError> {
        if read_only {
            Ok(MemoryTransaction::new_read_only(self))
        } else {
            let guard = self.acquire_write_lock()?;
            Ok(MemoryTransaction::new_writable(self, guard))
        }
    }

    fn create_cf(&self, name: &str) -> Result<(), StoreError> {
        let mut cfs = self.cfs.write().unwrap();
        cfs.entry(name.to_string())
            .or_insert_with(|| Arc::new(ArcSwap::new(Arc::new(OrdMap::new()))));
        Ok(())
    }

    fn drop_cf(&self, name: &str) -> Result<(), StoreError> {
        let mut cfs = self.cfs.write().unwrap();
        cfs.remove(name);
        Ok(())
    }

    fn delete_range(&self, cf: &str, range: impl RangeBounds<Vec<u8>>) -> Result<(), StoreError> {
        let cfs = self.cfs.read().unwrap();
        let arc = cfs
            .get(cf)
            .ok_or_else(|| StoreError::Storage(format!("column family not found: {cf}")))?;

        let mut data = (**arc.load()).clone();

        let keys_to_delete: Vec<Vec<u8>> = data
            .range(range_to_ord_bounds(&range))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_delete {
            data.remove(&key);
        }

        arc.store(Arc::new(data));
        Ok(())
    }
}

/// Convert RangeBounds<Vec<u8>> to concrete bounds for OrdMap::range.
fn range_to_ord_bounds(range: &impl RangeBounds<Vec<u8>>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
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
    (start, end)
}
