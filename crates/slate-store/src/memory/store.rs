use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use imbl::OrdMap;

use crate::error::StoreError;
use crate::store::Store;

use super::transaction::MemoryTransaction;

pub(crate) type ColumnFamily = OrdMap<Vec<u8>, Vec<u8>>;

pub struct MemoryStore {
    cfs: RwLock<HashMap<String, Arc<ArcSwap<ColumnFamily>>>>,
    write_lock: Arc<Mutex<()>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            cfs: RwLock::new(HashMap::new()),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Snapshot all column families. Cheap due to imbl structural sharing.
    pub(crate) fn snapshot_cfs(&self) -> HashMap<String, ColumnFamily> {
        let cfs = self.cfs.read().unwrap();
        cfs.iter()
            .map(|(name, arc)| (name.clone(), (**arc.load()).clone()))
            .collect()
    }

    /// Swap a column family's data with a new OrdMap.
    pub(crate) fn swap_cf(&self, name: &str, data: ColumnFamily) {
        let cfs = self.cfs.read().unwrap();
        if let Some(arc) = cfs.get(name) {
            arc.store(Arc::new(data));
        }
    }

    /// Get a reference to the CF map for checking existence.
    pub(crate) fn cf_exists(&self, name: &str) -> bool {
        let cfs = self.cfs.read().unwrap();
        cfs.contains_key(name)
    }
}

impl Store for MemoryStore {
    type Txn<'a> = MemoryTransaction<'a>;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, StoreError> {
        let write_guard = if read_only {
            None
        } else {
            Some(
                self.write_lock
                    .lock()
                    .map_err(|e| StoreError::Storage(format!("write lock poisoned: {e}")))?,
            )
        };

        let snapshot = self.snapshot_cfs();

        Ok(MemoryTransaction::new(
            self,
            snapshot,
            write_guard,
            read_only,
        ))
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
