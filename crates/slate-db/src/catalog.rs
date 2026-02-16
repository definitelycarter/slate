use slate_store::Transaction;

use crate::error::DbError;

const SYS_CF: &str = "_sys";
const IDX_PREFIX: &[u8] = b"__idx__:";
const COL_PREFIX: &[u8] = b"__col__:";

fn col_key(name: &str) -> Vec<u8> {
    let mut key = COL_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

fn idx_key(collection: &str, field: &str) -> Vec<u8> {
    let mut key = IDX_PREFIX.to_vec();
    key.extend_from_slice(collection.as_bytes());
    key.push(b':');
    key.extend_from_slice(field.as_bytes());
    key
}

fn idx_collection_prefix(collection: &str) -> Vec<u8> {
    let mut key = IDX_PREFIX.to_vec();
    key.extend_from_slice(collection.as_bytes());
    key.push(b':');
    key
}

pub struct Catalog;

impl Catalog {
    // ── Collections ─────────────────────────────────────────────

    /// Ensure a collection exists: create its column family and write a marker.
    pub fn ensure_collection<T: Transaction>(
        &self,
        txn: &mut T,
        name: &str,
    ) -> Result<(), DbError> {
        let key = col_key(name);
        if txn.get(SYS_CF, &key)?.is_none() {
            txn.create_cf(name)?;
            txn.put(SYS_CF, &key, &[])?;
        }
        Ok(())
    }

    pub fn list_collections<T: Transaction>(&self, txn: &mut T) -> Result<Vec<String>, DbError> {
        let iter = txn.scan_prefix(SYS_CF, COL_PREFIX)?;
        let mut collections = Vec::new();
        for result in iter {
            let (key, _) = result?;
            if let Some(name) = key.strip_prefix(COL_PREFIX) {
                if let Ok(s) = std::str::from_utf8(name) {
                    collections.push(s.to_string());
                }
            }
        }
        Ok(collections)
    }

    pub fn drop_collection<T: Transaction>(&self, txn: &mut T, name: &str) -> Result<(), DbError> {
        // Remove collection marker
        txn.delete(SYS_CF, &col_key(name))?;

        // Remove all index metadata for this collection
        let prefix = idx_collection_prefix(name);
        let keys: Vec<Vec<u8>> = txn
            .scan_prefix(SYS_CF, &prefix)?
            .filter_map(|r| r.ok().map(|(k, _)| k.to_vec()))
            .collect();
        for key in keys {
            txn.delete(SYS_CF, &key)?;
        }

        Ok(())
    }

    // ── Indexes ─────────────────────────────────────────────────

    pub fn create_index<T: Transaction>(
        &self,
        txn: &mut T,
        collection: &str,
        field: &str,
    ) -> Result<(), DbError> {
        let key = idx_key(collection, field);
        txn.put(SYS_CF, &key, &[])?;
        Ok(())
    }

    pub fn drop_index<T: Transaction>(
        &self,
        txn: &mut T,
        collection: &str,
        field: &str,
    ) -> Result<(), DbError> {
        let key = idx_key(collection, field);
        txn.delete(SYS_CF, &key)?;
        Ok(())
    }

    pub fn list_indexes<T: Transaction>(
        &self,
        txn: &mut T,
        collection: &str,
    ) -> Result<Vec<String>, DbError> {
        let prefix = idx_collection_prefix(collection);
        let iter = txn.scan_prefix(SYS_CF, &prefix)?;
        let mut fields = Vec::new();
        for result in iter {
            let (key, _) = result?;
            if let Some(rest) = key.strip_prefix(prefix.as_slice()) {
                if let Ok(field) = std::str::from_utf8(rest) {
                    fields.push(field.to_string());
                }
            }
        }
        Ok(fields)
    }
}
