use slate_store::Transaction;

use crate::collection::CollectionConfig;
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

    /// Create a collection with the given config. Idempotent — if the collection
    /// already exists, this is a no-op.
    pub fn create_collection<T: Transaction>(
        &self,
        txn: &mut T,
        config: &CollectionConfig,
    ) -> Result<(), DbError> {
        let sys = txn.cf(SYS_CF)?;
        let key = col_key(&config.name);
        if txn.get(&sys, &key)?.is_none() {
            txn.create_cf(&config.name)?;
            let value = bson::to_vec(config)?;
            txn.put(&sys, &key, &value)?;
        }
        Ok(())
    }

    pub fn list_collections<T: Transaction>(
        &self,
        txn: &T,
        sys: &T::Cf,
    ) -> Result<Vec<String>, DbError> {
        let iter = txn.scan_prefix(sys, COL_PREFIX)?;
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
        let sys = txn.cf(SYS_CF)?;

        // Remove collection marker
        txn.delete(&sys, &col_key(name))?;

        // Remove all index metadata for this collection
        let prefix = idx_collection_prefix(name);
        let keys: Vec<Vec<u8>> = txn
            .scan_prefix(&sys, &prefix)?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(DbError::Store)?;
        for key in keys {
            txn.delete(&sys, &key)?;
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
        let sys = txn.cf(SYS_CF)?;
        let key = idx_key(collection, field);
        txn.put(&sys, &key, &[])?;
        Ok(())
    }

    pub fn drop_index<T: Transaction>(
        &self,
        txn: &mut T,
        collection: &str,
        field: &str,
    ) -> Result<(), DbError> {
        let sys = txn.cf(SYS_CF)?;
        let key = idx_key(collection, field);
        txn.delete(&sys, &key)?;
        Ok(())
    }

    pub fn list_indexes<T: Transaction>(
        &self,
        txn: &T,
        sys: &T::Cf,
        collection: &str,
    ) -> Result<Vec<String>, DbError> {
        let prefix = idx_collection_prefix(collection);
        let iter = txn.scan_prefix(sys, &prefix)?;
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
