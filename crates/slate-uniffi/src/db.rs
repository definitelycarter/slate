use std::sync::Arc;

use slate_db::{Database, DatabaseConfig, DatabaseTransaction, DbError};
use slate_query::FindOptions;

use crate::error::SlateError;

// --- Feature-gated store imports and type alias ---

#[cfg(feature = "memory")]
type StoreImpl = slate_store::MemoryStore;

#[cfg(feature = "redb")]
type StoreImpl = slate_store::RedbStore;

#[cfg(feature = "rocksdb")]
type StoreImpl = slate_store::RocksStore;

type Db = Database<StoreImpl>;
type Txn<'a> = DatabaseTransaction<'a, StoreImpl>;

// --- SlateDatabase ---

#[derive(uniffi::Object)]
pub struct SlateDatabase {
    db: Db,
}

// Auto-commit helpers (mirrors session.rs pattern)
impl SlateDatabase {
    fn read<F, R>(&self, f: F) -> Result<R, SlateError>
    where
        F: FnOnce(&mut Txn<'_>) -> Result<R, DbError>,
    {
        let mut txn = self.db.begin(true).map_err(SlateError::from)?;
        let result = f(&mut txn).map_err(SlateError::from)?;
        Ok(result)
    }

    fn write<F, R>(&self, f: F) -> Result<R, SlateError>
    where
        F: FnOnce(&mut Txn<'_>) -> Result<R, DbError>,
    {
        let mut txn = self.db.begin(false).map_err(SlateError::from)?;
        let result = f(&mut txn).map_err(SlateError::from)?;
        txn.commit().map_err(SlateError::from)?;
        Ok(result)
    }

    fn parse_options(options: Option<Vec<u8>>) -> Result<FindOptions, SlateError> {
        match options {
            Some(bytes) => bson::deserialize_from_slice(&bytes).map_err(|e| SlateError::InvalidQuery {
                message: e.to_string(),
            }),
            None => Ok(FindOptions::default()),
        }
    }
}

#[cfg(feature = "memory")]
#[uniffi::export]
impl SlateDatabase {
    #[uniffi::constructor]
    pub fn memory() -> Arc<Self> {
        let store = slate_store::MemoryStore::new();
        let db = Database::open(store, DatabaseConfig::default());
        Arc::new(Self { db })
    }
}

#[cfg(any(feature = "redb", feature = "rocksdb"))]
#[uniffi::export]
impl SlateDatabase {
    #[uniffi::constructor]
    pub fn open(path: String) -> Result<Arc<Self>, SlateError> {
        let store =
            StoreImpl::open(std::path::Path::new(&path)).map_err(|e| SlateError::Store {
                message: e.to_string(),
            })?;
        let db = Database::open(store, DatabaseConfig::default());
        Ok(Arc::new(Self { db }))
    }
}

#[uniffi::export]
impl SlateDatabase {
    // --- Insert ---

    pub fn insert_one(&self, collection: String, doc: Vec<u8>) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.insert_one(&collection, doc)?.drain()?;
            Ok(affected)
        })
    }

    pub fn insert_many(
        &self,
        collection: String,
        docs: Vec<Vec<u8>>,
    ) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.insert_many(&collection, docs)?.drain()?;
            Ok(affected)
        })
    }

    // --- Query ---

    pub fn find(
        &self,
        collection: String,
        filter: Vec<u8>,
        options: Option<Vec<u8>>,
    ) -> Result<Vec<Vec<u8>>, SlateError> {
        let options = Self::parse_options(options)?;
        self.read(|txn| {
            let results: Vec<Vec<u8>> = txn
                .find(&collection, filter, options)?
                .iter()?
                .map(|r| r.map(|doc| doc.into_bytes()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(results)
        })
    }

    pub fn find_one(
        &self,
        collection: String,
        filter: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, SlateError> {
        self.read(|txn| {
            let raw = txn.find_one(&collection, filter)?;
            Ok(raw.map(|r| r.into_bytes()))
        })
    }

    // --- Update ---

    pub fn update_one(
        &self,
        collection: String,
        filter: Vec<u8>,
        update: Vec<u8>,
    ) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.update_one(&collection, filter, update)?.drain()?;
            Ok(affected)
        })
    }

    pub fn update_many(
        &self,
        collection: String,
        filter: Vec<u8>,
        update: Vec<u8>,
    ) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.update_many(&collection, filter, update)?.drain()?;
            Ok(affected)
        })
    }

    pub fn replace_one(
        &self,
        collection: String,
        filter: Vec<u8>,
        replacement: Vec<u8>,
    ) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn
                .replace_one(&collection, filter, replacement)?
                .drain()?;
            Ok(affected)
        })
    }

    // --- Delete ---

    pub fn delete_one(&self, collection: String, filter: Vec<u8>) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.delete_one(&collection, filter)?.drain()?;
            Ok(affected)
        })
    }

    pub fn delete_many(&self, collection: String, filter: Vec<u8>) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.delete_many(&collection, filter)?.drain()?;
            Ok(affected)
        })
    }

    // --- Count ---

    pub fn count(
        &self,
        collection: String,
        filter: Option<Vec<u8>>,
    ) -> Result<u64, SlateError> {
        let filter = filter.unwrap_or_else(|| bson::rawdoc! {}.into_bytes());
        self.read(|txn| {
            let count = txn.count(&collection, filter)?;
            Ok(count)
        })
    }

    // --- Bulk ---

    pub fn upsert_many(&self, collection: String, docs: Vec<Vec<u8>>) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.upsert_many(&collection, docs)?.drain()?;
            Ok(affected)
        })
    }

    pub fn merge_many(&self, collection: String, docs: Vec<Vec<u8>>) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.merge_many(&collection, docs)?.drain()?;
            Ok(affected)
        })
    }

    // --- Collections ---

    pub fn create_collection(&self, name: String, indexes: Vec<String>) -> Result<(), SlateError> {
        let config = slate_db::CollectionConfig { name, indexes };
        self.write(|txn| {
            txn.create_collection(&config)?;
            Ok(())
        })
    }

    pub fn drop_collection(&self, collection: String) -> Result<(), SlateError> {
        self.write(|txn| {
            txn.drop_collection(&collection)?;
            Ok(())
        })
    }

    pub fn list_collections(&self) -> Result<Vec<String>, SlateError> {
        self.read(|txn| {
            let collections = txn.list_collections()?;
            Ok(collections)
        })
    }

    // --- Indexes ---

    pub fn create_index(&self, collection: String, field: String) -> Result<(), SlateError> {
        self.write(|txn| {
            txn.create_index(&collection, &field)?;
            Ok(())
        })
    }

    pub fn drop_index(&self, collection: String, field: String) -> Result<(), SlateError> {
        self.write(|txn| {
            txn.drop_index(&collection, &field)?;
            Ok(())
        })
    }

    pub fn list_indexes(&self, collection: String) -> Result<Vec<String>, SlateError> {
        self.read(|txn| {
            let indexes = txn.list_indexes(&collection)?;
            Ok(indexes)
        })
    }
}
