use std::sync::Arc;

use serde::Deserialize;
use slate_db::{Database, DatabaseConfig, DatabaseTransaction, DbError};
use slate_query::{Query, Sort};

use crate::error::SlateError;

/// JSON-deserializable query representation for the uniffi boundary.
#[derive(Deserialize)]
struct QueryJson {
    #[serde(default)]
    filter: Option<bson::Document>,
    #[serde(default)]
    sort: Vec<Sort>,
    #[serde(default)]
    skip: Option<usize>,
    #[serde(default)]
    take: Option<usize>,
    #[serde(default)]
    columns: Option<Vec<String>>,
}

impl QueryJson {
    fn into_query(self) -> Result<Query, bson::raw::Error> {
        let filter = self
            .filter
            .map(|d| bson::RawDocumentBuf::from_document(&d))
            .transpose()?;
        Ok(Query {
            filter,
            sort: self.sort,
            skip: self.skip,
            take: self.take,
            columns: self.columns,
        })
    }
}

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

    pub fn insert_one(&self, collection: String, doc: Vec<u8>) -> Result<String, SlateError> {
        self.write(|txn| {
            let result = txn.insert_one(&collection, doc)?;
            Ok(result.id)
        })
    }

    pub fn insert_many(
        &self,
        collection: String,
        docs: Vec<Vec<u8>>,
    ) -> Result<Vec<String>, SlateError> {
        self.write(|txn| {
            let results = txn.insert_many(&collection, docs)?;
            Ok(results.into_iter().map(|r| r.id).collect())
        })
    }

    // --- Query ---

    pub fn find(&self, collection: String, query_json: String) -> Result<Vec<Vec<u8>>, SlateError> {
        let query = serde_json::from_str::<QueryJson>(&query_json)
            .map_err(|e| SlateError::InvalidQuery {
                message: e.to_string(),
            })?
            .into_query()
            .map_err(|e| SlateError::InvalidQuery {
                message: e.to_string(),
            })?;
        self.read(|txn| {
            let results: Vec<Vec<u8>> = txn
                .find(&collection, query)?
                .iter()?
                .map(|r| r.map(|doc| doc.into_bytes()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(results)
        })
    }

    pub fn find_one(
        &self,
        collection: String,
        query_json: String,
    ) -> Result<Option<Vec<u8>>, SlateError> {
        let query = serde_json::from_str::<QueryJson>(&query_json)
            .map_err(|e| SlateError::InvalidQuery {
                message: e.to_string(),
            })?
            .into_query()
            .map_err(|e| SlateError::InvalidQuery {
                message: e.to_string(),
            })?;
        self.read(|txn| {
            let raw = txn.find_one(&collection, query)?;
            Ok(raw.map(|r| r.into_bytes()))
        })
    }

    pub fn find_by_id(
        &self,
        collection: String,
        id: String,
        columns: Option<Vec<String>>,
    ) -> Result<Option<Vec<u8>>, SlateError> {
        self.read(|txn| {
            let cols: Option<Vec<&str>> = columns
                .as_ref()
                .map(|c| c.iter().map(|s| s.as_str()).collect());
            let doc = txn.find_by_id(&collection, &id, cols.as_deref())?;
            match doc {
                Some(d) => {
                    let raw = bson::RawDocumentBuf::from_document(&d).map_err(DbError::from)?;
                    Ok(Some(raw.into_bytes()))
                }
                None => Ok(None),
            }
        })
    }

    // --- Update ---

    pub fn update_one(
        &self,
        collection: String,
        filter_json: String,
        update: Vec<u8>,
    ) -> Result<u64, SlateError> {
        let filter: bson::Document = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let affected = txn.update_one(&collection, &filter, update)?.drain()?;
            Ok(affected)
        })
    }

    pub fn update_many(
        &self,
        collection: String,
        filter_json: String,
        update: Vec<u8>,
    ) -> Result<u64, SlateError> {
        let filter: bson::Document = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let affected = txn.update_many(&collection, &filter, update)?.drain()?;
            Ok(affected)
        })
    }

    pub fn replace_one(
        &self,
        collection: String,
        filter_json: String,
        replacement: Vec<u8>,
    ) -> Result<u64, SlateError> {
        let filter: bson::Document = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let affected = txn.replace_one(&collection, &filter, replacement)?.drain()?;
            Ok(affected)
        })
    }

    // --- Delete ---

    pub fn delete_one(&self, collection: String, filter_json: String) -> Result<u64, SlateError> {
        let filter: bson::Document = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let affected = txn.delete_one(&collection, &filter)?.drain()?;
            Ok(affected)
        })
    }

    pub fn delete_many(&self, collection: String, filter_json: String) -> Result<u64, SlateError> {
        let filter: bson::Document = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let affected = txn.delete_many(&collection, &filter)?.drain()?;
            Ok(affected)
        })
    }

    // --- Count ---

    pub fn count(
        &self,
        collection: String,
        filter_json: Option<String>,
    ) -> Result<u64, SlateError> {
        let filter: Option<bson::RawDocumentBuf> = match filter_json {
            Some(json) => {
                let doc: bson::Document = serde_json::from_str(&json)?;
                Some(bson::RawDocumentBuf::from_document(&doc).map_err(|e| {
                    SlateError::InvalidQuery {
                        message: e.to_string(),
                    }
                })?)
            }
            None => None,
        };
        self.read(|txn| {
            let count = txn.count(&collection, filter)?;
            Ok(count)
        })
    }

    // --- Bulk ---

    pub fn upsert_many(
        &self,
        collection: String,
        docs: Vec<Vec<u8>>,
    ) -> Result<u64, SlateError> {
        self.write(|txn| {
            let affected = txn.upsert_many(&collection, docs)?.drain()?;
            Ok(affected)
        })
    }

    pub fn merge_many(
        &self,
        collection: String,
        docs: Vec<Vec<u8>>,
    ) -> Result<u64, SlateError> {
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
