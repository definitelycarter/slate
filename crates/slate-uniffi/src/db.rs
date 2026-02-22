use std::sync::Arc;

use slate_db::{Database, DatabaseConfig, DatabaseTransaction, DbError};
use slate_query::{FilterGroup, Query};

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

// --- Result types ---

#[derive(uniffi::Record)]
pub struct SlateUpdateResult {
    pub matched: u64,
    pub modified: u64,
    pub upserted_id: Option<String>,
}

#[derive(uniffi::Record)]
pub struct SlateUpsertResult {
    pub inserted: u64,
    pub updated: u64,
}

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

    pub fn insert_one(&self, collection: String, doc_json: String) -> Result<String, SlateError> {
        let doc: bson::Document = serde_json::from_str(&doc_json)?;
        self.write(|txn| {
            let result = txn.insert_one(&collection, doc)?;
            Ok(result.id)
        })
    }

    pub fn insert_many(
        &self,
        collection: String,
        docs_json: String,
    ) -> Result<Vec<String>, SlateError> {
        let docs: Vec<bson::Document> = serde_json::from_str(&docs_json)?;
        self.write(|txn| {
            let results = txn.insert_many(&collection, docs)?;
            Ok(results.into_iter().map(|r| r.id).collect())
        })
    }

    // --- Query ---

    pub fn find(&self, collection: String, query_json: String) -> Result<Vec<String>, SlateError> {
        let query: Query = serde_json::from_str(&query_json)?;
        self.read(|txn| {
            let raw_records: Vec<_> = txn
                .find(&collection, &query)?
                .iter()?
                .collect::<Result<Vec<_>, _>>()?;
            let mut results = Vec::with_capacity(raw_records.len());
            for raw in &raw_records {
                let doc = raw.to_document().map_err(DbError::from)?;
                let json = serde_json::to_string(&doc)
                    .map_err(|e| DbError::Serialization(e.to_string()))?;
                results.push(json);
            }
            Ok(results)
        })
    }

    pub fn find_one(
        &self,
        collection: String,
        query_json: String,
    ) -> Result<Option<String>, SlateError> {
        let query: Query = serde_json::from_str(&query_json)?;
        self.read(|txn| {
            let raw = txn.find_one(&collection, &query)?;
            match raw {
                Some(r) => {
                    let doc = r.to_document().map_err(DbError::from)?;
                    let json = serde_json::to_string(&doc)
                        .map_err(|e| DbError::Serialization(e.to_string()))?;
                    Ok(Some(json))
                }
                None => Ok(None),
            }
        })
    }

    pub fn find_by_id(
        &self,
        collection: String,
        id: String,
        columns: Option<Vec<String>>,
    ) -> Result<Option<String>, SlateError> {
        self.read(|txn| {
            let cols: Option<Vec<&str>> = columns
                .as_ref()
                .map(|c| c.iter().map(|s| s.as_str()).collect());
            let doc = txn.find_by_id(&collection, &id, cols.as_deref())?;
            match doc {
                Some(d) => {
                    let json = serde_json::to_string(&d)
                        .map_err(|e| DbError::Serialization(e.to_string()))?;
                    Ok(Some(json))
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
        update_json: String,
        upsert: bool,
    ) -> Result<SlateUpdateResult, SlateError> {
        let filter: FilterGroup = serde_json::from_str(&filter_json)?;
        let update: bson::Document = serde_json::from_str(&update_json)?;
        self.write(|txn| {
            let result = txn.update_one(&collection, &filter, update, upsert)?;
            Ok(SlateUpdateResult {
                matched: result.matched,
                modified: result.modified,
                upserted_id: result.upserted_id,
            })
        })
    }

    pub fn update_many(
        &self,
        collection: String,
        filter_json: String,
        update_json: String,
    ) -> Result<SlateUpdateResult, SlateError> {
        let filter: FilterGroup = serde_json::from_str(&filter_json)?;
        let update: bson::Document = serde_json::from_str(&update_json)?;
        self.write(|txn| {
            let result = txn.update_many(&collection, &filter, update)?;
            Ok(SlateUpdateResult {
                matched: result.matched,
                modified: result.modified,
                upserted_id: result.upserted_id,
            })
        })
    }

    pub fn replace_one(
        &self,
        collection: String,
        filter_json: String,
        doc_json: String,
    ) -> Result<SlateUpdateResult, SlateError> {
        let filter: FilterGroup = serde_json::from_str(&filter_json)?;
        let doc: bson::Document = serde_json::from_str(&doc_json)?;
        self.write(|txn| {
            let result = txn.replace_one(&collection, &filter, doc)?;
            Ok(SlateUpdateResult {
                matched: result.matched,
                modified: result.modified,
                upserted_id: result.upserted_id,
            })
        })
    }

    // --- Delete ---

    pub fn delete_one(&self, collection: String, filter_json: String) -> Result<u64, SlateError> {
        let filter: FilterGroup = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let result = txn.delete_one(&collection, &filter)?;
            Ok(result.deleted)
        })
    }

    pub fn delete_many(&self, collection: String, filter_json: String) -> Result<u64, SlateError> {
        let filter: FilterGroup = serde_json::from_str(&filter_json)?;
        self.write(|txn| {
            let result = txn.delete_many(&collection, &filter)?;
            Ok(result.deleted)
        })
    }

    // --- Count ---

    pub fn count(
        &self,
        collection: String,
        filter_json: Option<String>,
    ) -> Result<u64, SlateError> {
        let filter: Option<FilterGroup> = match filter_json {
            Some(json) => Some(serde_json::from_str(&json)?),
            None => None,
        };
        self.read(|txn| {
            let count = txn.count(&collection, filter.as_ref())?;
            Ok(count)
        })
    }

    // --- Bulk ---

    pub fn upsert_many(
        &self,
        collection: String,
        docs_json: String,
    ) -> Result<SlateUpsertResult, SlateError> {
        let docs: Vec<bson::Document> = serde_json::from_str(&docs_json)?;
        self.write(|txn| {
            let result = txn.upsert_many(&collection, docs)?;
            Ok(SlateUpsertResult {
                inserted: result.inserted,
                updated: result.updated,
            })
        })
    }

    pub fn merge_many(
        &self,
        collection: String,
        docs_json: String,
    ) -> Result<SlateUpsertResult, SlateError> {
        let docs: Vec<bson::Document> = serde_json::from_str(&docs_json)?;
        self.write(|txn| {
            let result = txn.merge_many(&collection, docs)?;
            Ok(SlateUpsertResult {
                inserted: result.inserted,
                updated: result.updated,
            })
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
