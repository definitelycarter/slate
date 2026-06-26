use std::sync::Arc;

use bson::RawDocumentBuf;
use slate_db::v2::IndexOptions;
use slate_db::{Database, DatabaseBuilder, DatabaseTransaction, DbError, FindOptions};

use crate::error::SlateError;

fn parse_doc(bytes: Vec<u8>) -> Result<RawDocumentBuf, SlateError> {
    RawDocumentBuf::from_bytes(bytes).map_err(|e| SlateError::Serialization {
        message: e.to_string(),
    })
}

fn parse_docs(docs: Vec<Vec<u8>>) -> Result<Vec<RawDocumentBuf>, SlateError> {
    docs.into_iter().map(parse_doc).collect()
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

    fn parse_options(options: Option<Vec<u8>>) -> Result<FindOptions, SlateError> {
        match options {
            Some(bytes) => {
                bson::deserialize_from_slice(&bytes).map_err(|e| SlateError::InvalidQuery {
                    message: e.to_string(),
                })
            }
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
        let db = DatabaseBuilder::new()
            .open(store)
            .expect("failed to open database");
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
        let db = DatabaseBuilder::new()
            .open(store)
            .map_err(SlateError::from)?;
        Ok(Arc::new(Self { db }))
    }
}

#[uniffi::export]
impl SlateDatabase {
    // --- Insert ---

    pub fn insert_one(&self, collection: String, doc: Vec<u8>) -> Result<u64, SlateError> {
        let doc = parse_doc(doc)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .insert_one(doc)
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    pub fn insert_many(&self, collection: String, docs: Vec<Vec<u8>>) -> Result<u64, SlateError> {
        let docs = parse_docs(docs)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .insert_many(docs)
                .execute(txn)?
                .affected;
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
        let filter = parse_doc(filter)?;
        let options = Self::parse_options(options)?;
        self.read(|txn| {
            let coll = self.db.collection(&collection);
            let mut builder = coll.find(filter);
            for sort in options.sort {
                builder = builder.sort(&sort.field, sort.direction);
            }
            if let Some(skip) = options.skip {
                builder = builder.offset(skip);
            }
            if let Some(take) = options.take {
                builder = builder.limit(take);
            }
            if let Some(columns) = options.columns {
                builder = builder.project(columns);
            }
            let results: Vec<Vec<u8>> = builder
                .iter_raw(txn)?
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
        let filter = parse_doc(filter)?;
        self.read(|txn| {
            let raw = self
                .db
                .collection(&collection)
                .find(filter)
                .iter_raw(txn)?
                .next()
                .transpose()?;
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
        let filter = parse_doc(filter)?;
        let update = parse_doc(update)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .find(filter)
                .update(update)
                .one()
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    pub fn update_many(
        &self,
        collection: String,
        filter: Vec<u8>,
        update: Vec<u8>,
    ) -> Result<u64, SlateError> {
        let filter = parse_doc(filter)?;
        let update = parse_doc(update)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .find(filter)
                .update(update)
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    pub fn replace_one(
        &self,
        collection: String,
        filter: Vec<u8>,
        replacement: Vec<u8>,
    ) -> Result<u64, SlateError> {
        let filter = parse_doc(filter)?;
        let replacement = parse_doc(replacement)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .find(filter)
                .replace(replacement)
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    // --- Delete ---

    pub fn delete_one(&self, collection: String, filter: Vec<u8>) -> Result<u64, SlateError> {
        let filter = parse_doc(filter)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .find(filter)
                .delete()
                .one()
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    pub fn delete_many(&self, collection: String, filter: Vec<u8>) -> Result<u64, SlateError> {
        let filter = parse_doc(filter)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .find(filter)
                .delete()
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    // --- Count ---

    pub fn count(&self, collection: String, filter: Option<Vec<u8>>) -> Result<u64, SlateError> {
        let filter = match filter {
            Some(bytes) => parse_doc(bytes)?,
            None => bson::rawdoc! {},
        };
        self.read(|txn| {
            let count = self
                .db
                .collection(&collection)
                .find(filter)
                .iter_raw(txn)?
                .count();
            Ok(count as u64)
        })
    }

    // --- Bulk ---

    pub fn upsert_many(&self, collection: String, docs: Vec<Vec<u8>>) -> Result<u64, SlateError> {
        let docs = parse_docs(docs)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .upsert_many(docs)
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    pub fn merge_many(&self, collection: String, docs: Vec<Vec<u8>>) -> Result<u64, SlateError> {
        let docs = parse_docs(docs)?;
        self.write(|txn| {
            let affected = self
                .db
                .collection(&collection)
                .merge_many(docs)
                .execute(txn)?
                .affected;
            Ok(affected)
        })
    }

    // --- Collections ---

    pub fn create_collection(&self, name: String, indexes: Vec<String>) -> Result<(), SlateError> {
        self.write(|txn| {
            self.db.collections().create(&name).execute(txn)?;
            let coll = self.db.collection(&name);
            for field in &indexes {
                coll.indexes()
                    .create(field.as_str(), IndexOptions::default())
                    .execute(txn)?;
            }
            Ok(())
        })
    }

    pub fn drop_collection(&self, collection: String) -> Result<(), SlateError> {
        self.write(|txn| {
            self.db.collections().remove(&collection).execute(txn)?;
            Ok(())
        })
    }

    pub fn list_collections(&self) -> Result<Vec<String>, SlateError> {
        self.read(|txn| {
            // v2: no global list (collections().list() is cf-scoped); keep flat
            let collections = txn.list_collections()?;
            Ok(collections.into_iter().map(|(_, name)| name).collect())
        })
    }

    // --- Indexes ---

    pub fn create_index(&self, collection: String, field: String) -> Result<(), SlateError> {
        self.write(|txn| {
            self.db
                .collection(&collection)
                .indexes()
                .create(field.as_str(), IndexOptions::default())
                .execute(txn)?;
            Ok(())
        })
    }

    pub fn drop_index(&self, collection: String, field: String) -> Result<(), SlateError> {
        self.write(|txn| {
            self.db
                .collection(&collection)
                .indexes()
                .remove(field.as_str())
                .execute(txn)?;
            Ok(())
        })
    }

    pub fn list_indexes(&self, collection: String) -> Result<Vec<String>, SlateError> {
        self.read(|txn| {
            let indexes = self.db.collection(&collection).indexes().list(txn)?;
            Ok(indexes)
        })
    }
}
