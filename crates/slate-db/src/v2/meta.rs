//! v2 metadata terminals on the collection handle: `stats` / `schema` / `purge`.
//!
//! Phase 0, slice D. Single-outcome reads/commands, so they skip the builder and
//! take `&txn` directly. Self-contained bodies over the engine transaction's
//! catalog + scan surface (the stats scan is replicated here rather than calling
//! v1's `collection_stats`, the deliberate build-then-invert doubling).

use std::collections::HashSet;

use slate_engine::{Catalog, EngineTransaction, IndexRange};
use slate_store::Store;

use super::Collection;
use crate::database::Transaction;
use crate::error::DbError;
use crate::stats::{CollectionStats, IndexStats};
use crate::{CollectionSchema, RawDocumentBuf};

impl Collection {
    /// Size/cardinality statistics as of this transaction's snapshot: live
    /// document count plus per-index entry and distinct-value counts. Computed by
    /// scanning, so the numbers are exact but the call is O(documents + index
    /// entries) — an introspection surface, not a hot path.
    pub fn stats<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<CollectionStats, DbError> {
        collection_stats_core(&self.cf, &self.collection, txn)
    }

    /// Read-only catalog metadata: key paths and indexed fields (with the unique
    /// subset called out). For introspection, not planning.
    pub fn schema<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<CollectionSchema, DbError> {
        collection_schema_core(&self.cf, &self.collection, txn)
    }

    /// Purge expired documents (those past their TTL) and return how many were
    /// removed.
    pub fn purge<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<u64, DbError> {
        purge_core(&self.cf, &self.collection, txn)
    }
}

/// The shared collection-stats core (scan-based cardinality). Called by both
/// [`Collection::stats`] and the (inverted) flat `Transaction::collection_stats`,
/// so the two run one body.
pub(crate) fn collection_stats_core<S: Store>(
    cf: &str,
    collection: &str,
    txn: &Transaction<'_, S>,
) -> Result<CollectionStats, DbError> {
    let etxn = txn.engine_txn();
    let handle = etxn.collection(cf, collection)?;

    // Live document count via a full scan.
    let mut document_count: u64 = 0;
    for doc in etxn.scan(&handle)? {
        doc?;
        document_count += 1;
    }

    // Per-index: entries and distinct values, deduped by canonical BSON bytes
    // (no decode). The single-field wrapper key is constant, so build it once.
    let value_key =
        bson::raw::CString::try_from("v").map_err(|e| DbError::InvalidQuery(e.to_string()))?;
    let mut indexes = Vec::with_capacity(handle.indexes().len());
    for field in handle.indexes() {
        let mut entry_count: u64 = 0;
        let mut distinct: HashSet<Vec<u8>> = HashSet::new();
        for entry in etxn.scan_index(&handle, field, IndexRange::Full, false)? {
            let entry = entry?;
            entry_count += 1;
            let value = entry.value()?;
            let mut doc = RawDocumentBuf::new();
            doc.append(&value_key, value);
            distinct.insert(doc.into_bytes());
        }
        indexes.push(IndexStats {
            field: field.to_string(),
            entry_count,
            cardinality: distinct.len() as u64,
        });
    }

    Ok(CollectionStats {
        cf: cf.to_string(),
        name: collection.to_string(),
        document_count,
        indexes,
        approximate: false,
    })
}

/// The shared collection-schema core. Called by both [`Collection::schema`] and
/// the (inverted) flat `Transaction::collection_schema`.
pub(crate) fn collection_schema_core<S: Store>(
    cf: &str,
    collection: &str,
    txn: &Transaction<'_, S>,
) -> Result<CollectionSchema, DbError> {
    let handle = txn.engine_txn().collection(cf, collection)?;
    Ok(CollectionSchema {
        cf: handle.cf_name().to_string(),
        name: handle.name().to_string(),
        pk_path: handle.pk_path().to_string(),
        ttl_path: handle.ttl_path().to_string(),
        indexes: handle.indexes().to_vec(),
        unique_indexes: handle.unique_indexes().to_vec(),
    })
}

/// The shared purge core. Called by both [`Collection::purge`] and the
/// (inverted) flat `Transaction::purge_expired`.
pub(crate) fn purge_core<S: Store>(
    cf: &str,
    collection: &str,
    txn: &Transaction<'_, S>,
) -> Result<u64, DbError> {
    let etxn = txn.engine_txn();
    let handle = etxn.collection(cf, collection)?;
    Ok(etxn.purge(&handle)?)
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::v2::IndexOptions;
    use crate::{DEFAULT_CF, Database, DatabaseBuilder};

    fn seeded() -> Database<MemoryStore> {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        let users = db.collections().create("users").execute(&txn).unwrap();
        users
            .indexes()
            .create("age", IndexOptions::default())
            .execute(&txn)
            .unwrap();
        users
            .insert_many(vec![
                doc! { "_id": 1, "age": 30 },
                doc! { "_id": 2, "age": 30 },
                doc! { "_id": 3, "age": 40 },
            ])
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();
        db
    }

    #[test]
    fn stats_and_schema_match_v1() {
        let db = seeded();
        let txn = db.begin(true).unwrap();
        let users = db.collection("users");

        let stats = users.stats(&txn).unwrap();
        assert_eq!(stats.document_count, 3);
        // the "age" index has 3 entries over 2 distinct values (30, 40)
        let age = stats.indexes.iter().find(|i| i.field == "age").unwrap();
        assert_eq!(age.entry_count, 3);
        assert_eq!(age.cardinality, 2);
        assert_eq!(stats, txn.collection_stats(DEFAULT_CF, "users").unwrap());

        assert_eq!(
            users.schema(&txn).unwrap(),
            txn.collection_schema(DEFAULT_CF, "users").unwrap()
        );
    }

    #[test]
    fn purge_removes_expired() {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        let events = db.collections().create("events").execute(&txn).unwrap();
        // `ttl` is a BSON DateTime; one in the past is expired, one ahead is fresh.
        let past = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 60_000);
        let future =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 600_000);
        events
            .insert_many(vec![
                doc! { "_id": 1, "ttl": past },
                doc! { "_id": 2, "ttl": past },
                doc! { "_id": 3, "ttl": future },
            ])
            .execute(&txn)
            .unwrap();
        txn.commit().unwrap();

        let txn = db.begin(false).unwrap();
        let purged = db.collection("events").purge(&txn).unwrap();
        assert_eq!(purged, 2); // the two past-ttl docs; the future one survives
        txn.commit().unwrap();
    }
}
