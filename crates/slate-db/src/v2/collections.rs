//! v2 schema surface: the `collections()` namespace.
//!
//! Phase 0, slice D. Operations on the *set* of collections in a column family,
//! parallel to the singular [`Collection`] handle. Reached from
//! [`Database::collections`](super::Collection) (default cf) or
//! `db.cf(cf).collections()` — the cf is the **scope**, so `create` needs no cf
//! argument and `list`/`remove` are scoped to it.
//!
//! `create(name)` is a builder (like every other v2 create): `.pk_path(..)` /
//! `.ttl_path(..)` stages tune it, `.execute(&txn)` runs it and returns the new
//! [`Collection`] handle. Bodies are self-contained — they call the engine
//! transaction's catalog methods directly, not a v1 verb.

use std::sync::Arc;

use slate_engine::{Catalog, CreateCollectionOptions, EngineError};
use slate_store::Store;

use super::Collection;
use crate::WatchRegistry;
use crate::database::Transaction;
use crate::error::DbError;
use slate_udf::UdfBag;

/// The collection-management namespace for one column family. Built by
/// [`Database::collections`](super::Collection) or `db.cf(cf).collections()`.
pub struct Collections {
    cf: String,
    /// The database's watch registry, carried so a [`Collection`] built by
    /// `create` is a fully-formed reactive root like any other handle.
    watch: Arc<WatchRegistry>,
    /// The database's UDF bag, carried for the same reason — a `create`d handle
    /// shares the live bag like any other.
    udf_bag: Arc<UdfBag>,
}

impl Collections {
    pub(super) fn new(cf: String, watch: Arc<WatchRegistry>, udf_bag: Arc<UdfBag>) -> Self {
        Self { cf, watch, udf_bag }
    }

    /// Create a collection named `name` in this scope's column family. Returns a
    /// builder; nothing runs until `.execute(&txn)`, which yields the new
    /// [`Collection`] handle.
    pub fn create(&self, name: &str) -> CreateCollection<'_> {
        CreateCollection {
            cf: &self.cf,
            watch: &self.watch,
            udf_bag: &self.udf_bag,
            name: name.to_string(),
            pk_path: "_id".to_string(),
            ttl_path: "ttl".to_string(),
        }
    }

    /// List the names of the collections in this scope's column family.
    pub fn list<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<Vec<String>, DbError> {
        Ok(txn
            .engine_txn()
            .list_collections(Some(&self.cf))?
            .into_iter()
            .map(|handle| handle.name().to_string())
            .collect())
    }

    /// Drop the collection `name` (and all its data, indexes, and scripts).
    /// Returns a builder; nothing runs until `.execute(&txn)`.
    pub fn remove(&self, name: &str) -> RemoveCollection<'_> {
        RemoveCollection {
            cf: &self.cf,
            name: name.to_string(),
        }
    }
}

/// A pending collection creation, from [`Collections::create`]. Tune the key and
/// TTL paths with the stages, then run it with `.execute(&txn)`.
#[must_use = "a create-collection builder does nothing until .execute(&txn) runs it"]
pub struct CreateCollection<'a> {
    cf: &'a str,
    watch: &'a Arc<WatchRegistry>,
    udf_bag: &'a Arc<UdfBag>,
    name: String,
    pk_path: String,
    ttl_path: String,
}

impl CreateCollection<'_> {
    /// Set the primary-key field path (default `"_id"`).
    pub fn pk_path(mut self, path: &str) -> Self {
        self.pk_path = path.to_string();
        self
    }

    /// Set the TTL field path (default `"ttl"`).
    pub fn ttl_path(mut self, path: &str) -> Self {
        self.ttl_path = path.to_string();
        self
    }

    /// Create the collection and return a handle to it. Also auto-creates the TTL
    /// index on the TTL path (an existing one is fine — idempotent).
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<Collection, DbError> {
        create_collection_core(self.cf, &self.name, &self.pk_path, &self.ttl_path, txn)?;
        Ok(Collection {
            cf: self.cf.to_string(),
            collection: self.name,
            // `Arc` refcount bump: the new handle is a reactive root like any other.
            watch: self.watch.clone(),
            udf_bag: self.udf_bag.clone(),
        })
    }
}

/// The shared collection-create core: create the collection and auto-create its
/// TTL index. Called by both [`CreateCollection::execute`] and the (inverted)
/// flat `Transaction::create_collection`, so the two run one body.
pub(crate) fn create_collection_core<S: Store>(
    cf: &str,
    name: &str,
    pk_path: &str,
    ttl_path: &str,
    txn: &Transaction<'_, S>,
) -> Result<(), DbError> {
    let options = CreateCollectionOptions {
        pk_path: Some(pk_path.to_string()),
        ttl_path: Some(ttl_path.to_string()),
    };
    txn.engine_txn().create_collection(cf, name, &options)?;
    // A pre-existing TTL index is fine — the create is idempotent.
    if let Err(e) = txn.engine_txn().create_index(cf, name, ttl_path)
        && !matches!(e, EngineError::IndexExists(_))
    {
        return Err(e.into());
    }
    Ok(())
}

/// The shared collection-drop core: drop the collection and mark the hook
/// snapshot stale (a dropped collection takes its triggers/validators with it).
pub(crate) fn drop_collection_core<S: Store>(
    cf: &str,
    name: &str,
    txn: &Transaction<'_, S>,
) -> Result<(), DbError> {
    txn.engine_txn().drop_collection(cf, name)?;
    txn.mark_hooks_dirty();
    Ok(())
}

/// A pending collection removal, from [`Collections::remove`]. Run it with
/// `.execute(&txn)`.
#[must_use = "a remove-collection builder does nothing until .execute(&txn) runs it"]
pub struct RemoveCollection<'a> {
    cf: &'a str,
    name: String,
}

impl RemoveCollection<'_> {
    /// Drop the collection and everything in it.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        drop_collection_core(self.cf, &self.name, txn)
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::{Database, DatabaseBuilder};

    fn db() -> Database<MemoryStore> {
        DatabaseBuilder::new().open(MemoryStore::new()).unwrap()
    }

    #[test]
    fn create_returns_a_usable_handle() {
        let db = db();
        let txn = db.begin(false).unwrap();

        let orders = db.collections().create("orders").execute(&txn).unwrap();
        // the returned handle works immediately
        orders
            .insert_one(doc! { "_id": 1, "total": 9 })
            .execute(&txn)
            .unwrap();
        assert_eq!(orders.find(doc! {}).iter_raw(&txn).unwrap().count(), 1);
        txn.commit().unwrap();
    }

    #[test]
    fn create_list_remove() {
        let db = db();
        let txn = db.begin(false).unwrap();
        db.collections().create("a").execute(&txn).unwrap();
        db.collections().create("b").execute(&txn).unwrap();

        let mut listed = db.collections().list(&txn).unwrap();
        listed.sort();
        assert_eq!(listed, vec!["a".to_string(), "b".to_string()]);

        db.collections().remove("a").execute(&txn).unwrap();
        assert_eq!(db.collections().list(&txn).unwrap(), vec!["b".to_string()]);
        txn.commit().unwrap();
    }

    #[test]
    fn create_with_custom_pk_path() {
        let db = db();
        let txn = db.begin(false).unwrap();
        let things = db
            .collections()
            .create("things")
            .pk_path("key")
            .execute(&txn)
            .unwrap();
        // documents are keyed by `key`, not `_id`
        things
            .insert_one(doc! { "key": "x", "v": 1 })
            .execute(&txn)
            .unwrap();
        assert_eq!(things.schema(&txn).unwrap().pk_path, "key".to_string());
        txn.commit().unwrap();
    }

    #[test]
    fn cf_scoped_collections_are_isolated() {
        let db = db();
        let txn = db.begin(false).unwrap();
        db.cf("tenant_a")
            .collections()
            .create("orders")
            .execute(&txn)
            .unwrap();
        db.cf("tenant_b")
            .collections()
            .create("invoices")
            .execute(&txn)
            .unwrap();

        // each cf lists only its own
        assert_eq!(
            db.cf("tenant_a").collections().list(&txn).unwrap(),
            vec!["orders".to_string()]
        );
        assert_eq!(
            db.cf("tenant_b").collections().list(&txn).unwrap(),
            vec!["invoices".to_string()]
        );
        txn.commit().unwrap();
    }
}
