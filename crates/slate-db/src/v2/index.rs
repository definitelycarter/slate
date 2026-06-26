//! v2 schema surface: the `indexes()` sub-handle.
//!
//! Phase 0, slice C. Index management is infrequent, so it groups under a named
//! sub-handle ([`Collection::indexes`](super::Collection::indexes)) rather than
//! sitting on the collection directly. The handle exposes three operations:
//!
//! - `create(paths, opts)` — the **one** constructor that folds all five v1
//!   `create_*_index` methods. `paths` is a single field or a list (via
//!   [`Into<IndexPaths>`]); the *options type* carries the index kind —
//!   [`IndexOptions`] for a secondary/unique/compound index,
//!   [`VectorIndexOptions`] for a flat vector index — dispatched by the sealed
//!   [`IndexBuild`] trait, so a wrong-variant call can't compile rather than
//!   failing at runtime.
//! - `remove(field)` — drop an index (pass the joined identity for a compound).
//! - `list(&txn)` — the indexed identities of the collection.
//!
//! Each mutating op is a `#[must_use]` builder finished with `.execute(&txn)`;
//! `create`/`remove` return `()` (there is no plan to explain, unlike a write).
//! Bodies are self-contained — they call the engine transaction's catalog
//! methods directly through [`Transaction::engine_txn`], not a v1 verb.

use slate_engine::Catalog;
use slate_store::Store;

use crate::database::Transaction;
use crate::error::DbError;
use crate::{VectorDataType, VectorMetric};

/// The set of field paths an index covers: one for a single-field/vector index,
/// several for a compound one. Built from a `&str`, a `String`, or any list of
/// them — `create("status", …)` or `create(["status", "created_at"], …)`.
pub struct IndexPaths(Vec<String>);

impl IndexPaths {
    /// Consume into the owned component list.
    fn into_vec(self) -> Vec<String> {
        self.0
    }

    /// The single path, if this names exactly one field (`None` for a compound
    /// or empty set) — the form a vector index requires.
    fn into_single(self) -> Option<String> {
        let mut paths = self.0;
        match paths.len() {
            1 => paths.pop(),
            _ => None,
        }
    }
}

impl From<&str> for IndexPaths {
    fn from(path: &str) -> Self {
        Self(vec![path.to_string()])
    }
}

impl From<String> for IndexPaths {
    fn from(path: String) -> Self {
        Self(vec![path])
    }
}

impl<const N: usize> From<[&str; N]> for IndexPaths {
    fn from(paths: [&str; N]) -> Self {
        Self(paths.iter().map(|p| p.to_string()).collect())
    }
}

impl From<&[&str]> for IndexPaths {
    fn from(paths: &[&str]) -> Self {
        Self(paths.iter().map(|p| p.to_string()).collect())
    }
}

impl From<Vec<&str>> for IndexPaths {
    fn from(paths: Vec<&str>) -> Self {
        Self(paths.iter().map(|p| p.to_string()).collect())
    }
}

impl From<Vec<String>> for IndexPaths {
    fn from(paths: Vec<String>) -> Self {
        Self(paths)
    }
}

impl From<&[String]> for IndexPaths {
    fn from(paths: &[String]) -> Self {
        Self(paths.to_vec())
    }
}

/// Options for a secondary index — single, compound, or unique. The number of
/// fields (single vs compound) comes from the `paths` passed to
/// [`Indexes::create`]; this only carries the uniqueness flag.
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// Enforce that no two live documents share the value (or, for a compound
    /// index, the value combination) of the indexed path(s).
    pub unique: bool,
}

impl IndexOptions {
    /// A unique index — rejects duplicate values across live documents.
    pub fn unique() -> Self {
        Self { unique: true }
    }
}

/// Options for a flat vector index: the embedding shape (dimensionality,
/// distance metric, element width). The field path comes from the `paths`
/// passed to [`Indexes::create`], which must name exactly one field.
#[derive(Debug, Clone)]
pub struct VectorIndexOptions {
    dims: u32,
    metric: VectorMetric,
    dtype: VectorDataType,
}

impl VectorIndexOptions {
    /// A Phase-1 `float32` vector index of `dims` dimensions, built for `metric`.
    pub fn float32(dims: u32, metric: VectorMetric) -> Self {
        Self {
            dims,
            metric,
            dtype: VectorDataType::Float32,
        }
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::IndexOptions {}
    impl Sealed for super::VectorIndexOptions {}
}

/// The kind-specific build step behind [`Indexes::create`]. Sealed — the only
/// implementors are [`IndexOptions`] and [`VectorIndexOptions`], so the options
/// *type* a caller passes selects the index kind at compile time.
pub trait IndexBuild: sealed::Sealed {
    /// Create the index this options value describes over `paths`.
    fn build<S: Store>(
        self,
        txn: &Transaction<'_, S>,
        cf: &str,
        collection: &str,
        paths: IndexPaths,
    ) -> Result<(), DbError>
    where
        Self: Sized;
}

impl IndexBuild for IndexOptions {
    fn build<S: Store>(
        self,
        txn: &Transaction<'_, S>,
        cf: &str,
        collection: &str,
        paths: IndexPaths,
    ) -> Result<(), DbError> {
        let options = slate_engine::IndexOptions {
            unique: self.unique,
        };
        let fields = paths.into_vec();
        // A single path is a secondary index; more than one is compound. The
        // engine backfills existing records as part of the create.
        if fields.len() == 1 {
            txn.engine_txn()
                .create_index_with_options(cf, collection, &fields[0], &options)?;
        } else {
            txn.engine_txn()
                .create_compound_index_with_options(cf, collection, &fields, &options)?;
        }
        Ok(())
    }
}

impl IndexBuild for VectorIndexOptions {
    fn build<S: Store>(
        self,
        txn: &Transaction<'_, S>,
        cf: &str,
        collection: &str,
        paths: IndexPaths,
    ) -> Result<(), DbError> {
        let path = paths.into_single().ok_or_else(|| {
            DbError::InvalidQuery("a vector index requires exactly one field path".to_string())
        })?;
        let spec = slate_engine::VectorIndexSpec {
            path,
            dims: self.dims,
            metric: self.metric,
            dtype: self.dtype,
        };
        txn.engine_txn()
            .create_vector_index(cf, collection, &spec)?;
        Ok(())
    }
}

/// The `indexes()` sub-handle: index management for one collection. Built by
/// [`Collection::indexes`](super::Collection::indexes).
pub struct Indexes<'a> {
    cf: &'a str,
    collection: &'a str,
}

impl<'a> Indexes<'a> {
    pub(crate) fn new(cf: &'a str, collection: &'a str) -> Self {
        Self { cf, collection }
    }

    /// Create an index over `paths`. The options *type* picks the kind:
    /// [`IndexOptions`] (secondary / unique / compound) or
    /// [`VectorIndexOptions`] (flat vector). Returns a builder; nothing runs
    /// until `.execute(&txn)`.
    ///
    /// ```ignore
    /// orders.indexes().create("status", IndexOptions::default()).execute(&txn)?;
    /// orders.indexes().create(["org_id", "email"], IndexOptions::unique()).execute(&txn)?;
    /// orders.indexes().create("embedding",
    ///     VectorIndexOptions::float32(1536, VectorMetric::Cosine)).execute(&txn)?;
    /// ```
    pub fn create<P, O>(&self, paths: P, options: O) -> CreateIndex<'a, O>
    where
        P: Into<IndexPaths>,
        O: IndexBuild,
    {
        CreateIndex {
            cf: self.cf,
            collection: self.collection,
            paths: paths.into(),
            options,
        }
    }

    /// Drop the index identified by `field` (for a compound index, the joined
    /// identity from [`list`](Self::list)). Returns a builder; nothing runs until
    /// `.execute(&txn)`.
    pub fn remove(&self, field: &str) -> RemoveIndex<'a> {
        RemoveIndex {
            cf: self.cf,
            collection: self.collection,
            field: field.to_string(),
        }
    }

    /// List the collection's indexed identities (compound indexes appear as their
    /// joined identity). A direct read — no builder.
    pub fn list<S: Store>(&self, txn: &Transaction<'_, S>) -> Result<Vec<String>, DbError> {
        let handle = txn.engine_txn().collection(self.cf, self.collection)?;
        Ok(handle.indexes().to_vec())
    }
}

/// A pending index creation, built by [`Indexes::create`]. Run it with
/// `.execute(&txn)`.
#[must_use = "a create-index builder does nothing until .execute(&txn) runs it"]
pub struct CreateIndex<'a, O> {
    cf: &'a str,
    collection: &'a str,
    paths: IndexPaths,
    options: O,
}

impl<O: IndexBuild> CreateIndex<'_, O> {
    /// Create the index (backfilling existing records).
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        self.options
            .build(txn, self.cf, self.collection, self.paths)
    }
}

/// A pending index removal, built by [`Indexes::remove`]. Run it with
/// `.execute(&txn)`.
#[must_use = "a remove-index builder does nothing until .execute(&txn) runs it"]
pub struct RemoveIndex<'a> {
    cf: &'a str,
    collection: &'a str,
    field: String,
}

impl RemoveIndex<'_> {
    /// Drop the index and remove all its entries.
    pub fn execute<S: Store>(self, txn: &Transaction<'_, S>) -> Result<(), DbError> {
        txn.engine_txn()
            .drop_index(self.cf, self.collection, &self.field)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use slate_store::MemoryStore;

    use crate::v2::{IndexOptions, VectorIndexOptions};
    use crate::{CollectionConfig, Database, DatabaseBuilder, VectorMetric, join_index_fields};

    fn db_with_users() -> Database<MemoryStore> {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        let txn = db.begin(false).unwrap();
        txn.create_collection(&CollectionConfig {
            name: "users".to_string(),
            ..Default::default()
        })
        .unwrap();
        txn.commit().unwrap();
        db
    }

    #[test]
    fn create_single_compound_and_list() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");

        users
            .indexes()
            .create("age", IndexOptions::default())
            .execute(&txn)
            .unwrap();
        users
            .indexes()
            .create(["status", "created_at"], IndexOptions::default())
            .execute(&txn)
            .unwrap();

        // membership, not equality: the collection also carries a default `ttl`
        // index. The compound shows up as its joined identity.
        let listed = users.indexes().list(&txn).unwrap();
        assert!(listed.contains(&"age".to_string()));
        assert!(listed.contains(&join_index_fields(&[
            "status".to_string(),
            "created_at".to_string()
        ])));
        txn.commit().unwrap();
    }

    #[test]
    fn unique_index_enforces_uniqueness() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");
        users
            .insert_many(vec![
                doc! { "_id": 1, "email": "a@x.com" },
                doc! { "_id": 2, "email": "b@x.com" },
            ])
            .execute(&txn)
            .unwrap();

        users
            .indexes()
            .create("email", IndexOptions::unique())
            .execute(&txn)
            .unwrap();

        // a duplicate email now violates the unique index
        let dup = users
            .insert_one(doc! { "_id": 3, "email": "a@x.com" })
            .execute(&txn);
        assert!(dup.is_err());
        txn.commit().unwrap();
    }

    #[test]
    fn remove_index() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");
        users
            .indexes()
            .create("age", IndexOptions::default())
            .execute(&txn)
            .unwrap();
        assert!(
            users
                .indexes()
                .list(&txn)
                .unwrap()
                .contains(&"age".to_string())
        );

        users.indexes().remove("age").execute(&txn).unwrap();
        assert!(
            !users
                .indexes()
                .list(&txn)
                .unwrap()
                .contains(&"age".to_string())
        );
        txn.commit().unwrap();
    }

    #[test]
    fn vector_index_create_and_query() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let users = db.collection("users");
        users
            .insert_many(vec![
                doc! { "_id": 1, "embedding": [1.0, 0.0] },
                doc! { "_id": 2, "embedding": [0.0, 1.0] },
            ])
            .execute(&txn)
            .unwrap();

        users
            .indexes()
            .create(
                "embedding",
                VectorIndexOptions::float32(2, VectorMetric::Cosine),
            )
            .execute(&txn)
            .unwrap();

        // Vector indexes live in their own catalog space, not the secondary-index
        // `list()`. Prove it persisted by a duplicate create failing...
        let dup = users
            .indexes()
            .create(
                "embedding",
                VectorIndexOptions::float32(2, VectorMetric::Cosine),
            )
            .execute(&txn);
        assert!(dup.is_err(), "a second vector index on the field must fail");

        // ...and that it serves a top-k query — nearest to [1,0] by cosine is id 1.
        let hits = users
            .query(
                "SELECT VALUE c._id FROM c \
                 ORDER BY VECTORDISTANCE(c.embedding, [1.0, 0.0]) DESC LIMIT 1",
            )
            .iter_raw(&txn)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(hits, vec![bson::RawBson::Int32(1)]);
        txn.commit().unwrap();
    }

    #[test]
    fn vector_index_rejects_compound_paths() {
        let db = db_with_users();
        let txn = db.begin(false).unwrap();
        let err = db
            .collection("users")
            .indexes()
            .create(
                ["a", "b"],
                VectorIndexOptions::float32(2, VectorMetric::Cosine),
            )
            .execute(&txn);
        assert!(err.is_err());
        txn.commit().unwrap();
    }
}
