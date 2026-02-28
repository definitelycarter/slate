use std::fmt;
use std::sync::Arc;

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::RawBson;

use crate::error::EngineError;

// ── CollectionHandle ────────────────────────────────────────

struct CollectionHandleInner<Cf> {
    name: String,
    cf: Cf,
    indexes: Vec<String>,
    pk_path: String,
    ttl_path: String,
}

/// A resolved collection handle with a live CF reference.
///
/// Obtained from [`Catalog::collection`]. Cheap to clone (Arc bump).
pub struct CollectionHandle<Cf> {
    inner: Arc<CollectionHandleInner<Cf>>,
}

impl<Cf: Clone> Clone for CollectionHandle<Cf> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Cf: fmt::Debug> fmt::Debug for CollectionHandle<Cf> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionHandle")
            .field("name", &self.inner.name)
            .field("cf", &self.inner.cf)
            .field("indexes", &self.inner.indexes)
            .field("pk_path", &self.inner.pk_path)
            .field("ttl_path", &self.inner.ttl_path)
            .finish()
    }
}

impl<Cf: Clone> CollectionHandle<Cf> {
    pub fn new(
        name: String,
        cf: Cf,
        indexes: Vec<String>,
        pk_path: String,
        ttl_path: String,
    ) -> Self {
        Self {
            inner: Arc::new(CollectionHandleInner {
                name,
                cf,
                indexes,
                pk_path,
                ttl_path,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn cf(&self) -> &Cf {
        &self.inner.cf
    }

    pub fn indexes(&self) -> &[String] {
        &self.inner.indexes
    }

    pub fn pk_path(&self) -> &str {
        &self.inner.pk_path
    }

    pub fn ttl_path(&self) -> &str {
        &self.inner.ttl_path
    }
}

// ── Engine + Transaction traits ─────────────────────────────

pub trait Engine {
    type Txn<'a>: EngineTransaction
    where
        Self: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, EngineError>;
}

pub trait EngineTransaction {
    type Cf: Clone;

    // ── Document operations ────────────────────────────────────

    /// Fetch a document by `_id`. Constructs the internal key encoding
    /// from the raw BSON reference. Expired documents are filtered internally.
    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError>;

    /// Insert or overwrite a document, extracting `_id` internally.
    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError>;

    /// Insert a document if no existing doc with the same `_id` exists.
    /// Treats expired documents as absent. Extracts `_id` internally.
    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError>;

    /// Delete a document by `_id`. Constructs the internal key encoding
    /// from the raw BSON reference.
    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<(), EngineError>;

    /// Scan all live documents in a collection. Expired documents are
    /// filtered internally.
    fn scan<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'a>,
        EngineError,
    >;

    // ── Index operations ───────────────────────────────────────

    /// Scan an index, returning entries in sort order. Expired entries
    /// are filtered internally.
    fn scan_index<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: IndexRange<'_>,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'a>, EngineError>;

    // ── Purge ──────────────────────────────────────────────────

    /// Physically delete all expired documents and their index entries.
    /// Uses the engine's internal clock.
    fn purge(&self, handle: &CollectionHandle<Self::Cf>) -> Result<u64, EngineError>;

    /// Physically delete documents expired before `as_of_millis`.
    fn purge_before(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        as_of_millis: i64,
    ) -> Result<u64, EngineError>;

    // ── Lifecycle ──────────────────────────────────────────────

    fn commit(self) -> Result<(), EngineError>;
    fn rollback(self) -> Result<(), EngineError>;
}

/// Range filter for index scans.
///
/// Values are high-level `bson::Bson`; the engine encodes them
/// internally into its sortable representation.
pub enum IndexRange<'a> {
    /// Scan all entries for the field.
    Full,
    /// Exact value match (narrow prefix scan).
    Eq(&'a bson::Bson),
    /// One- or two-sided range with inclusive/exclusive bounds.
    Range {
        lower: Option<(&'a bson::Bson, bool)>,
        upper: Option<(&'a bson::Bson, bool)>,
    },
}

/// A decoded index scan entry.
pub struct IndexEntry {
    pub doc_id: RawBson,
    pub value: RawBson,
    pub metadata: Vec<u8>,
}

/// Options for creating a new collection. All fields are optional
/// and fall back to engine defaults when `None`.
#[derive(Debug, Clone, Default)]
pub struct CreateCollectionOptions {
    /// Column family name. Defaults to `"default_cf"`.
    pub cf: Option<String>,
    /// Primary key field path. Defaults to `"_id"`.
    pub pk_path: Option<String>,
    /// TTL field path. Defaults to `"ttl"`.
    pub ttl_path: Option<String>,
}

/// Catalog operations for collection and index metadata.
///
/// Operates on a global `_sys_` column family internally.
pub trait Catalog: EngineTransaction {
    /// Resolve a collection by name into a live handle.
    fn collection(&self, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError>;

    fn list_collections(&self) -> Result<Vec<CollectionHandle<Self::Cf>>, EngineError>;

    fn create_collection(
        &mut self,
        name: &str,
        options: &CreateCollectionOptions,
    ) -> Result<(), EngineError>;

    fn drop_collection(&mut self, name: &str) -> Result<(), EngineError>;

    fn create_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;

    fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;
}
