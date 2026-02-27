use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::RawBson;

use crate::error::EngineError;

/// Configuration for a collection stored in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionConfig {
    pub name: String,
    pub cf: String,
    pub indexes: Vec<String>,
}

/// A resolved collection handle with a live CF reference.
///
/// Obtained from [`Catalog::collection`]. The caller holds ownership
/// and passes `&CollectionHandle` to data operations.
#[derive(Debug, Clone)]
pub struct CollectionHandle<Cf> {
    pub name: String,
    pub cf: Cf,
    pub indexes: Vec<String>,
}

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
    /// from the raw BSON reference.
    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
        ttl: i64,
    ) -> Result<Option<RawDocumentBuf>, EngineError>;

    /// Insert or overwrite a document, extracting `_id` internally.
    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError>;

    /// Insert a document if no existing doc with the same `_id` exists.
    /// Extracts `_id` internally.
    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
        ttl: i64,
    ) -> Result<(), EngineError>;

    /// Delete a document by `_id`. Constructs the internal key encoding
    /// from the raw BSON reference.
    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<(), EngineError>;

    fn scan<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
        ttl: i64,
    ) -> Result<
        Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'a>,
        EngineError,
    >;

    // ── Index operations ───────────────────────────────────────

    fn scan_index<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: IndexRange<'_>,
        reverse: bool,
        ttl: i64,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'a>, EngineError>;

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

/// Catalog operations for collection and index metadata.
///
/// Operates on a global `_sys_` column family internally.
pub trait Catalog: EngineTransaction {
    /// Resolve a collection by name into a live handle.
    fn collection(&self, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError>;

    fn list_collections(&self) -> Result<Vec<CollectionConfig>, EngineError>;

    fn create_collection(&mut self, cf: Option<&str>, name: &str) -> Result<(), EngineError>;

    fn drop_collection(&mut self, name: &str) -> Result<(), EngineError>;

    fn create_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;

    fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;
}
