use bson::raw::{RawDocument, RawDocumentBuf};

use crate::encoding::bson_value::BsonValue;
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

    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &BsonValue<'_>,
        ttl: i64,
    ) -> Result<Option<RawDocumentBuf>, EngineError>;

    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError>;

    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError>;

    fn scan<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
        ttl: i64,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(BsonValue<'a>, RawDocumentBuf), EngineError>> + 'a>,
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
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry<'a>, EngineError>> + 'a>, EngineError>;

    // ── Lifecycle ──────────────────────────────────────────────

    fn commit(self) -> Result<(), EngineError>;
    fn rollback(self) -> Result<(), EngineError>;
}

/// Range filter for index scans.
pub enum IndexRange<'a> {
    /// Scan all entries for the field.
    Full,
    /// Exact value match (narrow prefix scan).
    Eq(&'a [u8]),
    /// One- or two-sided range with inclusive/exclusive bounds.
    Range {
        lower: Option<&'a [u8]>,
        lower_inclusive: bool,
        upper: Option<&'a [u8]>,
        upper_inclusive: bool,
    },
}

/// A decoded index scan entry.
pub struct IndexEntry<'a> {
    pub doc_id: BsonValue<'a>,
    pub value_bytes: Vec<u8>,
    pub metadata: Vec<u8>,
}

/// Catalog operations for collection and index metadata.
///
/// Operates on a global `_sys_` column family internally.
pub trait Catalog: EngineTransaction {
    /// Resolve a collection by name into a live handle.
    fn collection(
        &self,
        name: &str,
    ) -> Result<CollectionHandle<Self::Cf>, EngineError>;

    fn list_collections(&self) -> Result<Vec<CollectionConfig>, EngineError>;

    fn create_collection(
        &mut self,
        cf: Option<&str>,
        name: &str,
    ) -> Result<(), EngineError>;

    fn drop_collection(&mut self, name: &str) -> Result<(), EngineError>;

    fn create_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;

    fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError>;
}
