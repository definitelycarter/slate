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

/// A raw index scan entry with lazy decoding.
///
/// Holds the raw key and metadata bytes from the store. Values are
/// decoded on demand via accessor methods, avoiding conversion overhead
/// for entries that are filtered out or where only `doc_id` is needed.
pub struct IndexEntry {
    key: Vec<u8>,
    metadata: Vec<u8>,
    value_start: usize,
    doc_id_start: usize,
}

impl IndexEntry {
    /// Construct from raw store key-value bytes.
    ///
    /// `field_prefix_len` is the length of the known prefix
    /// `i\0{collection}\0{field}\0` — this gives us `value_start` for free,
    /// avoiding re-parse of collection and field.
    pub(crate) fn from_raw(
        key: Vec<u8>,
        metadata: Vec<u8>,
        field_prefix_len: usize,
    ) -> Option<Self> {
        if metadata.is_empty() || key.len() < field_prefix_len {
            return None;
        }
        let tail = &key[field_prefix_len..];
        let (value_portion, _) = crate::encoding::key::split_trailing_doc_id(tail)?;
        let doc_id_start = field_prefix_len + value_portion.len();
        Some(IndexEntry {
            key,
            metadata,
            value_start: field_prefix_len,
            doc_id_start,
        })
    }

    /// Raw sortable-encoded value bytes (no type tag).
    #[inline]
    pub(crate) fn value_bytes(&self) -> &[u8] {
        &self.key[self.value_start..self.doc_id_start]
    }

    /// O(1) TTL expiry check on the metadata bytes.
    #[inline]
    pub(crate) fn is_expired(&self, now_millis: i64) -> bool {
        crate::encoding::index_record::is_index_expired(&self.metadata, now_millis)
    }

    /// Lazily decode the doc_id to `RawBson`.
    pub fn doc_id(&self) -> Result<RawBson, EngineError> {
        let (bv, _) = crate::encoding::bson_value::BsonValue::parse_length_prefixed(
            &self.key[self.doc_id_start..],
        )
        .ok_or_else(|| EngineError::InvalidKey("malformed doc_id in index key".into()))?;
        bv.to_raw_bson()
            .ok_or_else(|| EngineError::InvalidKey("unsupported doc_id type in index key".into()))
    }

    /// Lazily decode the indexed value to `RawBson`.
    pub fn value(&self) -> Result<RawBson, EngineError> {
        let tag = bson::spec::ElementType::from(self.metadata[0])
            .ok_or_else(|| EngineError::InvalidKey("unknown type byte in index metadata".into()))?;
        crate::encoding::bson_value::BsonValue::from_parts(tag, self.value_bytes())
            .to_raw_bson()
            .ok_or_else(|| EngineError::InvalidKey("malformed value in index key".into()))
    }
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
