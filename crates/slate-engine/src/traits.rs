use std::fmt;
use std::sync::Arc;

use bson::RawBson;
use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};

use crate::error::EngineError;

// ── Function types ──────────────────────────────────────────

/// The kind of function stored in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    Trigger,
    Validator,
    Udf,
}

/// Runtime tag constants for function storage.
pub mod runtime_tag {
    pub const LUA: u8 = 0x01;
    pub const WASM: u8 = 0x02;
}

/// A named function definition loaded from the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionEntry {
    pub name: String,
    pub runtime: u8,
    pub source: Vec<u8>,
}

// ── CollectionHandle ────────────────────────────────────────

struct CollectionHandleInner<Cf> {
    name: String,
    cf_name: String,
    cf: Cf,
    indexes: Vec<String>,
    unique_indexes: Vec<String>,
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
            .field("cf_name", &self.inner.cf_name)
            .field("cf", &self.inner.cf)
            .field("indexes", &self.inner.indexes)
            .field("unique_indexes", &self.inner.unique_indexes)
            .field("pk_path", &self.inner.pk_path)
            .field("ttl_path", &self.inner.ttl_path)
            .finish()
    }
}

impl<Cf: Clone> CollectionHandle<Cf> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        cf_name: String,
        cf: Cf,
        indexes: Vec<String>,
        unique_indexes: Vec<String>,
        pk_path: String,
        ttl_path: String,
    ) -> Self {
        Self {
            inner: Arc::new(CollectionHandleInner {
                name,
                cf_name,
                cf,
                indexes,
                unique_indexes,
                pk_path,
                ttl_path,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn cf_name(&self) -> &str {
        &self.inner.cf_name
    }

    pub fn cf(&self) -> &Cf {
        &self.inner.cf
    }

    pub fn indexes(&self) -> &[String] {
        &self.inner.indexes
    }

    /// The subset of indexed paths that carry a unique constraint.
    pub fn unique_indexes(&self) -> &[String] {
        &self.inner.unique_indexes
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

    /// Epoch milliseconds captured when the transaction began (from the engine's
    /// clock, which is injectable — e.g. for wasm). Used by the SQL `GETCURRENT*`
    /// functions so "now" is consistent across the transaction and platform-clean.
    fn now_millis(&self) -> i64;

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
    ) -> Result<Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'a>, EngineError>;

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

    /// Scan a *compound* index over its leftmost-prefix range. `field` is the
    /// joined compound identity (`f1\x01f2`). Expired entries are filtered
    /// internally; the leading-component byte-prefix over-read is *not* — the
    /// returned [`IndexEntry`]s carry per-component values so the caller can
    /// recheck each leading equality exactly (see [`CompoundRange`]).
    fn scan_compound_index<'a>(
        &'a self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: CompoundRange<'_>,
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
    /// All entries whose value bytes start with this string's UTF-8 bytes — a
    /// prefix scan for `STARTSWITH` / `LIKE 'pre%'`. Like `Range`, a conservative
    /// superset: it can sweep in cross-type values whose sortable bytes share the
    /// prefix, which the caller's recheck drops.
    Prefix(&'a str),
}

/// Range filter for a *compound* (multi-component) index scan.
///
/// The leftmost-prefix model: `eq_prefix` pins the leading components to exact
/// values, and `tail` optionally constrains the *next* component (equality,
/// range, or unconstrained). Components beyond the tail are unconstrained at the
/// engine and filtered in memory by the caller.
///
/// The scan is a conservative superset — a variable-width (string) leading
/// component's byte prefix can sweep in entries whose later bytes happen to
/// continue the prefix (e.g. `"active"` reaching `"active2"`), so the caller
/// rechecks every leading component exactly via [`IndexEntry::component_value`].
pub struct CompoundRange<'a> {
    /// Exact values for the leading components, in field order.
    pub eq_prefix: &'a [bson::Bson],
    /// The predicate on the component right after the equality prefix.
    pub tail: CompoundTail<'a>,
}

/// The trailing predicate of a [`CompoundRange`] — on the component immediately
/// after the equality prefix.
pub enum CompoundTail<'a> {
    /// No constraint past the equality prefix — scan the whole prefix group.
    Unbounded,
    /// Equality on the next component.
    Eq(&'a bson::Bson),
    /// Range on the next component.
    Range {
        lower: Option<(&'a bson::Bson, bool)>,
        upper: Option<(&'a bson::Bson, bool)>,
    },
}

/// A raw index scan entry with lazy decoding.
///
/// Holds the raw key and metadata bytes from the store. Values are decoded on
/// demand via accessor methods, avoiding conversion overhead for entries that are
/// filtered out or where only `doc_id` is needed.
///
/// Single value-side-offsets decode path (no single/compound fork): construction
/// just stores the four fields below — the per-component value/doc_id boundaries
/// are precomputed on the *value* side (in the metadata) at write time, so there
/// is no offset-resolution loop to run on read.
///
/// - Key:      `i\0{collection}\0{field}\0{v1}…{vN}{doc_id_lp}` (no length suffix).
/// - Metadata: `[t1…tN][end_1…end_N : u32 LE][ttl: i64 LE, optional]`, where
///   `end_k` is the cumulative byte length of values `0..=k` *relative to
///   `value_start`* — so `end_{N-1}` is the total value length and the doc_id
///   begins at `value_start + end_{N-1}`.
///
/// A single-field index is the `n == 1` case (no special path).
pub struct IndexEntry {
    key: Vec<u8>,
    metadata: Vec<u8>,
    /// Absolute offset into `key` where the first value byte starts (the
    /// `i\0{collection}\0{field}\0` prefix length).
    value_start: usize,
    /// Number of components (1 for a single-field index).
    n: usize,
}

impl IndexEntry {
    /// Construct from raw store key-value bytes.
    ///
    /// `field_prefix_len` is the length of the known prefix
    /// `i\0{collection}\0{field}\0` — the first value starts there, avoiding a
    /// re-parse of collection and field. `n` is the component count (1 for a
    /// single-field index). Bounds-checks that the metadata holds the `n` type
    /// bytes plus the `n` u32 end-offsets and that `value_start` is within the key,
    /// then stores the fields — no offset-resolution loop.
    pub(crate) fn from_raw(
        key: Vec<u8>,
        metadata: Vec<u8>,
        field_prefix_len: usize,
        n: usize,
    ) -> Option<Self> {
        use crate::encoding::key::VALUE_OFFSET_WIDTH;
        // `n` type bytes + `n` u32 end-offsets. (TTL, when present, follows.)
        let header_len = n.checked_add(n.checked_mul(VALUE_OFFSET_WIDTH)?)?;
        if metadata.len() < header_len || key.len() < field_prefix_len {
            return None;
        }
        Some(IndexEntry {
            key,
            metadata,
            value_start: field_prefix_len,
            n,
        })
    }

    /// The number of components (1 for a single-field index).
    #[inline]
    fn component_count(&self) -> usize {
        self.n
    }

    /// The cumulative END offset (relative to `value_start`) of component `k`,
    /// read O(1) as a `u32` LE from the metadata's value-side offset region (which
    /// begins right after the `n` type bytes).
    #[inline]
    fn end_offset(&self, k: usize) -> Option<usize> {
        use crate::encoding::key::VALUE_OFFSET_WIDTH;
        let at = self.n + VALUE_OFFSET_WIDTH * k;
        let bytes = self.metadata.get(at..at + VALUE_OFFSET_WIDTH)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?) as usize)
    }

    /// Raw sortable-encoded value bytes of component `idx` (no type tag): the
    /// half-open range `[start, end)` where `start` is the previous component's end
    /// (0 for component 0) and `end` is `end_offset(idx)`.
    #[inline]
    fn component_value_bytes(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.n {
            return None;
        }
        let rel_start = if idx == 0 {
            0
        } else {
            self.end_offset(idx - 1)?
        };
        let rel_end = self.end_offset(idx)?;
        self.key
            .get(self.value_start + rel_start..self.value_start + rel_end)
    }

    /// Raw sortable-encoded value bytes (no type tag) of the *first* component —
    /// for a single-field index this is the whole value.
    #[inline]
    pub(crate) fn value_bytes(&self) -> &[u8] {
        self.component_value_bytes(0).unwrap_or(&[])
    }

    /// The element type of component `idx`, read O(1) from the metadata type region.
    #[inline]
    fn component_element_type(&self, idx: usize) -> Option<bson::spec::ElementType> {
        if idx >= self.n {
            return None;
        }
        bson::spec::ElementType::from(*self.metadata.get(idx)?)
    }

    /// The first component's BSON element type, read O(1) from the metadata tag
    /// byte (no value decode). `from_raw` guarantees enough metadata bytes.
    #[inline]
    pub(crate) fn element_type(&self) -> Option<bson::spec::ElementType> {
        self.component_element_type(0)
    }

    /// Decode component `idx`'s value to `RawBson`, or `None` if out of range.
    pub fn component_value(&self, idx: usize) -> Result<Option<RawBson>, EngineError> {
        let Some(bytes) = self.component_value_bytes(idx) else {
            return Ok(None);
        };
        let tag = self
            .component_element_type(idx)
            .ok_or_else(|| EngineError::InvalidKey("unknown type byte in index metadata".into()))?;
        crate::encoding::numeric_key::decode_index_value(tag, bytes)
            .map(Some)
            .ok_or_else(|| EngineError::InvalidKey("malformed value in index key".into()))
    }

    /// O(1) TTL expiry check on the metadata bytes — the TTL, when present, follows
    /// the `n` type bytes and the `n` u32 end-offsets.
    #[inline]
    pub(crate) fn is_expired(&self, now_millis: i64) -> bool {
        crate::encoding::index_record::is_index_expired_n(
            &self.metadata,
            self.component_count(),
            now_millis,
        )
    }

    /// Lazily decode the doc_id to `RawBson`. The doc_id begins at
    /// `value_start + end_offset(n - 1)` (right after the last component value).
    pub fn doc_id(&self) -> Result<RawBson, EngineError> {
        let last = self
            .n
            .checked_sub(1)
            .ok_or_else(|| EngineError::InvalidKey("index entry has no components".into()))?;
        let rel_end = self
            .end_offset(last)
            .ok_or_else(|| EngineError::InvalidKey("missing doc_id offset in index key".into()))?;
        let doc_id_start = self.value_start + rel_end;
        let tail = self
            .key
            .get(doc_id_start..)
            .ok_or_else(|| EngineError::InvalidKey("doc_id offset past end of index key".into()))?;
        let (bv, _) = crate::encoding::bson_value::BsonValue::parse_length_prefixed(tail)
            .ok_or_else(|| EngineError::InvalidKey("malformed doc_id in index key".into()))?;
        bv.to_raw_bson()
            .ok_or_else(|| EngineError::InvalidKey("unsupported doc_id type in index key".into()))
    }

    /// Lazily decode the *first* component's value to `RawBson` (the whole value
    /// for a single-field index).
    pub fn value(&self) -> Result<RawBson, EngineError> {
        self.component_value(0)?
            .ok_or_else(|| EngineError::InvalidKey("missing value in index key".into()))
    }
}

/// A loaded index definition: the field path and whether it is unique.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub path: String,
    pub unique: bool,
}

/// Options for creating a new index. Extensible — future index knobs
/// (sparse, collation, compound order) land here.
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// Enforce that no two live documents share the same value for this field.
    pub unique: bool,
}

/// Options for creating a new collection. All fields are optional
/// and fall back to engine defaults when `None`.
#[derive(Debug, Clone, Default)]
pub struct CreateCollectionOptions {
    /// Primary key field path. Defaults to `"_id"`.
    pub pk_path: Option<String>,
    /// TTL field path. Defaults to `"ttl"`.
    pub ttl_path: Option<String>,
}

/// Catalog operations for collection and index metadata.
///
/// Operates on a global `_sys_` column family internally.
/// Collections are scoped per column family: the pair `(cf, name)` is the
/// unique identity of a collection.
pub trait Catalog: EngineTransaction {
    /// Resolve a collection by CF and name into a live handle.
    fn collection(&self, cf: &str, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError>;

    /// List collections, optionally filtered to a single CF.
    fn list_collections(
        &self,
        cf: Option<&str>,
    ) -> Result<Vec<CollectionHandle<Self::Cf>>, EngineError>;

    fn create_collection(
        &self,
        cf: &str,
        name: &str,
        options: &CreateCollectionOptions,
    ) -> Result<(), EngineError>;

    fn drop_collection(&self, cf: &str, name: &str) -> Result<(), EngineError>;

    /// Create an index with explicit options (uniqueness, etc.) and backfill
    /// existing records.
    fn create_index_with_options(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        options: &IndexOptions,
    ) -> Result<(), EngineError>;

    /// Create a *compound* (multi-field) index with explicit options and backfill
    /// existing records. The component `fields` are matched left-to-right
    /// (leftmost-prefix rule). A single-element `fields` is equivalent to
    /// [`create_index_with_options`](Self::create_index_with_options).
    fn create_compound_index_with_options(
        &self,
        cf: &str,
        collection: &str,
        fields: &[String],
        options: &IndexOptions,
    ) -> Result<(), EngineError>;

    /// Create a non-unique index and backfill existing records.
    fn create_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), EngineError> {
        self.create_index_with_options(cf, collection, field, &IndexOptions::default())
    }

    /// Create a non-unique compound index and backfill existing records.
    fn create_compound_index(
        &self,
        cf: &str,
        collection: &str,
        fields: &[String],
    ) -> Result<(), EngineError> {
        self.create_compound_index_with_options(cf, collection, fields, &IndexOptions::default())
    }

    fn drop_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), EngineError>;

    /// Store a named function (trigger, validator, or computed field) for a collection.
    fn create_function(
        &self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
        name: &str,
        runtime: u8,
        source: &[u8],
    ) -> Result<(), EngineError>;

    /// Remove a named function from a collection.
    fn drop_function(
        &self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
        name: &str,
    ) -> Result<(), EngineError>;

    /// Load all function entries of a given kind for a collection.
    fn load_functions(
        &self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
    ) -> Result<Vec<FunctionEntry>, EngineError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::bson_value::BsonValue;
    use crate::encoding::index_record::IndexRecord;

    /// `IndexEntry::from_raw` bounds-checks the metadata: it must hold the `n`
    /// type bytes plus the `n` u32 value-side end-offsets, else construction
    /// rejects (returns `None`). Distinct from `IndexRecord::from_pair`'s own
    /// validation — this guards the scan-path decoder's header check directly.
    #[test]
    fn from_raw_rejects_short_metadata() {
        // n = 2 needs 2 + 2*4 = 10 metadata bytes; give it 9.
        let entry = IndexEntry::from_raw(vec![b'i', 0, b'k', 0, b'f', 0], vec![0u8; 9], 6, 2);
        assert!(entry.is_none());
    }

    /// `IndexEntry::from_raw` decodes a real production-encoded compound entry:
    /// every component value and the doc_id come back correctly via the value-side
    /// offsets. This pins the scan-path decoder (`from_raw` + accessors) directly,
    /// independent of the `IndexRecord::from_pair` read path.
    #[test]
    fn from_raw_decodes_production_entry() {
        let doc_id = BsonValue::from_bson(&bson::Bson::String("order-1".to_string())).unwrap();
        // status (String) + created_at (Int64, projected onto the f64 index key).
        let status = BsonValue::from_bson(&bson::Bson::String("active".to_string()))
            .and_then(BsonValue::into_index_value)
            .unwrap();
        let created = BsonValue::from_bson(&bson::Bson::Int64(20))
            .and_then(BsonValue::into_index_value)
            .unwrap();
        let field = "status\u{1}created_at";
        let (key, metadata) =
            IndexRecord::encode_compound("orders", field, &doc_id, &[status, created], None)
                .into_parts();

        let prefix_len = 2 + "orders".len() + 1 + field.len() + 1;
        let entry = IndexEntry::from_raw(key, metadata, prefix_len, 2).expect("decodes");

        // Component 0 is the status string; component 1 the numeric.
        let c0 = entry.component_value(0).unwrap().unwrap();
        assert_eq!(
            format!("{c0:?}"),
            format!("{:?}", bson::RawBson::String("active".into()))
        );
        let c1 = entry.component_value(1).unwrap().unwrap();
        assert_eq!(format!("{c1:?}"), format!("{:?}", bson::RawBson::Int64(20)));
        // One past the end is `None`.
        assert!(entry.component_value(2).unwrap().is_none());
        // doc_id round-trips, and with no TTL the entry never expires.
        assert_eq!(
            format!("{:?}", entry.doc_id().unwrap()),
            format!("{:?}", bson::RawBson::String("order-1".into()))
        );
        assert!(!entry.is_expired(i64::MAX));
    }
}
