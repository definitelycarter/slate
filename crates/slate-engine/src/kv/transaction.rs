use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Bound;

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::spec::ElementType;
use slate_store::{Store, StoreError, Transaction};

use crate::encoding::bson_value::BsonValue;
use crate::encoding::index_record::is_index_expired;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::index_sync::{IndexChanges, IndexDiff};
use crate::traits::{
    CollectionHandle, CompoundRange, CompoundTail, EngineTransaction, IndexCursor, IndexEntry,
    IndexRange, VectorScanEntry,
};
use crate::validate::validate_raw_document;
use crate::vector::{
    decode_vector_entry, encode_vector_entry, is_vector_entry_expired, pack_vector,
};

/// A short name for a Bson value the index cannot encode as a key (the
/// `_ => None` arm of [`BsonValue::from_bson`]) — for the `UnscannableBound`
/// error message.
fn unscannable_bson_type(value: &bson::Bson) -> &'static str {
    use bson::Bson;
    match value {
        Bson::Null => "null",
        Bson::Undefined => "undefined",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Binary(_) => "binary",
        Bson::Decimal128(_) => "decimal128",
        Bson::Double(f) if f.is_nan() => "NaN",
        _ => "non-scalar",
    }
}

/// How an `Eq` recheck constrains a candidate entry's type tag.
enum ExactTag {
    /// Non-numeric `Eq`: the tag must equal this exact type — distinct types can
    /// share sortable bytes, so the tag disambiguates.
    Exact(ElementType),
    /// Numeric `Eq`: the f64 key collapses Int32/Int64/Double, so any numeric tag
    /// matches — but a byte-coincident `DateTime` (also an 8-byte value) must
    /// not, since `compare_bson` treats number vs DateTime as incomparable.
    Numeric,
}

/// What an index scan resolves to: the byte range to seek (a `(Bound, Bound)`
/// pair that `scan_range` consumes as `RangeBounds<Vec<u8>>`), the per-entry
/// `Eq` exact-match, and the field-prefix length for decoding entries.
struct ResolvedScan {
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    /// `Eq` exact-match: the value bytes plus a type-tag constraint. `None` for
    /// `Range`/`Full` (a `Range` is a conservative superset the executor refines).
    exact: Option<(ExactTag, Vec<u8>)>,
    field_prefix_len: usize,
}

/// Turn an [`IndexRange`] into a byte range over a field's index keyspace.
///
/// `Eq(v)` seeks the value's prefix range and carries an exact-match — a byte
/// range can't reject same-bytes-different-type or longer prefix-sharing values
/// (the type isn't in the key). A `Range` becomes a *conservative superset*: it
/// seeks to the lower bound (no over-scan) and stops just past the upper,
/// ignoring inclusivity — the executor's coercing `compare_bson` recheck applies
/// the exact bounds (and cross-type) over whatever we yield. A bound the sparse
/// index can't encode (null / non-scalar) is a contract violation → error.
fn resolve_index_scan(
    collection: &str,
    field: &str,
    range: IndexRange<'_>,
) -> Result<ResolvedScan, EngineError> {
    let field_prefix =
        KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
    let field_prefix_len = field_prefix.len();

    // The key at which a value's entries begin: `i\0coll\0field\0` + value bytes.
    let value_key = |value: &[u8]| {
        KeyPrefix::IndexValue(Cow::Borrowed(collection), Cow::Borrowed(field), value).encode()
    };
    // Exclusive upper covering everything sharing `start` as a prefix (one value's
    // entries, or the whole field). `Unbounded` when `start` is all `0xFF`.
    let upper_after = |start: &[u8]| match increment_key(start) {
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };

    let (range, exact) = match range {
        IndexRange::Full => {
            // Compute the exclusive upper from a borrow, then move `field_prefix`
            // into the lower bound — no clone.
            let end = upper_after(&field_prefix);
            ((Bound::Included(field_prefix), end), None)
        }
        IndexRange::Eq(value) => {
            let bv = encode_index_value(value, field)?;
            // A numeric Eq accepts any numeric tag (the f64 key already collapses
            // Int32/Int64/Double); a non-numeric Eq must match its exact tag.
            let exact_tag = if is_numeric_tag(bv.tag) {
                ExactTag::Numeric
            } else {
                ExactTag::Exact(bv.tag)
            };
            let value_bytes = bv.bytes.into_owned();
            let start = value_key(&value_bytes);
            let end = upper_after(&start);
            (
                (Bound::Included(start), end),
                Some((exact_tag, value_bytes)),
            )
        }
        IndexRange::Range { lower, upper } => {
            // Resolve the upper first (a borrow of `field_prefix` when unbounded)
            // so the unbounded-lower case can *move* `field_prefix` into the lower
            // bound rather than clone it.
            let hi = match upper {
                Some((v, _incl)) => {
                    let key = value_key(encode_index_value(v, field)?.bytes.as_ref());
                    upper_after(&key)
                }
                None => upper_after(&field_prefix),
            };
            let lo = match lower {
                Some((v, _incl)) => {
                    Bound::Included(value_key(encode_index_value(v, field)?.bytes.as_ref()))
                }
                None => Bound::Included(field_prefix),
            };
            ((lo, hi), None)
        }
        IndexRange::Prefix(prefix) => {
            // A string's index value is its raw UTF-8 bytes (no length prefix in
            // the value portion of the key), so the prefix bytes seek straight in:
            // every entry whose key continues `i\0coll\0field\0{prefix}…` shares
            // the prefix. `upper_after` stops just past them (the final prefix byte
            // + 1 — never a carry, as no UTF-8 byte is 0xFF). No exact-match: every
            // prefix-sharing value is a wanted candidate; the executor's residual
            // recheck drops cross-type byte coincidences. Empty prefixes never
            // reach here — the planner keeps those a full scan.
            let start = value_key(prefix.as_bytes());
            let end = upper_after(&start);
            ((Bound::Included(start), end), None)
        }
    };

    Ok(ResolvedScan {
        range,
        exact,
        field_prefix_len,
    })
}

/// What a compound index scan resolves to: the byte range to seek and the
/// field-prefix length for decoding entries. Unlike the single-field
/// [`ResolvedScan`], no per-entry exact-match is computed here — the executor
/// rechecks the leading equalities (and the trailing range) exactly, since a
/// variable-width leading component's byte prefix is a conservative superset.
struct ResolvedCompoundScan {
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    field_prefix_len: usize,
    type_byte_count: usize,
}

/// Turn a [`CompoundRange`] into a byte range over a compound index's keyspace.
///
/// The lower bound concatenates the equality-prefix value bytes onto the field
/// prefix, plus the tail's lower bound; the upper bound is the exclusive
/// successor of that prefix, narrowed by the tail's upper bound. Inclusivity is
/// ignored at the byte level (the executor's recheck applies exact bounds), so
/// this is a conservative superset — exactly the single-field `Range` contract,
/// lifted to the compound prefix.
fn resolve_compound_scan(
    collection: &str,
    field: &str,
    range: CompoundRange<'_>,
) -> Result<ResolvedCompoundScan, EngineError> {
    let field_prefix =
        KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
    let field_prefix_len = field_prefix.len();
    let type_byte_count = field.as_bytes().iter().filter(|&&b| b == 0x01).count() + 1;

    // Common prefix: field prefix + the equality components' value bytes.
    let mut prefix = field_prefix;
    for value in range.eq_prefix {
        prefix.extend_from_slice(encode_index_value(value, field)?.bytes.as_ref());
    }

    let upper_after = |start: &[u8]| match increment_key(start) {
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };

    let (lo, hi) = match range.tail {
        CompoundTail::Unbounded => {
            let end = upper_after(&prefix);
            (Bound::Included(prefix), end)
        }
        CompoundTail::Eq(value) => {
            let mut lower = prefix;
            lower.extend_from_slice(encode_index_value(value, field)?.bytes.as_ref());
            let end = upper_after(&lower);
            (Bound::Included(lower), end)
        }
        CompoundTail::Range { lower, upper } => {
            // Upper first so the unbounded-lower case can move `prefix`.
            let hi = match upper {
                Some((v, _incl)) => {
                    // Clone justified: when both bounds are present the lower-bound
                    // branch below still needs `prefix`, so the upper key is built
                    // from a copy. Bounded both-sided ranges are the rare case.
                    let mut key = prefix.clone();
                    key.extend_from_slice(encode_index_value(v, field)?.bytes.as_ref());
                    upper_after(&key)
                }
                None => upper_after(&prefix),
            };
            let lo = match lower {
                Some((v, _incl)) => {
                    let mut key = prefix;
                    key.extend_from_slice(encode_index_value(v, field)?.bytes.as_ref());
                    Bound::Included(key)
                }
                None => Bound::Included(prefix),
            };
            (lo, hi)
        }
    };

    Ok(ResolvedCompoundScan {
        range: (lo, hi),
        field_prefix_len,
        type_byte_count,
    })
}

/// Encode a value as index-key bytes, or `UnscannableBound` if the sparse index
/// can't hold it (null / non-scalar). Shared by `Eq` and `Range` resolution.
fn encode_index_value(value: &bson::Bson, field: &str) -> Result<BsonValue<'static>, EngineError> {
    // Project numerics onto the f64 index key so the seek bound matches stored
    // entries (the write path does the same); non-numerics pass through. `None`
    // covers null / non-scalar (unencodable) and NaN (incomparable).
    BsonValue::from_bson(value)
        .and_then(BsonValue::into_index_value)
        .ok_or_else(|| EngineError::UnscannableBound {
            field: field.to_string(),
            value_type: unscannable_bson_type(value),
        })
}

/// Whether `tag` is one of the numeric types unified onto the f64 index key.
fn is_numeric_tag(tag: ElementType) -> bool {
    matches!(
        tag,
        ElementType::Int32 | ElementType::Int64 | ElementType::Double
    )
}

/// Decode one raw store entry into an `IndexEntry`, applying the single-field
/// scan's per-entry filter: expiry first (the common drop), then the `Eq`
/// exact-match (a byte range can't reject same-bytes-different-type or longer
/// prefix-sharing values). `Ok(None)` ⇒ skip (expired / non-match), `Ok(Some)`
/// ⇒ keep, `Err` ⇒ malformed key. Shared by `scan_index` and the seekable
/// [`KvIndexCursor`] so both honour the identical contract.
fn filter_entry(
    key: Vec<u8>,
    metadata: Vec<u8>,
    field_prefix_len: usize,
    exact: &Option<(ExactTag, Vec<u8>)>,
    ttl: i64,
) -> Result<Option<IndexEntry>, EngineError> {
    // Single-field scan: one component (n = 1).
    let Some(entry) = IndexEntry::from_raw(key, metadata, field_prefix_len, 1) else {
        return Err(EngineError::InvalidKey("invalid index key".into()));
    };
    if entry.is_expired(ttl) {
        return Ok(None);
    }
    if let Some((want_tag, want_bytes)) = exact {
        let tag_ok = match want_tag {
            ExactTag::Exact(t) => entry.element_type() == Some(*t),
            ExactTag::Numeric => entry.element_type().is_some_and(is_numeric_tag),
        };
        if entry.value_bytes() != want_bytes.as_slice() || !tag_ok {
            return Ok(None);
        }
    }
    Ok(Some(entry))
}

/// Cheap `next()` steps a galloping [`IndexCursor::seek`] takes before paying one
/// fresh range seek for the residual gap. Keeps the balanced (interleaved) case
/// on sequential advances — a fresh seek costs several× a `next()` on every
/// backend — while still bounding the skewed case to a per-output seek. The spike
/// found `8` a good balance; revisit with the real intersection bench.
const GALLOP_LIMIT: usize = 8;

/// The exclusive successor of a byte key: its last non-`0xFF` byte incremented,
/// trailing `0xFF`s carried. `None` when every byte is `0xFF`.
fn increment_key(key: &[u8]) -> Option<Vec<u8>> {
    let mut upper = key.to_vec();
    for byte in upper.iter_mut().rev() {
        match byte.checked_add(1) {
            Some(b) => {
                *byte = b;
                return Some(upper);
            }
            None => *byte = 0x00,
        }
    }
    None
}

// ── KvTransaction ──────────────────────────────────────────────

pub struct KvTransaction<'a, S: Store + 'a> {
    pub(crate) txn: S::Txn<'a>,
    pub(crate) now_millis: i64,
    /// Per-transaction memoization of resolved collection handles, keyed by
    /// `(cf, name)`. Resolving a handle costs a sys-CF get, an index prefix
    /// scan, and a deserialize; a single query resolves it twice (find meta and
    /// executor node), and a `find_one` loop repeats that per call. Cached
    /// handles are invalidated by the DDL methods that change a collection's
    /// shape (`drop_collection`, `create_index_with_options`, `drop_index`).
    #[allow(clippy::type_complexity)]
    pub(crate) catalog_cache:
        RefCell<HashMap<(String, String), CollectionHandle<<S::Txn<'a> as Transaction>::Cf>>>,
}

// ── Private helpers ─────────────────────────────────────────────

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    pub(crate) fn extract_pk(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        doc: &RawDocument,
    ) -> Result<BsonValue<'static>, EngineError> {
        match doc.get(handle.pk_path()) {
            Ok(Some(val)) => {
                let bv = BsonValue::from_raw_bson_ref(val)
                    .ok_or_else(|| EngineError::InvalidDocument("unsupported pk type".into()))?;
                Ok(BsonValue {
                    tag: bv.tag,
                    bytes: Cow::Owned(bv.bytes.into_owned()),
                })
            }
            _ => Err(EngineError::InvalidDocument(format!(
                "missing pk field '{}'",
                handle.pk_path()
            ))),
        }
    }

    pub(crate) fn apply_index_changes(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        changes: &IndexChanges,
    ) -> Result<(), EngineError> {
        if !changes.deletes.is_empty() {
            let refs: Vec<&[u8]> = changes.deletes.iter().map(|k| k.as_slice()).collect();
            self.txn.delete_batch(handle.cf(), &refs)?;
        }
        if !changes.puts.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = changes
                .puts
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.txn.put_batch(handle.cf(), &refs)?;
        }

        // Unique-index entries. Deletes are blind — a `u` slot is owned by
        // exactly one document, so nothing else can have taken it. Process them
        // before puts so a value moved within this document frees its old slot
        // first.
        for key in &changes.unique_deletes {
            self.txn.delete(handle.cf(), key)?;
        }
        // Puts must pass a collision check. Any present slot owned by a
        // *different* document is a violation — including one whose owner has
        // expired but not yet been purged (we intentionally fail rather than
        // steal the slot). A slot already owned by this document is idempotent.
        // Two concurrent inserts of the same value write the same `u` key and
        // collide as a write-write conflict at commit, so this in-snapshot check
        // need only catch already-visible duplicates.
        for (key, value) in &changes.unique_puts {
            if let Some(existing) = self.txn.get(handle.cf(), key)?
                && existing != *value
            {
                return Err(unique_violation(handle.name(), key, &existing));
            }
            self.txn.put(handle.cf(), key, value)?;
        }
        Ok(())
    }

    /// Bring the vector indexes in sync with a written document.
    ///
    /// For each declared vector field: pack the new embedding, frame it with the
    /// document's TTL (so a vector scan can drop expired entries reading only the
    /// vector keyspace — see [`encode_vector_entry`]), and write it at the doc's
    /// vector key (overwriting in place — vector entries are keyed by doc_id, not
    /// value, so a changed embedding needs no separate delete), or, if the field
    /// is now absent, blind-delete any stale entry. A wrong-dimensionality or
    /// non-numeric vector returns an error, aborting the write (Cosmos parity).
    /// `ttl_millis` is the document's TTL (the same value stamped into its record
    /// and index entries). Zero-cost when the collection has no vector indexes.
    fn maintain_vector_indexes_on_put(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        doc_id: &BsonValue<'_>,
        doc: &RawDocument,
        ttl_millis: Option<i64>,
    ) -> Result<(), EngineError> {
        for spec in handle.vector_indexes() {
            let key = Key::encode_vector_key(handle.name(), &spec.path, doc_id);
            match pack_vector(doc, &spec.path, spec.dims)? {
                Some(packed) => {
                    let entry = encode_vector_entry(ttl_millis, &packed);
                    self.txn.put(handle.cf(), &key, &entry)?
                }
                None => self.txn.delete(handle.cf(), &key)?,
            }
        }
        Ok(())
    }

    /// Remove a document's vector entries (delete path). Blind deletes — a vector
    /// key is owned by exactly one document. No-op when there are no vector indexes.
    fn maintain_vector_indexes_on_delete(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        for spec in handle.vector_indexes() {
            let key = Key::encode_vector_key(handle.name(), &spec.path, doc_id);
            self.txn.delete(handle.cf(), &key)?;
        }
        Ok(())
    }
}

/// Build a [`EngineError::UniqueViolation`] from the colliding `u` key and the
/// existing slot value (the owning doc_id, length-prefixed).
pub(crate) fn unique_violation(collection: &str, key: &[u8], existing_value: &[u8]) -> EngineError {
    let (field, value) = match Key::decode_unique_index(key) {
        Some((_, field, keyed)) => {
            // keyed value is `[type_byte][sortable_value_bytes]`
            let value = keyed
                .split_first()
                .and_then(|(tag, bytes)| {
                    ElementType::from(*tag).map(|t| BsonValue::from_parts(t, bytes).to_string())
                })
                .unwrap_or_else(|| "<unknown>".to_string());
            (field.to_string(), value)
        }
        None => ("<unknown>".to_string(), "<unknown>".to_string()),
    };
    let existing_id = BsonValue::parse_length_prefixed(existing_value)
        .map(|(bv, _)| bv.to_string())
        .unwrap_or_else(|| "<unknown>".to_string());
    EngineError::UniqueViolation {
        index: format!("{collection}.{field}"),
        value,
        existing_id,
    }
}

// ── EngineTransaction impl ──────────────────────────────────────

impl<'a, S: Store + 'a> EngineTransaction for KvTransaction<'a, S> {
    type Cf = <S::Txn<'a> as Transaction>::Cf;

    fn now_millis(&self) -> i64 {
        self.now_millis
    }

    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(handle.name(), &doc_id);
        match self.txn.get(handle.cf(), &encoded)? {
            None => Ok(None),
            Some(data) if Record::is_expired(&data, self.now_millis) => Ok(None),
            Some(data) => Ok(Some(RawDocumentBuf::try_from(Record::from_bytes(data)?)?)),
        }
    }

    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError> {
        let doc_id = self.extract_pk(handle, doc)?;

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(handle.name(), &doc_id);
        let record = Record::encoder()
            .with_ttl_at_path(handle.ttl_path())
            .encode(doc);
        let old_data = self.txn.get(handle.cf(), &encoded_key)?;

        // Fast path: identical record bytes means nothing changed — skip
        // the index diff entirely. Unlikely in practice (overwriting a
        // document with the same data) but avoids decoding both records
        // and diffing index entries for zero result.
        if old_data.as_deref() == Some(record.as_bytes()) {
            return Ok(());
        }

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(handle.indexes())
            .with_unique_property_paths(handle.unique_indexes())
            .with_property_path(handle.ttl_path())
            .diff(handle.name())?;

        self.apply_index_changes(handle, &changes)?;
        self.maintain_vector_indexes_on_put(handle, &doc_id, doc, record.ttl_millis())?;
        self.txn.put(handle.cf(), &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError> {
        let doc_id = self.extract_pk(handle, doc)?;

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(handle.name(), &doc_id);
        let old_data = self.txn.get(handle.cf(), &encoded_key)?;

        if let Some(ref data) = old_data
            && !Record::is_expired(data, self.now_millis)
        {
            return Err(EngineError::DuplicateKey(doc_id.to_string()));
        }

        let record = Record::encoder()
            .with_ttl_at_path(handle.ttl_path())
            .encode(doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(handle.indexes())
            .with_unique_property_paths(handle.unique_indexes())
            .with_property_path(handle.ttl_path())
            .diff(handle.name())?;

        self.apply_index_changes(handle, &changes)?;
        self.maintain_vector_indexes_on_put(handle, &doc_id, doc, record.ttl_millis())?;
        self.txn.put(handle.cf(), &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<(), EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(handle.name(), &doc_id);

        let old_data = self.txn.get(handle.cf(), &encoded)?;
        if let Some(ref data) = old_data {
            let changes = IndexDiff::for_delete(&doc_id)
                .with_old_record(Some(data.as_slice()))
                .with_property_paths(handle.indexes())
                .with_unique_property_paths(handle.unique_indexes())
                .with_property_path(handle.ttl_path())
                .diff(handle.name())?;

            self.apply_index_changes(handle, &changes)?;
            self.maintain_vector_indexes_on_delete(handle, &doc_id)?;
            self.txn.delete(handle.cf(), &encoded)?;
        }

        Ok(())
    }

    fn scan<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
    ) -> Result<Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'b>, EngineError>
    {
        let now = self.now_millis;
        let prefix = KeyPrefix::Record(Cow::Borrowed(handle.name())).encode();
        let iter = self.txn.scan_prefix(handle.cf(), &prefix)?;
        Ok(Box::new(iter.filter_map(move |result| match result {
            Err(e) => Some(Err(EngineError::Store(e))),
            Ok((_key_bytes, value_bytes)) => {
                if Record::is_expired(&value_bytes, now) {
                    return None;
                }
                match Record::from_bytes(value_bytes).and_then(RawDocumentBuf::try_from) {
                    Ok(doc) => Some(Ok(doc)),
                    Err(e) => Some(Err(e)),
                }
            }
        })))
    }

    fn scan_index<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: IndexRange<'_>,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'b>, EngineError> {
        let ttl = self.now_millis;
        let ResolvedScan {
            range,
            exact,
            field_prefix_len,
        } = resolve_index_scan(handle.name(), field, range)?;

        // `scan_range` seeks straight to the lower bound (no lower-bound
        // over-scan) and stops past the upper, so the per-entry work is only the
        // expiry check and `Eq`'s exact-match — no manual bounds/early-termination.
        let mut iter = self.txn.scan_range(handle.cf(), range, reverse)?;
        let mut done = false;

        Ok(Box::new(std::iter::from_fn(move || {
            if done {
                return None;
            }
            for result in iter.by_ref() {
                let (key_bytes, metadata_bytes) = match result {
                    Ok(kv) => kv,
                    Err(e) => {
                        done = true;
                        return Some(Err(EngineError::Store(e)));
                    }
                };
                // Per-entry filter (expiry, then `Eq` exact-match) — shared with
                // the seekable cursor so both honour the identical contract.
                match filter_entry(key_bytes, metadata_bytes, field_prefix_len, &exact, ttl) {
                    Ok(Some(entry)) => return Some(Ok(entry)),
                    Ok(None) => continue,
                    Err(e) => {
                        done = true;
                        return Some(Err(e));
                    }
                }
            }
            done = true;
            None
        })))
    }

    fn scan_compound_index<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: CompoundRange<'_>,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'b>, EngineError> {
        let ttl = self.now_millis;
        let ResolvedCompoundScan {
            range,
            field_prefix_len,
            type_byte_count,
        } = resolve_compound_scan(handle.name(), field, range)?;

        let mut iter = self.txn.scan_range(handle.cf(), range, reverse)?;
        let mut done = false;

        Ok(Box::new(std::iter::from_fn(move || {
            if done {
                return None;
            }
            for result in iter.by_ref() {
                let (key_bytes, metadata_bytes) = match result {
                    Ok(kv) => kv,
                    Err(e) => {
                        done = true;
                        return Some(Err(EngineError::Store(e)));
                    }
                };
                let Some(entry) = IndexEntry::from_raw(
                    key_bytes,
                    metadata_bytes,
                    field_prefix_len,
                    type_byte_count,
                ) else {
                    done = true;
                    return Some(Err(EngineError::InvalidKey("invalid index key".into())));
                };
                if entry.is_expired(ttl) {
                    continue;
                }
                // No engine-side exact-match: the byte seek is a conservative
                // superset over the compound prefix, and the executor rechecks
                // every leading equality (and the trailing range) exactly via the
                // entry's per-component values.
                return Some(Ok(entry));
            }
            done = true;
            None
        })))
    }

    fn open_index_cursor<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        value: &bson::Bson,
        reverse: bool,
    ) -> Result<Box<dyn IndexCursor + 'b>, EngineError> {
        let ttl = self.now_millis;
        let ResolvedScan {
            range,
            exact,
            field_prefix_len,
        } = resolve_index_scan(handle.name(), field, IndexRange::Eq(value))?;
        let (lower, upper) = range;
        // An `Eq` scan always resolves to an inclusive value-prefix lower bound;
        // the cursor seeks by appending raw doc-id bytes onto that prefix.
        let Bound::Included(value_prefix) = lower else {
            return Err(EngineError::InvalidKey(
                "equality index scan must resolve to an inclusive lower bound".into(),
            ));
        };
        // Clone the cf handle (a cheap Arc bump): the cursor outlives the borrowed
        // `handle` and re-issues `scan_range` on the cf for every seek. The cloned
        // `value_prefix`/`upper` feed the initial scan while the cursor keeps the
        // originals for later seek-key construction (`scan_range` consumes its
        // owned bounds).
        let cf = handle.cf().clone();
        let init: (Bound<Vec<u8>>, Bound<Vec<u8>>) =
            (Bound::Included(value_prefix.clone()), upper.clone());
        let iter = self.txn.scan_range(&cf, init, reverse)?;
        let mut cursor: KvIndexCursor<'b, 'a, S> = KvIndexCursor {
            txn: &self.txn,
            cf,
            value_prefix,
            upper,
            exact,
            field_prefix_len,
            ttl,
            reverse,
            iter,
            current: None,
        };
        cursor.pull()?;
        Ok(Box::new(cursor))
    }

    fn scan_vectors<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<VectorScanEntry, EngineError>> + 'b>, EngineError>
    {
        let now = self.now_millis;
        let prefix =
            KeyPrefix::VectorField(Cow::Borrowed(handle.name()), Cow::Borrowed(field)).encode();
        let iter = self.txn.scan_prefix(handle.cf(), &prefix)?;
        // Own `field` for the (cold) error path so the iterator doesn't borrow the
        // caller's `&str` past this call.
        let field = field.to_string();
        Ok(Box::new(iter.filter_map(move |result| {
            let (key_bytes, value_bytes) = match result {
                Ok(kv) => kv,
                Err(e) => return Some(Err(EngineError::Store(e))),
            };
            // Expiry first — an O(1) tag-byte check (no record read), matching how
            // a traditional index scan reads the TTL from the entry's metadata. An
            // expired-but-not-yet-purged document is dropped here rather than
            // yielded only to be discarded by the downstream key-lookup (which
            // would return fewer than k from a top-k). A TTL-free collection pays
            // just the single tag read.
            if is_vector_entry_expired(&value_bytes, now) {
                return None;
            }
            let doc_id = match Key::decode(&key_bytes) {
                Some(Key::Vector(_, _, doc_id)) => match doc_id.to_raw_bson() {
                    Some(id) => id,
                    None => {
                        return Some(Err(EngineError::InvalidKey(
                            "unsupported doc_id type in vector key".into(),
                        )));
                    }
                },
                _ => return Some(Err(EngineError::InvalidKey("invalid vector key".into()))),
            };
            match decode_vector_entry(&value_bytes) {
                Some(vector) => Some(Ok((doc_id, vector))),
                None => Some(Err(EngineError::InvalidDocument(format!(
                    "vector entry for '{field}' is malformed"
                )))),
            }
        })))
    }

    fn purge(&self, handle: &CollectionHandle<Self::Cf>) -> Result<u64, EngineError> {
        self.purge_before(handle, self.now_millis)
    }

    fn purge_before(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        as_of_millis: i64,
    ) -> Result<u64, EngineError> {
        let ttl_prefix = KeyPrefix::IndexField(
            Cow::Borrowed(handle.name()),
            Cow::Borrowed(handle.ttl_path()),
        )
        .encode();
        let iter = self.txn.scan_prefix(handle.cf(), &ttl_prefix)?;

        let mut expired_ids: Vec<BsonValue<'static>> = Vec::new();
        for result in iter {
            let (key_bytes, metadata_bytes) = result?;
            if !is_index_expired(&metadata_bytes, as_of_millis) {
                break;
            }
            let Some(record) = IndexRecord::from_pair(key_bytes, metadata_bytes) else {
                continue;
            };
            let Some(id) = record.doc_id() else {
                continue;
            };
            expired_ids.push(BsonValue {
                tag: id.tag,
                bytes: Cow::Owned(id.bytes.into_owned()),
            });
        }

        let mut deleted = 0u64;
        for doc_id in &expired_ids {
            let encoded = Key::encode_record_key(handle.name(), doc_id);
            let old_data = self.txn.get(handle.cf(), &encoded)?;
            if let Some(ref data) = old_data {
                let changes = IndexDiff::for_delete(doc_id)
                    .with_old_record(Some(data.as_slice()))
                    .with_property_paths(handle.indexes())
                    .with_unique_property_paths(handle.unique_indexes())
                    .with_property_path(handle.ttl_path())
                    .diff(handle.name())?;
                self.apply_index_changes(handle, &changes)?;
                self.maintain_vector_indexes_on_delete(handle, doc_id)?;
                self.txn.delete(handle.cf(), &encoded)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    fn commit(self) -> Result<(), EngineError> {
        Ok(self.txn.commit()?)
    }

    fn rollback(self) -> Result<(), EngineError> {
        Ok(self.txn.rollback()?)
    }
}

// ── KvIndexCursor ───────────────────────────────────────────────

/// The raw `(key, value)` iterator a store range scan yields, borrowed for `'t`.
type ScanIter<'t> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 't>;

/// A seekable cursor over one equality index range — the engine seam the
/// index-intersection skip-merge zig-zags. Holds an open `scan_range` over the
/// value's keyspace; `seek` carries a small gap with cheap `next()`s and re-issues
/// a fresh range scan only to skip a large one (see [`IndexCursor`]).
///
/// `'t` is the borrow of the engine transaction; `'a` is the transaction's own
/// lifetime (`'a: 't`).
struct KvIndexCursor<'t, 'a: 't, S: Store + 'a> {
    txn: &'t S::Txn<'a>,
    cf: <S::Txn<'a> as Transaction>::Cf,
    /// `i\0coll\0field\0value_bytes` — the fixed value prefix. A seek key is this
    /// followed by the target's raw doc-id bytes.
    value_prefix: Vec<u8>,
    /// Exclusive successor of `value_prefix` (the value range's upper bound), or
    /// `Unbounded` when the prefix is all-`0xFF`.
    upper: Bound<Vec<u8>>,
    exact: Option<(ExactTag, Vec<u8>)>,
    field_prefix_len: usize,
    ttl: i64,
    reverse: bool,
    iter: ScanIter<'t>,
    current: Option<IndexEntry>,
}

impl<'t, 'a: 't, S: Store + 'a> KvIndexCursor<'t, 'a, S> {
    /// Pull the next kept entry from the open iterator into `current` (skipping
    /// expired / `Eq`-mismatched entries), or `None` at the range's end.
    fn pull(&mut self) -> Result<(), EngineError> {
        loop {
            match self.iter.next() {
                None => {
                    self.current = None;
                    return Ok(());
                }
                Some(Err(e)) => {
                    self.current = None;
                    return Err(EngineError::Store(e));
                }
                Some(Ok((key, metadata))) => {
                    match filter_entry(key, metadata, self.field_prefix_len, &self.exact, self.ttl)?
                    {
                        Some(entry) => {
                            self.current = Some(entry);
                            return Ok(());
                        }
                        None => continue,
                    }
                }
            }
        }
    }

    /// Re-open the underlying iterator positioned at the first entry at-or-past
    /// `doc_id` in scan order, then pull. Forward: lower-bound the value range at
    /// `value_prefix ++ doc_id`. Reverse: upper-bound it there (inclusive) and let
    /// the reverse scan yield the largest ≤ entry first. The `value_prefix` /
    /// `upper` clones are required — `scan_range` consumes its owned bounds and the
    /// cursor keeps the originals for the next seek (both are short key prefixes).
    fn reseek(&mut self, doc_id: &[u8]) -> Result<(), EngineError> {
        let mut bound_key = self.value_prefix.clone();
        bound_key.extend_from_slice(doc_id);
        let range: (Bound<Vec<u8>>, Bound<Vec<u8>>) = if self.reverse {
            (
                Bound::Included(self.value_prefix.clone()),
                Bound::Included(bound_key),
            )
        } else {
            (Bound::Included(bound_key), self.upper.clone())
        };
        self.iter = self.txn.scan_range(&self.cf, range, self.reverse)?;
        self.pull()
    }

    /// Whether `current` has reached `doc_id` in scan order — or the cursor is
    /// exhausted, in which case there is nothing further to reach.
    fn reached(&self, doc_id: &[u8]) -> Result<bool, EngineError> {
        match &self.current {
            None => Ok(true),
            Some(entry) => {
                let cur = entry.doc_id_bytes()?;
                Ok(if self.reverse {
                    cur <= doc_id
                } else {
                    cur >= doc_id
                })
            }
        }
    }
}

impl<'t, 'a: 't, S: Store + 'a> IndexCursor for KvIndexCursor<'t, 'a, S> {
    fn peek(&self) -> Option<&IndexEntry> {
        self.current.as_ref()
    }

    fn advance(&mut self) -> Result<(), EngineError> {
        self.pull()
    }

    fn seek(&mut self, doc_id: &[u8]) -> Result<(), EngineError> {
        // Gallop: cheap sequential advances carry a small gap (gaps ≈ 1 in the
        // balanced/interleaved case, so this never seeks there)...
        for _ in 0..GALLOP_LIMIT {
            if self.reached(doc_id)? {
                return Ok(());
            }
            self.pull()?;
        }
        // ...and one fresh range seek pays for a large residual gap (the skewed
        // case), landing exactly at the target.
        if !self.reached(doc_id)? {
            self.reseek(doc_id)?;
        }
        Ok(())
    }
}
