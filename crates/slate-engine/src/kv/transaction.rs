use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Bound;

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use bson::spec::ElementType;
use slate_store::{Store, Transaction};

use crate::encoding::bson_value::BsonValue;
use crate::encoding::index_record::is_index_expired;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::index_sync::{IndexChanges, IndexDiff};
use crate::traits::{CollectionHandle, EngineTransaction, IndexEntry, IndexRange};
use crate::validate::validate_raw_document;

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
                let Some(entry) = IndexEntry::from_raw(key_bytes, metadata_bytes, field_prefix_len)
                else {
                    done = true;
                    return Some(Err(EngineError::InvalidKey("invalid index key".into())));
                };
                // Expiry first — a cheap metadata check, the common reason to drop a row.
                if entry.is_expired(ttl) {
                    continue;
                }
                // Eq only: the byte range seeks the value prefix but can't filter
                // by type or string length, so the recheck drops longer
                // prefix-sharing entries (value-bytes) and type mismatches (tag).
                // A numeric Eq accepts any numeric tag (cross-type collapse) but
                // not a byte-coincident DateTime; a non-numeric Eq wants its exact
                // tag. Ranges/Full need none — `scan_range` bounds them (and the
                // executor's `compare_bson` recheck refines ranges).
                if let Some((want_tag, want_bytes)) = &exact {
                    let tag_ok = match want_tag {
                        ExactTag::Exact(t) => entry.element_type() == Some(*t),
                        ExactTag::Numeric => entry.element_type().is_some_and(is_numeric_tag),
                    };
                    if entry.value_bytes() != want_bytes.as_slice() || !tag_ok {
                        continue;
                    }
                }
                return Some(Ok(entry));
            }
            done = true;
            None
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
