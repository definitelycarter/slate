use bson::raw::RawDocument;
use bson::spec::ElementType;

use super::bson_value::{self, BsonValue};
use super::key::{self, Key};

// ── IndexRecord ──────────────────────────────────────────────
//
// An owned index entry backed by raw key-value bytes from the store.
//
// Key layout:  `i\0{collection}\0{field}\0{v1}…{vN}{doc_id_lp}` (no length suffix)
// Metadata:    `[t1…tN][end_1…end_N : u32 LE][ttl : i64 LE, optional]`, where
//              `end_k` is the cumulative value byte length through component `k`
//              (the value-side precomputed offsets that recover the value/doc_id
//              boundary). A single-field index is the `N == 1` case.
//
// Follows the Record pattern: raw owned bytes + pre-parsed offsets
// for O(1) typed access.

/// An owned index entry with typed accessors over raw bytes.
///
/// Construct via [`IndexRecord::from_pair`] (read side) or
/// [`IndexRecord::encode`] (write side).
#[allow(dead_code)]
pub struct IndexRecord {
    index_key: Vec<u8>,
    metadata: Vec<u8>,
    // Pre-parsed byte offsets into index_key:
    //   index_key[2..field_start-1]             = collection
    //   index_key[field_start..value_start-1]   = field
    //   index_key[value_start..doc_id_start]    = value_bytes
    //   index_key[doc_id_start..]               = doc_id (length-prefixed)
    field_start: usize,
    value_start: usize,
    doc_id_start: usize,
}

#[allow(dead_code)]
impl IndexRecord {
    /// Parse an index entry from raw store key-value bytes (read side).
    ///
    /// Takes ownership of the byte vectors. Returns `None` if the key
    /// is not a valid index key or the metadata is empty.
    pub fn from_pair(key_bytes: Vec<u8>, metadata: Vec<u8>) -> Option<Self> {
        if metadata.is_empty() {
            return None;
        }
        // Validate and compute offsets via a scoped borrow.
        let (field_start, value_start, doc_id_start) = {
            // Parse only the collection/field (the `i` tag is validated here). The
            // value/doc_id boundary is the value-side precomputed offset in the
            // metadata — the cumulative end of the *last* component — never an
            // ambiguous key scan. A single-field index is the one-component case,
            // so this also covers plain indexes.
            let (collection, field) = super::key::parse_index_collection_field(&key_bytes)?;
            let field_start = 2 + collection.len() + 1;
            let value_start = field_start + field.len() + 1;
            let n = component_count(field);
            let doc_id_rel = last_component_end(&metadata, n)?;
            let doc_id_start = value_start + doc_id_rel;
            if doc_id_start > key_bytes.len() {
                return None;
            }
            (field_start, value_start, doc_id_start)
        };
        Some(IndexRecord {
            index_key: key_bytes,
            metadata,
            field_start,
            value_start,
            doc_id_start,
        })
    }

    /// Encode a new index record from components (write side).
    ///
    /// Metadata layout: `[type_byte][end : u32 LE][ttl : i64 LE, optional]`, where
    /// `end` is the value byte length (the value-side precomputed offset that lets
    /// the read side recover the value/doc_id boundary without a key-side suffix).
    pub fn encode(
        collection: &str,
        field: &str,
        doc_id: &BsonValue<'_>,
        value: &BsonValue<'_>,
        ttl_millis: Option<i64>,
    ) -> Self {
        let mut metadata = Vec::with_capacity(1 + 4 + ttl_millis.map_or(0, |_| 8));
        metadata.push(value.tag as u8);
        metadata.extend_from_slice(&(value.bytes.len() as u32).to_le_bytes());
        if let Some(millis) = ttl_millis {
            metadata.extend_from_slice(&millis.to_le_bytes());
        }

        let index_key = Key::encode_index_key(collection, field, value, doc_id);

        let field_start = 2 + collection.len() + 1;
        let value_start = field_start + field.len() + 1;
        // The doc_id sits immediately after the value bytes (no key-side suffix).
        let doc_id_start = value_start + value.bytes.len();

        IndexRecord {
            index_key,
            metadata,
            field_start,
            value_start,
            doc_id_start,
        }
    }

    /// Encode a *compound* index record from N component values (write side).
    ///
    /// `field` is the joined compound identity (`f1\x01f2`); `values` are the
    /// per-component index values in field order. Metadata layout:
    /// `[t1…tN][end_1…end_N : u32 LE][ttl : i64 LE, optional]`, where `end_k` is
    /// the cumulative value byte length through component `k` (the value-side
    /// precomputed offsets). The read side recovers every component boundary from
    /// these — so the key carries no length suffixes. With a single value this is
    /// byte-for-byte identical to [`encode`](Self::encode).
    pub fn encode_compound(
        collection: &str,
        field: &str,
        doc_id: &BsonValue<'_>,
        values: &[BsonValue<'_>],
        ttl_millis: Option<i64>,
    ) -> Self {
        let mut metadata = Vec::with_capacity(values.len() * (1 + 4) + ttl_millis.map_or(0, |_| 8));
        for value in values {
            metadata.push(value.tag as u8);
        }
        // Cumulative end-offset per component, in field order.
        let mut cumulative: u32 = 0;
        for value in values {
            cumulative = cumulative.saturating_add(value.bytes.len() as u32);
            metadata.extend_from_slice(&cumulative.to_le_bytes());
        }
        if let Some(millis) = ttl_millis {
            metadata.extend_from_slice(&millis.to_le_bytes());
        }

        let mut index_key = Vec::new();
        Key::encode_compound_index_key_into(&mut index_key, collection, field, values, doc_id);

        let field_start = 2 + collection.len() + 1;
        let value_start = field_start + field.len() + 1;
        // The doc_id sits immediately after all the concatenated value bytes (no
        // key-side length suffixes).
        let values_len: usize = values.iter().map(|v| v.bytes.len()).sum();
        let doc_id_start = value_start + values_len;

        IndexRecord {
            index_key,
            metadata,
            field_start,
            value_start,
            doc_id_start,
        }
    }

    /// Compute index records for a document.
    ///
    /// Returns one `IndexRecord` per indexed field value. Each record contains
    /// the encoded index key and metadata (including TTL if present).
    ///
    /// A compound index identity (one containing the field separator) produces a
    /// single combined entry per document, sparse: if *any* component field is
    /// absent (or holds a non-scalar / multikey value), no entry is written —
    /// matching the leftmost-prefix model, which can only seek a fully-specified
    /// prefix. Single-field indexes keep their multikey (`[]`) fan-out.
    pub fn from_document(
        collection: &str,
        indexes: &[String],
        doc: &RawDocument,
        doc_id: &BsonValue<'_>,
        ttl_millis: Option<i64>,
    ) -> Vec<IndexRecord> {
        let mut entries = Vec::new();
        for field in indexes {
            if field.as_bytes().contains(&key::FIELD_SEP) {
                if let Some(values) = compound_index_values(field, doc) {
                    entries.push(IndexRecord::encode_compound(
                        collection, field, doc_id, &values, ttl_millis,
                    ));
                }
                continue;
            }
            for val in bson_value::extract_all(doc, field) {
                // Re-key numerics onto the f64 number tower (unified numeric
                // index key); a NaN value yields no entry.
                if let Some(val) = val.into_index_value() {
                    entries.push(IndexRecord::encode(
                        collection, field, doc_id, &val, ttl_millis,
                    ));
                }
            }
        }
        entries
    }

    // ── Typed accessors ─────────────────────────────────────────

    /// The collection name.
    pub fn collection(&self) -> Option<&str> {
        std::str::from_utf8(&self.index_key[2..self.field_start - 1]).ok()
    }

    /// The indexed field name.
    pub fn field(&self) -> Option<&str> {
        std::str::from_utf8(&self.index_key[self.field_start..self.value_start - 1]).ok()
    }

    /// The doc_id extracted from the key.
    pub fn doc_id(&self) -> Option<BsonValue<'_>> {
        Some(BsonValue::parse_length_prefixed(&self.index_key[self.doc_id_start..])?.0)
    }

    /// Raw sortable-encoded value bytes (no type tag).
    pub fn value_bytes(&self) -> &[u8] {
        &self.index_key[self.value_start..self.doc_id_start]
    }

    /// The BSON element type of the indexed value (from the metadata).
    pub fn type_byte(&self) -> u8 {
        self.metadata[0]
    }

    // ── Conversions ─────────────────────────────────────────────

    /// Convert the doc_id to `RawBson`.
    pub fn doc_id_bson(&self) -> Option<bson::RawBson> {
        self.doc_id()?.to_raw_bson()
    }

    /// Convert the indexed value to `RawBson`.
    ///
    /// Reconstructs the value from the type tag (metadata) and raw bytes (key).
    pub fn value_bson(&self) -> Option<bson::RawBson> {
        let tag = ElementType::from(self.type_byte())?;
        super::numeric_key::decode_index_value(tag, self.value_bytes())
    }

    // ── TTL ─────────────────────────────────────────────────────

    /// O(1) TTL expiry check on the metadata bytes.
    ///
    /// Returns `false` for entries without a TTL block. The TTL, when present,
    /// follows the `n` type bytes and the `n` u32 value-side end-offsets, so the
    /// offset is derived from the field's component count.
    #[inline]
    pub fn is_expired(&self, now_millis: i64) -> bool {
        let n = self.field().map(component_count).unwrap_or(1);
        is_index_expired_n(&self.metadata, n, now_millis)
    }

    // ── Raw byte access ─────────────────────────────────────────

    /// The raw encoded index key bytes.
    pub fn key_bytes(&self) -> &[u8] {
        &self.index_key
    }

    /// The raw metadata bytes.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// Consume the record, returning `(index_key, metadata)`.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.index_key, self.metadata)
    }
}

/// The component count of an index field identity: `1` for a plain field, or one
/// more than the number of `FIELD_SEP` bytes for a compound identity.
pub(crate) fn component_count(field: &str) -> usize {
    field
        .as_bytes()
        .iter()
        .filter(|&&b| b == key::FIELD_SEP)
        .count()
        + 1
}

/// The cumulative end-offset of the *last* of `n` components, read from the
/// metadata's value-side offset region.
///
/// Metadata layout: `[t1…tN][end_1…end_N : u32 LE][ttl…]`. The last component's
/// end (= total value byte length = doc_id start relative to `value_start`) is the
/// `n`-th u32, at byte offset `n + 4*(n-1)`. Returns `None` if the metadata is too
/// short to hold the full type+offset header.
pub(crate) fn last_component_end(metadata: &[u8], n: usize) -> Option<usize> {
    const OFF: usize = 4;
    let last = n.checked_sub(1)?;
    let at = n.checked_add(OFF.checked_mul(last)?)?;
    let bytes = metadata.get(at..at.checked_add(OFF)?)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?) as usize)
}

/// Extract the per-component index values for a compound index, in field order.
///
/// Returns `None` (no entry) if any component is absent or resolves to a
/// non-scalar / multikey value — the sparse, fully-specified-prefix rule. Each
/// component is re-keyed onto the unified numeric f64 key, matching the
/// single-field write path.
fn compound_index_values(field: &str, doc: &RawDocument) -> Option<Vec<BsonValue<'static>>> {
    let mut values = Vec::new();
    for component in key::split_index_fields(field) {
        // A compound component must be a single scalar — a multikey path would
        // imply a cross-product we don't expand in this phase. `extract_all`
        // yields exactly one value for a scalar path; take it, else bail.
        let mut vals = bson_value::extract_all(doc, &component).into_iter();
        let val = vals.next()?.into_index_value()?;
        if vals.next().is_some() {
            // More than one value (multikey) — not a compound-indexable shape.
            return None;
        }
        values.push(val);
    }
    Some(values)
}

/// O(1) TTL expiry check on raw single-field index metadata bytes.
///
/// Metadata layout: `[type_byte][end : u32 LE]` (5 bytes, no TTL) or
/// `[type_byte][end : u32 LE][8-byte LE i64 millis]` (13 bytes, with TTL). The TTL
/// index is always single-field, so this `n = 1` variant serves the purge path.
#[inline]
pub fn is_index_expired(data: &[u8], now_millis: i64) -> bool {
    is_index_expired_n(data, 1, now_millis)
}

/// O(1) TTL expiry check for an entry with `n` components.
///
/// Metadata is `[t1…tN][end_1…end_N : u32 LE]` (no TTL) or that block followed by
/// an `[8-byte LE i64 millis]` TTL. The millis, when present, begin right after
/// the `n` type bytes and the `n` u32 end-offsets, i.e. at byte offset `n + 4*n`.
/// `false` for any entry with no trailing TTL block.
#[inline]
pub fn is_index_expired_n(data: &[u8], n: usize, now_millis: i64) -> bool {
    const TTL_BYTES: usize = 8;
    const OFF: usize = 4;
    // TTL starts after the `n` type bytes and the `n` u32 value-side end-offsets.
    // Saturating arithmetic is safe here: an overflowed offset reads past the end,
    // so `data.get(..)` yields nothing and the entry reads as not-expired.
    let ttl_start = n.saturating_add(OFF.saturating_mul(n));
    let end = ttl_start.saturating_add(TTL_BYTES);
    if let Ok(bytes) = data.get(ttl_start..end).unwrap_or_default().try_into() {
        i64::from_le_bytes(bytes) < now_millis
    } else {
        false
    }
}

/// Compute unique-index entries for a document.
///
/// Returns `(u_key, entry_value)` pairs — one per unique path that resolves to
/// a scalar value. Sparse by construction: a path that is absent or holds a
/// non-scalar (array/document) produces no entry, since [`extract_all`] yields
/// nothing for it on a non-multikey path.
///
/// Layout:
/// - key:   `u\0{collection}\0{field}\0{type_byte}{sortable_value_bytes}`
///   (the type byte is folded into the key so distinct BSON types never alias
///   onto the same unique slot, even if their sortable encodings collide)
/// - value: the owning `doc_id`, length-prefixed
///
/// [`extract_all`]: bson_value::extract_all
pub fn unique_entries_from_document(
    collection: &str,
    unique_paths: &[String],
    doc: &RawDocument,
    doc_id: &BsonValue<'_>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    for path in unique_paths {
        if path.as_bytes().contains(&key::FIELD_SEP) {
            // Compound unique index: concatenate each component's `[type][value]`
            // block into one `u` key, enforcing uniqueness of the *combination*.
            // Sparse: a missing/multikey component yields no entry.
            if let Some(values) = compound_index_values(path, doc) {
                let mut keyed_value = Vec::new();
                for val in &values {
                    keyed_value.push(val.tag as u8);
                    keyed_value.extend_from_slice(&val.bytes);
                }
                let unique_key = Key::encode_unique_index(collection, path, &keyed_value);
                let mut value = Vec::with_capacity(3 + doc_id.bytes.len());
                doc_id.write_length_prefixed(&mut value);
                entries.push((unique_key, value));
            }
            continue;
        }
        for val in bson_value::extract_all(doc, path) {
            let mut keyed_value = Vec::with_capacity(1 + val.bytes.len());
            keyed_value.push(val.tag as u8);
            keyed_value.extend_from_slice(&val.bytes);
            let unique_key = Key::encode_unique_index(collection, path, &keyed_value);

            // length-prefix header is 1 type byte + 2 length bytes
            let mut value = Vec::with_capacity(3 + doc_id.bytes.len());
            doc_id.write_length_prefixed(&mut value);

            entries.push((unique_key, value));
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn make_index_key(
        collection: &str,
        field: &str,
        value_bytes: &[u8],
        doc_id: &BsonValue<'_>,
    ) -> Vec<u8> {
        // These helpers exercise string (variable-width) values.
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(value_bytes),
        };
        Key::encode_index_key(collection, field, &value, doc_id)
    }

    fn str_id(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: ElementType::String,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    /// Build single-component String metadata in the value-side-offsets format:
    /// `[String][end:u32 LE][ttl:i64 LE?]`, where `end` = value byte length.
    fn str_meta(value_len: usize, ttl: Option<i64>) -> Vec<u8> {
        let mut m = vec![ElementType::String as u8];
        m.extend_from_slice(&(value_len as u32).to_le_bytes());
        if let Some(t) = ttl {
            m.extend_from_slice(&t.to_le_bytes());
        }
        m
    }

    #[test]
    fn from_pair_parses_index_entry() {
        let doc_id = str_id("doc1");
        let value_bytes = b"Alice";
        let key_bytes = make_index_key("users", "name", value_bytes, &doc_id);
        let metadata = str_meta(value_bytes.len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert_eq!(record.collection().unwrap(), "users");
        assert_eq!(record.field().unwrap(), "name");
        assert_eq!(record.doc_id().unwrap(), doc_id);
        assert_eq!(record.value_bytes(), value_bytes);
        assert_eq!(record.type_byte(), ElementType::String as u8);
    }

    #[test]
    fn from_pair_returns_none_for_empty_metadata() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        assert!(IndexRecord::from_pair(key_bytes, vec![]).is_none());
    }

    #[test]
    fn from_pair_returns_none_for_non_index_key() {
        assert!(IndexRecord::from_pair(b"r\x00users\x00".to_vec(), vec![0x02]).is_none());
    }

    #[test]
    fn encode_produces_valid_record() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let record = IndexRecord::encode("users", "name", &doc_id, &value, None);

        assert_eq!(record.collection().unwrap(), "users");
        assert_eq!(record.field().unwrap(), "name");
        assert_eq!(record.doc_id().unwrap(), doc_id);
        assert_eq!(record.value_bytes(), b"Alice");
        assert_eq!(record.type_byte(), ElementType::String as u8);
    }

    #[test]
    fn encode_with_ttl_produces_9_byte_metadata() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let record = IndexRecord::encode("users", "name", &doc_id, &value, Some(1_000));

        // [type(1)] + [end:u32(4)] + [ttl:i64(8)] = 13 bytes.
        assert_eq!(record.metadata().len(), 13);
        assert_eq!(record.type_byte(), ElementType::String as u8);
        assert!(record.is_expired(2_000));
        assert!(!record.is_expired(500));
    }

    #[test]
    fn encode_without_ttl_produces_type_plus_offset_metadata() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let record = IndexRecord::encode("users", "name", &doc_id, &value, None);

        // [type(1)] + [end:u32(4)] = 5 bytes, no TTL.
        assert_eq!(record.metadata().len(), 5);
        // The end-offset records the value byte length ("Alice" = 5).
        assert_eq!(
            u32::from_le_bytes(record.metadata()[1..5].try_into().unwrap()),
            5
        );
        assert!(!record.is_expired(i64::MAX));
    }

    #[test]
    fn encode_matches_from_pair() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let encoded = IndexRecord::encode("users", "name", &doc_id, &value, None);

        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = str_meta(b"Alice".len(), None);
        let parsed = IndexRecord::from_pair(key_bytes, metadata).unwrap();

        assert_eq!(encoded.key_bytes(), parsed.key_bytes());
        assert_eq!(encoded.metadata(), parsed.metadata());
    }

    #[test]
    fn value_bson_reconstructs_string() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = str_meta(b"Alice".len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        let raw = record.value_bson().unwrap();
        assert_eq!(raw, bson::RawBson::String("Alice".into()));
    }

    #[test]
    fn from_pair_decodes_string_with_docid_like_bytes() {
        // The string's bytes embed `\x00` and a sequence that parses as a valid
        // length-prefixed doc_id (`[0x02][0x00][0x00]` = String, len 0) — exactly
        // the collision the old backward scan could mis-split. The value-side end
        // offset (in the metadata) makes the boundary unambiguous.
        let doc_id = str_id("user-42");
        let value_bytes: &[u8] = b"active\x02\x00\x00\x00trailing";
        let key_bytes = make_index_key("status", "state", value_bytes, &doc_id);
        let metadata = str_meta(value_bytes.len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert_eq!(record.value_bytes(), value_bytes);
        assert_eq!(record.doc_id().unwrap(), doc_id);
    }

    #[test]
    fn from_pair_decodes_string_with_oid_id_with_null_bytes() {
        // ObjectId doc_id whose tail bytes resemble a short length-prefixed value,
        // paired with a string value — both decode exactly.
        let oid =
            bson::oid::ObjectId::from_bytes([0x08, 0x00, 0x00, 0x02, 0x00, 0x00, 0, 0, 0, 0, 0, 0]);
        let doc_id = BsonValue {
            tag: ElementType::ObjectId,
            bytes: Cow::Owned(oid.bytes().to_vec()),
        };
        let value_bytes: &[u8] = b"omega";
        let key_bytes = make_index_key("k", "f", value_bytes, &doc_id);
        let metadata = str_meta(value_bytes.len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert_eq!(record.value_bytes(), value_bytes);
        assert_eq!(record.doc_id().unwrap(), doc_id);
        assert_eq!(
            record.value_bson().unwrap(),
            bson::RawBson::String("omega".into())
        );
    }

    #[test]
    fn from_pair_decodes_empty_string_value() {
        // A zero-length string value: the end offset records 0, value_bytes is
        // empty, and the doc_id still decodes.
        let doc_id = str_id("d1");
        let key_bytes = make_index_key("k", "f", b"", &doc_id);
        let metadata = str_meta(0, None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert_eq!(record.value_bytes(), b"");
        assert_eq!(record.doc_id().unwrap(), doc_id);
    }

    #[test]
    fn value_bson_reconstructs_int32_with_oid_id() {
        // Regression: the sortable encoding of an i32 embeds bytes the boundary scan
        // mis-read, so value_bson() returned None ("malformed value in index key").
        // from_pair must derive the value length from the type byte. An ObjectId _id
        // exercises the value/doc_id boundary; the number round-trips intact.
        let oid = bson::oid::ObjectId::from_bytes([0x07; 12]);
        let doc_id = BsonValue {
            tag: ElementType::ObjectId,
            bytes: Cow::Owned(oid.bytes().to_vec()),
        };
        // The index-write path projects numerics onto the 8-byte f64 key.
        let value = BsonValue::from_bson(&bson::Bson::Int32(3))
            .unwrap()
            .into_index_value()
            .unwrap();

        let (key_bytes, metadata) =
            IndexRecord::encode("nba", "priority", &doc_id, &value, None).into_parts();

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert_eq!(record.value_bson().unwrap(), bson::RawBson::Int32(3));
        assert_eq!(record.doc_id().unwrap(), doc_id);
    }

    #[test]
    fn doc_id_bson_converts() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = str_meta(b"Alice".len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        let raw = record.doc_id_bson().unwrap();
        assert_eq!(raw, bson::RawBson::String("doc1".into()));
    }

    #[test]
    fn into_parts_returns_owned_bytes() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let record = IndexRecord::encode("users", "name", &doc_id, &value, None);

        let expected_key = record.key_bytes().to_vec();
        let expected_meta = record.metadata().to_vec();
        let (key, meta) = record.into_parts();
        assert_eq!(key, expected_key);
        assert_eq!(meta, expected_meta);
    }

    #[test]
    fn is_expired_with_ttl_metadata() {
        // [String][end:u32 = 5][ttl:i64 = 1000].
        let metadata = str_meta(b"Alice".len(), Some(1_000));

        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert!(record.is_expired(2_000));
        assert!(!record.is_expired(500));
    }

    #[test]
    fn is_expired_without_ttl() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = str_meta(b"Alice".len(), None);

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert!(!record.is_expired(i64::MAX));
    }

    #[test]
    fn is_index_expired_standalone() {
        // Single-field metadata: [type][end:u32][ttl:i64]. The TTL sits at byte
        // offset n + 4n = 5 (n = 1).
        let with_ttl = str_meta(8, Some(1_000));
        assert!(is_index_expired(&with_ttl, 2_000));
        assert!(!is_index_expired(&with_ttl, 500));

        // No TTL block → never expired (type + end only).
        let no_ttl = str_meta(8, None);
        assert!(!is_index_expired(&no_ttl, i64::MAX));
    }

    // ── from_document tests ────────────────────────────────────

    #[test]
    fn from_document_produces_records() {
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice", "age": 30 };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into(), "age".into()];

        let entries = IndexRecord::from_document("test", &indexes, &doc, &doc_id, None);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].collection().unwrap(), "test");
        assert_eq!(entries[0].field().unwrap(), "name");
        assert_eq!(entries[1].field().unwrap(), "age");
    }

    #[test]
    fn from_document_array_multi_key() {
        let doc = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "db", "engine"] };
        let doc_id = str_id("doc1");
        let indexes = vec!["tags.[]".into()];

        let entries = IndexRecord::from_document("test", &indexes, &doc, &doc_id, None);

        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn from_document_missing_field_produces_nothing() {
        let doc = bson::rawdoc! { "_id": "doc1", "age": 30 };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let entries = IndexRecord::from_document("test", &indexes, &doc, &doc_id, None);
        assert!(entries.is_empty());
    }

    #[test]
    fn from_document_with_ttl_sets_metadata() {
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let entries = IndexRecord::from_document("test", &indexes, &doc, &doc_id, Some(5_000));

        assert_eq!(entries.len(), 1);
        // [type(1)] + [end:u32(4)] + [ttl:i64(8)] = 13 bytes.
        assert_eq!(entries[0].metadata().len(), 13);
        assert!(entries[0].is_expired(6_000));
        assert!(!entries[0].is_expired(4_000));
    }

    #[test]
    fn from_document_same_doc_produces_identical_keys() {
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let a = IndexRecord::from_document("test", &indexes, &doc, &doc_id, None);
        let b = IndexRecord::from_document("test", &indexes, &doc, &doc_id, None);

        assert_eq!(a[0].key_bytes(), b[0].key_bytes());
        assert_eq!(a[0].metadata(), b[0].metadata());
    }

    #[test]
    fn from_document_different_values_produce_different_keys() {
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let doc1 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let doc2 = bson::rawdoc! { "_id": "doc1", "name": "Bob" };

        let a = IndexRecord::from_document("test", &indexes, &doc1, &doc_id, None);
        let b = IndexRecord::from_document("test", &indexes, &doc2, &doc_id, None);

        assert_eq!(a.len(), b.len());
        assert_ne!(a[0].key_bytes(), b[0].key_bytes());
    }

    // ── Compound (multi-field) index records ────────────────────

    fn compound_identity(fields: &[&str]) -> String {
        super::super::key::join_index_fields(
            &fields.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
    }

    #[test]
    fn single_field_encode_matches_compound_one_component() {
        // The single-value `encode` and the one-component `encode_compound` must
        // be byte-identical (single-field is the N==1 case).
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let single = IndexRecord::encode("users", "name", &doc_id, &value, None);
        let compound = IndexRecord::encode_compound(
            "users",
            "name",
            &doc_id,
            std::slice::from_ref(&value),
            None,
        );
        assert_eq!(single.key_bytes(), compound.key_bytes());
        assert_eq!(single.metadata(), compound.metadata());
    }

    #[test]
    fn compound_from_document_produces_one_entry() {
        let doc = bson::rawdoc! { "_id": "d1", "status": "active", "created_at": 20i64 };
        let doc_id = str_id("d1");
        let identity = compound_identity(&["status", "created_at"]);
        let indexes = vec![identity.clone()];

        let entries = IndexRecord::from_document("orders", &indexes, &doc, &doc_id, None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].field().unwrap(), identity);
        // 2 type bytes + 2 u32 value-side end-offsets = 2 + 8 = 10 bytes.
        assert_eq!(entries[0].metadata().len(), 10);
        // doc_id round-trips through the value-side-offset boundary resolution.
        assert_eq!(entries[0].doc_id().unwrap(), doc_id);
    }

    #[test]
    fn compound_from_pair_resolves_boundaries() {
        // A string + numeric compound entry must round-trip through `from_pair`'s
        // multi-component offset resolution.
        let doc = bson::rawdoc! { "_id": "d1", "status": "active", "created_at": 20i64 };
        let doc_id = str_id("d1");
        let identity = compound_identity(&["status", "created_at"]);
        let indexes = vec![identity];

        let entries = IndexRecord::from_document("orders", &indexes, &doc, &doc_id, None);
        let (key, meta) = entries.into_iter().next().unwrap().into_parts();

        let parsed = IndexRecord::from_pair(key, meta).unwrap();
        assert_eq!(parsed.doc_id().unwrap(), doc_id);
    }

    #[test]
    fn compound_missing_component_yields_no_entry() {
        // No `created_at` → the sparse rule produces no compound entry.
        let doc = bson::rawdoc! { "_id": "d1", "status": "active" };
        let doc_id = str_id("d1");
        let indexes = vec![compound_identity(&["status", "created_at"])];

        let entries = IndexRecord::from_document("orders", &indexes, &doc, &doc_id, None);
        assert!(entries.is_empty());
    }

    #[test]
    fn compound_two_strings_round_trip() {
        // Two variable-width components: both value-side end-offsets must resolve.
        let doc = bson::rawdoc! { "_id": "d1", "a": "hello", "b": "world" };
        let doc_id = str_id("d1");
        let indexes = vec![compound_identity(&["a", "b"])];

        let entries = IndexRecord::from_document("k", &indexes, &doc, &doc_id, None);
        let (key, meta) = entries.into_iter().next().unwrap().into_parts();
        let parsed = IndexRecord::from_pair(key, meta).unwrap();
        assert_eq!(parsed.doc_id().unwrap(), doc_id);
        // 2 type bytes + 2 u32 end-offsets = 10 bytes.
        assert_eq!(parsed.metadata().len(), 10);
    }

    #[test]
    fn compound_with_ttl_resolves_doc_id() {
        // TTL follows the per-component type bytes AND the value-side end-offsets;
        // the doc_id boundary must still resolve, and expiry must read the right
        // offset.
        let doc = bson::rawdoc! { "_id": "d1", "status": "active", "created_at": 20i64, "ttl": bson::DateTime::from_millis(1_000) };
        let doc_id = str_id("d1");
        let indexes = vec![compound_identity(&["status", "created_at"])];

        let entries = IndexRecord::from_document("orders", &indexes, &doc, &doc_id, Some(1_000));
        let entry = entries.into_iter().next().unwrap();
        // 2 type bytes + 2 u32 end-offsets (8) + 8 TTL bytes = 18 bytes.
        assert_eq!(entry.metadata().len(), 18);
        assert!(entry.is_expired(2_000));
        assert!(!entry.is_expired(500));
        let (key, meta) = entry.into_parts();
        let parsed = IndexRecord::from_pair(key, meta).unwrap();
        assert_eq!(parsed.doc_id().unwrap(), doc_id);
    }

    #[test]
    fn unique_compound_concatenates_components() {
        let doc = bson::rawdoc! { "_id": "d1", "org_id": "acme", "email": "x@test.com" };
        let doc_id = str_id("d1");
        let identity = compound_identity(&["org_id", "email"]);

        let entries = unique_entries_from_document("members", &[identity], &doc, &doc_id);
        assert_eq!(entries.len(), 1);
        // The value stores the owning doc_id.
        let (_, value) = &entries[0];
        let (parsed_id, _) = BsonValue::parse_length_prefixed(value).unwrap();
        assert_eq!(parsed_id, doc_id);
    }

    #[test]
    fn unique_compound_distinct_combinations_distinct_keys() {
        let doc_id = str_id("d1");
        let identity = compound_identity(&["org_id", "email"]);

        let same_email_diff_org_a =
            bson::rawdoc! { "_id": "d1", "org_id": "acme", "email": "x@t.com" };
        let same_email_diff_org_b =
            bson::rawdoc! { "_id": "d1", "org_id": "globex", "email": "x@t.com" };

        let a = unique_entries_from_document(
            "members",
            std::slice::from_ref(&identity),
            &same_email_diff_org_a,
            &doc_id,
        );
        let b =
            unique_entries_from_document("members", &[identity], &same_email_diff_org_b, &doc_id);
        // Same email but different org → different unique keys (allowed).
        assert_ne!(a[0].0, b[0].0);
    }
}
