use std::borrow::Cow;

use bson::raw::RawDocument;
use bson::spec::ElementType;

use super::bson_value::{self, BsonValue};
use super::key::Key;

// ── IndexRecord ──────────────────────────────────────────────
//
// An owned index entry backed by raw key-value bytes from the store.
//
// Key layout:  `i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}`
// Metadata:    `[type_byte]` or `[type_byte][8-byte LE TTL]`
//
// Follows the Record pattern: raw owned bytes + pre-parsed offsets
// for O(1) typed access.

/// An owned index entry with typed accessors over raw bytes.
///
/// Construct via [`IndexRecord::from_pair`] (read side) or
/// [`IndexRecord::encode`] (write side).
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
            let (key, value_bytes) = Key::decode_index(&key_bytes)?;
            let Key::Index(collection, field, _) = &key else {
                return None;
            };
            (
                2 + collection.len() + 1,
                2 + collection.len() + 1 + field.len() + 1,
                2 + collection.len() + 1 + field.len() + 1 + value_bytes.len(),
            )
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
    pub fn encode(
        collection: &str,
        field: &str,
        doc_id: &BsonValue<'_>,
        value: &BsonValue<'_>,
        ttl_millis: Option<i64>,
    ) -> Self {
        let val_bytes: &[u8] = &value.bytes;
        let metadata = match ttl_millis {
            Some(millis) => {
                let mut m = Vec::with_capacity(9);
                m.push(value.tag as u8);
                m.extend_from_slice(&millis.to_le_bytes());
                m
            }
            None => vec![value.tag as u8],
        };

        let index_key = Key::Index(
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
            doc_id.clone(),
        )
        .encode_index(val_bytes);

        let field_start = 2 + collection.len() + 1;
        let value_start = field_start + field.len() + 1;
        let doc_id_start = value_start + val_bytes.len();

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
    /// Returns one `IndexRecord` per indexed field value. Each record
    /// contains the encoded index key and metadata (including TTL if present).
    pub fn from_document(
        collection: &str,
        indexes: &[String],
        doc: &RawDocument,
        doc_id: &BsonValue<'_>,
        ttl_millis: Option<i64>,
    ) -> Vec<IndexRecord> {
        let mut entries = Vec::new();
        for field in indexes {
            for val in bson_value::extract_all(doc, field) {
                entries.push(IndexRecord::encode(collection, field, doc_id, &val, ttl_millis));
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
        Some(
            BsonValue::parse_length_prefixed(&self.index_key[self.doc_id_start..])?
                .0,
        )
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
        BsonValue::from_parts(tag, self.value_bytes()).to_raw_bson()
    }

    // ── TTL ─────────────────────────────────────────────────────

    /// O(1) TTL expiry check on the metadata bytes.
    ///
    /// Backward compatible: returns `false` for entries without TTL (1-byte metadata).
    #[inline]
    pub fn is_expired(&self, now_millis: i64) -> bool {
        is_index_expired(&self.metadata, now_millis)
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

/// O(1) TTL expiry check on raw index metadata bytes.
///
/// Metadata layout: `[type_byte]` (1 byte, no TTL) or
/// `[type_byte][8-byte LE i64 millis]` (9 bytes, with TTL).
#[inline]
pub fn is_index_expired(data: &[u8], now_millis: i64) -> bool {
    const TTL_OFFSET: usize = 1;
    const WITH_TTL_SIZE: usize = 9;
    if let Ok(bytes) = data.get(TTL_OFFSET..WITH_TTL_SIZE).unwrap_or_default().try_into() {
        i64::from_le_bytes(bytes) < now_millis
    } else {
        false
    }
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
        Key::Index(
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
            doc_id.clone(),
        )
        .encode_index(value_bytes)
    }

    fn str_id(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: ElementType::String,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    #[test]
    fn from_pair_parses_index_entry() {
        let doc_id = str_id("doc1");
        let value_bytes = b"Alice";
        let key_bytes = make_index_key("users", "name", value_bytes, &doc_id);
        let metadata = vec![ElementType::String as u8];

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

        assert_eq!(record.metadata().len(), 9);
        assert_eq!(record.type_byte(), ElementType::String as u8);
        assert!(record.is_expired(2_000));
        assert!(!record.is_expired(500));
    }

    #[test]
    fn encode_without_ttl_produces_1_byte_metadata() {
        let doc_id = str_id("doc1");
        let value = BsonValue {
            tag: ElementType::String,
            bytes: Cow::Borrowed(b"Alice"),
        };
        let record = IndexRecord::encode("users", "name", &doc_id, &value, None);

        assert_eq!(record.metadata().len(), 1);
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
        let metadata = vec![ElementType::String as u8];
        let parsed = IndexRecord::from_pair(key_bytes, metadata).unwrap();

        assert_eq!(encoded.key_bytes(), parsed.key_bytes());
        assert_eq!(encoded.metadata(), parsed.metadata());
    }

    #[test]
    fn value_bson_reconstructs_string() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = vec![ElementType::String as u8];

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        let raw = record.value_bson().unwrap();
        assert_eq!(raw, bson::RawBson::String("Alice".into()));
    }

    #[test]
    fn doc_id_bson_converts() {
        let doc_id = str_id("doc1");
        let key_bytes = make_index_key("users", "name", b"Alice", &doc_id);
        let metadata = vec![0x02];

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
    fn is_expired_with_old_ttl_metadata() {
        let mut metadata = vec![0x02];
        metadata.extend_from_slice(&1_000i64.to_le_bytes());

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
        let metadata = vec![0x02];

        let record = IndexRecord::from_pair(key_bytes, metadata).unwrap();
        assert!(!record.is_expired(i64::MAX));
    }

    #[test]
    fn is_index_expired_standalone() {
        let mut with_ttl = vec![0x02];
        with_ttl.extend_from_slice(&1_000i64.to_le_bytes());
        assert!(is_index_expired(&with_ttl, 2_000));
        assert!(!is_index_expired(&with_ttl, 500));

        assert!(!is_index_expired(&[0x02], i64::MAX));
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
        assert_eq!(entries[0].metadata().len(), 9);
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
}
