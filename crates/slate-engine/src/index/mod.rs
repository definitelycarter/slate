use std::borrow::Cow;

use bson::raw::RawDocument;

use crate::encoding::bson_value::{self, BsonValue};
use crate::encoding::{IndexMeta, Record};
use crate::key::Key;

/// Compute index + reverse-map entries for a document.
///
/// Returns a list of `(key, value)` pairs to insert via `put_batch`.
/// For each indexed field and each extracted value, produces two entries:
/// - Index entry: `i\0{collection}\0{field}\0{value_bytes}{doc_id}` → metadata
/// - IndexMap entry: `j\0{collection}\0{field}\0{doc_id}{value_bytes}` → index_key
pub(crate) fn index_entries(
    collection: &str,
    indexes: &[String],
    doc: &RawDocument,
    doc_id: &BsonValue<'_>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let ttl = Record::ttl_millis(doc);
    let mut entries = Vec::new();

    for field in indexes {
        for val in bson_value::extract_all(doc, field) {
            let val_bytes = val.to_vec();
            let meta = IndexMeta {
                type_byte: val.tag,
                ttl_millis: ttl,
            };
            let metadata = meta.encode();

            let index_key = Key::Index(
                Cow::Borrowed(collection),
                Cow::Borrowed(field),
                doc_id.clone(),
            )
            .encode_index(&val_bytes);

            let map_key = Key::IndexMap(
                Cow::Borrowed(collection),
                Cow::Borrowed(field),
                doc_id.clone(),
            )
            .encode_index_map(&val_bytes);

            entries.push((map_key, index_key.clone()));
            entries.push((index_key, metadata));
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn str_id(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: 0x02,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    #[test]
    fn index_entries_produces_pairs() {
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice", "age": 30 };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into(), "age".into()];

        let entries = index_entries("test", &indexes, &doc, &doc_id);

        // 2 fields × 1 value each × 2 entries (index + map) = 4
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn index_entries_array_multi_key() {
        let doc = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "db", "engine"] };
        let doc_id = str_id("doc1");
        let indexes = vec!["tags.[]".into()];

        let entries = index_entries("test", &indexes, &doc, &doc_id);

        // 3 array elements × 2 entries each = 6
        assert_eq!(entries.len(), 6);
    }

    #[test]
    fn index_entries_missing_field_produces_nothing() {
        let doc = bson::rawdoc! { "_id": "doc1", "age": 30 };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let entries = index_entries("test", &indexes, &doc, &doc_id);
        assert!(entries.is_empty());
    }

    #[test]
    fn index_entries_same_doc_produces_identical_keys() {
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let a = index_entries("test", &indexes, &doc, &doc_id);
        let b = index_entries("test", &indexes, &doc, &doc_id);

        assert_eq!(a, b);
    }

    #[test]
    fn index_entries_different_values_produce_different_keys() {
        let doc_id = str_id("doc1");
        let indexes = vec!["name".into()];

        let doc1 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let doc2 = bson::rawdoc! { "_id": "doc1", "name": "Bob" };

        let a = index_entries("test", &indexes, &doc1, &doc_id);
        let b = index_entries("test", &indexes, &doc2, &doc_id);

        // Same structure but different keys (different indexed value)
        assert_eq!(a.len(), b.len());
        assert_ne!(a[0].0, b[0].0); // map keys differ
        assert_ne!(a[1].0, b[1].0); // index keys differ
    }

    #[test]
    fn index_entries_array_partial_overlap() {
        let doc_id = str_id("doc1");
        let indexes = vec!["tags.[]".into()];

        let doc1 = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "db"] };
        let doc2 = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "engine"] };

        let a = index_entries("test", &indexes, &doc1, &doc_id);
        let b = index_entries("test", &indexes, &doc2, &doc_id);

        // "rust" entries should match, "db" vs "engine" should differ
        assert_eq!(a[0], b[0]); // rust map entry
        assert_eq!(a[1], b[1]); // rust index entry
        assert_ne!(a[2], b[2]); // db vs engine map entry
        assert_ne!(a[3], b[3]); // db vs engine index entry
    }
}
