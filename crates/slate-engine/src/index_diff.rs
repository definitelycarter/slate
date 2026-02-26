use std::borrow::Cow;
use std::collections::HashMap;

use bson::raw::RawDocument;
use slate_store::Transaction;

use crate::encoding::bson_value::{self, BsonValue};
use crate::encoding::{IndexMeta, Record};
use crate::error::EngineError;
use crate::key::{Key, KeyPrefix};
use crate::traits::CollectionHandle;

/// Write a single index + reverse-map entry pair.
pub(crate) fn put_index<T: Transaction>(
    txn: &T,
    cf: &T::Cf,
    collection: &str,
    field: &str,
    value: &[u8],
    doc_id: &BsonValue<'_>,
    metadata: &[u8],
) -> Result<(), EngineError> {
    let key = Key::Index(
        Cow::Borrowed(collection),
        Cow::Borrowed(field),
        doc_id.clone(),
    );
    let index_key = key.encode_index(value);
    txn.put(cf, &index_key, metadata)?;

    let map_key = Key::IndexMap(
        Cow::Borrowed(collection),
        Cow::Borrowed(field),
        doc_id.clone(),
    );
    let map_encoded = map_key.encode_index_map(value);
    txn.put(cf, &map_encoded, &index_key)?;
    Ok(())
}

/// Insert-only index path: extract values and write entries without scanning for old ones.
///
/// Used by `put_nx` where we know no prior entries exist.
pub(crate) fn insert_indexes<T: Transaction>(
    txn: &T,
    handle: &CollectionHandle<T::Cf>,
    doc: &RawDocument,
    doc_id: &BsonValue<'_>,
) -> Result<(), EngineError> {
    let new_ttl = Record::ttl_millis(doc);
    for field in &handle.indexes {
        for val in bson_value::extract_all(doc, field) {
            let meta = IndexMeta {
                type_byte: val.tag,
                ttl_millis: new_ttl,
            };
            put_index(
                txn,
                &handle.cf,
                &handle.name,
                field,
                &val.to_vec(),
                doc_id,
                &meta.encode(),
            )?;
        }
    }
    Ok(())
}

/// Diff-based index update: scan old entries, compare with new, only write the diff.
///
/// For each indexed field:
/// 1. Scan IndexMap prefix to collect old `{value_bytes → (map_key, index_key)}`
/// 2. Extract new values from the document
/// 3. Delete entries that exist in old but not new
/// 4. Insert entries that exist in new but not old
/// 5. Update metadata (TTL) if the value exists in both but metadata changed
pub(crate) fn diff_indexes<T: Transaction>(
    txn: &T,
    handle: &CollectionHandle<T::Cf>,
    doc: &RawDocument,
    doc_id: &BsonValue<'_>,
) -> Result<(), EngineError> {
    let new_ttl = Record::ttl_millis(doc);

    for field in &handle.indexes {
        // 1. Scan old entries from IndexMap
        let prefix = KeyPrefix::IndexMapRecord(
            Cow::Borrowed(&handle.name),
            Cow::Borrowed(field),
            doc_id.clone(),
        );
        let prefix_bytes = prefix.encode();
        let prefix_len = prefix_bytes.len();

        let old_entries: Vec<(Vec<u8>, Vec<u8>)> = txn
            .scan_prefix(&handle.cf, &prefix_bytes)?
            .collect::<Result<_, _>>()?;

        // Build map: value_bytes → (map_key, index_key)
        let mut old_map: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)> =
            HashMap::with_capacity(old_entries.len());
        for (map_key, index_key) in old_entries {
            // IndexMap key layout after prefix: {value_bytes}
            let value_bytes = map_key[prefix_len..].to_vec();
            old_map.insert(value_bytes, (map_key, index_key));
        }

        // 2. Extract new values
        let new_values: Vec<BsonValue<'static>> = bson_value::extract_all(doc, field);

        // 3. Process new values: insert if not in old, update metadata if changed
        for val in &new_values {
            let val_bytes = val.to_vec();
            let meta = IndexMeta {
                type_byte: val.tag,
                ttl_millis: new_ttl,
            };
            let new_metadata = meta.encode();

            if let Some((_, index_key)) = old_map.remove(&val_bytes) {
                // Value exists in both old and new — check if metadata changed
                let old_metadata = txn.get(&handle.cf, &index_key)?;
                if old_metadata.as_deref() != Some(&new_metadata) {
                    txn.put(&handle.cf, &index_key, &new_metadata)?;
                }
            } else {
                // New value — insert both index + map entries
                put_index(
                    txn,
                    &handle.cf,
                    &handle.name,
                    field,
                    &val_bytes,
                    doc_id,
                    &new_metadata,
                )?;
            }
        }

        // 4. Delete entries that are in old but not new (leftover in old_map)
        for (_, (map_key, index_key)) in old_map {
            txn.delete(&handle.cf, &index_key)?;
            txn.delete(&handle.cf, &map_key)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::raw::RawBsonRef;
    use slate_store::{MemoryStore, Store};

    use crate::key::Key;

    fn str_id(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: 0x02,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    fn make_handle<Cf: Clone>(cf: Cf, indexes: Vec<String>) -> CollectionHandle<Cf> {
        CollectionHandle {
            name: "test".to_string(),
            cf,
            indexes,
        }
    }

    /// Helper: count all index entries for a field via IndexField prefix scan.
    fn count_index_entries<T: Transaction>(
        txn: &T,
        cf: &T::Cf,
        collection: &str,
        field: &str,
    ) -> usize {
        let prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        txn.scan_prefix(cf, &prefix)
            .unwrap()
            .count()
    }

    /// Helper: count all index map entries for a (field, doc_id) via IndexMapRecord prefix scan.
    fn count_map_entries<T: Transaction>(
        txn: &T,
        cf: &T::Cf,
        collection: &str,
        field: &str,
        doc_id: &BsonValue<'_>,
    ) -> usize {
        let prefix = KeyPrefix::IndexMapRecord(
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
            doc_id.clone(),
        )
        .encode();
        txn.scan_prefix(cf, &prefix)
            .unwrap()
            .count()
    }

    #[test]
    fn insert_indexes_writes_index_and_map() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice", "age": 30 };
        let handle = make_handle(cf.clone(), vec!["name".into(), "age".into()]);

        insert_indexes(&txn, &handle, &doc, &doc_id).unwrap();

        // One index entry per field
        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);
        assert_eq!(count_index_entries(&txn, &cf, "test", "age"), 1);

        // One map entry per field
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 1);
        assert_eq!(count_map_entries(&txn, &cf, "test", "age", &doc_id), 1);

        txn.rollback().unwrap();
    }

    #[test]
    fn insert_indexes_multi_key_array() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let doc = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "db", "engine"] };
        let handle = make_handle(cf.clone(), vec!["tags.[]".into()]);

        insert_indexes(&txn, &handle, &doc, &doc_id).unwrap();

        // Three index entries for the array elements
        assert_eq!(count_index_entries(&txn, &cf, "test", "tags.[]"), 3);
        assert_eq!(count_map_entries(&txn, &cf, "test", "tags.[]", &doc_id), 3);

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_no_change_skips_writes() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // Insert first
        insert_indexes(&txn, &handle, &doc, &doc_id).unwrap();
        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);

        // Diff with same document — should leave entries unchanged
        diff_indexes(&txn, &handle, &doc, &doc_id).unwrap();
        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 1);

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_value_changed() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // Insert "Alice"
        let doc1 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        insert_indexes(&txn, &handle, &doc1, &doc_id).unwrap();

        // Diff with "Bob" — should remove Alice index, add Bob index
        let doc2 = bson::rawdoc! { "_id": "doc1", "name": "Bob" };
        diff_indexes(&txn, &handle, &doc2, &doc_id).unwrap();

        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 1);

        // Verify the remaining entry is for "Bob", not "Alice"
        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed("test"), Cow::Borrowed("name")).encode();
        let entries: Vec<_> = txn
            .scan_prefix(&cf, &field_prefix)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let (key_bytes, _) = &entries[0];
        let (value_bytes, _) = Key::parse_index_tail(key_bytes, field_prefix.len()).unwrap();
        // value_bytes should be the BsonValue encoding of "Bob": [0x02, b"Bob"]
        let bob_val =
            BsonValue::from_raw_bson_ref(RawBsonRef::String("Bob")).unwrap();
        assert_eq!(value_bytes, &bob_val.to_vec());

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_field_removed() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // Insert with "name" field
        let doc1 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        insert_indexes(&txn, &handle, &doc1, &doc_id).unwrap();
        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);

        // Diff with doc that has no "name" field — should delete the entry
        let doc2 = bson::rawdoc! { "_id": "doc1", "age": 30 };
        diff_indexes(&txn, &handle, &doc2, &doc_id).unwrap();

        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 0);
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 0);

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_field_added() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // Insert with no "name" field
        let doc1 = bson::rawdoc! { "_id": "doc1", "age": 30 };
        insert_indexes(&txn, &handle, &doc1, &doc_id).unwrap();
        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 0);

        // Diff with doc that has "name" — should add the entry
        let doc2 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        diff_indexes(&txn, &handle, &doc2, &doc_id).unwrap();

        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 1);

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_array_partial_change() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["tags.[]".into()]);

        // Insert with tags: ["rust", "db"]
        let doc1 = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "db"] };
        insert_indexes(&txn, &handle, &doc1, &doc_id).unwrap();
        assert_eq!(count_index_entries(&txn, &cf, "test", "tags.[]"), 2);

        // Diff with tags: ["rust", "engine"] — "db" removed, "engine" added, "rust" unchanged
        let doc2 = bson::rawdoc! { "_id": "doc1", "tags": ["rust", "engine"] };
        diff_indexes(&txn, &handle, &doc2, &doc_id).unwrap();

        assert_eq!(count_index_entries(&txn, &cf, "test", "tags.[]"), 2);
        assert_eq!(count_map_entries(&txn, &cf, "test", "tags.[]", &doc_id), 2);

        // Verify specific values: "rust" and "engine"
        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed("test"), Cow::Borrowed("tags.[]")).encode();
        let entries: Vec<_> = txn
            .scan_prefix(&cf, &field_prefix)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut values: Vec<Vec<u8>> = entries
            .iter()
            .map(|(k, _)| {
                let (vb, _) = Key::parse_index_tail(k, field_prefix.len()).unwrap();
                vb.to_vec()
            })
            .collect();
        values.sort();

        let engine_val =
            BsonValue::from_raw_bson_ref(RawBsonRef::String("engine")).unwrap().to_vec();
        let rust_val =
            BsonValue::from_raw_bson_ref(RawBsonRef::String("rust")).unwrap().to_vec();
        let mut expected = vec![engine_val, rust_val];
        expected.sort();
        assert_eq!(values, expected);

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_ttl_metadata_update() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // Insert without TTL
        let doc1 = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        insert_indexes(&txn, &handle, &doc1, &doc_id).unwrap();

        // Read the metadata of the index entry
        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed("test"), Cow::Borrowed("name")).encode();
        let entries: Vec<_> = txn
            .scan_prefix(&cf, &field_prefix)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let old_meta = IndexMeta::decode(&entries[0].1).unwrap();
        assert!(old_meta.ttl_millis.is_none());

        // Diff with TTL — same "name" value, but TTL added
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let doc2 = bson::rawdoc! { "_id": "doc1", "name": "Alice", "ttl": dt };
        diff_indexes(&txn, &handle, &doc2, &doc_id).unwrap();

        // Metadata should now have TTL
        let entries: Vec<_> = txn
            .scan_prefix(&cf, &field_prefix)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let new_meta = IndexMeta::decode(&entries[0].1).unwrap();
        assert_eq!(new_meta.ttl_millis, Some(1_700_000_000_000));

        txn.rollback().unwrap();
    }

    #[test]
    fn diff_indexes_on_empty_old_behaves_like_insert() {
        let store = MemoryStore::new();
        store.create_cf("cf").unwrap();
        let txn = store.begin(false).unwrap();
        let cf = txn.cf("cf").unwrap();

        let doc_id = str_id("doc1");
        let handle = make_handle(cf.clone(), vec!["name".into()]);

        // No prior entries — diff should insert
        let doc = bson::rawdoc! { "_id": "doc1", "name": "Alice" };
        diff_indexes(&txn, &handle, &doc, &doc_id).unwrap();

        assert_eq!(count_index_entries(&txn, &cf, "test", "name"), 1);
        assert_eq!(count_map_entries(&txn, &cf, "test", "name", &doc_id), 1);

        txn.rollback().unwrap();
    }
}
