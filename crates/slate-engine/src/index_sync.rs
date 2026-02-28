use std::collections::HashMap;

use crate::encoding::bson_value::BsonValue;
use crate::encoding::{IndexRecord, Record};
use crate::error::EngineError;

/// The result of an index diff computation.
///
/// Contains the raw key-value pairs to write and the keys to delete.
pub struct IndexChanges {
    /// Index entries to write: `(i_key, metadata)`.
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    /// Index keys to delete.
    pub deletes: Vec<Vec<u8>>,
}

/// Pure-computation builder for computing index diffs.
///
/// Accumulates property paths and optional old record data, then
/// computes the diff between old and new index entries without
/// any store access.
///
/// # Example
///
/// ```ignore
/// let changes = IndexDiff::new(&record, &doc_id)
///     .with_property_paths(&handle.indexes)
///     .with_property_path("ttl")
///     .diff("users")?;
/// ```
pub struct IndexDiff<'a> {
    record: Option<&'a Record>,
    doc_id: &'a BsonValue<'a>,
    old_record: Option<&'a [u8]>,
    property_paths: Vec<String>,
}

impl<'a> IndexDiff<'a> {
    pub fn new(record: &'a Record, doc_id: &'a BsonValue<'a>) -> Self {
        Self {
            record: Some(record),
            doc_id,
            old_record: None,
            property_paths: Vec::new(),
        }
    }

    /// Create a diff builder for a delete operation (no new record).
    pub fn for_delete(doc_id: &'a BsonValue<'a>) -> Self {
        Self {
            record: None,
            doc_id,
            old_record: None,
            property_paths: Vec::new(),
        }
    }

    pub fn with_old_record(mut self, data: Option<&'a [u8]>) -> Self {
        self.old_record = data;
        self
    }

    pub fn with_property_path(mut self, path: &str) -> Self {
        self.property_paths.push(path.to_string());
        self
    }

    pub fn with_property_paths(mut self, paths: &[String]) -> Self {
        self.property_paths.extend(paths.iter().cloned());
        self
    }

    /// Compute the index diff between old and new documents.
    ///
    /// Pure computation — no store access. Returns the puts and deletes
    /// needed to bring the index entries in sync with the new document.
    pub fn diff(&self, collection: &str) -> Result<IndexChanges, EngineError> {
        if self.property_paths.is_empty() {
            return Ok(IndexChanges {
                puts: Vec::new(),
                deletes: Vec::new(),
            });
        }

        // Build new index entries (empty when deleting).
        let new_entries = match self.record {
            Some(record) => {
                let new_doc = record.doc()?;
                let new_ttl = record.ttl_millis();
                IndexRecord::from_document(
                    collection,
                    &self.property_paths,
                    new_doc,
                    self.doc_id,
                    new_ttl,
                )
            }
            None => Vec::new(),
        };

        // Build old index entries (if old record exists).
        let old_entries = match self.old_record {
            Some(data) => {
                let old_rec = Record::from_bytes(data.to_vec())?;
                let old_ttl = old_rec.ttl_millis();
                let old_doc = old_rec.doc()?;
                IndexRecord::from_document(
                    collection,
                    &self.property_paths,
                    old_doc,
                    self.doc_id,
                    old_ttl,
                )
            }
            None => Vec::new(),
        };

        // Diff by key + metadata. Entries with matching keys but different
        // metadata (e.g. TTL changed) are treated as updates (delete + put).
        let old_map: HashMap<&[u8], &[u8]> = old_entries
            .iter()
            .map(|e| (e.key_bytes(), e.metadata()))
            .collect();
        let new_map: HashMap<&[u8], &[u8]> = new_entries
            .iter()
            .map(|e| (e.key_bytes(), e.metadata()))
            .collect();

        let mut puts = Vec::new();
        let mut deletes = Vec::new();

        for entry in &new_entries {
            match old_map.get(entry.key_bytes()) {
                Some(old_meta) if *old_meta == entry.metadata() => {}
                _ => puts.push((entry.key_bytes().to_vec(), entry.metadata().to_vec())),
            }
        }

        for entry in &old_entries {
            match new_map.get(entry.key_bytes()) {
                Some(new_meta) if *new_meta == entry.metadata() => {}
                _ => deletes.push(entry.key_bytes().to_vec()),
            }
        }

        Ok(IndexChanges { puts, deletes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::spec::ElementType;
    use std::borrow::Cow;

    fn str_id(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: ElementType::String,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    fn make_record(doc: &bson::RawDocumentBuf) -> Record {
        Record::encoder().with_ttl_at_path("ttl").encode(doc)
    }

    #[test]
    fn diff_no_old_record_all_puts() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");
        let record = make_record(&doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_property_path("name")
            .diff("test")
            .unwrap();

        assert_eq!(changes.puts.len(), 1);
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_same_document_no_changes() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");
        let record = make_record(&doc);
        let old_bytes = record.as_bytes().to_vec();

        let new_record = make_record(&doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .diff("test")
            .unwrap();

        assert!(changes.puts.is_empty());
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_changed_value_puts_and_deletes() {
        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Bob" };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .diff("test")
            .unwrap();

        assert_eq!(changes.puts.len(), 1);
        assert_eq!(changes.deletes.len(), 1);
    }

    #[test]
    fn diff_ttl_change_full_replace() {
        let dt1 = bson::DateTime::from_millis(1_000);
        let dt2 = bson::DateTime::from_millis(2_000);

        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "ttl": dt1 };
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "ttl": dt2 };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .with_property_path("ttl")
            .diff("test")
            .unwrap();

        // TTL changed → metadata differs for all entries → full replace
        // delete old "name" + old "ttl", insert new "name" + new "ttl"
        assert_eq!(changes.deletes.len(), 2);
        assert_eq!(changes.puts.len(), 2);
    }

    #[test]
    fn diff_ttl_removed_full_replace() {
        let dt = bson::DateTime::from_millis(1_000);
        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "ttl": dt };
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .with_property_path("ttl")
            .diff("test")
            .unwrap();

        // Old had TTL → metadata differs for "name" entry; "ttl" entry removed
        // Old entries: name + ttl = 2 deletes
        // New entries: name only (ttl field missing) = 1 put
        assert_eq!(changes.deletes.len(), 2);
        assert_eq!(changes.puts.len(), 1);
    }

    #[test]
    fn diff_no_property_paths_empty() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");
        let record = make_record(&doc);

        let changes = IndexDiff::new(&record, &doc_id).diff("test").unwrap();

        assert!(changes.puts.is_empty());
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_no_old_record_missing_field() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");
        let record = make_record(&doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_property_path("email") // not in doc
            .diff("test")
            .unwrap();

        assert!(changes.puts.is_empty());
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_field_missing_in_both() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");
        let record = make_record(&doc);
        let old_bytes = record.as_bytes().to_vec();

        let new_record = make_record(&doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("email") // not in either doc
            .diff("test")
            .unwrap();

        assert!(changes.puts.is_empty());
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_field_added_on_update() {
        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "email": "a@b.c" };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .with_property_path("email")
            .diff("test")
            .unwrap();

        // name unchanged → no change; email added → 1 put
        assert_eq!(changes.puts.len(), 1);
        assert!(changes.deletes.is_empty());
    }

    #[test]
    fn diff_field_removed_on_update() {
        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "email": "a@b.c" };
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .with_property_path("email")
            .diff("test")
            .unwrap();

        // name unchanged → no change; email removed → 1 delete
        assert!(changes.puts.is_empty());
        assert_eq!(changes.deletes.len(), 1);
    }

    #[test]
    fn diff_ttl_added() {
        let old_doc = bson::rawdoc! { "_id": "d1", "name": "Alice" };
        let dt = bson::DateTime::from_millis(5_000);
        let new_doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "ttl": dt };
        let doc_id = str_id("d1");

        let old_record = make_record(&old_doc);
        let old_bytes = old_record.as_bytes().to_vec();

        let new_record = make_record(&new_doc);
        let changes = IndexDiff::new(&new_record, &doc_id)
            .with_old_record(Some(&old_bytes))
            .with_property_path("name")
            .with_property_path("ttl")
            .diff("test")
            .unwrap();

        // Old had no TTL → metadata differs for "name" entry; "ttl" entry added
        // Old entries: name = 1 delete
        // New entries: name + ttl = 2 puts
        assert_eq!(changes.deletes.len(), 1);
        assert_eq!(changes.puts.len(), 2);
    }

    #[test]
    fn with_property_path_accumulates() {
        let doc = bson::rawdoc! { "_id": "d1", "name": "Alice", "age": 30 };
        let doc_id = str_id("d1");
        let record = make_record(&doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_property_path("name")
            .with_property_path("age")
            .diff("test")
            .unwrap();

        assert_eq!(changes.puts.len(), 2);
    }
}
