use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;

use bson::raw::{RawBsonRef, RawDocumentBuf};
use bson::{RawBson, RawDocument};
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::exec;
use crate::executor::field_tree::{FieldTree, walk};
use crate::executor::{RawIter, RawValue};
use crate::planner::UpsertMode;

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    mode: &'a UpsertMode,
    indexed_fields: &'a [String],
    source: RawIter<'a>,
    inserted: Rc<Cell<u64>>,
    updated: Rc<Cell<u64>>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    let mut paths: Vec<String> = indexed_fields.to_vec();
    if !paths.iter().any(|p| p == "ttl") {
        paths.push("ttl".into());
    }
    let tree = FieldTree::from_paths(&paths);

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let val = match opt_val {
            Some(v) => v,
            None => {
                return Err(DbError::InvalidQuery("Upsert requires document".into()));
            }
        };

        let new_raw = val
            .as_document()
            .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

        let (id_str, new_doc) = extract_id(new_raw)?;
        let new_raw_ref = match &new_doc {
            Some(buf) => buf.as_ref(),
            None => new_raw,
        };

        let record_key = encoding::record_key(&id_str);
        let old_bytes = txn.get(cf, &record_key)?;

        // Check if existing doc is expired (ttl < now)
        let expired = old_bytes
            .as_ref()
            .map_or(false, |data| exec::is_ttl_expired(data, now_millis));

        if let Some(ref old_data) = old_bytes {
            let (_, bson_slice) = encoding::decode_record(old_data)?;
            let old_raw = RawDocument::from_bytes(bson_slice)?;
            delete_old_indexes(txn, cf, old_raw, &tree, &id_str)?;

            if expired {
                // Expired doc — clean up old indexes, then treat as fresh insert
                let doc_to_write = normalize_id(new_doc, new_raw, &id_str)?;
                txn.put(
                    cf,
                    &record_key,
                    &encoding::encode_record(doc_to_write.as_bytes()),
                )?;
                inserted.set(inserted.get() + 1);
                Ok(Some(RawValue::Owned(RawBson::Document(doc_to_write))))
            } else {
                let written = build_doc(mode, &id_str, new_raw_ref, old_raw)?;
                let written = match written {
                    Some(doc) => doc,
                    None => {
                        // Merge no-op: doc unchanged, still counts as matched.
                        updated.set(updated.get() + 1);
                        return Ok(Some(RawValue::Owned(RawBson::Document(
                            old_raw.to_raw_document_buf(),
                        ))));
                    }
                };

                txn.put(
                    cf,
                    &record_key,
                    &encoding::encode_record(written.as_bytes()),
                )?;
                updated.set(updated.get() + 1);
                Ok(Some(RawValue::Owned(RawBson::Document(written))))
            }
        } else {
            let doc_to_write = normalize_id(new_doc, new_raw, &id_str)?;
            txn.put(
                cf,
                &record_key,
                &encoding::encode_record(doc_to_write.as_bytes()),
            )?;
            inserted.set(inserted.get() + 1);
            Ok(Some(RawValue::Owned(RawBson::Document(doc_to_write))))
        }
    })))
}

/// Extract `_id` from the incoming doc, generating a UUID if missing.
/// Returns `(id_string, Some(buf_with_id_prepended))` when an id was generated,
/// or `(id_string, None)` when the doc already had one.
fn extract_id(raw: &RawDocument) -> Result<(String, Option<RawDocumentBuf>), DbError> {
    match raw.get("_id")? {
        Some(RawBsonRef::String(s)) => Ok((s.to_string(), None)),
        Some(other) => Ok((format!("{:?}", other), None)),
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            let mut buf = RawDocumentBuf::new();
            buf.append("_id", id.as_str());
            for entry in raw.iter() {
                let (k, v) = entry?;
                buf.append_ref(k, v);
            }
            Ok((id, Some(buf)))
        }
    }
}

/// Walk the old document's indexed fields and delete their index entries.
fn delete_old_indexes<T: Transaction>(
    txn: &T,
    cf: &T::Cf,
    old_raw: &RawDocument,
    tree: &HashMap<String, FieldTree>,
    id: &str,
) -> Result<(), DbError> {
    let mut err: Option<DbError> = None;
    walk(old_raw, tree, |path, value| {
        if err.is_some() {
            return;
        }
        if path == "ttl" && !matches!(value, RawBsonRef::DateTime(_)) {
            return;
        }
        let idx_key = encoding::raw_index_key(path, value, id);
        if let Err(e) = txn.delete(cf, &idx_key) {
            err = Some(DbError::Store(e));
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Build the document to write based on upsert mode.
/// Returns `None` for a merge no-op (all fields unchanged).
fn build_doc(
    mode: &UpsertMode,
    id: &str,
    new_raw: &RawDocument,
    old_raw: &RawDocument,
) -> Result<Option<RawDocumentBuf>, DbError> {
    match mode {
        UpsertMode::Replace => {
            let mut buf = RawDocumentBuf::new();
            buf.append("_id", id);
            for entry in new_raw.iter() {
                let (k, v) = entry?;
                if k != "_id" {
                    buf.append_ref(k, v);
                }
            }
            Ok(Some(buf))
        }
        UpsertMode::Merge => Ok(exec::raw_merge_doc(old_raw, new_raw)?),
    }
}

/// Ensure `_id` is the first field in the stored document.
/// If the caller already built a doc with `_id` prepended (`Some(buf)`), use it.
/// Otherwise, rebuild with `_id` first.
fn normalize_id(
    existing: Option<RawDocumentBuf>,
    raw: &RawDocument,
    id: &str,
) -> Result<RawDocumentBuf, DbError> {
    match existing {
        Some(buf) => Ok(buf),
        None => {
            let mut buf = RawDocumentBuf::new();
            buf.append("_id", id);
            for entry in raw.iter() {
                let (k, v) = entry?;
                if k != "_id" {
                    buf.append_ref(k, v);
                }
            }
            Ok(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    // ── extract_id ──────────────────────────────────────────────

    #[test]
    fn extract_id_string() {
        let doc = rawdoc! { "_id": "abc", "name": "Alice" };
        let (id, rebuilt) = extract_id(&doc).unwrap();
        assert_eq!(id, "abc");
        assert!(rebuilt.is_none());
    }

    #[test]
    fn extract_id_non_string() {
        let doc = rawdoc! { "_id": 42_i32, "name": "Alice" };
        let (id, rebuilt) = extract_id(&doc).unwrap();
        // Non-string IDs are formatted via Debug
        assert!(id.contains("42"));
        assert!(rebuilt.is_none());
    }

    #[test]
    fn extract_id_missing_generates_uuid() {
        let doc = rawdoc! { "name": "Alice", "score": 100 };
        let (id, rebuilt) = extract_id(&doc).unwrap();

        // Should be a valid UUID
        assert!(uuid::Uuid::parse_str(&id).is_ok());

        // Rebuilt doc should have _id first, then original fields
        let buf = rebuilt.unwrap();
        assert_eq!(buf.get_str("_id").unwrap(), id);
        assert_eq!(buf.get_str("name").unwrap(), "Alice");
        assert_eq!(buf.get_i32("score").unwrap(), 100);
    }

    #[test]
    fn extract_id_missing_preserves_all_fields() {
        let doc = rawdoc! { "a": "1", "b": "2", "c": "3" };
        let (_, rebuilt) = extract_id(&doc).unwrap();
        let buf = rebuilt.unwrap();

        // _id + 3 original fields
        let keys: Vec<&str> = buf.iter().map(|e| e.unwrap().0).collect();
        assert_eq!(keys.len(), 4);
        assert_eq!(keys[0], "_id");
        assert_eq!(keys[1], "a");
        assert_eq!(keys[2], "b");
        assert_eq!(keys[3], "c");
    }

    // ── build_doc ───────────────────────────────────────────────

    #[test]
    fn build_doc_replace() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "name": "Bob", "status": "active" };

        let result = build_doc(&UpsertMode::Replace, "1", &new, &old)
            .unwrap()
            .unwrap();
        assert_eq!(result.get_str("_id").unwrap(), "1");
        assert_eq!(result.get_str("name").unwrap(), "Bob");
        assert_eq!(result.get_str("status").unwrap(), "active");
        // Old field "score" should be gone
        assert!(result.get("score").unwrap().is_none());
    }

    #[test]
    fn build_doc_replace_strips_duplicate_id() {
        let old = rawdoc! { "_id": "1", "name": "Alice" };
        let new = rawdoc! { "_id": "1", "name": "Bob" };

        let result = build_doc(&UpsertMode::Replace, "1", &new, &old)
            .unwrap()
            .unwrap();
        // _id should appear exactly once
        let keys: Vec<&str> = result.iter().map(|e| e.unwrap().0).collect();
        assert_eq!(keys.iter().filter(|&&k| k == "_id").count(), 1);
    }

    #[test]
    fn build_doc_merge_updates_field() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "score": 99 };

        let result = build_doc(&UpsertMode::Merge, "1", &new, &old)
            .unwrap()
            .unwrap();
        // name preserved from old, score updated from new
        assert_eq!(result.get_str("name").unwrap(), "Alice");
        assert_eq!(result.get_i32("score").unwrap(), 99);
    }

    #[test]
    fn build_doc_merge_adds_new_field() {
        let old = rawdoc! { "_id": "1", "name": "Alice" };
        let new = rawdoc! { "_id": "1", "email": "a@test.com" };

        let result = build_doc(&UpsertMode::Merge, "1", &new, &old)
            .unwrap()
            .unwrap();
        assert_eq!(result.get_str("name").unwrap(), "Alice");
        assert_eq!(result.get_str("email").unwrap(), "a@test.com");
    }

    #[test]
    fn build_doc_merge_noop_returns_none() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };

        let result = build_doc(&UpsertMode::Merge, "1", &new, &old).unwrap();
        assert!(result.is_none());
    }

    // ── normalize_id ────────────────────────────────────────────

    #[test]
    fn normalize_id_passthrough_existing() {
        let existing = rawdoc! { "_id": "1", "name": "Alice" };
        let raw = rawdoc! { "name": "ignored" };
        let result = normalize_id(Some(existing.clone()), &raw, "1").unwrap();
        assert_eq!(result.as_bytes(), existing.as_bytes());
    }

    #[test]
    fn normalize_id_rebuilds_with_id_first() {
        let raw = rawdoc! { "name": "Alice", "_id": "1", "score": 70 };
        let result = normalize_id(None, &raw, "1").unwrap();

        let keys: Vec<&str> = result.iter().map(|e| e.unwrap().0).collect();
        assert_eq!(keys[0], "_id");
        assert_eq!(keys[1], "name");
        assert_eq!(keys[2], "score");
        // _id should only appear once (skipped from original iteration)
        assert_eq!(keys.len(), 3);
    }

    // ── delete_old_indexes ──────────────────────────────────────

    #[test]
    fn delete_old_indexes_deletes_indexed_fields() {
        use crate::executor::field_tree::FieldTree;
        use slate_store::Transaction;
        use std::cell::RefCell;

        // Minimal mock that only tracks deletes
        struct DeleteTracker {
            deletes: RefCell<Vec<Vec<u8>>>,
        }
        impl Transaction for DeleteTracker {
            type Cf = ();
            fn cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn get<'c>(
                &self,
                _: &'c (),
                _: &[u8],
            ) -> Result<Option<std::borrow::Cow<'c, [u8]>>, slate_store::StoreError> {
                Ok(None)
            }
            fn multi_get<'c>(
                &self,
                _: &'c (),
                _: &[&[u8]],
            ) -> Result<Vec<Option<std::borrow::Cow<'c, [u8]>>>, slate_store::StoreError>
            {
                Ok(vec![])
            }
            fn scan_prefix<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn scan_prefix_rev<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn put(&self, _: &(), _: &[u8], _: &[u8]) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn put_batch(
                &self,
                _: &(),
                _: &[(&[u8], &[u8])],
            ) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn delete(&self, _: &(), key: &[u8]) -> Result<(), slate_store::StoreError> {
                self.deletes.borrow_mut().push(key.to_vec());
                Ok(())
            }
            fn create_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn drop_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn commit(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn rollback(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
        }

        let txn = DeleteTracker {
            deletes: RefCell::new(Vec::new()),
        };
        let cf = ();
        let old_doc = rawdoc! { "_id": "1", "status": "active", "score": 80 };
        let paths = vec!["status".to_string()];
        let tree = FieldTree::from_paths(&paths);

        delete_old_indexes(&txn, &cf, &old_doc, &tree, "1").unwrap();

        let deletes = txn.deletes.borrow();
        assert_eq!(deletes.len(), 1);
        assert_eq!(
            deletes[0],
            encoding::raw_index_key("status", bson::raw::RawBsonRef::String("active"), "1")
        );
    }

    #[test]
    fn delete_old_indexes_skips_non_datetime_ttl() {
        use crate::executor::field_tree::FieldTree;
        use std::cell::RefCell;

        struct DeleteTracker {
            deletes: RefCell<Vec<Vec<u8>>>,
        }
        impl slate_store::Transaction for DeleteTracker {
            type Cf = ();
            fn cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn get<'c>(
                &self,
                _: &'c (),
                _: &[u8],
            ) -> Result<Option<std::borrow::Cow<'c, [u8]>>, slate_store::StoreError> {
                Ok(None)
            }
            fn multi_get<'c>(
                &self,
                _: &'c (),
                _: &[&[u8]],
            ) -> Result<Vec<Option<std::borrow::Cow<'c, [u8]>>>, slate_store::StoreError>
            {
                Ok(vec![])
            }
            fn scan_prefix<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn scan_prefix_rev<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn put(&self, _: &(), _: &[u8], _: &[u8]) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn put_batch(
                &self,
                _: &(),
                _: &[(&[u8], &[u8])],
            ) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn delete(&self, _: &(), key: &[u8]) -> Result<(), slate_store::StoreError> {
                self.deletes.borrow_mut().push(key.to_vec());
                Ok(())
            }
            fn create_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn drop_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn commit(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn rollback(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
        }

        let txn = DeleteTracker {
            deletes: RefCell::new(Vec::new()),
        };
        let cf = ();
        // ttl is a string, not DateTime — should be skipped
        let old_doc = rawdoc! { "_id": "1", "ttl": "not-a-date" };
        let paths = vec!["ttl".to_string()];
        let tree = FieldTree::from_paths(&paths);

        delete_old_indexes(&txn, &cf, &old_doc, &tree, "1").unwrap();

        assert!(txn.deletes.borrow().is_empty());
    }

    #[test]
    fn delete_old_indexes_handles_ttl_datetime() {
        use crate::executor::field_tree::FieldTree;
        use std::cell::RefCell;

        struct DeleteTracker {
            deletes: RefCell<Vec<Vec<u8>>>,
        }
        impl slate_store::Transaction for DeleteTracker {
            type Cf = ();
            fn cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn get<'c>(
                &self,
                _: &'c (),
                _: &[u8],
            ) -> Result<Option<std::borrow::Cow<'c, [u8]>>, slate_store::StoreError> {
                Ok(None)
            }
            fn multi_get<'c>(
                &self,
                _: &'c (),
                _: &[&[u8]],
            ) -> Result<Vec<Option<std::borrow::Cow<'c, [u8]>>>, slate_store::StoreError>
            {
                Ok(vec![])
            }
            fn scan_prefix<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn scan_prefix_rev<'c>(
                &'c self,
                _: &'c (),
                _: &[u8],
            ) -> Result<
                Box<
                    dyn Iterator<
                            Item = Result<
                                (std::borrow::Cow<'c, [u8]>, std::borrow::Cow<'c, [u8]>),
                                slate_store::StoreError,
                            >,
                        > + 'c,
                >,
                slate_store::StoreError,
            > {
                Ok(Box::new(std::iter::empty()))
            }
            fn put(&self, _: &(), _: &[u8], _: &[u8]) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn put_batch(
                &self,
                _: &(),
                _: &[(&[u8], &[u8])],
            ) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn delete(&self, _: &(), key: &[u8]) -> Result<(), slate_store::StoreError> {
                self.deletes.borrow_mut().push(key.to_vec());
                Ok(())
            }
            fn create_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn drop_cf(&mut self, _: &str) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn commit(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
            fn rollback(self) -> Result<(), slate_store::StoreError> {
                Ok(())
            }
        }

        let txn = DeleteTracker {
            deletes: RefCell::new(Vec::new()),
        };
        let cf = ();
        let dt = bson::DateTime::from_millis(1700000000000);
        let old_doc = rawdoc! { "_id": "1", "ttl": dt };
        let paths = vec!["ttl".to_string()];
        let tree = FieldTree::from_paths(&paths);

        delete_old_indexes(&txn, &cf, &old_doc, &tree, "1").unwrap();

        let deletes = txn.deletes.borrow();
        assert_eq!(deletes.len(), 1);
        assert_eq!(
            deletes[0],
            encoding::raw_index_key("ttl", bson::raw::RawBsonRef::DateTime(dt), "1")
        );
    }
}
