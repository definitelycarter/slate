use bson::raw::RawDocumentBuf;
use bson::{RawBson, RawDocument};
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;
use crate::executor::raw_mutation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpsertMode {
    Replace,
    Merge,
}

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    mode: UpsertMode,
    source: RawIter<'a>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let mut new_doc = match opt_val {
            Some(RawBson::Document(d)) => d,
            Some(_) => return Err(DbError::InvalidQuery("expected document".into())),
            None => return Err(DbError::InvalidQuery("Upsert requires document".into())),
        };

        let doc_id = match exec::extract_doc_id(&new_doc)? {
            Some(id) => id,
            None => {
                let oid = bson::oid::ObjectId::new();
                new_doc.append(bson::cstr!("_id"), oid);
                slate_engine::BsonValue::from_raw_bson_ref(bson::raw::RawBsonRef::ObjectId(oid))
                    .expect("ObjectId is always valid")
            }
        };

        let old_doc = txn.get(&handle, &doc_id, now_millis)?;

        if let Some(ref old_raw_doc) = old_doc {
            let written = build_doc(&mode, &new_doc, old_raw_doc)?;
            let written = match written {
                Some(doc) => doc,
                None => {
                    // Merge no-op: doc unchanged, still counts as matched.
                    return Ok(Some(RawBson::Document(old_raw_doc.clone())));
                }
            };

            txn.put(&handle, &written, &doc_id)?;
            Ok(Some(RawBson::Document(written)))
        } else {
            txn.put_nx(&handle, &new_doc, &doc_id, now_millis)?;
            Ok(Some(RawBson::Document(new_doc)))
        }
    })))
}

/// Build the document to write based on upsert mode.
/// Returns `None` for a merge no-op (all fields unchanged).
fn build_doc(
    mode: &UpsertMode,
    new_raw: &RawDocument,
    old_raw: &RawDocument,
) -> Result<Option<RawDocumentBuf>, DbError> {
    match mode {
        UpsertMode::Replace => {
            // Copy _id from old doc (preserves original type), then new fields
            let mut buf = RawDocumentBuf::new();
            if let Ok(Some(id_ref)) = old_raw.get("_id") {
                buf.append(bson::cstr!("_id"), id_ref);
            }
            for entry in new_raw.iter() {
                let (k, v) = entry?;
                if k != "_id" {
                    buf.append(k, v);
                }
            }
            Ok(Some(buf))
        }
        UpsertMode::Merge => Ok(raw_mutation::raw_merge(old_raw, new_raw)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    // ── build_doc ───────────────────────────────────────────────

    #[test]
    fn build_doc_replace() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "name": "Bob", "status": "active" };

        let result = build_doc(&UpsertMode::Replace, &new, &old)
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

        let result = build_doc(&UpsertMode::Replace, &new, &old)
            .unwrap()
            .unwrap();
        let keys: Vec<&str> = result.iter().map(|e| e.unwrap().0.as_str()).collect();
        assert_eq!(keys.iter().filter(|&&k| k == "_id").count(), 1);
    }

    #[test]
    fn build_doc_replace_preserves_objectid() {
        let oid = bson::oid::ObjectId::new();
        let old = rawdoc! { "_id": oid, "name": "Alice" };
        let new = rawdoc! { "name": "Bob" };

        let result = build_doc(&UpsertMode::Replace, &new, &old)
            .unwrap()
            .unwrap();
        assert_eq!(result.get_object_id("_id").unwrap(), oid);
        assert_eq!(result.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn build_doc_merge_updates_field() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "score": 99 };

        let result = build_doc(&UpsertMode::Merge, &new, &old).unwrap().unwrap();
        assert_eq!(result.get_str("name").unwrap(), "Alice");
        assert_eq!(result.get_i32("score").unwrap(), 99);
    }

    #[test]
    fn build_doc_merge_adds_new_field() {
        let old = rawdoc! { "_id": "1", "name": "Alice" };
        let new = rawdoc! { "_id": "1", "email": "a@test.com" };

        let result = build_doc(&UpsertMode::Merge, &new, &old).unwrap().unwrap();
        assert_eq!(result.get_str("name").unwrap(), "Alice");
        assert_eq!(result.get_str("email").unwrap(), "a@test.com");
    }

    #[test]
    fn build_doc_merge_noop_returns_none() {
        let old = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };
        let new = rawdoc! { "_id": "1", "name": "Alice", "score": 70 };

        let result = build_doc(&UpsertMode::Merge, &new, &old).unwrap();
        assert!(result.is_none());
    }

    // ── extract_doc_id ────────────────────────────────────────

    #[test]
    fn extract_doc_id_string() {
        let doc = rawdoc! { "_id": "abc", "name": "Alice" };
        let id = exec::extract_doc_id(&doc).unwrap().unwrap();
        assert_eq!(id.tag, 0x02);
        assert_eq!(&*id.bytes, b"abc");
    }

    #[test]
    fn extract_doc_id_objectid() {
        let oid = bson::oid::ObjectId::new();
        let doc = rawdoc! { "_id": oid, "name": "Alice" };
        let id = exec::extract_doc_id(&doc).unwrap().unwrap();
        assert_eq!(id.tag, 0x07);
    }

    #[test]
    fn extract_doc_id_int() {
        let doc = rawdoc! { "_id": 42_i32, "name": "Alice" };
        let id = exec::extract_doc_id(&doc).unwrap().unwrap();
        assert_eq!(id.tag, 0x10);
    }

    #[test]
    fn extract_doc_id_missing_returns_none() {
        let doc = rawdoc! { "name": "Alice", "score": 100 };
        assert!(exec::extract_doc_id(&doc).unwrap().is_none());
    }
}
