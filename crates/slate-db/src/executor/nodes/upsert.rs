use bson::raw::{RawBsonRef, RawDocumentBuf};
use bson::{RawBson, RawDocument};
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
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
        let val = match opt_val {
            Some(v) => v,
            None => {
                return Err(DbError::InvalidQuery("Upsert requires document".into()));
            }
        };

        let new_raw = match &val {
            RawBson::Document(d) => d.as_ref(),
            _ => return Err(DbError::InvalidQuery("expected document".into())),
        };

        let (id_str, new_doc) = extract_id(new_raw)?;
        let new_raw_ref = match &new_doc {
            Some(buf) => buf.as_ref(),
            None => new_raw,
        };

        let doc_id = BsonValue::from_raw_bson_ref(RawBsonRef::String(&id_str))
            .expect("string is always a valid BsonValue");

        let old_doc = txn.get(&handle, &doc_id, now_millis)?;

        if let Some(ref old_raw_doc) = old_doc {
            let written = build_doc(&mode, &id_str, new_raw_ref, old_raw_doc)?;
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
            let doc_to_write = normalize_id(new_doc, new_raw, &id_str)?;
            txn.put(&handle, &doc_to_write, &doc_id)?;
            Ok(Some(RawBson::Document(doc_to_write)))
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
        UpsertMode::Merge => Ok(raw_mutation::raw_merge(old_raw, new_raw)?),
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
}
