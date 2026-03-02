use bson::raw::RawDocumentBuf;
use bson::{RawBson, RawDocument};
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{Context, RawIter};
use crate::mutation::raw as raw_mutation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpsertMode {
    Replace,
    Merge,
}

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    ctx: Context<'a, T>,
    handle: CollectionHandle<T::Cf>,
    mode: UpsertMode,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let cf = handle.cf_name().to_string();
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let mut new_doc = match opt_val {
            Some(RawBson::Document(d)) => d,
            Some(_) => return Err(DbError::InvalidQuery("expected document".into())),
            None => return Err(DbError::InvalidQuery("Upsert requires document".into())),
        };

        // Ensure _id exists, generating one if missing.
        let has_id = new_doc
            .get("_id")
            .map_err(|e| DbError::Serialization(e.to_string()))?
            .is_some();
        if !has_id {
            let oid = bson::oid::ObjectId::new();
            new_doc.append(bson::cstr!("_id"), oid);
        }

        let raw_id = new_doc
            .get("_id")
            .map_err(|e| DbError::Serialization(e.to_string()))?
            .expect("_id was just ensured");

        let old_doc = ctx.txn.get(&handle, &raw_id)?;

        if let Some(ref old_raw_doc) = old_doc {
            ctx.fire_triggers(&cf, "updating", old_raw_doc)?;

            let written = build_doc(&mode, &new_doc, old_raw_doc)?;
            let written = match written {
                Some(doc) => doc,
                None => {
                    // Merge no-op: doc unchanged, still counts as matched.
                    return Ok(Some(RawBson::Document(old_raw_doc.clone())));
                }
            };

            ctx.txn.put(&handle, &written)?;
            ctx.fire_triggers(&cf, "updated", &written)?;
            Ok(Some(RawBson::Document(written)))
        } else {
            ctx.fire_triggers(&cf, "inserting", &new_doc)?;
            ctx.txn.put_nx(&handle, &new_doc)?;
            ctx.fire_triggers(&cf, "inserted", &new_doc)?;
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

}
