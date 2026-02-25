use bson::RawBson;
use bson::raw::{RawBsonRef, RawDocumentBuf};
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            Some(_) => {
                return Err(DbError::InvalidQuery("expected document".into()));
            }
            None => {
                return Err(DbError::InvalidQuery(
                    "InsertRecord requires document".into(),
                ));
            }
        };

        let (id_str, doc_to_write) = match raw.get("_id")? {
            Some(RawBsonRef::String(s)) => (s.to_string(), None),
            Some(other) => (format!("{:?}", other), None),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                let mut buf = RawDocumentBuf::new();
                buf.append("_id", id.as_str());
                for entry in raw.iter() {
                    let (k, v) = entry?;
                    buf.append_ref(k, v);
                }
                (id, Some(buf))
            }
        };

        let doc_id = BsonValue::from_raw_bson_ref(RawBsonRef::String(&id_str))
            .expect("string is always a valid BsonValue");

        // Duplicate key check (expired docs are treated as non-existent)
        if txn.get(&handle, &doc_id, now_millis)?.is_some() {
            return Err(DbError::DuplicateKey(id_str));
        }

        // Write via engine — handles encoding + index maintenance
        let write_doc = match &doc_to_write {
            Some(buf) => buf.as_ref(),
            None => raw,
        };
        txn.put(&handle, write_doc, &doc_id)?;

        match doc_to_write {
            Some(buf) => Ok(Some(RawBson::Document(buf))),
            None => Ok(opt_val),
        }
    })))
}
