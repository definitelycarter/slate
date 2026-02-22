use bson::RawBson;
use bson::raw::{RawBsonRef, RawDocumentBuf};
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let val = match &opt_val {
            Some(v) => v,
            None => {
                return Err(DbError::InvalidQuery(
                    "InsertRecord requires document".into(),
                ));
            }
        };

        let raw = val
            .as_document()
            .ok_or_else(|| DbError::InvalidQuery("expected document".into()))?;

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

        // Duplicate key check
        let key = encoding::record_key(&id_str);
        if txn.get(cf, &key)?.is_some() {
            return Err(DbError::DuplicateKey(id_str));
        }

        match &doc_to_write {
            Some(buf) => txn.put(cf, &key, &encoding::encode_record(buf.as_bytes()))?,
            None => txn.put(cf, &key, &encoding::encode_record(raw.as_bytes()))?,
        }

        match doc_to_write {
            Some(buf) => Ok(Some(RawValue::Owned(RawBson::Document(buf)))),
            None => Ok(opt_val),
        }
    })))
}
