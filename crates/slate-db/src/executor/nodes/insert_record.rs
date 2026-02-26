use bson::RawBson;
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let mut doc = match opt_val {
            Some(RawBson::Document(d)) => d,
            Some(_) => {
                return Err(DbError::InvalidQuery("expected document".into()));
            }
            None => {
                return Err(DbError::InvalidQuery(
                    "InsertRecord requires document".into(),
                ));
            }
        };

        let doc_id = match exec::extract_doc_id(&doc)? {
            Some(id) => id,
            None => {
                let oid = bson::oid::ObjectId::new();
                doc.append(bson::cstr!("_id"), oid);
                BsonValue::from_raw_bson_ref(bson::raw::RawBsonRef::ObjectId(oid))
                    .expect("ObjectId is always valid")
            }
        };

        txn.put_nx(&handle, &doc, &doc_id, now_millis)?;
        Ok(Some(RawBson::Document(doc)))
    })))
}
