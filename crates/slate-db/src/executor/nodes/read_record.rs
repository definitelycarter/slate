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
    // IndexScan/IndexMerge path: extract _id, fetch full record
    Ok(Box::new(source.filter_map(move |result| {
        let opt_val = match result {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let val = match opt_val {
            Some(v) => v,
            None => return None,
        };
        // Accept bare id (from IndexScan) or Document with _id
        let doc_id = match &val {
            RawBson::Document(d) => match exec::extract_doc_id(d) {
                Ok(Some(id)) => Some(id.into_owned()),
                _ => return None,
            },
            other => BsonValue::from_raw_bson(other),
        };
        let doc_id = match doc_id {
            Some(id) => id,
            None => return None,
        };
        match txn.get(&handle, &doc_id, now_millis) {
            Ok(Some(doc)) => Some(Ok(Some(RawBson::Document(doc)))),
            Ok(None) => None,
            Err(e) => Some(Err(DbError::from(e))),
        }
    })))
}
