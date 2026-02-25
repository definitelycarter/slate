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
        // Accept bare String id (from IndexScan) or Document with _id
        let id_str = match &val {
            RawBson::String(s) => s.clone(),
            RawBson::Document(d) => match exec::raw_extract_id(d) {
                Ok(Some(s)) => s.to_string(),
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            },
            _ => return None,
        };
        let doc_id = BsonValue::from_raw_bson_ref(bson::raw::RawBsonRef::String(&id_str))
            .expect("string is always a valid BsonValue");
        match txn.get(&handle, &doc_id, now_millis) {
            Ok(Some(doc)) => Some(Ok(Some(RawBson::Document(doc)))),
            Ok(None) => None,
            Err(e) => Some(Err(DbError::from(e))),
        }
    })))
}
