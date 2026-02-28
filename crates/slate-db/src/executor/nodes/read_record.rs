use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
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
        let raw_ref = match &val {
            RawBson::Document(d) => match d.get("_id") {
                Ok(Some(id)) => id,
                _ => return None,
            },
            other => other.as_raw_bson_ref(),
        };
        match txn.get(&handle, &raw_ref) {
            Ok(Some(doc)) => Some(Ok(Some(RawBson::Document(doc)))),
            Ok(None) => None,
            Err(e) => Some(Err(DbError::from(e))),
        }
    })))
}
