use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a, T: EngineTransaction + 'a>(
    txn: &'a T,
    handle: &'a CollectionHandle<T::Cf>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        if let Some(ref val) = opt_val {
            if let Some(raw) = val.as_document() {
                if let Some(id_str) = exec::raw_extract_id(raw)? {
                    let doc_id = BsonValue::from_raw_bson_ref(
                        bson::raw::RawBsonRef::String(id_str),
                    )
                    .ok_or_else(|| DbError::Serialization("invalid _id".into()))?;
                    txn.delete(handle, &doc_id)?;
                }
            }
        }
        Ok(None)
    })))
}
