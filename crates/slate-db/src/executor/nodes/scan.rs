use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: EngineTransaction + 'a>(
    txn: &'a T,
    handle: &'a CollectionHandle<T::Cf>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    let iter = txn.scan(handle, now_millis)?;

    Ok(Box::new(iter.map(|result| match result {
        Ok((_doc_id, doc)) => Ok(Some(RawValue::Owned(RawBson::Document(doc)))),
        Err(e) => Err(DbError::from(e)),
    })))
}
