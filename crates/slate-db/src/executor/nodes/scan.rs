use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
) -> Result<RawIter<'a>, DbError> {
    let iter = txn.scan(&handle)?;

    Ok(Box::new(iter.map(|result| match result {
        Ok(doc) => Ok(Some(RawBson::Document(doc))),
        Err(e) => Err(DbError::from(e)),
    })))
}
