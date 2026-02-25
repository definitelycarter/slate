use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction, Record};

use crate::error::DbError;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: EngineTransaction + 'a>(
    txn: &'a T,
    handle: &'a CollectionHandle<T::Cf>,
    now_millis: i64,
) -> Result<RawIter<'a>, DbError> {
    let iter = txn.scan(handle)?;

    Ok(Box::new(iter.filter_map(move |result| match result {
        Ok((_doc_id, doc)) => {
            if now_millis != i64::MIN {
                if let Some(millis) = Record::ttl_millis(&doc) {
                    if millis < now_millis {
                        return None;
                    }
                }
            }
            Some(Ok(Some(RawValue::Owned(RawBson::Document(doc)))))
        }
        Err(e) => Some(Err(DbError::from(e))),
    })))
}
