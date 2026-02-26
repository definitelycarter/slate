use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        if let Some(RawBson::Document(ref d)) = opt_val {
            let doc_id = exec::extract_doc_id(d)?
                .ok_or_else(|| DbError::InvalidQuery("missing _id".into()))?;
            txn.delete(&handle, &doc_id)?;
        }
        Ok(None)
    })))
}
