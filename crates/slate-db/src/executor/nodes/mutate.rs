use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::mutation::Mutation;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    mutation: Mutation,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            Some(_) => return Ok(None),
            None => return Ok(None),
        };

        match mutation.apply(old_raw)? {
            Some(mutated) => {
                txn.put(&handle, &mutated)?;
                Ok(Some(RawBson::Document(mutated)))
            }
            None => Ok(None),
        }
    })))
}
