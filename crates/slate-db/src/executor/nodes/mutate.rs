use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};
use slate_query::Mutation;

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

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

        let doc_id = match exec::extract_doc_id(old_raw) {
            Ok(Some(id)) => id,
            _ => return Ok(None),
        };

        match exec::apply_mutation(old_raw, &mutation)? {
            Some(mutated) => {
                txn.put(&handle, &mutated, &doc_id)?;
                Ok(Some(RawBson::Document(mutated)))
            }
            None => Ok(None),
        }
    })))
}
