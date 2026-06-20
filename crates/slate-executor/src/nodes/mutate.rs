//! The `Update` write node — apply a `Mutation` to each source document.
//!
//! Each document is mutated via the shared `slate-mutation` engine and written
//! back. Yields the mutated documents; documents the mutation leaves unchanged
//! are dropped.

use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};
use slate_mutation::Mutation;

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    mutation: Mutation,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    Ok(Box::new(source.map(move |result| {
        let old = match result? {
            Some(RawBson::Document(d)) => d,
            _ => return Ok(None),
        };

        match mutation.apply(&old)? {
            Some(mutated) => {
                txn.put(&handle, &mutated)?;
                Ok(Some(RawBson::Document(mutated)))
            }
            None => Ok(None), // unchanged
        }
    })))
}
