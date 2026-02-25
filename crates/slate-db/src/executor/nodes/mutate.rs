use bson::RawBson;
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};
use slate_query::Mutation;

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: EngineTransaction + 'a>(
    txn: &'a T,
    handle: &'a CollectionHandle<T::Cf>,
    mutation: Mutation,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(val) => match val.as_document() {
                Some(r) => r,
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        let id_str = match exec::raw_extract_id(old_raw)? {
            Some(s) => s.to_string(),
            None => return Ok(None),
        };

        match exec::apply_mutation(old_raw, &mutation)? {
            Some(mutated) => {
                let doc_id = BsonValue::from_raw_bson_ref(
                    bson::raw::RawBsonRef::String(&id_str),
                )
                .expect("string is always a valid BsonValue");
                txn.put(handle, &mutated, &doc_id)?;
                Ok(Some(RawValue::Owned(RawBson::Document(mutated))))
            }
            None => Ok(None),
        }
    })))
}
