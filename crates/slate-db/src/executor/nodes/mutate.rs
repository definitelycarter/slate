use bson::RawBson;
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{Context, RawIter};
use crate::mutation::Mutation;

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    ctx: Context<'a, T>,
    handle: CollectionHandle<T::Cf>,
    mutation: Mutation,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let cf = handle.cf_name().to_string();
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            Some(_) => return Ok(None),
            None => return Ok(None),
        };

        ctx.fire_triggers(&cf, "updating", old_raw)?;

        match mutation.apply(old_raw)? {
            Some(mutated) => {
                ctx.txn.put(&handle, &mutated)?;
                ctx.fire_triggers(&cf, "updated", &mutated)?;
                Ok(Some(RawBson::Document(mutated)))
            }
            None => Ok(None),
        }
    })))
}
