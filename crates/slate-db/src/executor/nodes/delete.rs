use bson::RawBson;
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{Context, RawIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    ctx: Context<'a, T>,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        if let Some(RawBson::Document(ref d)) = opt_val {
            ctx.fire_triggers(handle.cf_name(), handle.name(), "deleting", d)?;

            let raw_id = d
                .get("_id")
                .map_err(|e| DbError::Serialization(e.to_string()))?
                .ok_or_else(|| DbError::InvalidQuery("missing _id".into()))?;
            ctx.txn.delete(&handle, &raw_id)?;

            ctx.fire_triggers(handle.cf_name(), handle.name(), "deleted", d)?;
        }
        Ok(None)
    })))
}
