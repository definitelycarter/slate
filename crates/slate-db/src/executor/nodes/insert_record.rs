use bson::RawBson;
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{Context, RawIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    ctx: Context<'a, T>,
    handle: CollectionHandle<T::Cf>,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let cf = handle.cf_name().to_string();
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let mut doc = match opt_val {
            Some(RawBson::Document(d)) => d,
            Some(_) => {
                return Err(DbError::InvalidQuery("expected document".into()));
            }
            None => {
                return Err(DbError::InvalidQuery(
                    "InsertRecord requires document".into(),
                ));
            }
        };

        // Generate _id if missing.
        let has_id = doc
            .get("_id")
            .map_err(|e| DbError::Serialization(e.to_string()))?
            .is_some();
        if !has_id {
            let oid = bson::oid::ObjectId::new();
            doc.append(bson::cstr!("_id"), oid);
        }
        ctx.fire_triggers(&cf, "inserting", &doc)?;
        ctx.txn.put_nx(&handle, &doc)?;
        ctx.fire_triggers(&cf, "inserted", &doc)?;

        Ok(Some(RawBson::Document(doc)))
    })))
}
