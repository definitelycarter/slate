use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_engine::{Catalog, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::{Context, RawIter};

pub(crate) fn execute<'a, T: EngineTransaction + Catalog>(
    ctx: Context<'a, T>,
    handle: CollectionHandle<T::Cf>,
    replacement: RawDocumentBuf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let replacement_raw = replacement;
    let cf = handle.cf_name().to_string();

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            _ => return Ok(None),
        };

        let has_id = old_raw
            .get("_id")
            .map_err(|e| DbError::Serialization(e.to_string()))?
            .is_some();
        if !has_id {
            return Err(DbError::InvalidQuery("missing _id".into()));
        }

        ctx.fire_triggers(&cf, "updating", old_raw)?;

        // Rebuild doc: copy _id from old doc (preserves type), then replacement fields
        let mut buf = RawDocumentBuf::new();
        if let Ok(Some(id_ref)) = old_raw.get("_id") {
            buf.append(bson::cstr!("_id"), id_ref);
        }
        for entry in replacement_raw.iter() {
            let (k, v) = entry?;
            if k != "_id" {
                buf.append(k, v);
            }
        }

        ctx.txn.put(&handle, &buf)?;
        ctx.fire_triggers(&cf, "updated", &buf)?;

        Ok(Some(RawBson::Document(buf)))
    })))
}
