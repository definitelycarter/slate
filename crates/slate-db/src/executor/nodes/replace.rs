use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    replacement: RawDocumentBuf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let replacement_raw = replacement;

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            _ => return Ok(None),
        };

        let doc_id = match exec::extract_doc_id(old_raw)? {
            Some(id) => id,
            None => return Err(DbError::InvalidQuery("missing _id".into())),
        };

        // Rebuild doc: copy _id from old doc (preserves type), then replacement fields
        let mut buf = RawDocumentBuf::new();
        if let Ok(Some(id_ref)) = old_raw.get("_id") {
            buf.append_ref("_id", id_ref);
        }
        for entry in replacement_raw.iter() {
            let (k, v) = entry?;
            if k != "_id" {
                buf.append_ref(k, v);
            }
        }

        txn.put(&handle, &buf, &doc_id)?;
        Ok(Some(RawBson::Document(buf)))
    })))
}
