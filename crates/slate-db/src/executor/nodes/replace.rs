use bson::RawBson;
use bson::raw::{CString, RawDocumentBuf};
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::RawIter;

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    replacement: RawDocumentBuf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let replacement_raw = replacement;
    let pk_key = CString::try_from(handle.pk_path())
        .map_err(|e| DbError::Serialization(e.to_string()))?;

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let old_raw = match &opt_val {
            Some(RawBson::Document(d)) => d.as_ref(),
            _ => return Ok(None),
        };

        let pk = handle.pk_path();
        let has_id = old_raw
            .get(pk)
            .map_err(|e| DbError::Serialization(e.to_string()))?
            .is_some();
        if !has_id {
            return Err(DbError::InvalidQuery("missing pk".into()));
        }

        // Rebuild doc: copy pk from old doc (preserves type), then replacement fields
        let mut buf = RawDocumentBuf::new();
        if let Ok(Some(id_ref)) = old_raw.get(pk) {
            buf.append(&pk_key, id_ref);
        }
        for entry in replacement_raw.iter() {
            let (k, v) = entry?;
            if k != pk {
                buf.append(k, v);
            }
        }

        txn.put(&handle, &buf)?;

        Ok(Some(RawBson::Document(buf)))
    })))
}
