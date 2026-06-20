//! The `Replace` write node — overwrite each source document with `replacement`.
//!
//! The original primary key is preserved (copied from the old document, so its
//! type is kept); all other fields come from `replacement`. Yields the new
//! documents.

use bson::RawBson;
use bson::raw::{CString, RawDocumentBuf};
use slate_engine::{CollectionHandle, EngineTransaction};
use slate_eval::EvalError;

use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    replacement: RawDocumentBuf,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    let pk_key = CString::try_from(handle.pk_path()).map_err(|e| EvalError {
        message: format!("invalid pk path: {e}"),
    })?;

    Ok(Box::new(source.map(move |result| {
        let old = match result? {
            Some(RawBson::Document(d)) => d,
            _ => return Ok(None),
        };

        let pk = handle.pk_path();
        let id = old
            .get(pk)
            .map_err(|e| EvalError {
                message: format!("malformed document: {e}"),
            })?
            .ok_or_else(|| EvalError {
                message: "replace requires a document with a primary key".into(),
            })?;

        // Rebuild: original pk first, then replacement fields (minus any pk).
        let mut buf = RawDocumentBuf::new();
        buf.append(&pk_key, id);
        for entry in replacement.iter() {
            let (key, value) = entry.map_err(|e| EvalError {
                message: format!("malformed replacement: {e}"),
            })?;
            if key != pk {
                buf.append(key, value);
            }
        }

        txn.put(&handle, &buf)?;
        Ok(Some(RawBson::Document(buf)))
    })))
}
