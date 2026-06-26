//! The `Delete` write node — delete each source document by its primary key.
//!
//! Yields the deleted documents (passed through unchanged).

use std::rc::Rc;

use bson::RawBson;
use slate_engine::{CollectionHandle, EngineTransaction};
use slate_eval::EvalError;

use crate::watch::WatchSink;
use crate::{ExecError, ValueIter};

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    source: ValueIter<'a>,
    watch: Option<Rc<WatchSink>>,
) -> Result<ValueIter<'a>, ExecError> {
    Ok(Box::new(source.map(move |result| {
        let value = result?;
        if let Some(RawBson::Document(ref doc)) = value {
            let id = doc
                .get(handle.pk_path())
                .map_err(|e| EvalError {
                    message: format!("malformed document: {e}"),
                })?
                .ok_or_else(|| EvalError {
                    message: "delete requires a document with a primary key".into(),
                })?;
            txn.delete(&handle, &id)?;
            // Delete has only an *old* document — capture it as a set exit.
            if let Some(sink) = &watch {
                sink.capture(
                    handle.cf_name(),
                    handle.name(),
                    handle.pk_path(),
                    Some(doc),
                    None,
                )?;
            }
        }
        Ok(value)
    })))
}
