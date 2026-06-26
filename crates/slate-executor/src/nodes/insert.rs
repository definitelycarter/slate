//! The `Insert` write node — write each source document with a unique key.
//!
//! Generates an `ObjectId` primary key when the document lacks one, then
//! `put_nx` (insert-if-absent). Yields the inserted documents.

use std::rc::Rc;

use bson::RawBson;
use bson::raw::CString;
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
    let pk_key = CString::try_from(handle.pk_path()).map_err(|e| EvalError {
        message: format!("invalid pk path: {e}"),
    })?;

    Ok(Box::new(source.map(move |result| {
        let Some(RawBson::Document(mut doc)) = result? else {
            return Err(ExecError::Eval(EvalError {
                message: "insert requires a document".into(),
            }));
        };

        let has_id = doc
            .get(handle.pk_path())
            .map_err(|e| EvalError {
                message: format!("malformed document: {e}"),
            })?
            .is_some();
        if !has_id {
            doc.append(&pk_key, bson::oid::ObjectId::new());
        }

        txn.put_nx(&handle, &doc)?;
        // Insert has only a *new* document — capture it as a set entry.
        if let Some(sink) = &watch {
            sink.capture(
                handle.cf_name(),
                handle.name(),
                handle.pk_path(),
                None,
                Some(&doc),
            )?;
        }
        Ok(Some(RawBson::Document(doc)))
    })))
}
