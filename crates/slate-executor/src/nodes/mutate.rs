//! The `Update` write node — apply an `UPDATE`'s assignments to each document.
//!
//! Each assignment's right-hand expression is evaluated against the document and
//! the result written to its target path (an undefined result removes the field)
//! via `slate-eval`, then the rebuilt document is written back. Yields the
//! mutated documents; documents left unchanged are dropped.

use std::rc::Rc;

use bson::{Document, RawBson};
use slate_ast::Assignment;
use slate_engine::{CollectionHandle, EngineTransaction};

use crate::watch::WatchSink;
use crate::{ExecError, ValueIter};

/// The alias the find front-end binds the matched document to (matches
/// `slate_query::ALIAS`); the assignments' right-hand sides reference document
/// fields through it. Hard-coded while `UPDATE` only originates from the Mongo
/// write path — a future SQL `UPDATE` would thread its own alias.
const UPDATE_ALIAS: &str = "c";

pub(crate) fn execute<'a, T: EngineTransaction>(
    txn: &'a T,
    handle: CollectionHandle<T::Cf>,
    assignments: Vec<Assignment>,
    source: ValueIter<'a>,
    watch: Option<Rc<WatchSink>>,
) -> Result<ValueIter<'a>, ExecError> {
    // Update assignments never reference `@parameters`, so an empty set suffices.
    let params = Document::new();
    Ok(Box::new(source.map(move |result| {
        let old = match result? {
            Some(RawBson::Document(d)) => d,
            _ => return Ok(None),
        };

        match slate_eval::apply_assignments(&old, UPDATE_ALIAS, &assignments, &params)? {
            Some(mutated) => {
                txn.put(&handle, &mutated)?;
                // Update has both states — the watch recasts (enter/leave/stay)
                // from the filter evaluated on each.
                if let Some(sink) = &watch {
                    sink.capture(
                        handle.cf_name(),
                        handle.name(),
                        handle.pk_path(),
                        Some(&old),
                        Some(&mutated),
                    )?;
                }
                Ok(Some(RawBson::Document(mutated)))
            }
            None => Ok(None), // unchanged
        }
    })))
}
