//! Change detection on the write path (watch queries).
//!
//! A *watch* is a compiled SQL `WHERE` predicate registered against a
//! `(cf, collection)`. As documents flow through the mutation nodes
//! (`insert`/`delete`/`mutate`/`replace`/`upsert`), each node hands the
//! before/after document to [`WatchSink::capture`], which evaluates every
//! matching watch's filter and buffers the change until the transaction
//! commits. The db layer (`slate-db`) owns the registry, the callbacks, and the
//! commit-time emit; this module owns the *detection* — filter evaluation and
//! set-transition recasting — which lives here because the executor is the only
//! place old *and* new are co-available (see the RFC's write-path seam table).
//!
//! ## Set-transition recasting
//!
//! The filter is evaluated against **both** old and new, so a watch knows
//! whether the document was in its filtered set *before* and *after* the write,
//! and re-casts the change against its own boundary:
//!
//! - `!before && after` → [`ChangeEvent::Insert`] (entered the set)
//! - `before && !after` → [`ChangeEvent::Delete`] (left the set)
//! - `before && after`  → [`ChangeEvent::Update`] (modified, still in set)
//! - `!before && !after` → not captured for this watch
//!
//! A watch is an **observer**: a filter that errors at evaluation time is
//! treated as a non-match rather than failing the user's write.

use std::cell::RefCell;
use std::sync::Arc;

use bson::RawDocumentBuf;
use bson::raw::{CString, RawBsonRef, RawDocument};
use slate_ast::Expression;
use slate_eval::EvalError;
use slate_eval::raweval::{self, RowEnv};

use crate::ExecError;

// Re-exported so the db layer can name the compiled-filter type (e.g. for an
// `Arc<Compiled>` registry field) without taking a direct dependency on
// `slate-eval`.
pub use slate_eval::raweval::Compiled;

/// Compile a watch's `WHERE` expression against its `FROM` alias into a
/// reusable predicate, shared (`Arc`) so the registry and the per-transaction
/// sink point at the same compiled tree.
///
/// Compiling with the alias as the *sole* binding collapses `<alias>.field`
/// member reads to direct row-field reads — the same fast form the `Filter`
/// node uses.
pub fn compile_filter(expr: &Expression, alias: &str) -> Arc<Compiled> {
    // Watch filters compile without a UDF bag — `udf.*` in a watch predicate is
    // out of scope for now (no bag is threaded through the registry path).
    Arc::new(raweval::compile(
        expr,
        Some(alias),
        raweval::UdfCtx::default(),
    ))
}

/// A change to a document, recast against a single watch's filtered set
/// boundary. The captured documents are **owned** so the events outlive the
/// transaction they were detected in.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    /// A document entered the watched set (a fresh insert, or an update that
    /// brought a previously-non-matching document into the filter).
    Insert { doc: RawDocumentBuf },
    /// A document was modified while staying in the watched set; both the prior
    /// and current states are provided so consumers can diff without re-reading.
    Update {
        old: RawDocumentBuf,
        new: RawDocumentBuf,
    },
    /// A document left the watched set (a delete, or an update that moved a
    /// previously-matching document out of the filter).
    Delete { doc: RawDocumentBuf },
}

/// One watch's compiled filter, ready for per-row evaluation on the write path.
///
/// `handle_id` ties a capture back to the registry's callback; `alias` is the
/// `FROM` alias the SQL filter was compiled against (the document binds to it);
/// `filter` is shared (`Arc`) with the registry snapshot so the compiled tree is
/// never duplicated.
pub struct CompiledWatch {
    pub handle_id: u64,
    pub alias: String,
    pub filter: Arc<Compiled>,
}

/// A buffered capture awaiting commit: which watch matched, the document's
/// primary-key bytes (for commit-time coalescing), and the recast event.
pub struct CapturedEvent {
    pub handle_id: u64,
    pub pk: Vec<u8>,
    pub event: ChangeEvent,
}

/// The watches active for one transaction, grouped by `(cf, collection)`, plus
/// the per-transaction capture buffer.
///
/// Threaded into the [`Executor`](crate::Executor) like `params`/`rand` and
/// shared (by `Rc`) with the owning db transaction, which drains
/// [`take`](Self::take) at commit time. `!Sync` (it holds a `RefCell`), matching
/// the single-threaded executor.
pub struct WatchSink {
    targets: Vec<WatchTarget>,
    buffer: RefCell<Vec<CapturedEvent>>,
}

struct WatchTarget {
    cf: String,
    collection: String,
    watches: Vec<CompiledWatch>,
}

impl WatchSink {
    /// Build a sink from per-collection watch groups. Each tuple is
    /// `(cf, collection, watches)`.
    pub fn new(targets: Vec<(String, String, Vec<CompiledWatch>)>) -> Self {
        Self {
            targets: targets
                .into_iter()
                .map(|(cf, collection, watches)| WatchTarget {
                    cf,
                    collection,
                    watches,
                })
                .collect(),
            buffer: RefCell::new(Vec::new()),
        }
    }

    /// The watches registered for a `(cf, collection)`, if any.
    fn target(&self, cf: &str, collection: &str) -> Option<&WatchTarget> {
        self.targets
            .iter()
            .find(|t| t.cf == cf && t.collection == collection)
    }

    /// Evaluate every watch on this collection against the document's before
    /// (`old`) and after (`new`) states and buffer the recast change.
    ///
    /// `old`/`new` follow the mutation-node availability: insert passes
    /// `(None, Some)`, delete `(Some, None)`, update/replace/upsert-update
    /// `(Some, Some)`. A watch whose filter matches neither state captures
    /// nothing. Filter-evaluation errors are swallowed (an observer never breaks
    /// the write); a missing primary key is the one hard error (a written
    /// document always has one).
    pub fn capture(
        &self,
        cf: &str,
        collection: &str,
        pk_path: &str,
        old: Option<&RawDocumentBuf>,
        new: Option<&RawDocumentBuf>,
    ) -> Result<(), ExecError> {
        let Some(target) = self.target(cf, collection) else {
            return Ok(());
        };
        if target.watches.is_empty() {
            return Ok(());
        }

        // The document identity for coalescing comes from whichever state is
        // present (they share a primary key for any normal write).
        let Some(pk_doc) = new.or(old) else {
            return Ok(());
        };
        let pk = pk_bytes(pk_doc, pk_path)?;

        for watch in &target.watches {
            let before = old
                .map(|d| matches(&watch.filter, &watch.alias, d))
                .unwrap_or(false);
            let after = new
                .map(|d| matches(&watch.filter, &watch.alias, d))
                .unwrap_or(false);

            let event = match (before, after) {
                (false, true) => ChangeEvent::Insert { doc: owned(new)? },
                (true, true) => ChangeEvent::Update {
                    old: owned(old)?,
                    new: owned(new)?,
                },
                (true, false) => ChangeEvent::Delete { doc: owned(old)? },
                // Matched neither boundary — nothing to deliver to this watch.
                (false, false) => continue,
            };

            self.buffer.borrow_mut().push(CapturedEvent {
                handle_id: watch.handle_id,
                // Each captured event keys on the same pk; cloning the small key
                // bytes per matching watch is cheaper than threading a shared
                // handle through the buffer.
                pk: pk.clone(),
                event,
            });
        }
        Ok(())
    }

    /// Whether anything has been buffered (used to skip emit on an empty commit).
    pub fn is_empty(&self) -> bool {
        self.buffer.borrow().is_empty()
    }

    /// Drain the buffer, leaving it empty. Called once at commit.
    pub fn take(&self) -> Vec<CapturedEvent> {
        std::mem::take(&mut self.buffer.borrow_mut())
    }
}

/// Evaluate a compiled filter against a document bound to `alias`.
///
/// Mirrors the `Filter` node's per-row contract (3-valued: only `Some(true)`
/// matches). An evaluation error is reported as a non-match, so a watch can
/// never break the write it observes.
fn matches(filter: &Compiled, alias: &str, doc: &RawDocumentBuf) -> bool {
    let doc: &RawDocument = doc;
    let binds = [(alias, RawBsonRef::Document(doc))];
    let env = RowEnv::new(&binds, None);
    match raweval::eval_compiled(filter, &env) {
        Ok(value) => value.as_bool() == Some(true),
        Err(_) => false,
    }
}

/// Own a borrowed document, or error if it was absent (an invariant break — the
/// caller only reaches here when the corresponding boundary matched, which
/// requires the document to be present).
fn owned(doc: Option<&RawDocumentBuf>) -> Result<RawDocumentBuf, ExecError> {
    doc.map(|d| d.to_owned()).ok_or_else(|| {
        ExecError::Eval(EvalError {
            message: "watch capture: matched boundary with no document".into(),
        })
    })
}

/// The document's primary key serialized to comparable bytes (a single-field
/// `{ k: <pk> }` document), used to coalesce repeated changes to the same
/// document within one commit.
fn pk_bytes(doc: &RawDocumentBuf, pk_path: &str) -> Result<Vec<u8>, ExecError> {
    let id = doc
        .get(pk_path)
        .map_err(|e| {
            ExecError::Eval(EvalError {
                message: format!("watch capture: malformed document: {e}"),
            })
        })?
        .ok_or_else(|| {
            ExecError::Eval(EvalError {
                message: "watch capture: document has no primary key".into(),
            })
        })?;
    // A single-field key wrapper; "k" has no interior NUL so the `CString`
    // conversion is infallible, but it is fallible in general.
    let key = CString::try_from("k").map_err(|e| {
        ExecError::Eval(EvalError {
            message: format!("watch capture: invalid key: {e}"),
        })
    })?;
    let mut buf = RawDocumentBuf::new();
    buf.append(key, id);
    Ok(buf.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Bson, rawdoc};
    use slate_ast::{BinOp, Expression};

    /// `c.temp > 80`, compiled against alias `c`.
    fn temp_gt_80() -> Arc<Compiled> {
        let expr = Expression::Binary {
            op: BinOp::Gt,
            lhs: Box::new(Expression::Member {
                base: Box::new(Expression::Identifier("c".into())),
                field: "temp".into(),
            }),
            rhs: Box::new(Expression::Value(Bson::Int32(80))),
        };
        compile_filter(&expr, "c")
    }

    fn sink() -> WatchSink {
        WatchSink::new(vec![(
            "cf".into(),
            "coll".into(),
            vec![CompiledWatch {
                handle_id: 7,
                alias: "c".into(),
                filter: temp_gt_80(),
            }],
        )])
    }

    #[test]
    fn insert_matching_is_captured_as_insert() {
        let s = sink();
        let new = rawdoc! { "_id": "1", "temp": 90 };
        s.capture("cf", "coll", "_id", None, Some(&new)).unwrap();
        let captured = s.take();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].handle_id, 7);
        assert!(matches!(captured[0].event, ChangeEvent::Insert { .. }));
    }

    #[test]
    fn insert_non_matching_is_dropped() {
        let s = sink();
        let new = rawdoc! { "_id": "1", "temp": 50 };
        s.capture("cf", "coll", "_id", None, Some(&new)).unwrap();
        assert!(s.take().is_empty());
    }

    #[test]
    fn update_entering_set_recasts_to_insert() {
        let s = sink();
        let old = rawdoc! { "_id": "1", "temp": 50 }; // not in set
        let new = rawdoc! { "_id": "1", "temp": 90 }; // in set
        s.capture("cf", "coll", "_id", Some(&old), Some(&new))
            .unwrap();
        let captured = s.take();
        assert_eq!(captured.len(), 1);
        assert!(matches!(captured[0].event, ChangeEvent::Insert { .. }));
    }

    #[test]
    fn update_leaving_set_recasts_to_delete() {
        let s = sink();
        let old = rawdoc! { "_id": "1", "temp": 90 };
        let new = rawdoc! { "_id": "1", "temp": 50 };
        s.capture("cf", "coll", "_id", Some(&old), Some(&new))
            .unwrap();
        match &s.take()[0].event {
            ChangeEvent::Delete { doc } => assert_eq!(doc.get_i32("temp").unwrap(), 90),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn update_staying_in_set_carries_old_and_new() {
        let s = sink();
        let old = rawdoc! { "_id": "1", "temp": 85 };
        let new = rawdoc! { "_id": "1", "temp": 95 };
        s.capture("cf", "coll", "_id", Some(&old), Some(&new))
            .unwrap();
        match &s.take()[0].event {
            ChangeEvent::Update { old, new } => {
                assert_eq!(old.get_i32("temp").unwrap(), 85);
                assert_eq!(new.get_i32("temp").unwrap(), 95);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn update_outside_set_on_both_sides_is_dropped() {
        let s = sink();
        let old = rawdoc! { "_id": "1", "temp": 10 };
        let new = rawdoc! { "_id": "1", "temp": 20 };
        s.capture("cf", "coll", "_id", Some(&old), Some(&new))
            .unwrap();
        assert!(s.take().is_empty());
    }

    #[test]
    fn delete_in_set_is_delete() {
        let s = sink();
        let old = rawdoc! { "_id": "1", "temp": 90 };
        s.capture("cf", "coll", "_id", Some(&old), None).unwrap();
        assert!(matches!(s.take()[0].event, ChangeEvent::Delete { .. }));
    }

    #[test]
    fn capture_on_unwatched_collection_is_noop() {
        let s = sink();
        let new = rawdoc! { "_id": "1", "temp": 90 };
        s.capture("cf", "other", "_id", None, Some(&new)).unwrap();
        assert!(s.take().is_empty());
    }
}
