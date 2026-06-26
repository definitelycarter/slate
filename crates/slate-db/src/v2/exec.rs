//! Shared v2 execution helper: `EXPLAIN ANALYZE`.
//!
//! `find` and `query` both render a plan (`.explain`) and run-and-measure it
//! (`.analyze`). Plain explain is just [`Plan::explain`], but analyze must
//! execute the plan to capture per-node actuals, so it sets up an `Executor`
//! exactly as the cursor does (`$now` injection + rand + pool) and calls
//! `execute_analyze`. This lives in v2 (not borrowed from v1) so the surface
//! stays self-contained; it shares only the engine transaction and the lower
//! crates.

use slate_store::Store;

use crate::RawDocumentBuf;
use crate::database::Transaction;
use crate::error::DbError;

/// Run `plan` under EXPLAIN ANALYZE and render the annotated operator tree.
///
/// `params` is the query's bound parameters (`None` for `find`); they are merged
/// with the injected `$now` exactly as the cursor does. The result rows are run
/// and dropped — only the per-node statistics are kept.
pub(super) fn analyze_plan<S: Store>(
    plan: slate_planner::Plan,
    params: Option<RawDocumentBuf>,
    txn: &Transaction<'_, S>,
) -> Result<String, DbError> {
    let mut doc: bson::Document = match &params {
        Some(p) => bson::deserialize_from_slice(p.as_bytes())?,
        None => bson::Document::new(),
    };
    doc.insert("$now", txn.now_millis());
    let params = Some(std::rc::Rc::new(bson::serialize_to_raw_document_buf(&doc)?));

    // Clone to render after execution consumes the plan — a one-off on the
    // analyze (debug/observability) path, well outside any hot loop.
    let render_plan = plan.clone();
    let (_rows, stats) =
        slate_executor::Executor::with_pool_and_params(txn.engine_txn(), txn.pool(), params)
            .with_rand(txn.exec_rand())
            .execute_analyze(plan)?;
    Ok(render_plan.explain_analyze(&stats))
}
