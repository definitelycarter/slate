//! Shared v2 execution helpers: `EXPLAIN ANALYZE` plus the write plumbing
//! (write `PlanContext` assembly, write-query lowering, and execute-and-count).
//!
//! `find` and `query` both render a plan (`.explain`) and run-and-measure it
//! (`.analyze`). Plain explain is just [`Plan::explain`], but analyze must
//! execute the plan to capture per-node actuals, so it sets up an `Executor`
//! exactly as the cursor does (`$now` injection + rand + pool) and calls
//! `execute_analyze`. The write side ([`super::write`]) lowers each mutation to
//! a plan and drains it for the affected count here. All of this lives in v2
//! (not borrowed from v1) so the surface stays self-contained; it shares only
//! the engine transaction, the catalog reads (`collection_meta` / `validators`
//! / `triggers`), and the lower crates.

use slate_store::Store;

use crate::cursor::Cursor;
use crate::database::Transaction;
use crate::error::DbError;
use crate::{FindOptions, RawDocumentBuf};

/// Run `plan` under EXPLAIN ANALYZE and render the annotated operator tree.
///
/// `params` is the query's bound parameters (`None` for `find`); they are merged
/// with the injected `$now` exactly as the cursor does. The result rows are run
/// and dropped — only the per-node statistics are kept.
pub(crate) fn analyze_plan<S: Store>(
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

/// Assemble the catalog context a write plans against: the container plus the
/// collection's validators and triggers (so the planner wraps the mutation in
/// the same hook nodes v1 does). `meta` is the caller's choice — filter-bearing
/// writes pass real index metadata so the matched-document source can use an
/// index; insert/upsert, which scan nothing, pass `CollectionMeta::default()`.
pub(super) fn write_context<S: Store>(
    txn: &Transaction<'_, S>,
    cf: &str,
    collection: &str,
    meta: slate_planner::CollectionMeta,
) -> slate_planner::PlanContext {
    slate_planner::PlanContext {
        container: slate_planner::CollectionRef {
            cf: cf.to_string(),
            collection: collection.to_string(),
        },
        meta,
        validators: txn.validators(cf, collection),
        triggers: txn.triggers(cf, collection),
    }
}

/// Lower a write's filter to the `Query` that selects the documents it targets.
/// This is the exact translation `find` uses (`take`-limited for the `.one()`
/// variants), so a write matches the same rows the equivalent `find` would; a
/// filter the front-end can't translate is a hard error.
pub(super) fn write_query(
    filter_raw: &RawDocumentBuf,
    take: Option<usize>,
) -> Result<slate_ast::Query, DbError> {
    let options = FindOptions {
        take,
        ..Default::default()
    };
    Ok(slate_query::find_to_query(filter_raw, &options)?)
}

/// Wrap a mutation plan in a [`Cursor`] — the cursor the flat `Transaction`
/// mutation methods return (the caller drains it for the count). `.cloned()`
/// rand/watch handles are `Arc`/`Rc` refcount bumps, the same handoff the v1
/// mutation path made.
pub(crate) fn write_cursor<'t, 'db, S>(
    plan: slate_planner::Plan,
    txn: &'t Transaction<'db, S>,
) -> Cursor<'db, 't, S>
where
    S: Store + 'db,
{
    Cursor::new(
        txn.engine_txn(),
        plan,
        txn.pool(),
        txn.rand().cloned(),
        txn.watch_sink().cloned(),
    )
}

/// Build a cursor over a mutation plan, drain it, and return the affected count —
/// the v2 write builders' `.execute` path. The mutation plan yields the written
/// documents; here only their count is kept.
pub(super) fn execute_write<'db, S>(
    plan: slate_planner::Plan,
    txn: &Transaction<'db, S>,
) -> Result<u64, DbError>
where
    S: Store + 'db,
{
    write_cursor(plan, txn).drain()
}
