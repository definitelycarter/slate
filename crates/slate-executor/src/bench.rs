//! Bench-only access to the per-node executors (feature `bench-internals`).
//!
//! Each node's `execute(...)` is `pub(crate)`, and the `Params`/`env` helpers
//! are crate-private, so an external Criterion target (`benches/nodes.rs`)
//! cannot drive a single node in isolation. This module is compiled only under
//! the `bench-internals` feature and lives *inside* the crate, so it can call
//! those `pub(crate)` functions. It re-exports thin `pub fn` wrappers over each,
//! plus the already-`pub` [`ValueIter`]/[`ExecError`]/[`collect`].
//!
//! Each transform wrapper defaults the optional query-`@`-parameter document to
//! `None` (the node benches never bind `@`-params); the bench builds every other
//! input (`OrderByItem`, `GroupKey`, `IndexScanRange`, …) directly from the
//! `pub` planner/ast types and passes it in.
//!
//! The `seeded_engine` fixture (ported from `nodes::test_support::seeded_people`
//! and `slate-db/benches/common::seeded_engine`) builds a `KvEngine<MemoryStore>`
//! for the storage-backed source nodes. Because dev-dependencies aren't visible
//! to library code, `slate-store` is pulled in as an optional normal dependency
//! gated by `bench-internals` (see this crate's `Cargo.toml`).

use bson::{RawBson, rawdoc};
use slate_ast::{Expression, OrderByItem};
use slate_engine::{Catalog, DEFAULT_CF, Engine, EngineTransaction, KvEngine};
use slate_planner::{
    AggregateExpr, CollectionRef, GroupKey, IndexScanRange, LogicalOp, RowBinding, ScanDirection,
};
use slate_store::MemoryStore;

use crate::nodes;

pub use crate::{ExecError, ValueIter, collect};

// ── Fixtures ────────────────────────────────────────────────────────────────

/// The `(cf, name)` of the [`seeded_engine`] collection.
pub fn collection_ref() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: "people".into(),
    }
}

/// A `KvEngine` with a `people` collection holding `n` documents, indexed on
/// `age` (numeric, 100 distinct values: `i % 100`) and `status` (low-cardinality
/// string). The shape mirrors `slate-db/benches/common::seeded_engine`; the
/// inserts go through the engine `put` API (no `slate-db` layer here).
pub fn seeded_engine(n: usize) -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    {
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, "people", &Default::default())
            .unwrap();
        txn.create_index(DEFAULT_CF, "people", "age").unwrap();
        txn.create_index(DEFAULT_CF, "people", "status").unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = engine.begin(false).unwrap();
        let handle = txn.collection(DEFAULT_CF, "people").unwrap();
        for i in 0..n {
            let doc = rawdoc! {
                "_id": format!("rec-{i}"),
                "name": format!("User {i}"),
                "status": if i % 2 == 0 { "active" } else { "rejected" },
                "age": (i % 100) as i32,
                "score": (i % 1000) as i32,
            };
            txn.put(&handle, &doc).unwrap();
        }
        txn.commit().unwrap();
    }
    engine
}

// ── Source nodes ────────────────────────────────────────────────────────────

/// `Values` — stream caller-provided raw values in order.
pub fn values(values: Vec<RawBson>) -> ValueIter<'static> {
    nodes::values::execute(values)
}

/// `Scan` — full-collection scan over the transaction.
pub fn scan<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::scan::execute(txn, collection)
}

/// `IndexScan` — yields bare doc-IDs from a field index.
pub fn index_scan<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::index_scan::execute(txn, collection, field, range, direction, limit)
}

// ── Transform nodes ─────────────────────────────────────────────────────────

/// `Bind` — wrap each source value as `{alias: value}`.
pub fn bind<'a>(alias: String, source: ValueIter<'a>) -> ValueIter<'a> {
    nodes::bind::execute(alias, source)
}

/// `Filter` — keep only rows where `predicate` evaluates to `true`.
pub fn filter<'a>(
    predicate: Expression,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> ValueIter<'a> {
    nodes::filter::execute(predicate, binding, source, None)
}

/// `Project` — evaluate `expr` against each row environment.
pub fn project<'a>(expr: Expression, binding: RowBinding, source: ValueIter<'a>) -> ValueIter<'a> {
    nodes::project::execute(expr, binding, source, None)
}

/// `Sort` — blocking buffer-and-sort by `keys`.
pub fn sort<'a>(
    keys: Vec<OrderByItem>,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::sort::execute(keys, binding, source, None)
}

/// `Limit` — skip `skip` rows, then take at most `take`.
pub fn limit<'a>(skip: usize, take: Option<usize>, source: ValueIter<'a>) -> ValueIter<'a> {
    nodes::limit::execute(skip, take, source)
}

/// `Distinct` — emit each distinct value once (one level of array flatten when set).
pub fn distinct<'a>(source: ValueIter<'a>, flatten: bool) -> ValueIter<'a> {
    nodes::distinct::execute(source, flatten)
}

/// `Unwind` — emit one row per element of `array`, extending the environment.
pub fn unwind<'a>(alias: String, array: Expression, source: ValueIter<'a>) -> ValueIter<'a> {
    nodes::unwind::execute(alias, array, source, None)
}

/// `KeyLookup` — point-read the full document for each incoming ID (or doc pk).
pub fn key_lookup<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::key_lookup::execute(txn, collection, source)
}

/// `IndexMerge` — union (`Or`) / intersect (`And`) two ID streams.
pub fn index_merge<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    logical: LogicalOp,
    left: ValueIter<'a>,
    right: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::index_merge::execute(txn, collection, logical, left, right)
}

/// `Aggregate` — group by `group_keys`, fold `aggregates`, emit one environment
/// row per group.
pub fn aggregate<'a>(
    group_keys: Vec<GroupKey>,
    aggregates: Vec<AggregateExpr>,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::aggregate::execute(group_keys, aggregates, binding, source, None)
}
