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
    AggregateExpr, CollectionRef, GroupKey, IndexIntersectPart, IndexScanRange, LogicalOp,
    RowBinding, ScanDirection,
};
use slate_store::MemoryStore;

use crate::budget::Ticker;
use crate::nodes;

pub use crate::{ExecEnv, ExecError, ValueIter, collect};

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

/// The `(cf, name)` of the [`intersect_engine`] collection.
pub fn intersect_collection_ref() -> CollectionRef {
    CollectionRef {
        cf: DEFAULT_CF.into(),
        collection: "items".into(),
    }
}

/// A `KvEngine` with an `items` collection of `n` documents, indexed on three
/// string fields engineered for the index-intersection benchmark — each doc
/// carries `"y"`/`"n"`:
///
/// - `big` = `"y"` on even `i` (≈ n/2 — the *large* side).
/// - `sel` = `"y"` on `i % 50 == 0` (≈ n/50, a subset of `big` — the *selective*
///   side). `big ∩ sel` is the **skew** case, bounded by `sel`.
/// - `tri` = `"y"` on `i % 3 == 0` (≈ n/3). `big ∩ tri` (= `i % 6 == 0`) is the
///   **balanced** case — two similar-size streams heavily interleaved in doc-id
///   space (galloping's worst case for seeks).
pub fn intersect_engine(n: usize) -> KvEngine<MemoryStore> {
    let engine = KvEngine::new(MemoryStore::new());
    {
        let txn = engine.begin(false).unwrap();
        txn.create_collection(DEFAULT_CF, "items", &Default::default())
            .unwrap();
        for field in ["big", "sel", "tri"] {
            txn.create_index(DEFAULT_CF, "items", field).unwrap();
        }
        txn.commit().unwrap();
    }
    {
        let txn = engine.begin(false).unwrap();
        let handle = txn.collection(DEFAULT_CF, "items").unwrap();
        let yn = |cond: bool| if cond { "y" } else { "n" };
        for i in 0..n {
            let doc = rawdoc! {
                "_id": format!("rec-{i}"),
                "big": yn(i % 2 == 0),
                "sel": yn(i % 50 == 0),
                "tri": yn(i % 3 == 0),
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

/// `Scan` — full-collection scan over the transaction. Benches drive the
/// no-deadline path (`Ticker::new(None)`), the common case.
pub fn scan<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::scan::execute(txn, collection, Ticker::new(None))
}

/// `IndexScan` — yields bare doc-IDs from a field index (non-covering; the
/// covering variant is exercised end-to-end by the `query` bench).
pub fn index_scan<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    field: String,
    range: &IndexScanRange,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::index_scan::execute(
        txn,
        collection,
        field,
        range,
        direction,
        limit,
        false,
        Ticker::new(None),
    )
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
    nodes::filter::execute(predicate, binding, source, ExecEnv::new())
}

/// `Project` — evaluate `expr` against each row environment.
pub fn project<'a>(expr: Expression, binding: RowBinding, source: ValueIter<'a>) -> ValueIter<'a> {
    nodes::project::execute(expr, binding, source, ExecEnv::new())
}

/// `Sort` — blocking buffer-and-sort by `keys`.
pub fn sort<'a>(
    keys: Vec<OrderByItem>,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::sort::execute(keys, binding, source, ExecEnv::new())
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
    nodes::unwind::execute(alias, array, source, ExecEnv::new())
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

/// `IndexIntersect` — the galloping all-equality skip-merge over `parts`.
pub fn index_intersect<'a, T: EngineTransaction + Catalog>(
    txn: &'a T,
    collection: &CollectionRef,
    parts: &[IndexIntersectPart],
) -> Result<ValueIter<'a>, ExecError> {
    nodes::index_intersect::execute(txn, collection, parts, Ticker::new(None))
}

/// `Aggregate` — group by `group_keys`, fold `aggregates`, emit one environment
/// row per group.
pub fn aggregate<'a>(
    group_keys: Vec<GroupKey>,
    aggregates: Vec<AggregateExpr>,
    binding: RowBinding,
    source: ValueIter<'a>,
) -> Result<ValueIter<'a>, ExecError> {
    nodes::aggregate::execute(group_keys, aggregates, binding, source, ExecEnv::new())
}
