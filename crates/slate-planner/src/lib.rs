//! `slate-planner` — the **v2** query plan IR and logical planning.
//!
//! This is the *logical* half of a clean-slate rebuild that runs alongside
//! (and does not touch) the v1 planner/executor in `slate-db`. It owns the
//! [`Plan`]/[`Node`] IR and — as it grows — the lowering from the query API
//! (`slate_ast::Query`, a find-style request) into a `Plan`. Physical
//! execution lives in the separate `slate-executor` crate; this crate makes
//! *decisions* (sargability, index choice, join order), not row streaming.
//!
//! ## The unified row model (why one IR serves find and SQL)
//!
//! A value flows through execution, and a "document" is just the case where
//! that value happens to be a document. That lets one executor — and crucially
//! one `WHERE`/expression evaluator — serve both the traditional `find` query
//! and CosmosDB-style `SELECT VALUE`, so the two can never drift. `find` is the
//! degenerate case: a single binding `c`, an identity projection, no unwind.
//!
//! The streamed unit (defined by `slate-executor`) is `Option<RawBson>`,
//! matching v1's `RawIter` shape: `Some` is a value, `None` is *undefined* and
//! is dropped at the output boundary. Keeping raw `RawBson` rather than owned
//! `Bson` keeps the `find` path zero-copy.
//!
//! ## Status
//!
//! Intentionally minimal to start: a [`Node::Values`] literal source. The IR
//! grows from here — `Scan`, `Filter { predicate: ScalarExpr }`,
//! `Project { expr: ScalarExpr }`, `Unwind { alias, array }`, `Sort`, `Limit`,
//! `Distinct` — with expression types and the evaluator coming from `slate-sql`
//! so there is exactly one of each across find and SQL.

pub mod lower;
pub mod plan;
pub mod planner;
pub mod sargable;
pub mod validate;

pub use lower::lower;
pub use plan::{
    AggregateExpr, CollectionRef, GroupKey, IndexScanRange, LogicalOp, Node, Plan, RowBinding,
    ScanDirection, UpsertMode,
};
pub use planner::{PlanContext, plan};
pub use sargable::CollectionMeta;
pub use slate_ast::Statement;
pub use validate::{PlanError, validate_bindings, validate_grouping};
