//! `slate-sql` — a CosmosDB-style SQL frontend for Slate.
//!
//! ## v1 surface
//!
//! ```text
//! SELECT VALUE <expr>
//! FROM <alias>
//! [JOIN <alias> IN <array-expr>]*
//! [WHERE <expr>]
//! [ORDER BY <expr> [ASC|DESC] (, ...)]
//! [OFFSET <n>] [LIMIT <n>]
//! ```
//!
//! This v1 is deliberately decoupled from `slate-db`'s planner and storage: a
//! query is evaluated **in memory** over a `&[bson::Bson]` source via
//! [`exec::execute`]. That lets us iterate on the language semantics —
//! path/value expressions, scalar functions, the Cosmos `undefined` model, and
//! intra-document array-unwind `JOIN`s — without touching scans or indexes.
//!
//! ## Module layout
//!
//! - [`token`] — the token enum produced by the lexer.
//! - [`lexer`] — source text → `Vec<Token>`.
//! - [`ast`] — the query / scalar-expression syntax tree.
//! - [`parser`] — tokens → [`ast::Query`] (recursive-descent + precedence).
//! - [`value`] — the [`value::Value`] domain (`Defined(Bson)` vs `Undefined`).
//! - [`eval`] — scalar-expression evaluation over a binding environment.
//! - [`functions`] — built-in scalar function dispatch.
//! - [`exec`] — the in-memory `SELECT VALUE` execution engine.
//! - [`agg`] — aggregate-function surface (planned; not yet wired into `exec`).
//!
//! ## Not yet supported (tracked for later milestones)
//!
//! - Tabular `SELECT a, b AS c` projections (only `SELECT VALUE` for now).
//! - Aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) and `GROUP BY` — see [`agg`].
//! - Cross-collection `JOIN` (a deliberate *extension* beyond Cosmos, which
//!   only supports intra-document array unwind).
//! - User-defined functions — the runtime exists in `slate-vm`; wiring is future.
//! - Lowering an [`ast::Query`] onto a `slate-db` plan for the scan-backed path.

pub mod agg;
pub mod ast;
pub mod error;
pub mod eval;
pub mod exec;
pub mod functions;
pub mod lexer;
pub mod parser;
pub mod token;
pub mod value;

pub use ast::Query;
pub use error::{Result, SqlError};
pub use value::Value;

/// Parse SQL source into a [`Query`] AST.
pub fn parse(sql: &str) -> Result<Query> {
    let tokens = lexer::tokenize(sql)?;
    let mut parser = parser::Parser::new(tokens);
    parser.parse_query()
}

/// Parse and execute a query in memory against a slice of documents.
///
/// Convenience wrapper over [`parse`] + [`exec::execute`].
pub fn query(sql: &str, docs: &[bson::Bson]) -> Result<Vec<bson::Bson>> {
    let q = parse(sql)?;
    exec::execute(&q, docs)
}
