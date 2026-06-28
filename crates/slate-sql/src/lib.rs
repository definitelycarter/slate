//! `slate-sql` — a CosmosDB-style SQL **front-end** for Slate.
//!
//! One of two query surfaces (the other being the Mongo-style `slate-query`).
//! Its job is narrow: lex and parse SQL text into the shared [`slate_ast`]
//! query AST. The AST's *meaning* lives in [`slate_eval`]; lowering it to a
//! physical plan lives in `slate-planner`. The in-memory [`exec`] engine here
//! is a convenience for iterating on language semantics over a `&[bson::Bson]`
//! source without touching storage.
//!
//! ## SQL surface
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
//! - [`parser`] — tokens → [`slate_ast::Query`] (recursive-descent + precedence).
//! - [`exec`] — the in-memory `SELECT VALUE` execution engine.
//! - [`agg`] — aggregate-function surface (planned; not yet wired into `exec`).
//!
//! The AST ([`slate_ast`]), the value domain and evaluators ([`slate_eval`]),
//! and the scalar functions ([`slate_eval::functions`]) live in their own
//! crates so the Mongo surface and the executor share them.
//!
//! ## Not yet supported (tracked for later milestones)
//!
//! - Tabular `SELECT a, b AS c` projections (only `SELECT VALUE` for now).
//! - Aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) and `GROUP BY` — see [`agg`].
//! - Cross-collection `JOIN` (a deliberate *extension* beyond Cosmos, which
//!   only supports intra-document array unwind).
//! - User-defined functions (`udf.*`) — resolved to native Rust functions
//!   (`slate-udf`) bound per collection.

pub mod agg;
pub mod error;
pub mod exec;
pub mod lexer;
pub mod parser;
pub mod token;

pub use error::{Result, SqlError};
pub use slate_ast::Query;
pub use slate_eval::Value;

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
