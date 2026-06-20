//! `slate-eval` — evaluation semantics for the shared [`slate_ast`] query AST.
//!
//! This is the *meaning* of a [`slate_ast::ScalarExpr`], split out from any
//! particular query surface so the storage executor and the in-memory engines
//! can share it without depending on a parser. There are two evaluators that
//! must agree:
//!
//! - [`eval`] — over owned `bson::Bson`. Simple and obviously correct.
//! - [`raweval`] — the storage-path twin, evaluating over raw BSON bytes
//!   zero-copy.
//!
//! Every leaf rule (comparison, numeric coercion, arithmetic, three-valued
//! logic, function dispatch) has a single definition the two share, so
//! `WHERE`/`ORDER BY` cannot drift between them. The [`value::Value`] domain
//! carries Cosmos's `undefined` vs `null` distinction.

pub mod error;
pub mod eval;
pub mod functions;
pub mod raweval;
pub mod value;

pub use error::{EvalError, Result};
pub use eval::compare_bson;
pub use value::Value;
