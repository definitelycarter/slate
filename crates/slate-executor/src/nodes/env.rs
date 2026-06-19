//! Row-environment helpers shared by the binding-aware nodes.
//!
//! A row flowing into `Filter`/`Project`/`Sort`/`Unwind` is an *environment
//! document*: a `RawBson::Document` whose top-level fields are the bound aliases
//! (`{c: <doc>, t: <elem>}`). These helpers decode such a row and evaluate
//! expressions against its bindings via the shared `slate-sql` evaluator.

use bson::{Bson, Document, RawBson};
use slate_sql::ast::ScalarExpr;
use slate_sql::eval::{Env, eval};
use slate_sql::{SqlError, Value};

use crate::ExecError;

/// Decode an environment-document row into an owned `Document` of bindings.
pub(crate) fn decode(row: &RawBson) -> Result<Document, ExecError> {
    match Bson::try_from(row.as_raw_bson_ref()) {
        Ok(Bson::Document(d)) => Ok(d),
        Ok(other) => Err(SqlError::Eval {
            message: format!(
                "expected an environment row, got {:?}",
                other.element_type()
            ),
        }
        .into()),
        Err(e) => Err(SqlError::Eval {
            message: format!("could not decode row: {e}"),
        }
        .into()),
    }
}

/// Evaluate `expr` against the bindings of an already-decoded environment.
pub(crate) fn eval_in(bindings: &Document, expr: &ScalarExpr) -> Result<Value, ExecError> {
    let params = Document::new();
    let binds: Vec<(&str, &Bson)> = bindings.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let env = Env::new(&binds, &params);
    eval(expr, &env).map_err(Into::into)
}
