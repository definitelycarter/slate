//! `IIF(cond, true_expr, false_expr)` — return `true_expr` when `cond` is the
//! boolean `true`, otherwise `false_expr`.
//!
//! Only the boolean `true` takes the true branch; any non-boolean condition
//! (a number, string, array, object) — or `false`, `null`, or undefined — takes
//! the false branch, matching Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 3)?;
    let cond_true = matches!(args[0], Value::Defined(Bson::Boolean(true)));
    let mut it = args.into_iter();
    it.next(); // condition
    let true_expr = it.next().unwrap_or(Value::Undefined);
    let false_expr = it.next().unwrap_or(Value::Undefined);
    Ok(if cond_true { true_expr } else { false_expr })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/iif
        assert_eq!(
            call("IIF", vec![def(true), def(123), def(456)]).unwrap(),
            def(123)
        );
        assert_eq!(
            call("IIF", vec![def(false), def(123), def(456)]).unwrap(),
            def(456)
        );
        // Non-boolean conditions all take the false branch.
        assert_eq!(
            call("IIF", vec![def(123), def(123), def(456)]).unwrap(),
            def(456)
        );
        assert_eq!(
            call("IIF", vec![def("ABC"), def(123), def(456)]).unwrap(),
            def(456)
        );
        assert_eq!(
            call(
                "IIF",
                vec![def(Bson::Array(vec![Bson::Int32(1)])), def(123), def(456)]
            )
            .unwrap(),
            def(456)
        );
        let obj = Bson::Document(bson::doc! { "name": "Alice" });
        assert_eq!(
            call("IIF", vec![def(obj), def(123), def(456)]).unwrap(),
            def(456)
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("IIF", vec![def(true), def(1)]).is_err());
    }
}
