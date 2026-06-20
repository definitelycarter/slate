//! `IS_FINITE_NUMBER(expr)` — whether the value is a finite number (not
//! infinity or NaN). Integers are always finite. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let finite = match &args[0] {
        Value::Defined(Bson::Int32(_) | Bson::Int64(_)) => true,
        Value::Defined(Bson::Double(f)) => f.is_finite(),
        _ => false,
    };
    Ok(Value::Defined(Bson::Boolean(finite)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-finite-number
        // 1234.567 -> finite; 8.9/0.0 -> +inf; SQRT(-1.0) -> NaN.
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![def(1234.567_f64)]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![def(8.9_f64 / 0.0)]).unwrap(),
            def(false)
        );
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![def((-1.0_f64).sqrt())]).unwrap(),
            def(false)
        );
    }
}
