//! `IS_FINITE_NUMBER(expr)` — whether the value is a finite number (not
//! infinity or NaN). Integers are always finite. (Type-test → boolean.)

use bson::Bson;

use crate::error::Result;
use crate::eval::decimal_to_f64;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let finite = match &args[0] {
        Value::Defined(Bson::Int32(_) | Bson::Int64(_)) => true,
        Value::Defined(Bson::Double(f)) => f.is_finite(),
        // A `Decimal128` is a number (see `is_number`); it's finite iff its f64
        // value is. `decimal_to_f64` parses `NaN`/`Infinity` to their f64 forms,
        // so the finiteness check is faithful; `None` (a non-parsing decimal) is
        // treated as not-finite, matching the `false` arm below.
        Value::Defined(Bson::Decimal128(d)) => decimal_to_f64(d).is_some_and(f64::is_finite),
        _ => false,
    };
    Ok(Value::Defined(Bson::Boolean(finite)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    #[test]
    fn decimal_finiteness() {
        // A finite decimal is finite; the non-finite decimals are numbers but
        // not finite (`IS_NUMBER` returns true for them — this does not).
        let dec = |s: &str| Value::Defined(Bson::Decimal128(s.parse().unwrap()));
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![dec("1234.567")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![dec("Infinity")]).unwrap(),
            def(false)
        );
        assert_eq!(
            call("IS_FINITE_NUMBER", vec![dec("NaN")]).unwrap(),
            def(false)
        );
    }

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
