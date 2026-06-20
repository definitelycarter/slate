//! `IS_INTEGER(expr)` — whether the value represents a signed 64-bit integer.
//!
//! This is a **value + range** test (matching Cosmos), not a type-identity one:
//! `Int32`/`Int64` always qualify, and a `Double` qualifies iff it is finite,
//! has no fractional part, and lies within the `i64` range (so `5.0` → true,
//! `5.5` → false, and a magnitude beyond i64 → false). Comparison still coerces
//! numerics by value; this just reports whether the value is an integer.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let is_int = match &args[0] {
        Value::Defined(Bson::Int32(_) | Bson::Int64(_)) => true,
        // `i64::MAX as f64` rounds up to 2^63 (= i64::MAX + 1), so the upper
        // bound is exclusive; `i64::MIN as f64` is exactly -2^63.
        Value::Defined(Bson::Double(f)) => {
            f.is_finite() && f.fract() == 0.0 && *f >= i64::MIN as f64 && *f < i64::MAX as f64
        }
        _ => false,
    };
    Ok(Value::Defined(Bson::Boolean(is_int)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/is-integer
        assert_eq!(
            call("IS_INTEGER", vec![def(3454.123_f64)]).unwrap(),
            def(false)
        );
        assert_eq!(call("IS_INTEGER", vec![def(5523432)]).unwrap(), def(true));
        assert_eq!(call("IS_INTEGER", vec![def(i64::MIN)]).unwrap(), def(true)); // -9223372036854775808
        assert_eq!(call("IS_INTEGER", vec![def(i64::MAX)]).unwrap(), def(true)); //  9223372036854775807
        // 18446744073709551615 (= 2^64 - 1) is beyond i64 range.
        assert_eq!(
            call("IS_INTEGER", vec![def(1.8446744e19_f64)]).unwrap(),
            def(false)
        );

        // Extra: a whole Double in range is an integer value; "5" is not a number.
        assert_eq!(call("IS_INTEGER", vec![def(5.0_f64)]).unwrap(), def(true));
        assert_eq!(call("IS_INTEGER", vec![def("5")]).unwrap(), def(false));
    }
}
