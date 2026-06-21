//! `inc(current, delta)` — numeric increment preserving the field's type (Mongo
//! `$inc`), distinct from `+` which follows Cosmos's widen-to-double model.
//!
//! Promotion: i32+i32 → i32 (overflow → i64); i32/i64 mixed → i64; any + double
//! → double. A missing (undefined) `current` starts from `0` of `delta`'s type.
//! A non-numeric operand yields `Undefined` (the evaluator's type-mismatch rule).

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity_err;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    let [current, delta] = match <[Value; 2]>::try_from(args) {
        Ok(a) => a,
        Err(_) => return Err(arity_err(name, "exactly 2")),
    };
    let delta = match delta {
        Value::Defined(b @ (Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))) => b,
        _ => return Ok(Value::Undefined),
    };
    let current = match current {
        Value::Defined(b) => b,
        // Missing field: start from zero of the increment's type.
        Value::Undefined => match delta {
            Bson::Int64(_) => Bson::Int64(0),
            Bson::Double(_) => Bson::Double(0.0),
            _ => Bson::Int32(0),
        },
    };
    let result = match (&current, &delta) {
        (Bson::Int32(a), Bson::Int32(b)) => match a.checked_add(*b) {
            Some(sum) => Bson::Int32(sum),
            None => Bson::Int64(*a as i64 + *b as i64),
        },
        (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 + b),
        (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(a + *b as i64),
        (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(a + b),
        (Bson::Double(a), Bson::Double(b)) => Bson::Double(a + b),
        (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
        (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
        (Bson::Double(a), Bson::Int32(b)) => Bson::Double(a + *b as f64),
        (Bson::Double(a), Bson::Int64(b)) => Bson::Double(a + *b as f64),
        _ => return Ok(Value::Undefined),
    };
    Ok(Value::Defined(result))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    #[test]
    fn i32_stays_i32() {
        assert_eq!(call("INC", vec![def(6), def(1)]).unwrap(), def(7));
    }

    #[test]
    fn missing_starts_from_zero() {
        assert_eq!(call("INC", vec![Value::Undefined, def(7)]).unwrap(), def(7));
    }

    #[test]
    fn double_widens() {
        assert_eq!(
            call("INC", vec![def(1.5), def(1)]).unwrap(),
            def(Bson::Double(2.5))
        );
    }

    #[test]
    fn overflow_promotes_to_i64() {
        assert_eq!(
            call("INC", vec![def(i32::MAX), def(1)]).unwrap(),
            def(Bson::Int64(i32::MAX as i64 + 1))
        );
    }
}
