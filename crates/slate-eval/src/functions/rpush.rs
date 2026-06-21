//! `rpush(arr, value)` — append `value` to the end of `arr` (Mongo `$push`).
//!
//! A missing/undefined array becomes `[value]`; a non-array array argument
//! yields `Undefined`; an undefined `value` leaves the array unchanged.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity_err;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    let [arr, value] = match <[Value; 2]>::try_from(args) {
        Ok(a) => a,
        Err(_) => return Err(arity_err(name, "exactly 2")),
    };
    let elem = match value {
        Value::Defined(b) => b,
        Value::Undefined => return Ok(arr),
    };
    match arr {
        Value::Undefined => Ok(Value::Defined(Bson::Array(vec![elem]))),
        Value::Defined(Bson::Array(mut a)) => {
            a.push(elem);
            Ok(Value::Defined(Bson::Array(a)))
        }
        Value::Defined(_) => Ok(Value::Undefined),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    fn arr(items: &[i32]) -> Bson {
        Bson::Array(items.iter().map(|n| Bson::Int32(*n)).collect())
    }

    #[test]
    fn appends_to_end() {
        assert_eq!(
            call("RPUSH", vec![def(arr(&[1, 2])), def(3)]).unwrap(),
            def(arr(&[1, 2, 3]))
        );
    }

    #[test]
    fn missing_array_creates_singleton() {
        assert_eq!(
            call("RPUSH", vec![Value::Undefined, def(3)]).unwrap(),
            def(arr(&[3]))
        );
    }

    #[test]
    fn non_array_is_undefined() {
        assert!(call("RPUSH", vec![def(1), def(2)]).unwrap().is_undefined());
    }
}
