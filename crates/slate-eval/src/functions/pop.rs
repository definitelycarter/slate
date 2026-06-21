//! `pop(arr)` — remove the last element of `arr` (Mongo `$pop`).
//!
//! An empty array stays empty; a missing/undefined or non-array argument yields
//! `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity_err;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    let [arr] = match <[Value; 1]>::try_from(args) {
        Ok(a) => a,
        Err(_) => return Err(arity_err(name, "exactly 1")),
    };
    match arr {
        Value::Defined(Bson::Array(mut a)) => {
            a.pop();
            Ok(Value::Defined(Bson::Array(a)))
        }
        _ => Ok(Value::Undefined),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn arr(items: &[i32]) -> Bson {
        Bson::Array(items.iter().map(|n| Bson::Int32(*n)).collect())
    }

    #[test]
    fn removes_last() {
        assert_eq!(
            call("POP", vec![def(arr(&[1, 2, 3]))]).unwrap(),
            def(arr(&[1, 2]))
        );
    }

    #[test]
    fn empty_stays_empty() {
        assert_eq!(call("POP", vec![def(arr(&[]))]).unwrap(), def(arr(&[])));
    }
}
