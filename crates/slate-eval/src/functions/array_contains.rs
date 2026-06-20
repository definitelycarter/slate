//! `ARRAY_CONTAINS(arr, value)` — whether an array contains a value.
//!
//! Elements are compared with the shared comparator, so numeric types coerce
//! like `=`. A non-array first argument yields `Undefined`.

use std::cmp::Ordering;

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (&args[0], &args[1]) {
        (Value::Defined(Bson::Array(arr)), Value::Defined(needle)) => {
            let found = arr
                .iter()
                .any(|e| crate::eval::compare_values(e, needle) == Some(Ordering::Equal));
            Value::Defined(Bson::Boolean(found))
        }
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    fn arr() -> Value {
        def(Bson::Array(vec![Bson::String("a".into()), Bson::Int32(7)]))
    }

    #[test]
    fn finds_value() {
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr(), def("a")]).unwrap(),
            def(true)
        );
    }

    #[test]
    fn coerces_numeric_types() {
        // array holds Int32(7); needle Int64(7) still matches.
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr(), def(7_i64)]).unwrap(),
            def(true)
        );
    }

    #[test]
    fn absent_value_is_false() {
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr(), def("z")]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn non_array_is_undefined() {
        assert!(
            call("ARRAY_CONTAINS", vec![def("a"), def("a")])
                .unwrap()
                .is_undefined()
        );
    }
}
