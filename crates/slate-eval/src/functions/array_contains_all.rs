//! `ARRAY_CONTAINS_ALL(arr, v1, v2, …)` — whether `arr` contains every one of
//! the given values.
//!
//! This is a three-valued AND over per-value membership: a defined value that
//! is missing makes the result `false`; if every defined value is present but
//! some argument is `undefined`, the result is `undefined` (an `undefined`
//! search value matches nothing), matching Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, contains_eq};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(arity_err(name, "at least 2"));
    }
    let Value::Defined(Bson::Array(arr)) = &args[0] else {
        return Ok(Value::Undefined);
    };
    let mut saw_undefined = false;
    for v in &args[1..] {
        match v {
            Value::Undefined => saw_undefined = true,
            Value::Defined(b) => {
                if !contains_eq(arr, b) {
                    return Ok(Value::Defined(Bson::Boolean(false)));
                }
            }
        }
    }
    Ok(if saw_undefined {
        Value::Undefined
    } else {
        Value::Defined(Bson::Boolean(true))
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    fn nums(v: &[i32]) -> Bson {
        Bson::Array(v.iter().map(|&n| Bson::Int32(n)).collect())
    }

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/array-contains-all
        let mixed = Bson::Array(vec![
            Bson::Int32(1),
            Bson::Boolean(true),
            Bson::String("3".into()),
            nums(&[1, 2, 3]),
        ]);
        assert_eq!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![
                    def(mixed),
                    def(1),
                    def(true),
                    def("3"),
                    def(nums(&[1, 2, 3]))
                ]
            )
            .unwrap(),
            def(true)
        );
        assert_eq!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![def(nums(&[1, 2, 3, 4])), def(2), def(3), def(4), def(5)]
            )
            .unwrap(),
            def(false)
        );
        // All defined values present, but an undefined argument poisons it.
        assert!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![def(nums(&[1, 2, 3, 4])), def(1), Value::Undefined]
            )
            .unwrap()
            .is_undefined()
        );
        assert_eq!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![def(nums(&[1, 2, 3, 4])), def(5), def(6), def(7), def(8)]
            )
            .unwrap(),
            def(false)
        );
        assert_eq!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![def(Bson::Array(vec![])), def(1), def(2), def(3)]
            )
            .unwrap(),
            def(false)
        );
        // A missing defined value short-circuits to false despite the undefined.
        assert_eq!(
            call(
                "ARRAY_CONTAINS_ALL",
                vec![def(nums(&[1, 2, 3, 4])), def(5), Value::Undefined]
            )
            .unwrap(),
            def(false)
        );
    }
}
