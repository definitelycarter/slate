//! `ARRAY_CONTAINS(arr, value [, partial])` — whether an array contains a value.
//!
//! Elements are compared with the shared comparator, so numeric types coerce
//! like `=`. With a third argument of `true`, objects match partially: an array
//! element matches when it contains all of `value`'s fields (recursively). A
//! non-array first argument yields `Undefined`.

use std::cmp::Ordering;

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity_2_or_3;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    let partial = match args.get(2) {
        None => false,
        Some(Value::Defined(Bson::Boolean(b))) => *b,
        Some(_) => return Ok(Value::Undefined),
    };
    Ok(match (&args[0], &args[1]) {
        (Value::Defined(Bson::Array(arr)), Value::Defined(needle)) => {
            let found = arr.iter().any(|e| {
                if partial {
                    contains_subset(e, needle)
                } else {
                    deep_equal(e, needle)
                }
            });
            Value::Defined(Bson::Boolean(found))
        }
        _ => Value::Undefined,
    })
}

/// Partial (subset) match: every field of `needle` is present in `elem` with a
/// matching value, recursing into nested objects. Non-objects compare by value.
fn contains_subset(elem: &Bson, needle: &Bson) -> bool {
    match (elem, needle) {
        (Bson::Document(e), Bson::Document(n)) => n
            .iter()
            .all(|(k, v)| e.get(k).is_some_and(|ev| contains_subset(ev, v))),
        _ => crate::eval::compare_values(elem, needle) == Some(Ordering::Equal),
    }
}

/// Full (exact) match. The shared comparator doesn't order documents, so compare
/// objects/arrays structurally and fall back to it for scalars (numeric coercion).
fn deep_equal(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && y.iter()
                    .all(|(k, v)| x.get(k).is_some_and(|xv| deep_equal(xv, v)))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| deep_equal(p, q))
        }
        _ => crate::eval::compare_values(a, b) == Some(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, doc};

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

    #[test]
    fn partial_object_match() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/array-contains
        let objs = def(Bson::Array(vec![Bson::Document(
            doc! { "category": "shirts", "color": "blue" },
        )]));
        // full match needs every field
        assert_eq!(
            call(
                "ARRAY_CONTAINS",
                vec![
                    objs.clone(),
                    def(Bson::Document(doc! { "category": "shirts" }))
                ]
            )
            .unwrap(),
            def(false)
        );
        // partial match (third arg true) accepts a subset
        assert_eq!(
            call(
                "ARRAY_CONTAINS",
                vec![
                    objs,
                    def(Bson::Document(doc! { "category": "shirts" })),
                    def(true)
                ]
            )
            .unwrap(),
            def(true)
        );
    }
}
