//! `VECTORDISTANCE(vector1, vector2, metric?)` — the similarity/distance between
//! two numeric embedding vectors. `metric` is an optional string:
//! `"cosine"` (default), `"dotproduct"`, or `"euclidean"`.
//!
//! Function-first slice of the [Vector Index RFC](../../../../book/src/rfcs/vector-index.md):
//! usable immediately as a full-scan scalar
//! (`ORDER BY VECTORDISTANCE(c.embedding, @q) … LIMIT k` for kNN), exactly the
//! `ST_*` precedent — the flat vector *index* that turns it into a seek is
//! deferred. Cosmos derives the metric from the container's vector embedding
//! policy (which is the deferred index); until that exists Slate takes it as the
//! optional 3rd argument.
//!
//! Following Cosmos's `VectorDistance` semantics, the returned value's sense
//! depends on the metric: **`cosine` and `dotproduct` are similarities** (higher
//! is closer → `ORDER BY … DESC`), while **`euclidean` is a distance** (lower is
//! closer → `ORDER BY … ASC`).
//!
//! Either vector being undefined, not an array, empty, of unequal length, or
//! holding a non-number yields `Undefined` (the function convention for bad
//! types). An unknown metric string is a hard error — a typo should be loud,
//! like wrong arity.

use bson::Bson;

use crate::error::{EvalError, Result};
use crate::value::Value;
use crate::vector::VectorMetric;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(EvalError {
            message: format!("{name} expects 2 or 3 argument(s)"),
        });
    }

    let metric = match args.get(2) {
        None => VectorMetric::Cosine,
        Some(Value::Defined(Bson::String(s))) => match VectorMetric::parse(s) {
            Some(m) => m,
            None => {
                return Err(EvalError {
                    message: format!(
                        "{name}: unknown metric {s:?} (expected \"cosine\", \"dotproduct\", or \"euclidean\")"
                    ),
                });
            }
        },
        // A present-but-non-string metric is a bad call → undefined, like a bad
        // vector argument.
        Some(_) => return Ok(Value::Undefined),
    };

    let (Some(a), Some(b)) = (vector(&args[0]), vector(&args[1])) else {
        return Ok(Value::Undefined);
    };
    if a.is_empty() || a.len() != b.len() {
        return Ok(Value::Undefined);
    }
    Ok(Value::Defined(Bson::Double(metric.measure(&a, &b))))
}

/// A vector argument: a defined BSON array whose every element is a number,
/// widened to `f64`. Anything else (undefined, non-array, a non-numeric element)
/// yields `None` → the call is `Undefined`.
fn vector(v: &Value) -> Option<Vec<f64>> {
    let Value::Defined(Bson::Array(arr)) = v else {
        return None;
    };
    arr.iter()
        .map(|e| match e {
            Bson::Int32(i) => Some(*i as f64),
            Bson::Int64(i) => Some(*i as f64),
            Bson::Double(f) => Some(*f),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::{Bson, bson};

    fn vec3(a: f64, b: f64, c: f64) -> Bson {
        bson!([a, b, c])
    }

    fn distance(args: Vec<Value>) -> f64 {
        match call("VECTORDISTANCE", args).unwrap() {
            Value::Defined(Bson::Double(d)) => d,
            other => panic!("expected a Double, got {other:?}"),
        }
    }

    #[test]
    fn dotproduct_is_the_inner_product() {
        // [1,2,3]·[4,5,6] = 4 + 10 + 18 = 32
        let d = distance(vec![
            def(vec3(1.0, 2.0, 3.0)),
            def(vec3(4.0, 5.0, 6.0)),
            def("dotproduct"),
        ]);
        assert!((d - 32.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn euclidean_is_l2_distance() {
        // |[0,0,0] - [3,4,0]| = 5
        let d = distance(vec![
            def(vec3(0.0, 0.0, 0.0)),
            def(vec3(3.0, 4.0, 0.0)),
            def("euclidean"),
        ]);
        assert!((d - 5.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn cosine_is_the_default_and_a_similarity() {
        // Identical direction → 1.0; orthogonal → 0.0. Default metric is cosine.
        let same = distance(vec![def(vec3(1.0, 0.0, 0.0)), def(vec3(2.0, 0.0, 0.0))]);
        assert!((same - 1.0).abs() < 1e-9, "got {same}");
        let orth = distance(vec![
            def(vec3(1.0, 0.0, 0.0)),
            def(vec3(0.0, 1.0, 0.0)),
            def("cosine"),
        ]);
        assert!(orth.abs() < 1e-9, "got {orth}");
    }

    #[test]
    fn metric_name_is_case_insensitive() {
        let d = distance(vec![
            def(vec3(1.0, 2.0, 3.0)),
            def(vec3(4.0, 5.0, 6.0)),
            def("DotProduct"),
        ]);
        assert!((d - 32.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn mixed_int_and_double_elements_widen() {
        let d = distance(vec![
            def(bson!([1_i32, 2_i64, 3.0])),
            def(bson!([4.0, 5_i32, 6_i64])),
            def("dotproduct"),
        ]);
        assert!((d - 32.0).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn unequal_length_is_undefined() {
        assert!(
            call(
                "VECTORDISTANCE",
                vec![def(bson!([1.0, 2.0])), def(vec3(1.0, 2.0, 3.0))]
            )
            .unwrap()
            .is_undefined()
        );
    }

    #[test]
    fn non_array_argument_is_undefined() {
        assert!(
            call("VECTORDISTANCE", vec![def("x"), def(vec3(1.0, 2.0, 3.0))])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn non_numeric_element_is_undefined() {
        assert!(
            call(
                "VECTORDISTANCE",
                vec![def(bson!([1.0, "x", 3.0])), def(vec3(1.0, 2.0, 3.0))]
            )
            .unwrap()
            .is_undefined()
        );
    }

    #[test]
    fn unknown_metric_is_an_error() {
        assert!(
            call(
                "VECTORDISTANCE",
                vec![
                    def(vec3(1.0, 2.0, 3.0)),
                    def(vec3(4.0, 5.0, 6.0)),
                    def("manhattan"),
                ]
            )
            .is_err()
        );
    }

    #[test]
    fn wrong_arity_is_an_error() {
        assert!(call("VECTORDISTANCE", vec![def(vec3(1.0, 2.0, 3.0))]).is_err());
    }
}
