//! Built-in scalar function dispatch.
//!
//! Conventions, matching Cosmos:
//! - Function names are case-insensitive.
//! - Wrong **arity** is a hard [`EvalError`].
//! - Wrong argument **types** (or undefined args) generally yield
//!   [`Value::Undefined`] rather than an error. The type-test functions
//!   (`IS_DEFINED`, `IS_NULL`, `IS_STRING`, …) are the deliberate exceptions.
//! - **Type-identity** tests are strict by BSON type (a `Double` is not an
//!   integer, so `IS_INTEGER` is the odd one out — it is a *value/range* test,
//!   matching Cosmos). Comparison still coerces numerics by value (`5 = 5.0`);
//!   that is a separate concern from a type test.
//! - **Math** functions follow Cosmos's numeric model — every number is an
//!   IEEE-754 double — so they widen integer inputs via [`f64_arg`] and return a
//!   `Double` (`CEILING(0)` → `0.0`). `ABS` is the exception: it preserves the
//!   input's integer type.
//! - Each function's tests mirror the worked example on its Cosmos doc page
//!   (`learn.microsoft.com/en-us/cosmos-db/query/<name>`), cited in the test, so
//!   we track the spec rather than our own guesses.
//!
//! One file per function: each `fn eval` lives next to its tests in
//! `functions/<name>.rs`, and [`call`] is the flat name→module dispatch. Adding
//! a function is a new file plus one arm here. Aggregate functions live in
//! [`crate::agg`].

use std::cmp::Ordering;

use bson::Bson;

use crate::error::{EvalError, Result};
use crate::value::Value;

mod abs;
mod acos;
mod array_concat;
mod array_contains;
mod array_contains_all;
mod array_contains_any;
mod array_length;
mod array_slice;
mod asin;
mod atan;
mod atn2;
mod ceiling;
mod choose;
mod concat;
mod contains;
mod cos;
mod cot;
mod datetime;
mod degrees;
mod endswith;
mod exp;
mod floor;
mod iif;
mod index_of;
mod intadd;
mod intbitand;
mod intbitleftshift;
mod intbitnot;
mod intbitor;
mod intbitrightshift;
mod intbitxor;
mod intdiv;
mod intmod;
mod intmul;
mod intsub;
mod is_array;
mod is_bool;
mod is_defined;
mod is_finite_number;
mod is_integer;
mod is_null;
mod is_number;
mod is_object;
mod is_primitive;
mod is_string;
mod left;
mod length;
mod log;
mod log10;
mod lower;
mod ltrim;
mod numberbin;
mod objecttoarray;
mod pi;
mod power;
mod radians;
mod regexmatch;
mod replace;
mod replicate;
mod reverse;
mod right;
mod round;
mod rtrim;
mod setintersect;
mod setunion;
mod sign;
mod sin;
mod sqrt;
mod square;
mod starts_with;
mod stringequals;
mod stringjoin;
mod stringsplit;
mod stringtoarray;
mod stringtoboolean;
mod stringtonull;
mod stringtonumber;
mod stringtoobject;
mod substring;
mod tan;
mod tostring;
mod trim;
mod trunc;
mod upper;

/// Call a scalar function by name with already-evaluated arguments.
pub fn call(name: &str, args: Vec<Value>) -> Result<Value> {
    match name.to_ascii_uppercase().as_str() {
        "IS_DEFINED" => is_defined::eval(name, args),
        "IS_NULL" => is_null::eval(name, args),
        "IS_STRING" => is_string::eval(name, args),
        "IS_NUMBER" => is_number::eval(name, args),
        "IS_BOOL" => is_bool::eval(name, args),
        "IS_ARRAY" => is_array::eval(name, args),
        "IS_OBJECT" => is_object::eval(name, args),
        "IS_PRIMITIVE" => is_primitive::eval(name, args),
        "IS_INTEGER" => is_integer::eval(name, args),
        "IS_FINITE_NUMBER" => is_finite_number::eval(name, args),
        "UPPER" => upper::eval(name, args),
        "LOWER" => lower::eval(name, args),
        "LENGTH" => length::eval(name, args),
        "CONCAT" => concat::eval(name, args),
        "ABS" => abs::eval(name, args),
        "CEILING" => ceiling::eval(name, args),
        "FLOOR" => floor::eval(name, args),
        "ROUND" => round::eval(name, args),
        "TRUNC" => trunc::eval(name, args),
        "SIGN" => sign::eval(name, args),
        "SQRT" => sqrt::eval(name, args),
        "SQUARE" => square::eval(name, args),
        "POWER" => power::eval(name, args),
        "EXP" => exp::eval(name, args),
        "LOG" => log::eval(name, args),
        "LOG10" => log10::eval(name, args),
        "NUMBERBIN" => numberbin::eval(name, args),
        "PI" => pi::eval(name, args),
        "SIN" => sin::eval(name, args),
        "COS" => cos::eval(name, args),
        "TAN" => tan::eval(name, args),
        "COT" => cot::eval(name, args),
        "ASIN" => asin::eval(name, args),
        "ACOS" => acos::eval(name, args),
        "ATAN" => atan::eval(name, args),
        "ATN2" => atn2::eval(name, args),
        "DEGREES" => degrees::eval(name, args),
        "RADIANS" => radians::eval(name, args),
        "INTADD" => intadd::eval(name, args),
        "INTSUB" => intsub::eval(name, args),
        "INTMUL" => intmul::eval(name, args),
        "INTDIV" => intdiv::eval(name, args),
        "INTMOD" => intmod::eval(name, args),
        "INTBITAND" => intbitand::eval(name, args),
        "INTBITOR" => intbitor::eval(name, args),
        "INTBITXOR" => intbitxor::eval(name, args),
        "INTBITNOT" => intbitnot::eval(name, args),
        "INTBITLEFTSHIFT" => intbitleftshift::eval(name, args),
        "INTBITRIGHTSHIFT" => intbitrightshift::eval(name, args),
        "DATETIMEADD" => datetime::add(name, args),
        "DATETIMEDIFF" => datetime::diff(name, args),
        "DATETIMEPART" => datetime::part(name, args),
        "DATETIMEBIN" => datetime::bin(name, args),
        "DATETIMEFROMPARTS" => datetime::from_parts(name, args),
        "DATETIMETOTICKS" => datetime::to_ticks(name, args),
        "DATETIMETOTIMESTAMP" => datetime::to_timestamp(name, args),
        "TICKSTODATETIME" => datetime::from_ticks(name, args),
        "TIMESTAMPTODATETIME" => datetime::from_timestamp(name, args),
        "ARRAY_LENGTH" => array_length::eval(name, args),
        "ARRAY_CONTAINS" => array_contains::eval(name, args),
        "ARRAY_CONTAINS_ALL" => array_contains_all::eval(name, args),
        "ARRAY_CONTAINS_ANY" => array_contains_any::eval(name, args),
        "ARRAY_CONCAT" => array_concat::eval(name, args),
        "ARRAY_SLICE" => array_slice::eval(name, args),
        "CHOOSE" => choose::eval(name, args),
        "SETINTERSECT" => setintersect::eval(name, args),
        "SETUNION" => setunion::eval(name, args),
        "OBJECTTOARRAY" => objecttoarray::eval(name, args),
        "CONTAINS" => contains::eval(name, args),
        "IIF" => iif::eval(name, args),
        "STARTSWITH" => starts_with::eval(name, args),
        "ENDSWITH" => endswith::eval(name, args),
        "STRINGEQUALS" => stringequals::eval(name, args),
        "STRINGJOIN" => stringjoin::eval(name, args),
        "STRINGSPLIT" => stringsplit::eval(name, args),
        "TOSTRING" => tostring::eval(name, args),
        "STRINGTONUMBER" => stringtonumber::eval(name, args),
        "STRINGTOBOOLEAN" => stringtoboolean::eval(name, args),
        "STRINGTONULL" => stringtonull::eval(name, args),
        "STRINGTOARRAY" => stringtoarray::eval(name, args),
        "STRINGTOOBJECT" => stringtoobject::eval(name, args),
        "INDEX_OF" => index_of::eval(name, args),
        "SUBSTRING" => substring::eval(name, args),
        "LEFT" => left::eval(name, args),
        "RIGHT" => right::eval(name, args),
        "TRIM" => trim::eval(name, args),
        "LTRIM" => ltrim::eval(name, args),
        "RTRIM" => rtrim::eval(name, args),
        "REPLACE" => replace::eval(name, args),
        "REPLICATE" => replicate::eval(name, args),
        "REVERSE" => reverse::eval(name, args),
        "REGEXMATCH" => regexmatch::eval(name, args),
        other => Err(EvalError {
            message: format!("unknown function: {other}"),
        }),
    }
}

/// Whether `name` is a clock-dependent `GETCURRENT*` function. The evaluator
/// handles these specially (they read the injected "now", not a syscall).
pub(crate) fn is_current_time(name: &str) -> bool {
    datetime::is_current(name)
}

/// Resolve a `GETCURRENT*` function from the injected epoch-millis "now".
pub(crate) fn current_time(name: &str, now_ms: i64) -> Value {
    datetime::current(name, now_ms)
}

// ── Shared helpers (visible to the function submodules) ─────────────

fn arity(name: &str, args: &[Value], n: usize) -> Result<()> {
    if args.len() == n {
        Ok(())
    } else {
        Err(arity_err(name, &n.to_string()))
    }
}

fn arity_err(name: &str, expected: &str) -> EvalError {
    EvalError {
        message: format!("{name} expects {expected} argument(s)"),
    }
}

fn str_arg(v: &Value) -> Option<&str> {
    match v {
        Value::Defined(Bson::String(s)) => Some(s),
        _ => None,
    }
}

fn map_str(v: &Value, f: impl Fn(&str) -> String) -> Value {
    match str_arg(v) {
        Some(s) => Value::Defined(Bson::String(f(s))),
        None => Value::Undefined,
    }
}

/// Normalize a numeric argument to either `Int64` or `Double`.
fn num_arg(v: &Value) -> Option<Bson> {
    match v {
        Value::Defined(Bson::Int32(i)) => Some(Bson::Int64(*i as i64)),
        Value::Defined(Bson::Int64(i)) => Some(Bson::Int64(*i)),
        Value::Defined(Bson::Double(f)) => Some(Bson::Double(*f)),
        _ => None,
    }
}

/// Extract a numeric argument as `f64`. The math functions follow Cosmos's
/// numeric model — every number is an IEEE-754 double — so they widen any
/// integer input through here and return a `Double` (e.g. `CEILING(0)` → `0.0`).
fn f64_arg(v: &Value) -> Option<f64> {
    match v {
        Value::Defined(Bson::Int32(i)) => Some(*i as f64),
        Value::Defined(Bson::Int64(i)) => Some(*i as f64),
        Value::Defined(Bson::Double(f)) => Some(*f),
        _ => None,
    }
}

/// Convert a parsed JSON value into BSON — the shared backbone of the
/// `STRINGTOARRAY` / `STRINGTOOBJECT` parsers. Integers become `Int64`, reals
/// `Double`; object key order is preserved (serde_json's `preserve_order`).
fn json_to_bson(v: serde_json::Value) -> Bson {
    use serde_json::Value as J;
    match v {
        J::Null => Bson::Null,
        J::Bool(b) => Bson::Boolean(b),
        J::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => Bson::Int64(i),
            (None, Some(f)) => Bson::Double(f),
            _ => Bson::Null,
        },
        J::String(s) => Bson::String(s),
        J::Array(a) => Bson::Array(a.into_iter().map(json_to_bson).collect()),
        J::Object(o) => {
            let mut doc = bson::Document::new();
            for (k, val) in o {
                doc.insert(k, json_to_bson(val));
            }
            Bson::Document(doc)
        }
    }
}

/// Extract a strict integer value — Cosmos's `INT*` and bitwise functions operate
/// on integers only. Integer-typed, or a `Double` with no fractional part; a
/// fractional or non-numeric argument yields `None` (→ undefined).
fn int_value(v: &Value) -> Option<i64> {
    match v {
        Value::Defined(Bson::Int32(i)) => Some(*i as i64),
        Value::Defined(Bson::Int64(i)) => Some(*i),
        Value::Defined(Bson::Double(f))
            if f.is_finite()
                && f.fract() == 0.0
                && *f >= i64::MIN as f64
                && *f <= i64::MAX as f64 =>
        {
            Some(*f as i64)
        }
        _ => None,
    }
}

/// Extract a numeric argument as `i64`, truncating a finite `Double`. Used by
/// the string functions for character positions, lengths, and counts.
fn int_arg(v: &Value) -> Option<i64> {
    match v {
        Value::Defined(Bson::Int32(i)) => Some(*i as i64),
        Value::Defined(Bson::Int64(i)) => Some(*i),
        Value::Defined(Bson::Double(f)) if f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

/// Consume a `Value` into an owned BSON array, or `None` if it is not an array.
/// Lets the array functions build their result by moving elements rather than
/// cloning them.
fn into_array(v: Value) -> Option<Vec<Bson>> {
    match v {
        Value::Defined(Bson::Array(a)) => Some(a),
        _ => None,
    }
}

/// Value-based element equality for the array/set functions: numerics coerce by
/// value (the same rule `=` and `ARRAY_CONTAINS` use), and the structural types
/// the scalar comparator can't order (arrays, documents) fall back to BSON
/// equality.
fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match crate::eval::compare_values(a, b) {
        Some(ord) => ord == Ordering::Equal,
        None => a == b,
    }
}

/// Whether `arr` contains an element equal to `needle` under [`bson_eq`].
fn contains_eq(arr: &[Bson], needle: &Bson) -> bool {
    arr.iter().any(|e| bson_eq(e, needle))
}

/// Shared logic for the string predicate functions (`STARTSWITH`, `ENDSWITH`,
/// `CONTAINS`, `STRINGEQUALS`): pull two string args plus an optional
/// case-insensitivity flag (arg index 2, default `false`), then apply `f`. A
/// non-string in either of the first two args yields `Undefined`. Callers must
/// arity-check that at least two args are present.
fn str_match(args: &[Value], f: impl Fn(&str, &str) -> bool) -> Value {
    let ignore_case = matches!(args.get(2), Some(Value::Defined(Bson::Boolean(true))));
    match (str_arg(&args[0]), str_arg(&args[1])) {
        (Some(a), Some(b)) => {
            let matched = if ignore_case {
                f(&a.to_lowercase(), &b.to_lowercase())
            } else {
                f(a, b)
            };
            Value::Defined(Bson::Boolean(matched))
        }
        _ => Value::Undefined,
    }
}

/// Arity check for the string predicates that take an optional ignore-case flag.
fn arity_2_or_3(name: &str, args: &[Value]) -> Result<()> {
    if args.len() == 2 || args.len() == 3 {
        Ok(())
    } else {
        Err(arity_err(name, "2 or 3"))
    }
}

/// Test helper shared by the per-function test modules.
#[cfg(test)]
fn def(b: impl Into<Bson>) -> Value {
    Value::Defined(b.into())
}

/// Assert a function returned a `Double` close to `expected`. The transcendental
/// math functions (`EXP`, `LOG`, `LOG10`) can differ from the doc's value in the
/// last ULP depending on the platform's libm, so they compare with a tolerance.
#[cfg(test)]
fn approx(got: Value, expected: f64) {
    match got {
        Value::Defined(Bson::Double(f)) => {
            let tol = 1e-9 * expected.abs().max(1.0);
            assert!((f - expected).abs() <= tol, "got {f}, want ~{expected}");
        }
        _ => panic!("expected a Double result"),
    }
}

#[cfg(test)]
mod tests {
    use super::{call, def};

    // Cross-function conventions (see the module docs).

    #[test]
    fn type_mismatch_is_undefined_not_error() {
        assert!(call("UPPER", vec![def(1)]).unwrap().is_undefined());
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("UPPER", vec![]).is_err());
    }

    #[test]
    fn unknown_function_is_error() {
        assert!(call("NOPE", vec![]).is_err());
    }

    #[test]
    fn names_are_case_insensitive() {
        assert_eq!(call("uPpEr", vec![def("ab")]).unwrap(), def("AB"));
    }
}
