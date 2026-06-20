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

use bson::Bson;

use crate::error::{EvalError, Result};
use crate::value::Value;

mod abs;
mod array_contains;
mod array_length;
mod ceiling;
mod concat;
mod contains;
mod exp;
mod floor;
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
mod length;
mod log;
mod log10;
mod lower;
mod pi;
mod power;
mod regexmatch;
mod round;
mod sign;
mod sqrt;
mod square;
mod starts_with;
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
        "PI" => pi::eval(name, args),
        "ARRAY_LENGTH" => array_length::eval(name, args),
        "ARRAY_CONTAINS" => array_contains::eval(name, args),
        "CONTAINS" => contains::eval(name, args),
        "STARTSWITH" => starts_with::eval(name, args),
        "REGEXMATCH" => regexmatch::eval(name, args),
        other => Err(EvalError {
            message: format!("unknown function: {other}"),
        }),
    }
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

fn str2_bool(a: &Value, b: &Value, f: impl Fn(&str, &str) -> bool) -> Value {
    match (str_arg(a), str_arg(b)) {
        (Some(s), Some(t)) => Value::Defined(Bson::Boolean(f(s, t))),
        _ => Value::Undefined,
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
