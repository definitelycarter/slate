//! Built-in scalar function dispatch.
//!
//! Conventions, matching Cosmos:
//! - Function names are case-insensitive.
//! - Wrong **arity** is a hard [`EvalError`].
//! - Wrong argument **types** (or undefined args) generally yield
//!   [`Value::Undefined`] rather than an error. The type-test functions
//!   (`IS_DEFINED`, `IS_NULL`) are the deliberate exceptions.
//!
//! This set is intentionally small for v1; it exercises the dispatch surface
//! and is easy to extend. Aggregate functions live in [`crate::agg`].

use bson::Bson;

use crate::error::{EvalError, Result};
use crate::value::Value;

/// Call a scalar function by name with already-evaluated arguments.
pub fn call(name: &str, args: Vec<Value>) -> Result<Value> {
    match name.to_ascii_uppercase().as_str() {
        "IS_DEFINED" => {
            arity(name, &args, 1)?;
            Ok(Value::Defined(Bson::Boolean(!args[0].is_undefined())))
        }
        "IS_NULL" => {
            arity(name, &args, 1)?;
            Ok(Value::Defined(Bson::Boolean(matches!(
                &args[0],
                Value::Defined(Bson::Null)
            ))))
        }
        "UPPER" => {
            arity(name, &args, 1)?;
            Ok(map_str(&args[0], |s| s.to_uppercase()))
        }
        "LOWER" => {
            arity(name, &args, 1)?;
            Ok(map_str(&args[0], |s| s.to_lowercase()))
        }
        "LENGTH" => {
            arity(name, &args, 1)?;
            Ok(match str_arg(&args[0]) {
                Some(s) => Value::Defined(Bson::Int64(s.chars().count() as i64)),
                None => Value::Undefined,
            })
        }
        "CONCAT" => {
            if args.len() < 2 {
                return Err(arity_err(name, "at least 2"));
            }
            let mut out = String::new();
            for a in &args {
                match str_arg(a) {
                    Some(s) => out.push_str(s),
                    None => return Ok(Value::Undefined),
                }
            }
            Ok(Value::Defined(Bson::String(out)))
        }
        "ABS" => {
            arity(name, &args, 1)?;
            Ok(match num_arg(&args[0]) {
                Some(Bson::Int64(i)) => Value::Defined(Bson::Int64(i.abs())),
                Some(Bson::Double(f)) => Value::Defined(Bson::Double(f.abs())),
                _ => Value::Undefined,
            })
        }
        "ARRAY_LENGTH" => {
            arity(name, &args, 1)?;
            Ok(match &args[0] {
                Value::Defined(Bson::Array(a)) => Value::Defined(Bson::Int64(a.len() as i64)),
                _ => Value::Undefined,
            })
        }
        "ARRAY_CONTAINS" => {
            arity(name, &args, 2)?;
            // True if the array contains the value (compared with the shared
            // comparator, so numeric types coerce like `=`). Non-array → undefined.
            Ok(match (&args[0], &args[1]) {
                (Value::Defined(Bson::Array(arr)), Value::Defined(needle)) => {
                    let found = arr.iter().any(|e| {
                        crate::eval::compare_values(e, needle) == Some(std::cmp::Ordering::Equal)
                    });
                    Value::Defined(Bson::Boolean(found))
                }
                _ => Value::Undefined,
            })
        }
        "CONTAINS" => {
            arity(name, &args, 2)?;
            Ok(str2_bool(&args[0], &args[1], |s, sub| s.contains(sub)))
        }
        "STARTSWITH" => {
            arity(name, &args, 2)?;
            Ok(str2_bool(&args[0], &args[1], |s, p| s.starts_with(p)))
        }
        "REGEXMATCH" => {
            arity(name, &args, 2)?;
            // Matches a string against a regex pattern. Inline flags (e.g.
            // `(?i)`) are honored. Non-strings or an invalid pattern → undefined.
            Ok(match (str_arg(&args[0]), str_arg(&args[1])) {
                (Some(s), Some(pat)) => match regex::Regex::new(pat) {
                    Ok(re) => Value::Defined(Bson::Boolean(re.is_match(s))),
                    Err(_) => Value::Undefined,
                },
                _ => Value::Undefined,
            })
        }
        other => Err(EvalError {
            message: format!("unknown function: {other}"),
        }),
    }
}

// ── Helpers ─────────────────────────────────────────────────────

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

fn str2_bool(a: &Value, b: &Value, f: impl Fn(&str, &str) -> bool) -> Value {
    match (str_arg(a), str_arg(b)) {
        (Some(s), Some(t)) => Value::Defined(Bson::Boolean(f(s, t))),
        _ => Value::Undefined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(b: impl Into<Bson>) -> Value {
        Value::Defined(b.into())
    }

    #[test]
    fn upper_lower() {
        assert_eq!(call("UPPER", vec![def("aBc")]).unwrap(), def("ABC"));
        assert_eq!(call("lower", vec![def("aBc")]).unwrap(), def("abc"));
    }

    #[test]
    fn length_and_array_length() {
        assert_eq!(call("LENGTH", vec![def("hello")]).unwrap(), def(5_i64));
        assert_eq!(
            call(
                "ARRAY_LENGTH",
                vec![def(Bson::Array(vec![def(1).into_bson().unwrap()]))]
            )
            .unwrap(),
            def(1_i64)
        );
    }

    #[test]
    fn is_defined_and_is_null() {
        assert_eq!(
            call("IS_DEFINED", vec![Value::Undefined]).unwrap(),
            def(false)
        );
        assert_eq!(call("IS_DEFINED", vec![def(1)]).unwrap(), def(true));
        assert_eq!(call("IS_NULL", vec![def(Bson::Null)]).unwrap(), def(true));
        assert_eq!(call("IS_NULL", vec![Value::Undefined]).unwrap(), def(false));
    }

    #[test]
    fn concat_requires_strings() {
        assert_eq!(call("CONCAT", vec![def("a"), def("b")]).unwrap(), def("ab"));
        assert!(
            call("CONCAT", vec![def("a"), def(1)])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn array_contains() {
        let arr = def(Bson::Array(vec![
            bson::Bson::String("a".into()),
            bson::Bson::Int32(7),
        ]));
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr.clone(), def("a")]).unwrap(),
            def(true)
        );
        // numeric coercion: array has Int32(7), needle Int64(7)
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr.clone(), def(7_i64)]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("ARRAY_CONTAINS", vec![arr, def("z")]).unwrap(),
            def(false)
        );
        // non-array → undefined
        assert!(
            call("ARRAY_CONTAINS", vec![def("a"), def("a")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn regexmatch() {
        assert_eq!(
            call("REGEXMATCH", vec![def("admin@x"), def("^admin")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("REGEXMATCH", vec![def("user@x"), def("^admin")]).unwrap(),
            def(false)
        );
        // inline case-insensitive flag
        assert_eq!(
            call("REGEXMATCH", vec![def("ADMIN"), def("(?i)^admin")]).unwrap(),
            def(true)
        );
        // non-string / invalid pattern → undefined
        assert!(
            call("REGEXMATCH", vec![def(1), def("x")])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("REGEXMATCH", vec![def("x"), def("[")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn contains_startswith() {
        assert_eq!(
            call("CONTAINS", vec![def("hello"), def("ell")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("STARTSWITH", vec![def("hello"), def("he")]).unwrap(),
            def(true)
        );
    }

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
}
