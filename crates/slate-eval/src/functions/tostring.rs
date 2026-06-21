//! `TOSTRING(value)` — a string representation of a scalar value.
//!
//! - A string is returned unchanged (no quotes).
//! - Numbers, booleans and `null` render as their JSON token (`125`, `false`,
//!   `null`); `NaN`/`±Infinity` render as `NaN`/`Infinity`/`-Infinity`.
//! - Arrays and objects render as compact JSON, recursively.
//! - `undefined` stays `undefined`.
//!
//! Number formatting follows Cosmos's model: integers print without a decimal
//! point and finite doubles use the shortest round-trippable decimal (Rust's
//! `Display`), which matches Cosmos for normal-magnitude values. Extreme
//! magnitudes where Cosmos switches to exponential notation (e.g. `1e21`) are a
//! known divergence tracked with the broader number-model question.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let Some(arg) = args.into_iter().next() else {
        return Ok(Value::Undefined);
    };
    Ok(match arg {
        Value::Undefined => Value::Undefined,
        // A top-level string is returned verbatim — only *nested* strings are
        // JSON-quoted.
        Value::Defined(Bson::String(s)) => Value::Defined(Bson::String(s)),
        Value::Defined(b) => {
            let mut out = String::new();
            if write_value(&mut out, &b) {
                Value::Defined(Bson::String(out))
            } else {
                Value::Undefined
            }
        }
    })
}

/// Append the compact-JSON rendering of `b` to `out`. Returns `false` for BSON
/// types outside Cosmos's JSON model (so `TOSTRING` of such a value is undefined).
fn write_value(out: &mut String, b: &Bson) -> bool {
    match b {
        Bson::String(s) => push_json_string(out, s),
        Bson::Boolean(x) => out.push_str(if *x { "true" } else { "false" }),
        Bson::Null => out.push_str("null"),
        Bson::Int32(i) => out.push_str(&i.to_string()),
        Bson::Int64(i) => out.push_str(&i.to_string()),
        Bson::Double(f) => out.push_str(&fmt_double(*f)),
        Bson::Array(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if !write_value(out, e) {
                    return false;
                }
            }
            out.push(']');
        }
        Bson::Document(d) => {
            out.push('{');
            for (i, (k, v)) in d.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(out, k);
                out.push(':');
                if !write_value(out, v) {
                    return false;
                }
            }
            out.push('}');
        }
        _ => return false,
    }
    true
}

/// Render a finite/non-finite double the way Cosmos does.
fn fmt_double(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f < 0.0 { "-Infinity" } else { "Infinity" }.to_string()
    } else {
        // Shortest round-trippable decimal; "1.0" → "1", "-0.0" → "-0".
        format!("{f}")
    }
}

/// Append a JSON-escaped, double-quoted string. serde_json gives spec-correct
/// escaping and serializing a `&str` cannot fail.
fn push_json_string(out: &mut String, s: &str) {
    match serde_json::to_string(s) {
        Ok(quoted) => out.push_str(&quoted),
        Err(_) => out.push_str("\"\""),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_doc_example() {
        // learn.microsoft.com/.../tostring
        assert_eq!(call("TOSTRING", vec![def(125)]).unwrap(), def("125"));
        assert_eq!(call("TOSTRING", vec![def(0.1234)]).unwrap(), def("0.1234"));
        assert_eq!(call("TOSTRING", vec![def(false)]).unwrap(), def("false"));
        assert_eq!(
            call("TOSTRING", vec![def("Hello World")]).unwrap(),
            def("Hello World")
        );
        assert_eq!(call("TOSTRING", vec![def(f64::NAN)]).unwrap(), def("NaN"));
        assert_eq!(
            call("TOSTRING", vec![def(f64::INFINITY)]).unwrap(),
            def("Infinity")
        );
    }

    #[test]
    fn renders_arrays_and_objects_as_compact_json() {
        let arr = bson::bson!([1, 2, 3]);
        assert_eq!(call("TOSTRING", vec![def(arr)]).unwrap(), def("[1,2,3]"));
        let obj = bson::bson!({ "department": "Bicycles" });
        assert_eq!(
            call("TOSTRING", vec![def(obj)]).unwrap(),
            def(r#"{"department":"Bicycles"}"#)
        );
        let nested = bson::bson!({ "a": [1, 2], "b": { "c": true } });
        assert_eq!(
            call("TOSTRING", vec![def(nested)]).unwrap(),
            def(r#"{"a":[1,2],"b":{"c":true}}"#)
        );
    }

    #[test]
    fn number_edges_match_cosmos() {
        assert_eq!(call("TOSTRING", vec![def(1.0)]).unwrap(), def("1"));
        assert_eq!(call("TOSTRING", vec![def(-0.0)]).unwrap(), def("-0"));
        assert_eq!(
            call("TOSTRING", vec![def(f64::NEG_INFINITY)]).unwrap(),
            def("-Infinity")
        );
        assert_eq!(
            call("TOSTRING", vec![def(bson::Bson::Null)]).unwrap(),
            def("null")
        );
    }

    #[test]
    fn undefined_stays_undefined() {
        assert!(
            call("TOSTRING", vec![crate::value::Value::Undefined])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("TOSTRING", vec![]).is_err());
        assert!(call("TOSTRING", vec![def(1), def(2)]).is_err());
    }
}
