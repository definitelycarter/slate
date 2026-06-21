//! `STRINGTONUMBER(str)` — parse a string to a number. Leading/trailing
//! whitespace is ignored. A non-string, or a string that isn't a finite number,
//! yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    let Some(s) = str_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    let s = s.trim();
    // Prefer an integer when the text has no fractional/exponent part.
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::Defined(Bson::Int64(i)));
    }
    Ok(match s.parse::<f64>() {
        Ok(f) if f.is_finite() => Value::Defined(Bson::Double(f)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is a Cosmos doc example, not π
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGTONUMBER", vec![def("100")]).unwrap(),
            def(100_i64)
        );
        assert_eq!(
            call("STRINGTONUMBER", vec![def("3.14")]).unwrap(),
            def(3.14)
        );
        assert_eq!(
            call("STRINGTONUMBER", vec![def("   60   ")]).unwrap(),
            def(60_i64)
        );
        assert!(
            call("STRINGTONUMBER", vec![def("Hello")])
                .unwrap()
                .is_undefined()
        );
        // "Infinity"/"NaN" strings are not finite numbers.
        assert!(
            call("STRINGTONUMBER", vec![def("Infinity")])
                .unwrap()
                .is_undefined()
        );
    }
}
