//! `NUMBERBIN(value [, bin_size])` — round `value` down to the nearest multiple
//! of `bin_size`, i.e. `floor(value / bin_size) * bin_size`. `bin_size` defaults
//! to `1`. A non-numeric argument, or a `bin_size` of zero, yields undefined.
//! Like the other math functions, the result is a `Double`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 && args.len() != 2 {
        return Err(arity_err(name, "1 or 2"));
    }
    let Some(value) = f64_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    let bin_size = match args.get(1) {
        Some(a) => match f64_arg(a) {
            Some(b) => b,
            None => return Ok(Value::Undefined),
        },
        None => 1.0,
    };
    if bin_size == 0.0 {
        return Ok(Value::Undefined);
    }
    Ok(Value::Defined(Bson::Double(
        (value / bin_size).floor() * bin_size,
    )))
}

#[cfg(test)]
mod tests {
    use super::super::{approx, call, def};

    #[test]
    fn cosmos_doc_example() {
        // learn.microsoft.com/.../numberbin — round 37.752 to various bins.
        assert_eq!(
            call("NUMBERBIN", vec![def(37.752), def(-100)]).unwrap(),
            def(100.0)
        );
        assert_eq!(
            call("NUMBERBIN", vec![def(37.752), def(10)]).unwrap(),
            def(30.0)
        );
        assert_eq!(
            call("NUMBERBIN", vec![def(37.752), def(1)]).unwrap(),
            def(37.0)
        );
        approx(
            call("NUMBERBIN", vec![def(37.752), def(0.1)]).unwrap(),
            37.7,
        );
        approx(
            call("NUMBERBIN", vec![def(37.752), def(0.01)]).unwrap(),
            37.75,
        );
        // bin size 0 → undefined (omitted from the doc's result object)
        assert!(
            call("NUMBERBIN", vec![def(37.752), def(0)])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn bin_size_defaults_to_one() {
        assert_eq!(call("NUMBERBIN", vec![def(37.752)]).unwrap(), def(37.0));
    }

    #[test]
    fn non_numeric_is_undefined() {
        assert!(
            call("NUMBERBIN", vec![def("x"), def(10)])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("NUMBERBIN", vec![def(37.752), def("x")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("NUMBERBIN", vec![]).is_err());
        assert!(call("NUMBERBIN", vec![def(1), def(2), def(3)]).is_err());
    }
}
