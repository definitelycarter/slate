//! `ABS(num)` — absolute value of a numeric expression.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, num_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match num_arg(&args[0]) {
        Some(Bson::Int64(i)) => Value::Defined(Bson::Int64(i.abs())),
        Some(Bson::Double(f)) => Value::Defined(Bson::Double(f.abs())),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn absolute_value() {
        assert_eq!(call("ABS", vec![def(-5_i32)]).unwrap(), def(5_i64));
        assert_eq!(call("ABS", vec![def(-2.5_f64)]).unwrap(), def(2.5_f64));
        assert_eq!(call("ABS", vec![def(7_i64)]).unwrap(), def(7_i64));
    }

    #[test]
    fn non_numeric_is_undefined() {
        assert!(call("ABS", vec![def("x")]).unwrap().is_undefined());
    }
}
