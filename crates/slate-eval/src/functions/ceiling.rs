//! `CEILING(num)` — smallest integer value greater than or equal to `num`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.ceil())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/ceiling
        assert_eq!(
            call("CEILING", vec![def(123.45_f64)]).unwrap(),
            def(124.0_f64)
        );
        assert_eq!(
            call("CEILING", vec![def(-45.72_f64)]).unwrap(),
            def(-45.0_f64)
        );
        assert_eq!(call("CEILING", vec![def(0)]).unwrap(), def(0.0_f64));
    }

    #[test]
    fn non_numeric_is_undefined() {
        assert!(call("CEILING", vec![def("x")]).unwrap().is_undefined());
    }
}
