//! `TRUNC(num)` — `num` truncated toward zero to the closest integer.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.trunc())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/trunc
        assert_eq!(call("TRUNC", vec![def(2.37_f64)]).unwrap(), def(2.0_f64));
        assert_eq!(call("TRUNC", vec![def(-2.78_f64)]).unwrap(), def(-2.0_f64));
        assert_eq!(call("TRUNC", vec![def(2)]).unwrap(), def(2.0_f64));
        assert_eq!(
            call("TRUNC", vec![def(0.0000714_f64)]).unwrap(),
            def(0.0_f64)
        );
        // TRUNC(PI()) — π truncates to 3.
        assert_eq!(
            call("TRUNC", vec![call("PI", vec![]).unwrap()]).unwrap(),
            def(3.0_f64)
        );
    }
}
