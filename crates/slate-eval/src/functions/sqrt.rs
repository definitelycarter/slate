//! `SQRT(num)` — the square root of `num`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match f64_arg(&args[0]) {
        Some(f) => Value::Defined(Bson::Double(f.sqrt())),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/sqrt
        // sqrt is IEEE-correctly-rounded, so exact equality holds.
        assert_eq!(call("SQRT", vec![def(0)]).unwrap(), def(0.0_f64));
        assert_eq!(call("SQRT", vec![def(1)]).unwrap(), def(1.0_f64));
        assert_eq!(call("SQRT", vec![def(4)]).unwrap(), def(2.0_f64));
        assert_eq!(
            call("SQRT", vec![def(17)]).unwrap(),
            def(4.123105625617661_f64)
        );
        assert_eq!(call("SQRT", vec![def(25)]).unwrap(), def(5.0_f64));
    }
}
