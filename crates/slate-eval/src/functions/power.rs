//! `POWER(base, exp)` — `base` raised to the power `exp`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, f64_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (f64_arg(&args[0]), f64_arg(&args[1])) {
        (Some(base), Some(exp)) => Value::Defined(Bson::Double(base.powf(exp))),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/power
        assert_eq!(call("POWER", vec![def(1), def(1)]).unwrap(), def(1.0_f64));
        assert_eq!(call("POWER", vec![def(2), def(2)]).unwrap(), def(4.0_f64));
        assert_eq!(call("POWER", vec![def(3), def(3)]).unwrap(), def(27.0_f64));
        assert_eq!(call("POWER", vec![def(4), def(4)]).unwrap(), def(256.0_f64));
        assert_eq!(
            call("POWER", vec![def(5), def(5)]).unwrap(),
            def(3125.0_f64)
        );
        assert_eq!(call("POWER", vec![def(0), def(2)]).unwrap(), def(0.0_f64));
        // A null operand makes the result undefined (dropped from the object).
        assert!(
            call("POWER", vec![def(Bson::Null), def(3)])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("POWER", vec![def(2), def(Bson::Null)])
                .unwrap()
                .is_undefined()
        );
    }
}
