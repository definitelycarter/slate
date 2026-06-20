//! `INTMOD(num1, num2)` — integer remainder (truncated division). Both arguments
//! must be integers; modulo by zero (or a fractional/non-numeric arg) yields
//! undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        (Some(a), Some(b)) => match a.checked_rem(b) {
            Some(r) => Value::Defined(Bson::Int64(r)),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(call("INTMOD", vec![def(12), def(5)]).unwrap(), def(2_i64));
        assert_eq!(
            call("INTMOD", vec![def(-12), def(-5)]).unwrap(),
            def(-2_i64)
        );
        assert!(
            call("INTMOD", vec![def(12), def(0)])
                .unwrap()
                .is_undefined()
        );
    }
}
