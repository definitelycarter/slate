//! `INTBITXOR(num1, num2)` — bitwise exclusive OR. Both arguments must be
//! integers; a fractional or non-numeric argument yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        (Some(a), Some(b)) => Value::Defined(Bson::Int64(a ^ b)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("INTBITXOR", vec![def(56), def(100)]).unwrap(),
            def(92_i64)
        );
        assert!(
            call("INTBITXOR", vec![def(56), def(0.1)])
                .unwrap()
                .is_undefined()
        );
    }
}
