//! `INTBITAND(num1, num2)` — bitwise AND. Both arguments must be integers; a
//! fractional or non-numeric argument yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        (Some(a), Some(b)) => Value::Defined(Bson::Int64(a & b)),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("INTBITAND", vec![def(15), def(25)]).unwrap(),
            def(9_i64)
        );
        assert!(
            call("INTBITAND", vec![def(15), def(1.5)])
                .unwrap()
                .is_undefined()
        );
    }
}
