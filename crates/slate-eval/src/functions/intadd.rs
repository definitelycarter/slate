//! `INTADD(num1, num2)` — integer sum. Both arguments must be integers; a
//! fractional or non-numeric argument yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_value};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (int_value(&args[0]), int_value(&args[1])) {
        (Some(a), Some(b)) => Value::Defined(Bson::Int64(a.wrapping_add(b))),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        assert_eq!(call("INTADD", vec![def(20), def(10)]).unwrap(), def(30_i64));
        assert!(
            call("INTADD", vec![def(20), def(0.10)])
                .unwrap()
                .is_undefined()
        );
    }
}
