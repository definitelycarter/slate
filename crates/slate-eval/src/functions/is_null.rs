//! `IS_NULL(expr)` — whether the value is JSON null.
//!
//! A type-test function: returns a boolean rather than `Undefined`. Undefined
//! is not null.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(matches!(
        &args[0],
        Value::Defined(Bson::Null)
    ))))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;
    use bson::Bson;

    #[test]
    fn null_vs_undefined_vs_value() {
        assert_eq!(call("IS_NULL", vec![def(Bson::Null)]).unwrap(), def(true));
        assert_eq!(call("IS_NULL", vec![Value::Undefined]).unwrap(), def(false));
        assert_eq!(call("IS_NULL", vec![def(1)]).unwrap(), def(false));
    }
}
