//! `IS_DEFINED(expr)` — whether the property has a value (not undefined).
//!
//! A type-test function: returns a boolean rather than `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(Value::Defined(Bson::Boolean(!args[0].is_undefined())))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use crate::value::Value;

    #[test]
    fn defined_vs_undefined() {
        assert_eq!(call("IS_DEFINED", vec![def(1)]).unwrap(), def(true));
        assert_eq!(
            call("IS_DEFINED", vec![Value::Undefined]).unwrap(),
            def(false)
        );
    }
}
