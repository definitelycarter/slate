//! `PI()` — the constant value of π.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::arity;

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 0)?;
    Ok(Value::Defined(Bson::Double(std::f64::consts::PI)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/pi
        assert_eq!(call("PI", vec![]).unwrap(), def(std::f64::consts::PI));
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("PI", vec![def(1)]).is_err());
    }
}
