//! `LENGTH(str)` — the number of characters in a string.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]) {
        Some(s) => Value::Defined(Bson::Int64(s.chars().count() as i64)),
        None => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn counts_chars() {
        assert_eq!(call("LENGTH", vec![def("hello")]).unwrap(), def(5_i64));
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("LENGTH", vec![def(1)]).unwrap().is_undefined());
    }
}
