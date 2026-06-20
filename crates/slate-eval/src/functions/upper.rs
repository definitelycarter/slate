//! `UPPER(str)` — convert a string to uppercase.

use crate::error::Result;
use crate::value::Value;

use super::{arity, map_str};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(map_str(&args[0], |s| s.to_uppercase()))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn uppercases() {
        assert_eq!(call("UPPER", vec![def("aBc")]).unwrap(), def("ABC"));
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("UPPER", vec![def(1)]).unwrap().is_undefined());
    }
}
