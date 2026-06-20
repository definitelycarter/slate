//! `LOWER(str)` — convert a string to lowercase.

use crate::error::Result;
use crate::value::Value;

use super::{arity, map_str};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(map_str(&args[0], |s| s.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn lowercases() {
        assert_eq!(call("LOWER", vec![def("aBc")]).unwrap(), def("abc"));
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(call("LOWER", vec![def(true)]).unwrap().is_undefined());
    }
}
