//! `STARTSWITH(str, prefix)` — whether a string starts with a prefix.

use crate::error::Result;
use crate::value::Value;

use super::{arity, str2_bool};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(str2_bool(&args[0], &args[1], |s, p| s.starts_with(p)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn prefix_match() {
        assert_eq!(
            call("STARTSWITH", vec![def("hello"), def("he")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("STARTSWITH", vec![def("hello"), def("lo")]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("STARTSWITH", vec![def("hi"), def(1)])
                .unwrap()
                .is_undefined()
        );
    }
}
