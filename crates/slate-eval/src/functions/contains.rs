//! `CONTAINS(str, substr)` — whether the first string contains the second.

use crate::error::Result;
use crate::value::Value;

use super::{arity, str2_bool};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(str2_bool(&args[0], &args[1], |s, sub| s.contains(sub)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn substring_present() {
        assert_eq!(
            call("CONTAINS", vec![def("hello"), def("ell")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("CONTAINS", vec![def("hello"), def("xyz")]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("CONTAINS", vec![def(1), def("a")])
                .unwrap()
                .is_undefined()
        );
    }
}
