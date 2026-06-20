//! `CONTAINS(str, substr [, ignoreCase])` — whether the first string contains
//! the second. An optional third argument requests a case-insensitive search.

use crate::error::Result;
use crate::value::Value;

use super::{arity_2_or_3, str_match};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    Ok(str_match(&args, |s, sub| s.contains(sub)))
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
    fn ignore_case_flag() {
        assert_eq!(
            call("CONTAINS", vec![def("Hello"), def("ELL")]).unwrap(),
            def(false)
        );
        assert_eq!(
            call("CONTAINS", vec![def("Hello"), def("ELL"), def(true)]).unwrap(),
            def(true)
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
