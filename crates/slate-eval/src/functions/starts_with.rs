//! `STARTSWITH(str, prefix [, ignoreCase])` — whether a string starts with a
//! prefix. An optional third argument requests a case-insensitive search.

use crate::error::Result;
use crate::value::Value;

use super::{arity_2_or_3, str_match};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    Ok(str_match(&args, |s, p| s.starts_with(p)))
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
    fn ignore_case_flag() {
        assert_eq!(
            call("STARTSWITH", vec![def("Hello"), def("HE")]).unwrap(),
            def(false)
        );
        assert_eq!(
            call("STARTSWITH", vec![def("Hello"), def("HE"), def(true)]).unwrap(),
            def(true)
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
