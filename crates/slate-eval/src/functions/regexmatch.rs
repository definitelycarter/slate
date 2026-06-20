//! `REGEXMATCH(str, pattern)` — whether a string matches a regular expression.
//!
//! Inline flags (e.g. `(?i)`) are honored. A non-string argument or an invalid
//! pattern yields `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    Ok(match (str_arg(&args[0]), str_arg(&args[1])) {
        (Some(s), Some(pat)) => match regex::Regex::new(pat) {
            Ok(re) => Value::Defined(Bson::Boolean(re.is_match(s))),
            Err(_) => Value::Undefined,
        },
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn matches_pattern() {
        assert_eq!(
            call("REGEXMATCH", vec![def("admin@x"), def("^admin")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("REGEXMATCH", vec![def("user@x"), def("^admin")]).unwrap(),
            def(false)
        );
    }

    #[test]
    fn honors_inline_flags() {
        assert_eq!(
            call("REGEXMATCH", vec![def("ADMIN"), def("(?i)^admin")]).unwrap(),
            def(true)
        );
    }

    #[test]
    fn non_string_or_invalid_pattern_is_undefined() {
        assert!(
            call("REGEXMATCH", vec![def(1), def("x")])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("REGEXMATCH", vec![def("x"), def("[")])
                .unwrap()
                .is_undefined()
        );
    }
}
