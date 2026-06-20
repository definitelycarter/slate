//! `REGEXMATCH(str, pattern [, modifiers])` — whether a string matches a regular
//! expression.
//!
//! The optional `modifiers` string accepts the Cosmos flags `i` (case-insensitive),
//! `m` (multi-line), `s` (dot matches newline), and `x` (ignore whitespace), which
//! map directly to the regex crate's inline flags. Inline flags in the pattern
//! (e.g. `(?i)`) are also honored. A non-string argument, an unknown modifier, or
//! an invalid pattern yields `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_2_or_3, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    let (Some(s), Some(pat)) = (str_arg(&args[0]), str_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };

    // Fold the optional modifiers into an inline flag group `(?imsx)`.
    let pattern = match args.get(2) {
        None => pat.to_string(),
        Some(arg) => {
            let Some(mods) = str_arg(arg) else {
                return Ok(Value::Undefined);
            };
            if !mods.chars().all(|c| matches!(c, 'i' | 'm' | 's' | 'x')) {
                return Ok(Value::Undefined);
            }
            if mods.is_empty() {
                pat.to_string()
            } else {
                format!("(?{mods}){pat}")
            }
        }
    };

    Ok(match regex::Regex::new(&pattern) {
        Ok(re) => Value::Defined(Bson::Boolean(re.is_match(s))),
        Err(_) => Value::Undefined,
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
    fn modifiers_argument() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/regexmatch
        assert_eq!(
            call("REGEXMATCH", vec![def("abcd"), def("ABC"), def("i")]).unwrap(),
            def(true)
        );
        // ignore-whitespace flag
        assert_eq!(
            call("REGEXMATCH", vec![def("abcd"), def("ab c"), def("x")]).unwrap(),
            def(true)
        );
        // no modifiers, case-sensitive
        assert_eq!(
            call("REGEXMATCH", vec![def("abcd"), def("ABC")]).unwrap(),
            def(false)
        );
        // unknown modifier yields undefined
        assert!(
            call("REGEXMATCH", vec![def("a"), def("a"), def("z")])
                .unwrap()
                .is_undefined()
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
