//! `SUBSTRING(str, start, length)` — a portion of a string.
//!
//! `start` is a zero-based character position and `length` is a character
//! count, matching Cosmos. A negative `length` yields an empty string; a
//! `start` at or past the end yields an empty string.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, int_arg, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 3)?;
    let (Some(s), Some(start), Some(len)) =
        (str_arg(&args[0]), int_arg(&args[1]), int_arg(&args[2]))
    else {
        return Ok(Value::Undefined);
    };
    if len < 0 {
        return Ok(Value::Defined(Bson::String(String::new())));
    }
    let chars: Vec<char> = s.chars().collect();
    let start = start.max(0) as usize;
    let out: String = if start >= chars.len() {
        String::new()
    } else {
        let end = (start + len as usize).min(chars.len());
        chars[start..end].iter().collect()
    };
    Ok(Value::Defined(Bson::String(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/substring
        let s = "AdventureWorks";
        assert_eq!(
            call("SUBSTRING", vec![def(s), def(0), def(9)]).unwrap(),
            def("Adventure")
        );
        assert_eq!(
            call("SUBSTRING", vec![def(s), def(9), def(5)]).unwrap(),
            def("Works")
        );
        assert_eq!(
            call("SUBSTRING", vec![def(s), def(0), def(14)]).unwrap(),
            def("AdventureWorks")
        );
        // Negative length yields an empty string.
        assert_eq!(
            call("SUBSTRING", vec![def(s), def(0), def(-1)]).unwrap(),
            def("")
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("SUBSTRING", vec![def(1), def(0), def(2)])
                .unwrap()
                .is_undefined()
        );
    }
}
