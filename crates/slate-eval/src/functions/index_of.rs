//! `INDEX_OF(str, search [, start])` — the zero-based character index of the
//! first occurrence of `search` in `str`, or `-1` if not found.
//!
//! Indexing is by character (not byte), matching Cosmos. An optional third
//! argument sets the character position to start searching from.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, int_arg, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(arity_err(name, "2 or 3"));
    }
    let (Some(s), Some(sub)) = (str_arg(&args[0]), str_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    // An explicit non-numeric start is undefined; a missing one defaults to 0.
    let start = match args.get(2) {
        Some(v) => match int_arg(v) {
            Some(n) => n.max(0) as usize,
            None => return Ok(Value::Undefined),
        },
        None => 0,
    };

    let hay: Vec<char> = s.chars().collect();
    let needle: Vec<char> = sub.chars().collect();
    let found = if needle.is_empty() {
        start.min(hay.len()) as i64
    } else if needle.len() <= hay.len() {
        let mut idx = -1i64;
        for i in start..=(hay.len() - needle.len()) {
            if hay[i..i + needle.len()] == needle[..] {
                idx = i as i64;
                break;
            }
        }
        idx
    } else {
        -1
    };
    Ok(Value::Defined(Bson::Int64(found)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/index-of
        let s = "AdventureWorks";
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("A")]).unwrap(),
            def(0_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("s")]).unwrap(),
            def(13_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("Adventure")]).unwrap(),
            def(0_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("Works")]).unwrap(),
            def(9_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("tureW")]).unwrap(),
            def(5_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("Cosmos")]).unwrap(),
            def(-1_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("Works"), def(5)]).unwrap(),
            def(9_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("Adventure"), def(5)]).unwrap(),
            def(-1_i64)
        );
        assert_eq!(
            call("INDEX_OF", vec![def(s), def("aD")]).unwrap(),
            def(-1_i64)
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("INDEX_OF", vec![def(1), def("a")])
                .unwrap()
                .is_undefined()
        );
    }
}
