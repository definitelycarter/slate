//! `STRINGSPLIT(string, delimiter)` — split a string into an array of substrings
//! on each occurrence of the delimiter. A non-string argument yields undefined.
//! An empty delimiter returns the whole string as a single element (matching
//! Cosmos — it does *not* split into characters).

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let (Some(s), Some(delim)) = (str_arg(&args[0]), str_arg(&args[1])) else {
        return Ok(Value::Undefined);
    };
    let parts: Vec<Bson> = if delim.is_empty() {
        vec![Bson::String(s.to_string())]
    } else {
        s.split(delim)
            .map(|p| Bson::String(p.to_string()))
            .collect()
    };
    Ok(Value::Defined(Bson::Array(parts)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn arr(items: &[&str]) -> crate::value::Value {
        def(Bson::Array(
            items.iter().map(|s| Bson::String((*s).into())).collect(),
        ))
    }

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGSPLIT", vec![def("a,b,c"), def(",")]).unwrap(),
            arr(&["a", "b", "c"])
        );
        assert_eq!(
            call("STRINGSPLIT", vec![def("a"), def(",")]).unwrap(),
            arr(&["a"])
        );
        // empty input → one empty element
        assert_eq!(
            call("STRINGSPLIT", vec![def(""), def(",")]).unwrap(),
            arr(&[""])
        );
        // consecutive delimiters preserve empty fields
        assert_eq!(
            call("STRINGSPLIT", vec![def("a,,b"), def(",")]).unwrap(),
            arr(&["a", "", "b"])
        );
        // empty delimiter → whole string, not per-character
        assert_eq!(
            call("STRINGSPLIT", vec![def("abc"), def("")]).unwrap(),
            arr(&["abc"])
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("STRINGSPLIT", vec![def(5), def(",")])
                .unwrap()
                .is_undefined()
        );
        assert!(
            call("STRINGSPLIT", vec![def("a,b"), def(5)])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn wrong_arity_is_error() {
        assert!(call("STRINGSPLIT", vec![def("a,b")]).is_err());
    }
}
