//! `STRINGTONULL(str)` — parse a string to null. Surrounding whitespace is
//! ignored; the match is case-sensitive (`"null"` only). Anything else (or a
//! non-string) yields undefined.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 1)?;
    Ok(match str_arg(&args[0]).map(str::trim) {
        Some("null") => Value::Defined(Bson::Null),
        _ => Value::Undefined,
    })
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    #[test]
    fn cosmos_examples() {
        assert_eq!(
            call("STRINGTONULL", vec![def("  null  ")]).unwrap(),
            def(Bson::Null)
        );
        // case-sensitive: "NULL"/"Null" do not match
        assert!(
            call("STRINGTONULL", vec![def("NULL")])
                .unwrap()
                .is_undefined()
        );
    }
}
