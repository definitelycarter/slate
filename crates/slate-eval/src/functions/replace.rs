//! `REPLACE(str, old, new)` — replace every occurrence of `old` with `new`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 3)?;
    Ok(
        match (str_arg(&args[0]), str_arg(&args[1]), str_arg(&args[2])) {
            (Some(s), Some(old), Some(new)) => Value::Defined(Bson::String(s.replace(old, new))),
            _ => Value::Undefined,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/replace
        assert_eq!(
            call(
                "REPLACE",
                vec![def("AdventureWorksLT"), def("LT"), def("LT2")]
            )
            .unwrap(),
            def("AdventureWorksLT2")
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("REPLACE", vec![def(1), def("a"), def("b")])
                .unwrap()
                .is_undefined()
        );
    }
}
