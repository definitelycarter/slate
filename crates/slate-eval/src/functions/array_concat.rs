//! `ARRAY_CONCAT(arr1, arr2, …)` — concatenate two or more arrays. Any
//! non-array argument yields `Undefined`.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, into_array};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(arity_err(name, "at least 2"));
    }
    let mut out: Vec<Bson> = Vec::new();
    for a in args {
        match into_array(a) {
            Some(mut v) => out.append(&mut v),
            None => return Ok(Value::Undefined),
        }
    }
    Ok(Value::Defined(Bson::Array(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn strs(items: &[&str]) -> Bson {
        Bson::Array(items.iter().map(|s| Bson::String((*s).into())).collect())
    }

    #[test]
    fn cosmos_example() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/array-concat
        assert_eq!(
            call(
                "ARRAY_CONCAT",
                vec![
                    def(strs(&["backpacks", "daypacks"])),
                    def(strs(&["hippacks"]))
                ]
            )
            .unwrap(),
            def(strs(&["backpacks", "daypacks", "hippacks"]))
        );
    }

    #[test]
    fn non_array_is_undefined() {
        assert!(
            call("ARRAY_CONCAT", vec![def(strs(&["a"])), def("b")])
                .unwrap()
                .is_undefined()
        );
    }

    #[test]
    fn fewer_than_two_args_is_error() {
        assert!(call("ARRAY_CONCAT", vec![def(strs(&["a"]))]).is_err());
    }
}
