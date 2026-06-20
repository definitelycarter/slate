//! `ENDSWITH(str, suffix [, ignoreCase])` — whether a string ends with a
//! suffix. An optional third argument requests a case-insensitive search.

use crate::error::Result;
use crate::value::Value;

use super::{arity_2_or_3, str_match};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity_2_or_3(name, &args)?;
    Ok(str_match(&args, |s, suffix| s.ends_with(suffix)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/endswith
        assert_eq!(
            call("ENDSWITH", vec![def("AdventureWorks"), def("Adventure")]).unwrap(),
            def(false)
        );
        assert_eq!(
            call("ENDSWITH", vec![def("AdventureWorks"), def("Works")]).unwrap(),
            def(true)
        );
        assert_eq!(
            call("ENDSWITH", vec![def("AdventureWorks"), def("works")]).unwrap(),
            def(false)
        );
        assert_eq!(
            call(
                "ENDSWITH",
                vec![def("AdventureWorks"), def("works"), def(true)]
            )
            .unwrap(),
            def(true)
        );
    }

    #[test]
    fn non_string_is_undefined() {
        assert!(
            call("ENDSWITH", vec![def(1), def("a")])
                .unwrap()
                .is_undefined()
        );
    }
}
