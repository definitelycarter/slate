//! `RTRIM(str [, chars])` — remove trailing whitespace, or any character in the
//! given set, from the end of a string (a *set*, not a substring), matching
//! Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, str_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(arity_err(name, "1 or 2"));
    }
    let Some(s) = str_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    let out = match args.get(1) {
        Some(v) => match str_arg(v) {
            Some(set) => s.trim_end_matches(|c| set.contains(c)).to_string(),
            None => return Ok(Value::Undefined),
        },
        None => s.trim_end().to_string(),
    };
    Ok(Value::Defined(Bson::String(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/rtrim
        assert_eq!(
            call("RTRIM", vec![def("  AdventureWorks")]).unwrap(),
            def("  AdventureWorks")
        );
        assert_eq!(
            call("RTRIM", vec![def("  AdventureWorks  ")]).unwrap(),
            def("  AdventureWorks")
        );
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks  ")]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks")]).unwrap(),
            def("AdventureWorks")
        );
        // The trailing "Works" characters are all in the set.
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks"), def("Works")]).unwrap(),
            def("Adventure")
        );
        // Trailing 's' is not in {A,d,v,e,n,t,u,r}, so nothing is trimmed.
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks"), def("Adventure")]).unwrap(),
            def("AdventureWorks")
        );
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks"), def("AdventureWorks")]).unwrap(),
            def("")
        );
        assert_eq!(
            call("RTRIM", vec![def("AdventureWorks"), def("")]).unwrap(),
            def("AdventureWorks")
        );
    }
}
