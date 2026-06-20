//! `CHOOSE(index, v1, v2, …)` — the value at the given **one-based** index in
//! the list, or `Undefined` if the index is out of range. Matches Cosmos.

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, int_arg};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(arity_err(name, "at least 2"));
    }
    let Some(idx) = int_arg(&args[0]) else {
        return Ok(Value::Undefined);
    };
    // One-based: value `i` lives at position `i` (position 0 is the index arg).
    if idx < 1 || idx as usize >= args.len() {
        return Ok(Value::Undefined);
    }
    Ok(args
        .into_iter()
        .nth(idx as usize)
        .unwrap_or(Value::Undefined))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/choose
        assert_eq!(
            call(
                "CHOOSE",
                vec![def(1), def("Vimero"), def("Hydration"), def("Pack")]
            )
            .unwrap(),
            def("Vimero")
        );

        let opts = || vec![def("Mt."), def("Hood"), def("Hydration"), def("Pack")];
        let with_index = |i: i32| {
            let mut a = vec![def(i)];
            a.extend(opts());
            call("CHOOSE", a).unwrap()
        };
        assert!(with_index(0).is_undefined());
        assert_eq!(with_index(1), def("Mt."));
        assert_eq!(with_index(2), def("Hood"));
        assert_eq!(with_index(3), def("Hydration"));
        assert_eq!(with_index(4), def("Pack"));
        assert!(with_index(5).is_undefined());
    }
}
