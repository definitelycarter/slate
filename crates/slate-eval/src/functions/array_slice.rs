//! `ARRAY_SLICE(arr, start [, length])` — a subset of an array.
//!
//! `start` is zero-based; a negative `start` counts from the end. The optional
//! `length` caps the number of elements returned. Matches Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity_err, int_arg, into_array};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(arity_err(name, "2 or 3"));
    }
    let Some(start_raw) = int_arg(&args[1]) else {
        return Ok(Value::Undefined);
    };
    let length = match args.get(2) {
        Some(v) => match int_arg(v) {
            Some(l) => Some(l),
            None => return Ok(Value::Undefined),
        },
        None => None,
    };
    let Some(arr) = args.into_iter().next().and_then(into_array) else {
        return Ok(Value::Undefined);
    };

    let len = arr.len() as i64;
    let start = if start_raw < 0 {
        (len + start_raw).max(0)
    } else {
        start_raw.min(len)
    } as usize;
    let avail = arr.len() - start;
    let take = match length {
        Some(l) if l < 0 => 0,
        Some(l) => (l as usize).min(avail),
        None => avail,
    };
    let out: Vec<Bson> = arr.into_iter().skip(start).take(take).collect();
    Ok(Value::Defined(Bson::Array(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    const ITEMS: [&str; 7] = [
        "Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf",
    ];

    fn strs(items: &[&str]) -> Bson {
        Bson::Array(items.iter().map(|s| Bson::String((*s).into())).collect())
    }

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/array-slice
        let all = || def(strs(&ITEMS));
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(0)]).unwrap(),
            def(strs(&ITEMS))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(1)]).unwrap(),
            def(strs(&ITEMS[1..]))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(-1)]).unwrap(),
            def(strs(&["Golf"]))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(-2)]).unwrap(),
            def(strs(&["Foxtrot", "Golf"]))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(0), def(3)]).unwrap(),
            def(strs(&["Alpha", "Bravo", "Charlie"]))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(0), def(12)]).unwrap(),
            def(strs(&ITEMS))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(3), def(5)]).unwrap(),
            def(strs(&["Delta", "Echo", "Foxtrot", "Golf"]))
        );
        assert_eq!(
            call("ARRAY_SLICE", vec![all(), def(-2), def(1)]).unwrap(),
            def(strs(&["Foxtrot"]))
        );
    }

    #[test]
    fn non_array_is_undefined() {
        assert!(
            call("ARRAY_SLICE", vec![def("x"), def(0)])
                .unwrap()
                .is_undefined()
        );
    }
}
