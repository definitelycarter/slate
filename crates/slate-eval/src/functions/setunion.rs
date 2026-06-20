//! `SETUNION(arr1, arr2)` — the set of all values from both arrays, with no
//! duplicates. Element order follows the first array, then new values from the
//! second. Matches Cosmos.

use bson::Bson;

use crate::error::Result;
use crate::value::Value;

use super::{arity, contains_eq, into_array};

pub(super) fn eval(name: &str, args: Vec<Value>) -> Result<Value> {
    arity(name, &args, 2)?;
    let mut it = args.into_iter();
    let (Some(arr1), Some(arr2)) = (
        it.next().and_then(into_array),
        it.next().and_then(into_array),
    ) else {
        return Ok(Value::Undefined);
    };
    let mut out: Vec<Bson> = Vec::new();
    for b in arr1.into_iter().chain(arr2) {
        if !contains_eq(&out, &b) {
            out.push(b);
        }
    }
    Ok(Value::Defined(Bson::Array(out)))
}

#[cfg(test)]
mod tests {
    use super::super::{call, def};
    use bson::Bson;

    fn nums(v: &[i32]) -> Bson {
        Bson::Array(v.iter().map(|&n| Bson::Int32(n)).collect())
    }

    #[test]
    fn cosmos_examples() {
        // https://learn.microsoft.com/en-us/cosmos-db/query/setunion
        assert_eq!(
            call(
                "SETUNION",
                vec![def(nums(&[1, 2, 3, 4])), def(nums(&[3, 4, 5, 6]))]
            )
            .unwrap(),
            def(nums(&[1, 2, 3, 4, 5, 6]))
        );
        assert_eq!(
            call("SETUNION", vec![def(nums(&[1, 2, 3, 4])), def(nums(&[]))]).unwrap(),
            def(nums(&[1, 2, 3, 4]))
        );
        assert_eq!(
            call(
                "SETUNION",
                vec![def(nums(&[1, 2, 3, 4])), def(nums(&[1, 1, 1, 1]))]
            )
            .unwrap(),
            def(nums(&[1, 2, 3, 4]))
        );
        // The second array adds nothing new: [1, 2, "A", "B"].
        let a1 = Bson::Array(vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::String("A".into()),
            Bson::String("B".into()),
        ]);
        let a2 = Bson::Array(vec![Bson::String("A".into()), Bson::Int32(1)]);
        let expected = Bson::Array(vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::String("A".into()),
            Bson::String("B".into()),
        ]);
        assert_eq!(
            call("SETUNION", vec![def(a1), def(a2)]).unwrap(),
            def(expected)
        );
    }
}
