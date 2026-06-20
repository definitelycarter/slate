//! `SETINTERSECT(arr1, arr2)` — the set of values present in both arrays, with
//! no duplicates. Element order follows the second array. Matches Cosmos.

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
    for b in arr2 {
        if contains_eq(&arr1, &b) && !contains_eq(&out, &b) {
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
        // https://learn.microsoft.com/en-us/cosmos-db/query/setintersect
        assert_eq!(
            call(
                "SETINTERSECT",
                vec![def(nums(&[1, 2, 3, 4])), def(nums(&[3, 4, 5, 6]))]
            )
            .unwrap(),
            def(nums(&[3, 4]))
        );
        assert_eq!(
            call(
                "SETINTERSECT",
                vec![def(nums(&[1, 2, 3, 4])), def(nums(&[]))]
            )
            .unwrap(),
            def(nums(&[]))
        );
        assert_eq!(
            call(
                "SETINTERSECT",
                vec![def(nums(&[1, 2, 3, 4])), def(nums(&[1, 1, 1, 1]))]
            )
            .unwrap(),
            def(nums(&[1]))
        );
        assert_eq!(
            call(
                "SETINTERSECT",
                vec![
                    def(nums(&[1, 2, 3, 4])),
                    def(Bson::Array(vec![
                        Bson::String("A".into()),
                        Bson::String("B".into())
                    ]))
                ]
            )
            .unwrap(),
            def(nums(&[]))
        );
        // Result order follows the second array: ["A", 1].
        let a1 = Bson::Array(vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::String("A".into()),
            Bson::String("B".into()),
        ]);
        let a2 = Bson::Array(vec![Bson::String("A".into()), Bson::Int32(1)]);
        let expected = Bson::Array(vec![Bson::String("A".into()), Bson::Int32(1)]);
        assert_eq!(
            call("SETINTERSECT", vec![def(a1), def(a2)]).unwrap(),
            def(expected)
        );
    }
}
