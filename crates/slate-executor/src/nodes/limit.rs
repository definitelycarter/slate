//! The `Limit` node — `OFFSET <skip> LIMIT <take>`.
//!
//! A lazy transform: skip `skip` rows, then take at most `take`. It counts
//! every streamed row (matching v1); whether undefined rows count is a function
//! of where the planner places `Limit` relative to `Project` — a lowering
//! concern, not this node's.

use crate::ValueIter;

/// Skip `skip` rows, then take at most `take` (unbounded when `None`).
pub(crate) fn execute<'a>(
    skip: usize,
    take: Option<usize>,
    source: ValueIter<'a>,
) -> ValueIter<'a> {
    let skipped = source.skip(skip);
    match take {
        Some(n) => Box::new(skipped.take(n)),
        None => Box::new(skipped),
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::collect;
    use crate::nodes::values;
    use bson::RawBson;

    fn nums(n: i32) -> Vec<RawBson> {
        (0..n).map(RawBson::Int32).collect()
    }

    fn limited(skip: usize, take: Option<usize>, n: i32) -> Vec<RawBson> {
        collect(execute(skip, take, values::execute(nums(n)))).unwrap()
    }

    #[test]
    fn take_only() {
        assert_eq!(
            limited(0, Some(2), 5),
            vec![RawBson::Int32(0), RawBson::Int32(1)]
        );
    }

    #[test]
    fn skip_only() {
        assert_eq!(
            limited(3, None, 5),
            vec![RawBson::Int32(3), RawBson::Int32(4)]
        );
    }

    #[test]
    fn skip_and_take() {
        assert_eq!(
            limited(1, Some(2), 5),
            vec![RawBson::Int32(1), RawBson::Int32(2)]
        );
    }

    #[test]
    fn take_past_end_is_clamped() {
        assert_eq!(limited(4, Some(10), 5), vec![RawBson::Int32(4)]);
    }

    #[test]
    fn skip_past_end_is_empty() {
        assert!(limited(10, None, 5).is_empty());
    }
}
