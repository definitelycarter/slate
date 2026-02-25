use bson::raw::{RawArrayBuf, RawBson};

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a>(
    skip: usize,
    take: Option<usize>,
    mut source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    // Check first item — if it's a single array (from Distinct), slice its elements.
    match source.next() {
        Some(Ok(Some(RawBson::Array(arr)))) => {
            let take_n = take.unwrap_or(usize::MAX);
            let mut buf = RawArrayBuf::new();
            for elem in arr.into_iter().skip(skip).take(take_n) {
                if let Ok(val) = elem {
                    exec::push_raw(&mut buf, val);
                }
            }
            Ok(Box::new(std::iter::once(Ok(Some(RawBson::Array(buf))))))
        }
        Some(first) => {
            let full = std::iter::once(first).chain(source);
            Ok(apply_limit(Box::new(full), skip, take))
        }
        None => Ok(Box::new(std::iter::empty())),
    }
}

fn apply_limit<'a, T: 'a>(
    iter: Box<dyn Iterator<Item = T> + 'a>,
    skip: usize,
    take: Option<usize>,
) -> Box<dyn Iterator<Item = T> + 'a> {
    let iter = iter.skip(skip);
    match take {
        Some(n) => Box::new(iter.take(n)),
        None => Box::new(iter),
    }
}
