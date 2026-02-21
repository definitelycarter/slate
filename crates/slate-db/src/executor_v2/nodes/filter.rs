use slate_query::FilterGroup;

use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a>(
    predicate: &'a FilterGroup,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.filter_map(move |result| match result {
        Err(e) => Some(Err(e)),
        Ok(Some(val)) => {
            let raw = match val.as_document() {
                Some(r) => r,
                None => {
                    return Some(Err(DbError::InvalidQuery("expected document".into())));
                }
            };
            match exec::raw_matches_group(raw, predicate) {
                Ok(true) => Some(Ok(Some(val))),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            }
        }
        Ok(None) => None,
    })))
}
