use bson::RawBson;

use crate::error::DbError;
use crate::executor::RawIter;
use crate::expression::Expression;

pub(crate) fn execute<'a>(
    predicate: Expression,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.filter_map(move |result| match result {
        Err(e) => Some(Err(e)),
        Ok(Some(val)) => {
            let raw = match &val {
                RawBson::Document(d) => d.as_ref(),
                _ => {
                    return Some(Err(DbError::InvalidQuery("expected document".into())));
                }
            };
            match predicate.matches(raw) {
                Ok(true) => Some(Ok(Some(val))),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            }
        }
        Ok(None) => None,
    })))
}
