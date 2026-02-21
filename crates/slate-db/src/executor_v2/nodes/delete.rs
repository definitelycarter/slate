use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::RawIter;
use crate::executor::exec;

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        if let Some(ref val) = opt_val {
            if let Some(raw) = val.as_document() {
                if let Some(id_str) = exec::raw_extract_id(raw)? {
                    let key = encoding::record_key(id_str);
                    txn.delete(cf, &key)?;
                }
            }
        }
        Ok(None)
    })))
}
