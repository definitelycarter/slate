use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_store::Transaction;

use crate::encoding;
use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: Transaction + 'a>(
    txn: &'a T,
    cf: &'a T::Cf,
    replacement: RawDocumentBuf,
    source: RawIter<'a>,
) -> Result<RawIter<'a>, DbError> {
    let replacement_raw = replacement;

    Ok(Box::new(source.map(move |result| {
        let opt_val = result?;
        let id_str = match &opt_val {
            Some(val) => match val.as_document() {
                Some(raw) => exec::raw_extract_id(raw)?.map(str::to_string),
                None => None,
            },
            None => None,
        };

        if let Some(ref id) = id_str {
            let mut buf = RawDocumentBuf::new();
            buf.append("_id", id.as_str());
            for entry in replacement_raw.iter() {
                let (k, v) = entry?;
                if k != "_id" {
                    buf.append_ref(k, v);
                }
            }
            let key = encoding::record_key(id);
            txn.put(cf, &key, &encoding::encode_record(buf.as_bytes()))?;
            Ok(Some(RawValue::Owned(RawBson::Document(buf))))
        } else {
            Ok(None)
        }
    })))
}
