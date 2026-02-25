use bson::RawBson;
use bson::raw::RawDocumentBuf;
use slate_engine::{BsonValue, CollectionHandle, EngineTransaction};

use crate::error::DbError;
use crate::executor::exec;
use crate::executor::{RawIter, RawValue};

pub(crate) fn execute<'a, T: EngineTransaction + 'a>(
    txn: &'a T,
    handle: &'a CollectionHandle<T::Cf>,
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
            let doc_id = BsonValue::from_raw_bson_ref(bson::raw::RawBsonRef::String(id))
                .expect("string is always a valid BsonValue");
            txn.put(handle, &buf, &doc_id)?;
            Ok(Some(RawValue::Owned(RawBson::Document(buf))))
        } else {
            Ok(None)
        }
    })))
}
