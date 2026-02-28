use std::borrow::Cow;
use std::cmp::Ordering;

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use slate_store::{Store, StoreError, Transaction};

use crate::encoding::bson_value::BsonValue;
use crate::encoding::index_record::is_index_expired;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::index_sync::{IndexChanges, IndexDiff};
use crate::traits::{CollectionHandle, EngineTransaction, IndexEntry, IndexRange};
use crate::validate::validate_raw_document;

// ── KvTransaction ──────────────────────────────────────────────

pub struct KvTransaction<'a, S: Store + 'a> {
    pub(crate) txn: S::Txn<'a>,
    pub(crate) now_millis: i64,
}

// ── Private helpers ─────────────────────────────────────────────

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    pub(crate) fn extract_pk(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        doc: &RawDocument,
    ) -> Result<BsonValue<'static>, EngineError> {
        match doc.get(handle.pk_path()) {
            Ok(Some(val)) => {
                let bv = BsonValue::from_raw_bson_ref(val).ok_or_else(|| {
                    EngineError::InvalidDocument("unsupported pk type".into())
                })?;
                Ok(BsonValue {
                    tag: bv.tag,
                    bytes: Cow::Owned(bv.bytes.into_owned()),
                })
            }
            _ => Err(EngineError::InvalidDocument(format!(
                "missing pk field '{}'",
                handle.pk_path()
            ))),
        }
    }

    pub(crate) fn apply_index_changes(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        changes: &IndexChanges,
    ) -> Result<(), EngineError> {
        if !changes.deletes.is_empty() {
            let refs: Vec<&[u8]> = changes.deletes.iter().map(|k| k.as_slice()).collect();
            self.txn.delete_batch(handle.cf(), &refs)?;
        }
        if !changes.puts.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = changes
                .puts
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.txn.put_batch(handle.cf(), &refs)?;
        }
        Ok(())
    }
}

// ── EngineTransaction impl ──────────────────────────────────────

impl<'a, S: Store + 'a> EngineTransaction for KvTransaction<'a, S> {
    type Cf = <S::Txn<'a> as Transaction>::Cf;

    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(handle.name(), &doc_id);
        match self.txn.get(handle.cf(), &encoded)? {
            None => Ok(None),
            Some(data) if Record::is_expired(&data, self.now_millis) => Ok(None),
            Some(data) => Ok(Some(RawDocumentBuf::try_from(Record::from_bytes(data)?)?)),
        }
    }

    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError> {
        let doc_id = self.extract_pk(handle, doc)?;

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(handle.name(), &doc_id);
        let record = Record::encoder()
            .with_ttl_at_path(handle.ttl_path())
            .encode(doc);
        let old_data = self.txn.get(handle.cf(), &encoded_key)?;

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(handle.indexes())
            .with_property_path(handle.ttl_path())
            .diff(handle.name())?;

        self.apply_index_changes(handle, &changes)?;
        self.txn
            .put(handle.cf(), &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError> {
        let doc_id = self.extract_pk(handle, doc)?;

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(handle.name(), &doc_id);
        let old_data = self.txn.get(handle.cf(), &encoded_key)?;

        if let Some(ref data) = old_data
            && !Record::is_expired(data, self.now_millis)
        {
            return Err(EngineError::DuplicateKey(doc_id.to_string()));
        }

        let record = Record::encoder()
            .with_ttl_at_path(handle.ttl_path())
            .encode(doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(handle.indexes())
            .with_property_path(handle.ttl_path())
            .diff(handle.name())?;

        self.apply_index_changes(handle, &changes)?;
        self.txn
            .put(handle.cf(), &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<(), EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(handle.name(), &doc_id);

        let old_data = self.txn.get(handle.cf(), &encoded)?;
        if let Some(ref data) = old_data {
            let changes = IndexDiff::for_delete(&doc_id)
                .with_old_record(Some(data.as_slice()))
                .with_property_paths(handle.indexes())
                .with_property_path(handle.ttl_path())
                .diff(handle.name())?;

            self.apply_index_changes(handle, &changes)?;
            self.txn.delete(handle.cf(), &encoded)?;
        }

        Ok(())
    }

    fn scan<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<RawDocumentBuf, EngineError>> + 'b>,
        EngineError,
    > {
        let now = self.now_millis;
        let prefix = KeyPrefix::Record(Cow::Borrowed(handle.name())).encode();
        let iter = self.txn.scan_prefix(handle.cf(), &prefix)?;
        Ok(Box::new(iter.filter_map(move |result| match result {
            Err(e) => Some(Err(EngineError::Store(e))),
            Ok((_key_bytes, value_bytes)) => {
                if Record::is_expired(&value_bytes, now) {
                    return None;
                }
                match Record::from_bytes(value_bytes)
                    .and_then(RawDocumentBuf::try_from)
                {
                    Ok(doc) => Some(Ok(doc)),
                    Err(e) => Some(Err(e)),
                }
            }
        })))
    }

    fn scan_index<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        field: &str,
        range: IndexRange<'_>,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry, EngineError>> + 'b>, EngineError>
    {
        let ttl = self.now_millis;
        let collection = handle.name();

        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        let eq_encoded = match &range {
            IndexRange::Eq(val) => BsonValue::from_bson(val).map(|bv| bv.bytes.into_owned()),
            _ => None,
        };
        let prefix = match &eq_encoded {
            Some(bytes) => KeyPrefix::IndexValue(
                Cow::Borrowed(collection),
                Cow::Borrowed(field),
                bytes,
            )
            .encode(),
            None => field_prefix.clone(),
        };

        #[allow(clippy::type_complexity)]
        let bounds: Option<(Option<Vec<u8>>, bool, Option<Vec<u8>>, bool)> = match range {
            IndexRange::Range { lower, upper } => Some((
                lower
                    .as_ref()
                    .and_then(|(v, _)| BsonValue::from_bson(v).map(|bv| bv.bytes.into_owned())),
                lower.as_ref().is_some_and(|(_, incl)| *incl),
                upper
                    .as_ref()
                    .and_then(|(v, _)| BsonValue::from_bson(v).map(|bv| bv.bytes.into_owned())),
                upper.as_ref().is_some_and(|(_, incl)| *incl),
            )),
            _ => None,
        };

        #[allow(clippy::type_complexity)]
        let mut iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StoreError>> + 'b> =
            if reverse {
                self.txn.scan_prefix_rev(handle.cf(), &prefix)?
            } else {
                self.txn.scan_prefix(handle.cf(), &prefix)?
            };

        let mut done = false;

        Ok(Box::new(std::iter::from_fn(move || {
            if done {
                return None;
            }

            for result in iter.by_ref() {
                match result {
                    Err(e) => {
                        done = true;
                        return Some(Err(EngineError::Store(e)));
                    }
                    Ok((key_bytes, metadata_bytes)) => {
                        let record = match IndexRecord::from_pair(key_bytes, metadata_bytes) {
                            Some(r) => r,
                            None => {
                                done = true;
                                return Some(Err(EngineError::InvalidKey(
                                    "invalid index key".into(),
                                )));
                            }
                        };

                        if let Some((ref lower, lower_inc, ref upper, upper_inc)) = bounds {
                            let value_bytes = record.value_bytes();
                            if let Some(lb) = lower {
                                let cmp = value_bytes.cmp(lb.as_slice());
                                if cmp == Ordering::Less
                                    || (cmp == Ordering::Equal && !lower_inc)
                                {
                                    if reverse {
                                        done = true;
                                        return None;
                                    }
                                    continue;
                                }
                            }
                            if let Some(ub) = upper {
                                let cmp = value_bytes.cmp(ub.as_slice());
                                if cmp == Ordering::Greater
                                    || (cmp == Ordering::Equal && !upper_inc)
                                {
                                    if !reverse {
                                        done = true;
                                        return None;
                                    }
                                    continue;
                                }
                            }
                        }

                        if record.is_expired(ttl) {
                            continue;
                        }

                        let doc_id = match record.doc_id_bson() {
                            Some(v) => v,
                            None => continue,
                        };
                        let value = match record.value_bson() {
                            Some(v) => v,
                            None => continue,
                        };
                        let (_, metadata) = record.into_parts();

                        return Some(Ok(IndexEntry {
                            doc_id,
                            value,
                            metadata,
                        }));
                    }
                }
            }

            done = true;
            None
        })))
    }

    fn purge(&self, handle: &CollectionHandle<Self::Cf>) -> Result<u64, EngineError> {
        self.purge_before(handle, self.now_millis)
    }

    fn purge_before(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        as_of_millis: i64,
    ) -> Result<u64, EngineError> {
        let ttl_prefix = KeyPrefix::IndexField(
            Cow::Borrowed(handle.name()),
            Cow::Borrowed(handle.ttl_path()),
        )
        .encode();
        let iter = self.txn.scan_prefix(handle.cf(), &ttl_prefix)?;

        let mut expired_ids: Vec<BsonValue<'static>> = Vec::new();
        for result in iter {
            let (key_bytes, metadata_bytes) = result?;
            if !is_index_expired(&metadata_bytes, as_of_millis) {
                break;
            }
            let Some(record) = IndexRecord::from_pair(key_bytes, metadata_bytes) else {
                continue;
            };
            let id = record.doc_id();
            expired_ids.push(BsonValue {
                tag: id.tag,
                bytes: Cow::Owned(id.bytes.into_owned()),
            });
        }

        let mut deleted = 0u64;
        for doc_id in &expired_ids {
            let encoded = Key::encode_record_key(handle.name(), doc_id);
            let old_data = self.txn.get(handle.cf(), &encoded)?;
            if let Some(ref data) = old_data {
                let changes = IndexDiff::for_delete(doc_id)
                    .with_old_record(Some(data.as_slice()))
                    .with_property_paths(handle.indexes())
                    .with_property_path(handle.ttl_path())
                    .diff(handle.name())?;
                self.apply_index_changes(handle, &changes)?;
                self.txn.delete(handle.cf(), &encoded)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    fn commit(self) -> Result<(), EngineError> {
        Ok(self.txn.commit()?)
    }

    fn rollback(self) -> Result<(), EngineError> {
        Ok(self.txn.rollback()?)
    }
}
