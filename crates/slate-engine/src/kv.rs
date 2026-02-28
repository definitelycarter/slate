use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use slate_store::{Store, StoreError, Transaction};

use crate::encoding::bson_value::BsonValue;
use crate::encoding::index_record::is_index_expired;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::index_sync::{IndexChanges, IndexDiff};
use crate::traits::{
    Catalog, CollectionConfig, CollectionHandle, EngineTransaction, IndexEntry, IndexRange,
};
use crate::validate::validate_raw_document;

pub const SYS_CF: &str = "_sys_";
const DEFAULT_CF: &str = "default_cf";

fn default_clock() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ── KvEngine ───────────────────────────────────────────────────

pub struct KvEngine<S> {
    store: S,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<S: Store> KvEngine<S> {
    pub fn new(store: S) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            clock: Arc::new(default_clock),
        }
    }

    pub fn with_clock(store: S, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        let _ = store.create_cf(SYS_CF);
        Self {
            store,
            clock: Arc::new(clock),
        }
    }
}

impl<S: Store> crate::traits::Engine for KvEngine<S> {
    type Txn<'a>
        = KvTransaction<'a, S>
    where
        S: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, EngineError> {
        let now_millis = (self.clock)();
        let txn = self.store.begin(read_only)?;
        Ok(KvTransaction { txn, now_millis })
    }
}

pub struct KvTransaction<'a, S: Store + 'a> {
    txn: S::Txn<'a>,
    now_millis: i64,
}

impl<'a, S: Store + 'a> EngineTransaction for KvTransaction<'a, S> {
    type Cf = <S::Txn<'a> as Transaction>::Cf;

    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(&handle.name, &doc_id);
        match self.txn.get(&handle.cf, &encoded)? {
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
        let doc_id = match doc.get("_id") {
            Ok(Some(val)) => BsonValue::from_raw_bson_ref(val)
                .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?,
            _ => return Err(EngineError::InvalidDocument("missing _id".into())),
        };

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(&handle.name, &doc_id);
        let record = Record::encoder().with_ttl_at_path("ttl").encode(doc);
        let old_data = self.txn.get(&handle.cf, &encoded_key)?;

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(&handle.indexes)
            .with_property_path("ttl")
            .diff(&handle.name)?;

        self.apply_index_changes(&handle.cf, &changes)?;
        self.txn.put(&handle.cf, &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
    ) -> Result<(), EngineError> {
        let doc_id = match doc.get("_id") {
            Ok(Some(val)) => BsonValue::from_raw_bson_ref(val)
                .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?,
            _ => return Err(EngineError::InvalidDocument("missing _id".into())),
        };

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(&handle.name, &doc_id);
        let old_data = self.txn.get(&handle.cf, &encoded_key)?;

        if let Some(ref data) = old_data
            && !Record::is_expired(data, self.now_millis)
        {
            return Err(EngineError::DuplicateKey(doc_id.to_string()));
        }

        let record = Record::encoder().with_ttl_at_path("ttl").encode(doc);

        let changes = IndexDiff::new(&record, &doc_id)
            .with_old_record(old_data.as_deref())
            .with_property_paths(&handle.indexes)
            .with_property_path("ttl")
            .diff(&handle.name)?;

        self.apply_index_changes(&handle.cf, &changes)?;
        self.txn.put(&handle.cf, &encoded_key, record.as_bytes())?;

        Ok(())
    }

    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
    ) -> Result<(), EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(&handle.name, &doc_id);

        let old_data = self.txn.get(&handle.cf, &encoded)?;
        if let Some(ref data) = old_data {
            let changes = IndexDiff::for_delete(&doc_id)
                .with_old_record(Some(data.as_slice()))
                .with_property_paths(&handle.indexes)
                .with_property_path("ttl")
                .diff(&handle.name)?;

            self.apply_index_changes(&handle.cf, &changes)?;
            self.txn.delete(&handle.cf, &encoded)?;
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
        let prefix = KeyPrefix::Record(Cow::Borrowed(&handle.name)).encode();
        let iter = self.txn.scan_prefix(&handle.cf, &prefix)?;
        Ok(Box::new(iter.filter_map(move |result| match result {
            Err(e) => Some(Err(EngineError::Store(e))),
            Ok((_key_bytes, value_bytes)) => {
                if Record::is_expired(&value_bytes, now) {
                    return None;
                }
                match Record::from_bytes(value_bytes)
                    .and_then(|r| RawDocumentBuf::try_from(r))
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
        let collection = &handle.name;

        // Build scan prefix based on range type.
        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        let eq_encoded = match &range {
            IndexRange::Eq(val) => {
                BsonValue::from_bson(val).map(|bv| bv.bytes.into_owned())
            }
            _ => None,
        };
        let prefix = match &eq_encoded {
            Some(bytes) => {
                KeyPrefix::IndexValue(
                    Cow::Borrowed(collection),
                    Cow::Borrowed(field),
                    bytes,
                )
                .encode()
            }
            None => field_prefix.clone(),
        };

        // Pre-encode range bounds for filtering (raw value bytes, no type tag).
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
                self.txn.scan_prefix_rev(&handle.cf, &prefix)?
            } else {
                self.txn.scan_prefix(&handle.cf, &prefix)?
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

                        // Range filtering on raw value bytes
                        if let Some((ref lower, lower_inc, ref upper, upper_inc)) = bounds {
                            let value_bytes = record.value_bytes();
                            if let Some(lb) = lower {
                                let cmp = value_bytes.cmp(lb.as_slice());
                                if cmp == Ordering::Less || (cmp == Ordering::Equal && !lower_inc) {
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
        let ttl_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(&handle.name), Cow::Borrowed("ttl")).encode();
        let iter = self.txn.scan_prefix(&handle.cf, &ttl_prefix)?;

        // Collect expired doc_ids. TTL index entries are sorted chronologically,
        // so we can stop as soon as we hit a non-expired entry.
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
            let encoded = Key::encode_record_key(&handle.name, doc_id);
            let old_data = self.txn.get(&handle.cf, &encoded)?;
            if let Some(ref data) = old_data {
                let changes = IndexDiff::for_delete(doc_id)
                    .with_old_record(Some(data.as_slice()))
                    .with_property_paths(&handle.indexes)
                    .with_property_path("ttl")
                    .diff(&handle.name)?;
                self.apply_index_changes(&handle.cf, &changes)?;
                self.txn.delete(&handle.cf, &encoded)?;
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

// ── Catalog impl ───────────────────────────────────────────────

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    fn sys_cf(&self) -> Result<<S::Txn<'a> as Transaction>::Cf, EngineError> {
        Ok(self.txn.cf(SYS_CF)?)
    }

    /// Apply index changes: delete removed entries, write new entries.
    fn apply_index_changes(
        &self,
        cf: &<S::Txn<'a> as Transaction>::Cf,
        changes: &IndexChanges,
    ) -> Result<(), EngineError> {
        if !changes.deletes.is_empty() {
            let refs: Vec<&[u8]> = changes.deletes.iter().map(|k| k.as_slice()).collect();
            self.txn.delete_batch(cf, &refs)?;
        }
        if !changes.puts.is_empty() {
            let refs: Vec<(&[u8], &[u8])> = changes
                .puts
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            self.txn.put_batch(cf, &refs)?;
        }
        Ok(())
    }

    /// Delete all keys under a prefix.
    fn delete_prefix(
        &self,
        cf: &<S::Txn<'a> as Transaction>::Cf,
        prefix: &[u8],
    ) -> Result<(), EngineError> {
        let keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(cf, prefix)?
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<_, _>>()?;
        for k in keys {
            self.txn.delete(cf, &k)?;
        }
        Ok(())
    }

    /// Point-lookup the CF name for a collection.
    /// Returns `CollectionNotFound` if the collection doesn't exist.
    fn resolve_collection_cf(&self, name: &str) -> Result<String, EngineError> {
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(name)).encode();
        match self.txn.get(&sys, &key)? {
            Some(value) => {
                let cf = std::str::from_utf8(&value)
                    .map_err(|e| EngineError::InvalidDocument(format!("invalid cf name: {e}")))?;
                Ok(cf.to_string())
            }
            None => Err(EngineError::CollectionNotFound(name.to_string())),
        }
    }

    /// Load index field names for a collection from the sys CF.
    fn load_indexes(&self, collection: &str) -> Result<Vec<String>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = KeyPrefix::IndexConfig(Cow::Borrowed(collection)).encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut fields = Vec::new();
        for result in iter {
            let (key_bytes, _) = result?;
            let key = Key::decode(&key_bytes);
            if let Some(Key::IndexConfig(_, field)) = key {
                fields.push(field.into_owned());
            }
        }
        Ok(fields)
    }
}

impl<'a, S: Store + 'a> Catalog for KvTransaction<'a, S> {
    fn collection(&self, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError> {
        let cf_name = self.resolve_collection_cf(name)?;
        let indexes = self.load_indexes(name)?;
        let cf = self.txn.cf(&cf_name)?;
        Ok(CollectionHandle {
            name: name.to_string(),
            cf,
            indexes,
        })
    }

    fn list_collections(&self) -> Result<Vec<CollectionConfig>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = KeyPrefix::Collection.encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut configs = Vec::new();
        for result in iter {
            let (key_bytes, value) = result?;
            if let Some(Key::Collection(name)) = Key::decode(&key_bytes) {
                let cf = std::str::from_utf8(&value)
                    .map_err(|e| EngineError::InvalidDocument(format!("invalid cf name: {e}")))?;
                let indexes = self.load_indexes(&name)?;
                configs.push(CollectionConfig {
                    name: name.into_owned(),
                    cf: cf.to_string(),
                    indexes,
                });
            }
        }
        Ok(configs)
    }

    fn create_collection(&mut self, cf: Option<&str>, name: &str) -> Result<(), EngineError> {
        let cf = cf.unwrap_or(DEFAULT_CF);
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(name)).encode();
        if self.txn.get(&sys, &key)?.is_none() {
            self.txn.create_cf(cf)?;
            self.txn.put(&sys, &key, cf.as_bytes())?;
        }
        Ok(())
    }

    fn drop_collection(&mut self, name: &str) -> Result<(), EngineError> {
        let sys = self.sys_cf()?;
        let meta_key = Key::Collection(Cow::Borrowed(name)).encode();

        // Check if collection exists; if not, no-op.
        let cf_name = match self.txn.get(&sys, &meta_key)? {
            Some(value) => std::str::from_utf8(&value)
                .map_err(|e| EngineError::InvalidDocument(format!("invalid cf name: {e}")))?
                .to_string(),
            None => return Ok(()),
        };
        let cf = self.txn.cf(&cf_name)?;

        // Delete all records.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(name)).encode();
        self.delete_prefix(&cf, &record_prefix)?;

        // Delete all index entries (catalog indexes + ttl).
        let indexes = self.load_indexes(name)?;
        for field in &indexes {
            let idx_prefix =
                KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(field)).encode();
            self.delete_prefix(&cf, &idx_prefix)?;
        }
        let ttl_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed("ttl")).encode();
        self.delete_prefix(&cf, &ttl_prefix)?;

        // Delete all index config keys from _sys_.
        let idx_config_prefix = KeyPrefix::IndexConfig(Cow::Borrowed(name)).encode();
        self.delete_prefix(&sys, &idx_config_prefix)?;

        // Delete the collection metadata key.
        self.txn.delete(&sys, &meta_key)?;

        Ok(())
    }

    fn create_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError> {
        let cf_name = self.resolve_collection_cf(collection)?;
        let cf = self.txn.cf(&cf_name)?;

        // Write the index config key.
        let sys = self.sys_cf()?;
        let config_key =
            Key::IndexConfig(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.txn.put(&sys, &config_key, &[])?;

        // Backfill: scan all existing records and create index entries.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
        let records: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf, &record_prefix)?
            .collect::<Result<_, _>>()?;

        let indexes = vec![field.to_string()];
        for (key_bytes, value_bytes) in &records {
            let Some(Key::Record(_, doc_id)) = Key::decode(key_bytes) else {
                continue;
            };
            let record = Record::from_bytes(value_bytes.clone())?;
            let ttl = record.ttl_millis();
            let doc = record.doc()?;
            let entries =
                IndexRecord::from_document(collection, &indexes, doc, &doc_id, ttl);
            if !entries.is_empty() {
                let refs: Vec<(&[u8], &[u8])> = entries
                    .iter()
                    .map(|e| (e.key_bytes(), e.metadata()))
                    .collect();
                self.txn.put_batch(&cf, &refs)?;
            }
        }

        Ok(())
    }

    fn drop_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError> {
        let cf_name = self.resolve_collection_cf(collection)?;
        let cf = self.txn.cf(&cf_name)?;

        // Delete all index entries for this field.
        let idx_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.delete_prefix(&cf, &idx_prefix)?;

        // Delete the index config key from _sys_.
        let sys = self.sys_cf()?;
        let key = Key::IndexConfig(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.txn.delete(&sys, &key)?;

        Ok(())
    }
}
