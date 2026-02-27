use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use bson::raw::{RawBsonRef, RawDocument, RawDocumentBuf};
use slate_store::{Store, StoreError, Transaction};

use crate::encoding::bson_value::BsonValue;
use crate::encoding::{IndexMeta, Record};
use crate::error::EngineError;
use crate::index;
use crate::key::{Key, KeyPrefix};
use crate::traits::{
    Catalog, CollectionConfig, CollectionHandle, EngineTransaction, IndexEntry, IndexRange,
};
use crate::validate::validate_raw_document;

pub const SYS_CF: &str = "_sys_";
const DEFAULT_CF: &str = "default_cf";

// ── KvEngine ───────────────────────────────────────────────────

pub struct KvEngine<S> {
    store: S,
}

impl<S: Store> KvEngine<S> {
    pub fn new(store: S) -> Self {
        // Ensure _sys_ CF exists.
        let _ = store.create_cf(SYS_CF);
        Self { store }
    }
}

impl<S: Store> crate::traits::Engine for KvEngine<S> {
    type Txn<'a>
        = KvTransaction<'a, S>
    where
        S: 'a;

    fn begin(&self, read_only: bool) -> Result<Self::Txn<'_>, EngineError> {
        let txn = self.store.begin(read_only)?;
        Ok(KvTransaction { txn })
    }
}

pub struct KvTransaction<'a, S: Store + 'a> {
    txn: S::Txn<'a>,
}

impl<'a, S: Store + 'a> EngineTransaction for KvTransaction<'a, S> {
    type Cf = <S::Txn<'a> as Transaction>::Cf;

    fn get(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &RawBsonRef<'_>,
        ttl: i64,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        let doc_id = BsonValue::from_raw_bson_ref(*doc_id)
            .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?;
        let encoded = Key::encode_record_key(&handle.name, &doc_id);
        match self.txn.get(&handle.cf, &encoded)? {
            None => Ok(None),
            Some(data) if Record::is_expired(&data, ttl) => Ok(None),
            Some(data) => {
                let record = Record::decode_owned(data)?;
                Ok(Some(record.doc))
            }
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
        let encoded_value = Record::encode(doc);

        if handle.indexes.is_empty() {
            return Ok(self.txn.put(&handle.cf, &encoded_key, &encoded_value)?);
        }

        // Scan IndexMap to discover old index keys.
        let mut old: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let prefix = Key::encode_index_map_prefix(&handle.name, &doc_id);
        for result in self.txn.scan_prefix(&handle.cf, &prefix)? {
            let (map_key, index_key) = result?;
            old.insert(map_key, index_key);
        }

        let new_entries = index::index_entries(&handle.name, &handle.indexes, doc, &doc_id);

        let mut deletes: Vec<&[u8]> = Vec::new();
        let mut puts: Vec<(&[u8], &[u8])> = Vec::with_capacity(new_entries.len() + 1);

        for pair in new_entries.chunks(2) {
            let (map_key, _) = &pair[0];
            if old.remove(map_key.as_slice()).is_none() {
                for (k, v) in pair {
                    puts.push((k.as_slice(), v.as_slice()));
                }
            }
        }

        for (map_key, index_key) in &old {
            deletes.push(map_key.as_slice());
            deletes.push(index_key.as_slice());
        }

        if !deletes.is_empty() {
            self.txn.delete_batch(&handle.cf, &deletes)?;
        }
        puts.push((&encoded_key, &encoded_value));
        self.txn.put_batch(&handle.cf, &puts)?;

        Ok(())
    }

    fn put_nx(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
        ttl: i64,
    ) -> Result<(), EngineError> {
        let doc_id = match doc.get("_id") {
            Ok(Some(val)) => BsonValue::from_raw_bson_ref(val)
                .ok_or_else(|| EngineError::InvalidDocument("unsupported _id type".into()))?,
            _ => return Err(EngineError::InvalidDocument("missing _id".into())),
        };

        validate_raw_document(doc)?;

        let encoded_key = Key::encode_record_key(&handle.name, &doc_id);

        if let Some(data) = self.txn.get(&handle.cf, &encoded_key)? {
            if !Record::is_expired(&data, ttl) {
                return Err(EngineError::DuplicateKey(doc_id.to_string()));
            }
        }

        let encoded_value = Record::encode(doc);

        if handle.indexes.is_empty() {
            return Ok(self.txn.put(&handle.cf, &encoded_key, &encoded_value)?);
        }

        let new_entries = index::index_entries(&handle.name, &handle.indexes, doc, &doc_id);
        let mut puts: Vec<(&[u8], &[u8])> = new_entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        puts.push((&encoded_key, &encoded_value));
        self.txn.put_batch(&handle.cf, &puts)?;

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

        if handle.indexes.is_empty() {
            return Ok(self.txn.delete(&handle.cf, &encoded)?);
        }

        // Scan IndexMap to discover old index keys (no doc decode needed).
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let prefix = Key::encode_index_map_prefix(&handle.name, &doc_id);
        for result in self.txn.scan_prefix(&handle.cf, &prefix)? {
            let (map_key, index_key) = result?;
            keys.push(index_key);
            keys.push(map_key);
        }

        let mut refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        refs.push(&encoded);
        self.txn.delete_batch(&handle.cf, &refs)?;

        Ok(())
    }

    fn scan<'b>(
        &'b self,
        handle: &CollectionHandle<Self::Cf>,
        ttl: i64,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(BsonValue<'b>, RawDocumentBuf), EngineError>> + 'b>,
        EngineError,
    > {
        let prefix = KeyPrefix::Record(Cow::Borrowed(&handle.name)).encode();
        let iter = self.txn.scan_prefix(&handle.cf, &prefix)?;
        Ok(Box::new(iter.filter_map(move |result| match result {
            Err(e) => Some(Err(EngineError::Store(e))),
            Ok((key_bytes, value_bytes)) => {
                if Record::is_expired(&value_bytes, ttl) {
                    return None;
                }
                let key = match Key::decode(&key_bytes) {
                    Some(k) => k,
                    None => return Some(Err(EngineError::InvalidKey("invalid record key".into()))),
                };
                let Key::Record(_, doc_id) = key else {
                    return Some(Err(EngineError::InvalidKey("expected record key".into())));
                };
                match Record::decode_owned(value_bytes) {
                    Ok(record) => Some(Ok((doc_id.into_owned(), record.doc))),
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
        ttl: i64,
    ) -> Result<Box<dyn Iterator<Item = Result<IndexEntry<'b>, EngineError>> + 'b>, EngineError>
    {
        let collection = &handle.name;

        // Build scan prefix based on range type.
        let field_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        let prefix = match &range {
            IndexRange::Eq(value) => {
                KeyPrefix::IndexValue(Cow::Borrowed(collection), Cow::Borrowed(field), value)
                    .encode()
            }
            _ => field_prefix.clone(),
        };
        let field_prefix_len = field_prefix.len();

        // Pre-encode range bounds for filtering.
        let bounds: Option<(Option<Vec<u8>>, bool, Option<Vec<u8>>, bool)> = match &range {
            IndexRange::Range {
                lower,
                lower_inclusive,
                upper,
                upper_inclusive,
            } => Some((
                lower.map(|v| v.to_vec()),
                *lower_inclusive,
                upper.map(|v| v.to_vec()),
                *upper_inclusive,
            )),
            _ => None,
        };

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
                        let (value_bytes, doc_id) =
                            match Key::parse_index_tail(&key_bytes, field_prefix_len) {
                                Some(pair) => pair,
                                None => {
                                    done = true;
                                    return Some(Err(EngineError::InvalidKey(
                                        "missing doc_id separator".into(),
                                    )));
                                }
                            };

                        // Range filtering
                        if let Some((ref lower, lower_inc, ref upper, upper_inc)) = bounds {
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

                        if IndexMeta::is_expired(&metadata_bytes, ttl) {
                            continue;
                        }

                        return Some(Ok(IndexEntry {
                            doc_id: doc_id.into_owned(),
                            value_bytes: value_bytes.to_vec(),
                            metadata: metadata_bytes,
                        }));
                    }
                }
            }

            done = true;
            None
        })))
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
                    .map_err(|e| EngineError::Encoding(format!("invalid cf name: {e}")))?;
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
                    .map_err(|e| EngineError::Encoding(format!("invalid cf name: {e}")))?;
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
                .map_err(|e| EngineError::Encoding(format!("invalid cf name: {e}")))?
                .to_string(),
            None => return Ok(()),
        };
        let cf = self.txn.cf(&cf_name)?;

        // Delete all records.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(name)).encode();
        self.delete_prefix(&cf, &record_prefix)?;

        // Delete all index entries and index map entries.
        let indexes = self.load_indexes(name)?;
        for field in &indexes {
            let idx_prefix =
                KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(field)).encode();
            self.delete_prefix(&cf, &idx_prefix)?;
        }
        let map_prefix = KeyPrefix::IndexMapCollection(Cow::Borrowed(name)).encode();
        self.delete_prefix(&cf, &map_prefix)?;

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
        let config_key = Key::IndexConfig(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
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
            let record = Record::decode(value_bytes)?;
            let entries = index::index_entries(collection, &indexes, &record.doc, &doc_id);
            if !entries.is_empty() {
                let refs: Vec<(&[u8], &[u8])> = entries
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
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

        // Delete all index map entries for this field.
        let map_prefix = KeyPrefix::IndexMapCollection(Cow::Borrowed(collection)).encode();
        let map_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(&cf, &map_prefix)?
            .filter_map(|r| match r {
                Ok((k, _)) => match Key::decode(&k) {
                    Some(Key::IndexMap(_, f, _)) if f == field => Some(Ok(k)),
                    _ => None,
                },
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<_, _>>()?;
        for k in &map_keys {
            self.txn.delete(&cf, k)?;
        }

        // Delete the index config key from _sys_.
        let sys = self.sys_cf()?;
        let key = Key::IndexConfig(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.txn.delete(&sys, &key)?;

        Ok(())
    }
}
