use std::borrow::Cow;
use std::cmp::Ordering;

use bson::raw::{RawDocument, RawDocumentBuf};
use slate_store::{Store, StoreError, Transaction};

use crate::encoding::bson_value::{self, BsonValue};
use crate::encoding::{IndexMeta, Record};
use crate::error::EngineError;
use crate::key::{Key, KeyPrefix};
use crate::traits::{
    Catalog, CollectionConfig, CollectionHandle, EngineTransaction, IndexEntry, IndexRange,
};

pub const SYS_CF: &str = "_sys_";
const DEFAULT_CF: &str = "default";

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
        doc_id: &BsonValue<'_>,
    ) -> Result<Option<RawDocumentBuf>, EngineError> {
        let key = Key::Record(Cow::Borrowed(&handle.name), doc_id.clone());
        let encoded = key.encode();
        match self.txn.get(&handle.cf, &encoded)? {
            None => Ok(None),
            Some(data) => {
                let record = Record::decode(&data)?;
                Ok(Some(record.doc))
            }
        }
    }

    fn put(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc: &RawDocument,
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        let key = Key::Record(Cow::Borrowed(&handle.name), doc_id.clone());
        let encoded_key = key.encode();

        // Index maintenance: remove old entries, insert new ones.
        if !handle.indexes.is_empty() {
            let old_doc = match self.txn.get(&handle.cf, &encoded_key)? {
                Some(data) => Some(Record::decode(&data)?),
                None => None,
            };
            let new_ttl = Record::ttl_millis(doc);

            for field in &handle.indexes {
                let old_vals: Vec<Vec<u8>> = old_doc
                    .as_ref()
                    .map(|r| {
                        bson_value::extract_all(&r.doc, field)
                            .into_iter()
                            .map(|v| v.to_vec())
                            .collect()
                    })
                    .unwrap_or_default();
                let new_vals: Vec<(Vec<u8>, u8)> = bson_value::extract_all(doc, field)
                    .into_iter()
                    .map(|v| {
                        let bytes = v.to_vec();
                        (bytes, v.tag)
                    })
                    .collect();
                let new_bytes: Vec<&[u8]> = new_vals.iter().map(|(b, _)| b.as_slice()).collect();

                // Remove old index entries not present in new values.
                for ov in &old_vals {
                    if !new_bytes.contains(&ov.as_slice()) {
                        self.delete_index(handle, field, ov, doc_id)?;
                    }
                }

                // Insert new index entries not present in old values.
                for (nv_bytes, nv_tag) in &new_vals {
                    if !old_vals.iter().any(|ov| ov == nv_bytes) {
                        let meta = IndexMeta {
                            type_byte: *nv_tag,
                            ttl_millis: new_ttl,
                        };
                        self.put_index(handle, field, nv_bytes, doc_id, &meta.encode())?;
                    }
                }
            }
        }

        let encoded_value = Record::encode(doc);
        Ok(self.txn.put(&handle.cf, &encoded_key, &encoded_value)?)
    }

    fn delete(
        &self,
        handle: &CollectionHandle<Self::Cf>,
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        let key = Key::Record(Cow::Borrowed(&handle.name), doc_id.clone());
        let encoded = key.encode();

        // Index maintenance: remove entries for the deleted doc.
        if !handle.indexes.is_empty() {
            if let Some(data) = self.txn.get(&handle.cf, &encoded)? {
                let record = Record::decode(&data)?;
                for field in &handle.indexes {
                    for val in bson_value::extract_all(&record.doc, field) {
                        self.delete_index(handle, field, &val.to_vec(), doc_id)?;
                    }
                }
            }
        }

        Ok(self.txn.delete(&handle.cf, &encoded)?)
    }

    fn scan<'b>(
        &'b self,
        handle: &'b CollectionHandle<Self::Cf>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(BsonValue<'b>, RawDocumentBuf), EngineError>> + 'b>,
        EngineError,
    > {
        let prefix = KeyPrefix::Record(Cow::Borrowed(&handle.name)).encode();
        let iter = self.txn.scan_prefix(&handle.cf, &prefix)?;
        Ok(Box::new(iter.map(|result| {
            let (key_bytes, value_bytes) = result.map_err(EngineError::Store)?;
            let key = Key::decode(&key_bytes)
                .ok_or_else(|| EngineError::InvalidKey("invalid record key".into()))?;
            let Key::Record(_, doc_id) = key else {
                return Err(EngineError::InvalidKey("expected record key".into()));
            };
            let record = Record::decode(&value_bytes)?;
            Ok((doc_id.into_owned(), record.doc))
        })))
    }

    fn scan_index<'b>(
        &'b self,
        handle: &'b CollectionHandle<Self::Cf>,
        field: &str,
        range: IndexRange<'_>,
        reverse: bool,
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

        let mut iter: Box<
            dyn Iterator<Item = Result<(Cow<'b, [u8]>, Cow<'b, [u8]>), StoreError>> + 'b,
        > = if reverse {
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

                        return Some(Ok(IndexEntry {
                            doc_id: doc_id.into_owned(),
                            value_bytes: Cow::Owned(value_bytes.to_vec()),
                            metadata: Cow::Owned(metadata_bytes.into_owned()),
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
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for k in keys {
            self.txn.delete(cf, &k)?;
        }
        Ok(())
    }

    fn put_index(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        field: &str,
        value: &[u8],
        doc_id: &BsonValue<'_>,
        metadata: &[u8],
    ) -> Result<(), EngineError> {
        let key = Key::Index(
            Cow::Borrowed(&handle.name),
            Cow::Borrowed(field),
            doc_id.clone(),
        );
        let encoded = key.encode_index(value);
        Ok(self.txn.put(&handle.cf, &encoded, metadata)?)
    }

    fn delete_index(
        &self,
        handle: &CollectionHandle<<S::Txn<'a> as Transaction>::Cf>,
        field: &str,
        value: &[u8],
        doc_id: &BsonValue<'_>,
    ) -> Result<(), EngineError> {
        let key = Key::Index(
            Cow::Borrowed(&handle.name),
            Cow::Borrowed(field),
            doc_id.clone(),
        );
        let encoded = key.encode_index(value);
        Ok(self.txn.delete(&handle.cf, &encoded)?)
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

        // Delete all index entries for each indexed field.
        let indexes = self.load_indexes(name)?;
        for field in &indexes {
            let idx_prefix =
                KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(field)).encode();
            self.delete_prefix(&cf, &idx_prefix)?;
        }

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
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf, &record_prefix)?
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()?;

        for (key_bytes, value_bytes) in &entries {
            let Some(Key::Record(_, doc_id)) = Key::decode(key_bytes) else {
                continue;
            };
            let record = Record::decode(value_bytes)?;
            let handle = CollectionHandle {
                name: collection.to_string(),
                cf: cf.clone(),
                indexes: vec![],
            };
            for field_val in bson_value::extract_all(&record.doc, field) {
                let meta = IndexMeta {
                    type_byte: field_val.tag,
                    ttl_millis: record.ttl_millis,
                };
                self.put_index(&handle, field, &field_val.to_vec(), &doc_id, &meta.encode())?;
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
