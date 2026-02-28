use std::borrow::Cow;

use slate_store::{Store, Transaction};

use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::traits::{Catalog, CollectionHandle, CreateCollectionOptions};

use super::transaction::KvTransaction;
use super::{CollectionMeta, DEFAULT_CF, SYS_CF};

// ── Catalog helpers ─────────────────────────────────────────────

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    pub(crate) fn sys_cf(&self) -> Result<<S::Txn<'a> as Transaction>::Cf, EngineError> {
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

    /// Load collection metadata from the sys CF.
    pub(crate) fn load_collection_meta(&self, name: &str) -> Result<CollectionMeta, EngineError> {
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(name)).encode();
        let value = self
            .txn
            .get(&sys, &key)?
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
        bson::deserialize_from_slice(&value)
            .map_err(|e| EngineError::InvalidDocument(format!("invalid collection meta: {e}")))
    }

    /// Load index field names for a collection from the sys CF.
    pub(crate) fn load_indexes(&self, collection: &str) -> Result<Vec<String>, EngineError> {
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

// ── Catalog impl ───────────────────────────────────────────────

impl<'a, S: Store + 'a> Catalog for KvTransaction<'a, S> {
    fn collection(&self, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError> {
        let meta = self.load_collection_meta(name)?;
        let indexes = self.load_indexes(name)?;
        let cf = self.txn.cf(&meta.cf)?;
        Ok(CollectionHandle::new(
            name.to_string(),
            cf,
            indexes,
            meta.pk,
            meta.ttl,
        ))
    }

    fn list_collections(&self) -> Result<Vec<CollectionHandle<Self::Cf>>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = KeyPrefix::Collection.encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut names = Vec::new();
        for result in iter {
            let (key_bytes, _) = result?;
            if let Some(Key::Collection(name)) = Key::decode(&key_bytes) {
                names.push(name.into_owned());
            }
        }
        let mut handles = Vec::new();
        for name in names {
            let meta = self.load_collection_meta(&name)?;
            let indexes = self.load_indexes(&name)?;
            let cf = self.txn.cf(&meta.cf)?;
            handles.push(CollectionHandle::new(
                name,
                cf,
                indexes,
                meta.pk,
                meta.ttl,
            ));
        }
        Ok(handles)
    }

    fn create_collection(
        &mut self,
        name: &str,
        options: &CreateCollectionOptions,
    ) -> Result<(), EngineError> {
        let meta = CollectionMeta {
            cf: options
                .cf
                .clone()
                .unwrap_or_else(|| DEFAULT_CF.to_string()),
            pk: options
                .pk_path
                .clone()
                .unwrap_or_else(|| "_id".to_string()),
            ttl: options
                .ttl_path
                .clone()
                .unwrap_or_else(|| "ttl".to_string()),
        };
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(name)).encode();
        if self.txn.get(&sys, &key)?.is_none() {
            self.txn.create_cf(&meta.cf)?;
            let blob = bson::serialize_to_vec(&meta)
                .map_err(|e| EngineError::InvalidDocument(format!("failed to serialize meta: {e}")))?;
            self.txn.put(&sys, &key, &blob)?;
        }
        Ok(())
    }

    fn drop_collection(&mut self, name: &str) -> Result<(), EngineError> {
        let meta = match self.load_collection_meta(name) {
            Ok(meta) => meta,
            Err(EngineError::CollectionNotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let cf = self.txn.cf(&meta.cf)?;

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
            KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(&meta.ttl)).encode();
        self.delete_prefix(&cf, &ttl_prefix)?;

        // Delete all index config keys from _sys_.
        let sys = self.sys_cf()?;
        let idx_config_prefix = KeyPrefix::IndexConfig(Cow::Borrowed(name)).encode();
        self.delete_prefix(&sys, &idx_config_prefix)?;

        // Delete the collection metadata key.
        let meta_key = Key::Collection(Cow::Borrowed(name)).encode();
        self.txn.delete(&sys, &meta_key)?;

        Ok(())
    }

    fn create_index(&mut self, collection: &str, field: &str) -> Result<(), EngineError> {
        let meta = self.load_collection_meta(collection)?;
        let cf = self.txn.cf(&meta.cf)?;

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
        let meta = self.load_collection_meta(collection)?;
        let cf = self.txn.cf(&meta.cf)?;

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
