use std::borrow::Cow;

use slate_store::{Store, Transaction};

use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::traits::{
    Catalog, CollectionHandle, CreateCollectionOptions, FunctionEntry, FunctionKind,
};

use super::transaction::KvTransaction;
use super::{CollectionMeta, SYS_CF};

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
    pub(crate) fn load_collection_meta(
        &self,
        cf: &str,
        name: &str,
    ) -> Result<CollectionMeta, EngineError> {
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        let value = self
            .txn
            .get(&sys, &key)?
            .ok_or_else(|| EngineError::CollectionNotFound(format!("{cf}:{name}")))?;
        bson::deserialize_from_slice(&value)
            .map_err(|e| EngineError::InvalidDocument(format!("invalid collection meta: {e}")))
    }

    /// Load index field names for a collection from the sys CF.
    pub(crate) fn load_indexes(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<Vec<String>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix =
            KeyPrefix::IndexConfig(Cow::Borrowed(cf), Cow::Borrowed(collection)).encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut fields = Vec::new();
        for result in iter {
            let (key_bytes, _) = result?;
            let key = Key::decode(&key_bytes);
            if let Some(Key::IndexConfig(_, _, field)) = key {
                fields.push(field.into_owned());
            }
        }
        Ok(fields)
    }
}

// ── Catalog impl ───────────────────────────────────────────────

impl<'a, S: Store + 'a> Catalog for KvTransaction<'a, S> {
    fn collection(&self, cf: &str, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError> {
        let meta = self.load_collection_meta(cf, name)?;
        let indexes = self.load_indexes(cf, name)?;
        let cf_handle = self.txn.cf(cf)?;
        Ok(CollectionHandle::new(
            name.to_string(),
            cf.to_string(),
            cf_handle,
            indexes,
            meta.pk,
            meta.ttl,
        ))
    }

    fn list_collections(
        &self,
        cf: Option<&str>,
    ) -> Result<Vec<CollectionHandle<Self::Cf>>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = match cf {
            Some(cf) => KeyPrefix::CollectionByCf(Cow::Borrowed(cf)).encode(),
            None => KeyPrefix::Collection.encode(),
        };
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut entries = Vec::new();
        for result in iter {
            let (key_bytes, _) = result?;
            if let Some(Key::Collection(cf_name, name)) = Key::decode(&key_bytes) {
                entries.push((cf_name.into_owned(), name.into_owned()));
            }
        }
        let mut handles = Vec::new();
        for (cf_name, name) in entries {
            let meta = self.load_collection_meta(&cf_name, &name)?;
            let indexes = self.load_indexes(&cf_name, &name)?;
            let cf_handle = self.txn.cf(&cf_name)?;
            handles.push(CollectionHandle::new(
                name,
                cf_name,
                cf_handle,
                indexes,
                meta.pk,
                meta.ttl,
            ));
        }
        Ok(handles)
    }

    fn create_collection(
        &mut self,
        cf: &str,
        name: &str,
        options: &CreateCollectionOptions,
    ) -> Result<(), EngineError> {
        let meta = CollectionMeta {
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
        let key = Key::Collection(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        if self.txn.get(&sys, &key)?.is_none() {
            self.txn.create_cf(cf)?;
            let blob = bson::serialize_to_vec(&meta)
                .map_err(|e| EngineError::InvalidDocument(format!("failed to serialize meta: {e}")))?;
            self.txn.put(&sys, &key, &blob)?;
        }
        Ok(())
    }

    fn drop_collection(&mut self, cf: &str, name: &str) -> Result<(), EngineError> {
        let meta = match self.load_collection_meta(cf, name) {
            Ok(meta) => meta,
            Err(EngineError::CollectionNotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let cf_handle = self.txn.cf(cf)?;

        // Delete all records.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(name)).encode();
        self.delete_prefix(&cf_handle, &record_prefix)?;

        // Delete all index entries (catalog indexes + ttl).
        let indexes = self.load_indexes(cf, name)?;
        for field in &indexes {
            let idx_prefix =
                KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(field)).encode();
            self.delete_prefix(&cf_handle, &idx_prefix)?;
        }
        let ttl_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(&meta.ttl)).encode();
        self.delete_prefix(&cf_handle, &ttl_prefix)?;

        // Delete all index config keys from _sys_.
        let sys = self.sys_cf()?;
        let idx_config_prefix =
            KeyPrefix::IndexConfig(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        self.delete_prefix(&sys, &idx_config_prefix)?;

        // Delete all function config keys from _sys_.
        for kind in [
            FunctionKind::Trigger,
            FunctionKind::Validator,
            FunctionKind::Udf,
        ] {
            let fn_prefix =
                KeyPrefix::FunctionConfig(kind, Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
            self.delete_prefix(&sys, &fn_prefix)?;
        }

        // Delete the collection metadata key.
        let meta_key = Key::Collection(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        self.txn.delete(&sys, &meta_key)?;

        Ok(())
    }

    fn create_index(
        &mut self,
        cf: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), EngineError> {
        self.load_collection_meta(cf, collection)?;
        let cf_handle = self.txn.cf(cf)?;

        // Check for duplicate index.
        let sys = self.sys_cf()?;
        let config_key = Key::IndexConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
        )
        .encode();
        if self.txn.get(&sys, &config_key)?.is_some() {
            return Err(EngineError::IndexExists(format!("{collection}.{field}")));
        }
        self.txn.put(&sys, &config_key, &[])?;

        // Backfill: scan all existing records and create index entries.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
        let records: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf_handle, &record_prefix)?
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
                self.txn.put_batch(&cf_handle, &refs)?;
            }
        }

        Ok(())
    }

    fn drop_index(&mut self, cf: &str, collection: &str, field: &str) -> Result<(), EngineError> {
        let cf_handle = self.txn.cf(cf)?;

        // Delete all index entries for this field.
        let idx_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.delete_prefix(&cf_handle, &idx_prefix)?;

        // Delete the index config key from _sys_.
        let sys = self.sys_cf()?;
        let key = Key::IndexConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
        )
        .encode();
        self.txn.delete(&sys, &key)?;

        Ok(())
    }

    fn create_function(
        &mut self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
        name: &str,
        source: &[u8],
    ) -> Result<(), EngineError> {
        // Verify collection exists.
        self.load_collection_meta(cf, collection)?;

        let sys = self.sys_cf()?;
        let key = Key::FunctionConfig(
            kind,
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(name),
        )
        .encode();
        if self.txn.get(&sys, &key)?.is_some() {
            return Err(EngineError::FunctionExists(format!(
                "{collection}.{kind:?}.{name}"
            )));
        }
        self.txn.put(&sys, &key, source)?;
        Ok(())
    }

    fn drop_function(
        &mut self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
        name: &str,
    ) -> Result<(), EngineError> {
        let sys = self.sys_cf()?;
        let key = Key::FunctionConfig(
            kind,
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(name),
        )
        .encode();
        self.txn.delete(&sys, &key)?;
        Ok(())
    }

    fn load_functions(
        &self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
    ) -> Result<Vec<FunctionEntry>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix =
            KeyPrefix::FunctionConfig(kind, Cow::Borrowed(cf), Cow::Borrowed(collection)).encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut entries = Vec::new();
        for result in iter {
            let (key_bytes, value) = result?;
            if let Some(Key::FunctionConfig(_, _, _, name)) = Key::decode(&key_bytes) {
                entries.push(FunctionEntry {
                    name: name.into_owned(),
                    source: value,
                });
            }
        }
        Ok(entries)
    }
}
