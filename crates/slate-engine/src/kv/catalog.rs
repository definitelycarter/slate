use std::borrow::Cow;
use std::collections::HashMap;

use slate_store::{Store, Transaction};

use crate::encoding::index_record::unique_entries_from_document;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::traits::{
    Catalog, CollectionHandle, CreateCollectionOptions, FunctionEntry, FunctionKind, IndexOptions,
    IndexSpec,
};
use crate::vector::{VectorIndexSpec, encode_vector_entry, pack_vector};

use super::formats::CATALOG_VERSION;
use super::transaction::{KvTransaction, unique_violation};
use super::{CollectionMeta, SYS_CF};

// ── Catalog helpers ─────────────────────────────────────────────

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    pub(crate) fn sys_cf(&self) -> Result<<S::Txn<'a> as Transaction>::Cf, EngineError> {
        Ok(self.txn.cf(SYS_CF)?)
    }

    /// Delete all keys under a prefix.
    pub(crate) fn delete_prefix(
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

    /// Load index definitions for a collection from the sys CF.
    ///
    /// The index config value encodes uniqueness: a leading `0x01` byte marks a
    /// unique index. Legacy/empty values are non-unique.
    pub(crate) fn load_indexes(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<Vec<IndexSpec>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = KeyPrefix::IndexConfig(Cow::Borrowed(cf), Cow::Borrowed(collection)).encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut specs = Vec::new();
        for result in iter {
            let (key_bytes, value) = result?;
            if let Some(Key::IndexConfig(_, _, field)) = Key::decode(&key_bytes) {
                specs.push(IndexSpec {
                    path: field.into_owned(),
                    unique: value.first() == Some(&1),
                });
            }
        }
        Ok(specs)
    }

    /// Load vector-index definitions for a collection from the sys CF.
    ///
    /// Each config value is a self-describing serialized [`VectorIndexSpec`]
    /// (mirroring `CollectionMeta`), deserialized whole. Lives in its own
    /// `y`-tagged keyspace, so the secondary index path ([`load_indexes`]) never
    /// sees these. Usually empty.
    pub(crate) fn load_vector_indexes(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<Vec<VectorIndexSpec>, EngineError> {
        let sys = self.sys_cf()?;
        let prefix = KeyPrefix::VectorConfig(Cow::Borrowed(cf), Cow::Borrowed(collection)).encode();
        let iter = self.txn.scan_prefix(&sys, &prefix)?;
        let mut specs = Vec::new();
        for result in iter {
            let (_key_bytes, value) = result?;
            let spec: VectorIndexSpec = bson::deserialize_from_slice(&value).map_err(|e| {
                EngineError::InvalidDocument(format!("invalid vector index config: {e}"))
            })?;
            specs.push(spec);
        }
        Ok(specs)
    }

    /// Split loaded index specs into the full path list (drives `i` entries)
    /// and the unique subset (drives `u` entries + enforcement).
    fn split_index_specs(specs: Vec<IndexSpec>) -> (Vec<String>, Vec<String>) {
        let mut indexes = Vec::with_capacity(specs.len());
        let mut unique_indexes = Vec::new();
        for spec in specs {
            if spec.unique {
                unique_indexes.push(spec.path.clone());
            }
            indexes.push(spec.path);
        }
        (indexes, unique_indexes)
    }

    /// Drop any cached handle for `(cf, name)` so the next `collection()` call
    /// rebuilds it. Must be called by every DDL op that changes a collection's
    /// shape (indexes, pk, ttl) so a stale handle can't outlive the change
    /// within the same transaction.
    fn invalidate_collection(&self, cf: &str, name: &str) {
        self.catalog_cache
            .borrow_mut()
            .remove(&(cf.to_string(), name.to_string()));
    }
}

// ── Catalog impl ───────────────────────────────────────────────

impl<'a, S: Store + 'a> Catalog for KvTransaction<'a, S> {
    fn collection(&self, cf: &str, name: &str) -> Result<CollectionHandle<Self::Cf>, EngineError> {
        // Handles are immutable for a given collection shape, so a hit returns a
        // cheap Arc-bump clone. Invalidated by DDL on this collection within the
        // same txn (see `invalidate_collection`).
        if let Some(handle) = self
            .catalog_cache
            .borrow()
            .get(&(cf.to_string(), name.to_string()))
        {
            return Ok(handle.clone());
        }
        let meta = self.load_collection_meta(cf, name)?;
        let (indexes, unique_indexes) = Self::split_index_specs(self.load_indexes(cf, name)?);
        let vector_indexes = self.load_vector_indexes(cf, name)?;
        let cf_handle = self.txn.cf(cf)?;
        let handle = CollectionHandle::new(
            name.to_string(),
            cf.to_string(),
            cf_handle,
            indexes,
            unique_indexes,
            vector_indexes,
            meta.pk,
            meta.ttl,
        );
        self.catalog_cache
            .borrow_mut()
            .insert((cf.to_string(), name.to_string()), handle.clone());
        Ok(handle)
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
            let (indexes, unique_indexes) =
                Self::split_index_specs(self.load_indexes(&cf_name, &name)?);
            let vector_indexes = self.load_vector_indexes(&cf_name, &name)?;
            let cf_handle = self.txn.cf(&cf_name)?;
            handles.push(CollectionHandle::new(
                name,
                cf_name,
                cf_handle,
                indexes,
                unique_indexes,
                vector_indexes,
                meta.pk,
                meta.ttl,
            ));
        }
        Ok(handles)
    }

    fn create_collection(
        &self,
        cf: &str,
        name: &str,
        options: &CreateCollectionOptions,
    ) -> Result<(), EngineError> {
        let pk = options.pk_path.clone().unwrap_or_else(|| "_id".to_string());
        if pk.contains('.') {
            return Err(EngineError::InvalidDocument(
                "pk_path must be a top-level field (dot-paths are not supported)".into(),
            ));
        }
        let meta = CollectionMeta {
            pk,
            ttl: options
                .ttl_path
                .clone()
                .unwrap_or_else(|| "ttl".to_string()),
            version: CATALOG_VERSION,
        };
        let sys = self.sys_cf()?;
        let key = Key::Collection(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        if self.txn.get(&sys, &key)?.is_none() {
            self.txn.create_cf(cf)?;
            let blob = bson::serialize_to_vec(&meta).map_err(|e| {
                EngineError::InvalidDocument(format!("failed to serialize meta: {e}"))
            })?;
            self.txn.put(&sys, &key, &blob)?;
        }
        Ok(())
    }

    fn drop_collection(&self, cf: &str, name: &str) -> Result<(), EngineError> {
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
        let specs = self.load_indexes(cf, name)?;
        for spec in &specs {
            let idx_prefix =
                KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(&spec.path)).encode();
            self.delete_prefix(&cf_handle, &idx_prefix)?;
            if spec.unique {
                let u_prefix =
                    KeyPrefix::UniqueIndexField(Cow::Borrowed(name), Cow::Borrowed(&spec.path))
                        .encode();
                self.delete_prefix(&cf_handle, &u_prefix)?;
            }
        }
        let ttl_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(&meta.ttl)).encode();
        self.delete_prefix(&cf_handle, &ttl_prefix)?;

        // Delete all vector data entries (one prefix per indexed vector field).
        let vector_specs = self.load_vector_indexes(cf, name)?;
        for spec in &vector_specs {
            let v_prefix =
                KeyPrefix::VectorField(Cow::Borrowed(name), Cow::Borrowed(&spec.path)).encode();
            self.delete_prefix(&cf_handle, &v_prefix)?;
        }

        // Delete all index config keys from _sys_.
        let sys = self.sys_cf()?;
        let idx_config_prefix =
            KeyPrefix::IndexConfig(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        self.delete_prefix(&sys, &idx_config_prefix)?;

        // Delete all vector index config keys from _sys_.
        let vec_config_prefix =
            KeyPrefix::VectorConfig(Cow::Borrowed(cf), Cow::Borrowed(name)).encode();
        self.delete_prefix(&sys, &vec_config_prefix)?;

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

        self.invalidate_collection(cf, name);
        Ok(())
    }

    fn create_index_with_options(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
        options: &IndexOptions,
    ) -> Result<(), EngineError> {
        // A single-field index is the one-component compound case.
        self.create_compound_index_with_options(cf, collection, &[field.to_string()], options)
    }

    fn create_compound_index_with_options(
        &self,
        cf: &str,
        collection: &str,
        fields: &[String],
        options: &IndexOptions,
    ) -> Result<(), EngineError> {
        self.load_collection_meta(cf, collection)?;
        let cf_handle = self.txn.cf(cf)?;

        if fields.is_empty() {
            return Err(EngineError::InvalidDocument(
                "an index must cover at least one field".into(),
            ));
        }

        // The compound identity is the joined field names; a single field joins
        // to itself, so existing single-field keys are unchanged.
        let identity = crate::encoding::key::join_index_fields(fields);

        // Unique indexes are scalar-only for now: a multikey (`[]`) component
        // would imply element-wise uniqueness semantics we don't yet support.
        if options.unique && fields.iter().any(|f| f.contains("[]")) {
            return Err(EngineError::InvalidDocument(format!(
                "unique index does not support multikey ('[]') paths: {identity}"
            )));
        }
        // Compound (multi-field) indexes are scalar-only in this phase: a multikey
        // component would require a value cross-product the leftmost-prefix seek
        // can't express.
        if fields.len() > 1 && fields.iter().any(|f| f.contains("[]")) {
            return Err(EngineError::InvalidDocument(format!(
                "compound index does not support multikey ('[]') paths: {identity}"
            )));
        }

        // Check for duplicate index.
        let sys = self.sys_cf()?;
        let config_key = Key::IndexConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(&identity),
        )
        .encode();
        if self.txn.get(&sys, &config_key)?.is_some() {
            return Err(EngineError::IndexExists(format!("{collection}.{identity}")));
        }
        // Config value encodes uniqueness: `0x01` for unique, empty otherwise.
        let config_value: &[u8] = if options.unique { &[1] } else { &[] };
        self.txn.put(&sys, &config_key, config_value)?;

        // Backfill: scan all existing records and create index entries.
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
        let records: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf_handle, &record_prefix)?
            .collect::<Result<_, _>>()?;

        let indexes = vec![identity.clone()];
        let unique_paths = if options.unique {
            vec![identity.clone()]
        } else {
            Vec::new()
        };
        // Tracks unique slots claimed during backfill (u_key → owning entry
        // value). A fresh index has no pre-existing `u` keys, so this in-memory
        // map is authoritative for collision detection.
        let mut seen_unique: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for (key_bytes, value_bytes) in &records {
            let Some(Key::Record(_, doc_id)) = Key::decode(key_bytes) else {
                continue;
            };
            let record = Record::from_bytes(value_bytes.clone())?;
            let ttl = record.ttl_millis();
            let doc = record.doc()?;

            // Regular `i` entries, written for every indexed path (compound
            // identities produce one combined entry; see `from_document`).
            let entries = IndexRecord::from_document(collection, &indexes, doc, &doc_id, ttl);
            if !entries.is_empty() {
                let refs: Vec<(&[u8], &[u8])> = entries
                    .iter()
                    .map(|e| (e.key_bytes(), e.metadata()))
                    .collect();
                self.txn.put_batch(&cf_handle, &refs)?;
            }

            // For a unique index, also write `u` entries — failing the whole
            // operation if existing data already holds a duplicate value.
            if options.unique {
                for (u_key, u_val) in
                    unique_entries_from_document(collection, &unique_paths, doc, &doc_id)
                {
                    if let Some(existing) = seen_unique.get(&u_key) {
                        return Err(unique_violation(collection, &u_key, existing));
                    }
                    self.txn.put(&cf_handle, &u_key, &u_val)?;
                    seen_unique.insert(u_key, u_val);
                }
            }
        }

        self.invalidate_collection(cf, collection);
        Ok(())
    }

    fn drop_index(&self, cf: &str, collection: &str, field: &str) -> Result<(), EngineError> {
        let cf_handle = self.txn.cf(cf)?;

        // Delete all index entries for this field (`i` and any `u` entries).
        let idx_prefix =
            KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.delete_prefix(&cf_handle, &idx_prefix)?;
        let u_prefix =
            KeyPrefix::UniqueIndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.delete_prefix(&cf_handle, &u_prefix)?;

        // Delete the index config key from _sys_.
        let sys = self.sys_cf()?;
        let key = Key::IndexConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
        )
        .encode();
        self.txn.delete(&sys, &key)?;

        self.invalidate_collection(cf, collection);
        Ok(())
    }

    fn create_vector_index(
        &self,
        cf: &str,
        collection: &str,
        spec: &VectorIndexSpec,
    ) -> Result<(), EngineError> {
        self.load_collection_meta(cf, collection)?;
        let cf_handle = self.txn.cf(cf)?;

        if spec.dims == 0 {
            return Err(EngineError::InvalidDocument(
                "a vector index must declare at least one dimension".into(),
            ));
        }

        // One vector index per (collection, field): reject a duplicate.
        let sys = self.sys_cf()?;
        let config_key = Key::VectorConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(&spec.path),
        )
        .encode();
        if self.txn.get(&sys, &config_key)?.is_some() {
            return Err(EngineError::IndexExists(format!(
                "{collection}.{} (vector)",
                spec.path
            )));
        }

        // Persist the self-describing spec (the on-disk value is just its
        // serialized form).
        let config_value = bson::serialize_to_vec(spec).map_err(|e| {
            EngineError::InvalidDocument(format!("failed to serialize vector index config: {e}"))
        })?;
        self.txn.put(&sys, &config_key, &config_value)?;

        // Backfill: pack each existing record's vector and write its data entry.
        // A dims mismatch in existing data fails the whole create (rolled back).
        //
        // Stream it: we can't write through `self.txn` while a scan iterator
        // borrows it, so — mirroring `delete_prefix` — we collect only the
        // lightweight record *keys* (not their bodies), drop the scan, then read
        // each record body lazily inside the write loop. This avoids buffering
        // every record body (hundreds of MB for a large collection); only one
        // record is resident at a time. (The secondary-index backfill in
        // `create_compound_index_with_options` still buffers full record bodies;
        // this is the minimal streaming improvement for the vector path.)
        let record_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
        let record_keys: Vec<Vec<u8>> = self
            .txn
            .scan_prefix(&cf_handle, &record_prefix)?
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<_, _>>()?;

        for key_bytes in record_keys {
            let Some(Key::Record(_, doc_id)) = Key::decode(&key_bytes) else {
                continue;
            };
            // Read the record body now (the scan iterator is dropped, so writing
            // through `self.txn` is free of the borrow). A record deleted between
            // the key scan and this read simply yields no vector entry.
            let Some(value_bytes) = self.txn.get(&cf_handle, &key_bytes)? else {
                continue;
            };
            // Consume `value_bytes` (no clone): `Record::from_bytes` takes
            // ownership, and the record isn't needed past this iteration.
            let record = Record::from_bytes(value_bytes)?;
            let doc = record.doc()?;
            if let Some(packed) = pack_vector(doc, &spec.path, spec.dims, spec.dtype)? {
                let entry = encode_vector_entry(record.ttl_millis(), &packed);
                let vec_key = Key::encode_vector_key(collection, &spec.path, &doc_id);
                self.txn.put(&cf_handle, &vec_key, &entry)?;
            }
        }

        self.invalidate_collection(cf, collection);
        Ok(())
    }

    fn drop_vector_index(
        &self,
        cf: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), EngineError> {
        let cf_handle = self.txn.cf(cf)?;

        // Delete every packed-vector data entry for this field.
        let data_prefix =
            KeyPrefix::VectorField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
        self.delete_prefix(&cf_handle, &data_prefix)?;

        // Delete the config key from _sys_.
        let sys = self.sys_cf()?;
        let config_key = Key::VectorConfig(
            Cow::Borrowed(cf),
            Cow::Borrowed(collection),
            Cow::Borrowed(field),
        )
        .encode();
        self.txn.delete(&sys, &config_key)?;

        self.invalidate_collection(cf, collection);
        Ok(())
    }

    fn create_function(
        &self,
        cf: &str,
        collection: &str,
        kind: FunctionKind,
        name: &str,
        runtime: u8,
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
        // Value format: [runtime_tag: u8][source_bytes...]
        let mut value = Vec::with_capacity(1 + source.len());
        value.push(runtime);
        value.extend_from_slice(source);
        self.txn.put(&sys, &key, &value)?;
        Ok(())
    }

    fn drop_function(
        &self,
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
                // Value format: [runtime_tag: u8][source_bytes...]
                let (runtime, source) = if value.is_empty() {
                    (crate::traits::runtime_tag::LUA, Vec::new())
                } else {
                    (value[0], value[1..].to_vec())
                };
                entries.push(FunctionEntry {
                    name: name.into_owned(),
                    runtime,
                    source,
                });
            }
        }
        Ok(entries)
    }
}
