//! Index-encoding migration.
//!
//! The on-disk index entry layout is versioned. When the engine opens a store
//! written by an older version, it re-encodes the affected index entries before
//! serving any transaction, so reads never observe the old (ambiguous) layout.
//!
//! The migration **rebuilds `i` index entries from the records**, which are the
//! unambiguous source of truth — so it cannot inherit the value/doc_id boundary
//! bug it exists to repair (decoding the old string entries directly could
//! mis-split them). Unique (`u`) entries are untouched: their value runs to the
//! end of the key with no doc_id suffix, so the boundary fix does not affect them.

use std::borrow::Cow;

use slate_store::{Store, Transaction};

use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::traits::{Engine, EngineTransaction};

use super::KvEngine;
use super::transaction::KvTransaction;

/// Current index-key encoding version. Bumped whenever the on-disk index entry
/// layout changes in a way that requires a re-index migration.
///
/// - `1` — variable-width (string) index values carry a trailing `u32`
///   value-length suffix, making the value/doc_id boundary exact (it was
///   previously located by an ambiguous backward scan that could undercount).
pub(crate) const INDEX_ENCODING_VERSION: u8 = 1;

/// Reserved `_sys_` key holding the stored [`INDEX_ENCODING_VERSION`]. The `m`
/// ("meta") tag byte is distinct from every [`Key`] tag, so the key is inert to
/// `Key::decode` and never collides with collection/index/function metadata.
const INDEX_ENCODING_VERSION_KEY: &[u8] = b"m\x00index_encoding_version";

impl<S: Store> KvEngine<S> {
    /// Bring the store's index entries up to [`INDEX_ENCODING_VERSION`].
    ///
    /// A no-op when the store is already current (a single read-only peek). When
    /// behind, it reindexes every collection in one write transaction and bumps
    /// the stored version, committing atomically — a failure rolls back and is
    /// retried on the next open, so the store is never left half-migrated.
    ///
    /// Run on open, before serving transactions, since an un-migrated string
    /// index would silently undercount.
    pub fn migrate_index_encoding(&self) -> Result<(), EngineError> {
        // Fast path: avoid opening a write transaction when already current.
        {
            let txn = self.begin(true)?;
            let current = txn.index_encoding_version()?;
            txn.rollback()?;
            if current >= INDEX_ENCODING_VERSION {
                return Ok(());
            }
        }

        let txn = self.begin(false)?;
        txn.run_index_encoding_migration()?;
        txn.commit()?;
        Ok(())
    }
}

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    /// Read the stored index-encoding version (`0` when the marker is absent,
    /// i.e. a store written before versioning existed).
    fn index_encoding_version(&self) -> Result<u8, EngineError> {
        let sys = self.sys_cf()?;
        Ok(self
            .txn
            .get(&sys, INDEX_ENCODING_VERSION_KEY)?
            .and_then(|v| v.first().copied())
            .unwrap_or(0))
    }

    /// Reindex every collection (across all CFs) and stamp the current version.
    /// Assumes the caller has already checked the version is behind.
    pub(crate) fn run_index_encoding_migration(&self) -> Result<(), EngineError> {
        let sys = self.sys_cf()?;

        // Enumerate collections first so the sys-CF scan's borrow is released
        // before we start writing.
        let prefix = KeyPrefix::Collection.encode();
        let mut collections: Vec<(String, String)> = Vec::new();
        for result in self.txn.scan_prefix(&sys, &prefix)? {
            let (key_bytes, _) = result?;
            if let Some(Key::Collection(cf, name)) = Key::decode(&key_bytes) {
                collections.push((cf.into_owned(), name.into_owned()));
            }
        }

        for (cf, name) in &collections {
            self.reindex_collection_indexes(cf, name)?;
        }

        let sys = self.sys_cf()?;
        self.txn
            .put(&sys, INDEX_ENCODING_VERSION_KEY, &[INDEX_ENCODING_VERSION])?;
        Ok(())
    }

    /// Drop and rebuild a collection's `i` index entries from its records.
    pub(crate) fn reindex_collection_indexes(
        &self,
        cf: &str,
        name: &str,
    ) -> Result<(), EngineError> {
        let cf_handle = self.txn.cf(cf)?;
        let meta = self.load_collection_meta(cf, name)?;
        let specs = self.load_indexes(cf, name)?;

        // Every path that produces `i` entries: the user index paths plus the
        // TTL path (whose `i` entries drive expiry sweeps).
        let mut paths: Vec<String> = specs.iter().map(|s| s.path.clone()).collect();
        if !paths.iter().any(|p| p == &meta.ttl) {
            paths.push(meta.ttl.clone());
        }

        // Drop all existing `i` entries (in any encoding) for those paths.
        for path in &paths {
            let pre = KeyPrefix::IndexField(Cow::Borrowed(name), Cow::Borrowed(path)).encode();
            self.delete_prefix(&cf_handle, &pre)?;
        }

        // Rebuild from the records (unambiguous), so the rebuild cannot inherit
        // the boundary bug it repairs.
        let rec_prefix = KeyPrefix::Record(Cow::Borrowed(name)).encode();
        let records: Vec<(Vec<u8>, Vec<u8>)> = self
            .txn
            .scan_prefix(&cf_handle, &rec_prefix)?
            .collect::<Result<_, _>>()?;

        for (key_bytes, value_bytes) in &records {
            let Some(Key::Record(_, doc_id)) = Key::decode(key_bytes) else {
                continue;
            };
            let record = Record::from_bytes(value_bytes.clone())?;
            let ttl = record.ttl_millis();
            let doc = record.doc()?;
            let entries = IndexRecord::from_document(name, &paths, doc, &doc_id, ttl);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_CF;
    use crate::traits::{
        Catalog, CreateCollectionOptions, EngineTransaction, IndexOptions, IndexRange,
    };
    use bson::spec::ElementType;
    use slate_store::{MemoryStore, Transaction};

    fn doc(id: &str, status: &str) -> bson::RawDocumentBuf {
        bson::rawdoc! { "_id": id, "status": status }
    }

    /// An index written in the old (no-suffix) string encoding migrates to the
    /// new encoding on upgrade and then decodes without undercounting — the
    /// invariant the boundary bug broke.
    #[test]
    fn migrates_legacy_string_index_entries() {
        let engine = KvEngine::new(MemoryStore::new());

        // status value -> doc id. Includes a value whose bytes embed a valid
        // length-prefixed doc-id header (the collision the old scan mis-split)
        // and an empty-string value.
        let rows = [
            ("active", "u1"),
            ("active", "u2"),
            ("active", "u3"),
            ("inactive", "u4"),
            ("act\x02\x00\x00ive", "u5"),
            ("", "u6"),
        ];

        // 1. Build the collection + string index and insert docs (new encoding).
        {
            let tx = engine.begin(false).unwrap();
            tx.create_collection(DEFAULT_CF, "c", &CreateCollectionOptions::default())
                .unwrap();
            tx.create_index_with_options(
                DEFAULT_CF,
                "c",
                "status",
                &IndexOptions { unique: false },
            )
            .unwrap();
            let handle = tx.collection(DEFAULT_CF, "c").unwrap();
            for (status, id) in rows {
                tx.put(&handle, &doc(id, status)).unwrap();
            }
            tx.commit().unwrap();
        }

        // 2. Downgrade every string `i` entry to the legacy layout by stripping
        //    the trailing u32 value-length suffix.
        let mut downgraded = 0usize;
        {
            let tx = engine.begin(false).unwrap();
            let cf = tx.txn.cf(DEFAULT_CF).unwrap();
            let prefix =
                KeyPrefix::IndexField(Cow::Borrowed("c"), Cow::Borrowed("status")).encode();
            let entries: Vec<(Vec<u8>, Vec<u8>)> = tx
                .txn
                .scan_prefix(&cf, &prefix)
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            for (key, meta) in entries {
                assert_eq!(meta[0], ElementType::String as u8);
                let legacy = key[..key.len() - 4].to_vec();
                assert_ne!(legacy, key);
                tx.txn.delete(&cf, &key).unwrap();
                tx.txn.put(&cf, &legacy, &meta).unwrap();
                downgraded += 1;
            }
            tx.commit().unwrap();
        }
        assert_eq!(downgraded, rows.len());

        // Version marker is absent → reads as 0 (pre-versioning / old).
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(tx.index_encoding_version().unwrap(), 0);
            tx.rollback().unwrap();
        }

        // 3. Upgrade.
        engine.migrate_index_encoding().unwrap();

        // 4a. Version stamped current.
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(tx.index_encoding_version().unwrap(), INDEX_ENCODING_VERSION);
            tx.rollback().unwrap();
        }

        // 4b. Every value scans back with the right count (no undercount), and
        //     the total entry count equals the document count.
        let tx = engine.begin(true).unwrap();
        let handle = tx.collection(DEFAULT_CF, "c").unwrap();
        for status in ["active", "inactive", "act\x02\x00\x00ive", ""] {
            let expected = rows.iter().filter(|(s, _)| *s == status).count();
            let val = bson::Bson::String(status.to_string());
            let got = tx
                .scan_index(&handle, "status", IndexRange::Eq(&val), false)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len();
            assert_eq!(got, expected, "status {status:?}");
        }
        let total = tx
            .scan_index(&handle, "status", IndexRange::Full, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len();
        assert_eq!(total, rows.len());
        tx.rollback().unwrap();
    }

    /// Migrating a fresh store is a no-op that just stamps the version.
    #[test]
    fn migrate_empty_store_stamps_version() {
        let engine = KvEngine::new(MemoryStore::new());
        engine.migrate_index_encoding().unwrap();
        let tx = engine.begin(true).unwrap();
        assert_eq!(tx.index_encoding_version().unwrap(), INDEX_ENCODING_VERSION);
        tx.rollback().unwrap();
    }

    /// Migration crash test (RFC Thread B3): a migration killed mid-rebuild must
    /// leave the version un-advanced, so the *next* open re-runs it cleanly. The
    /// migration runs in one transaction and commits the reindex + the version
    /// bump atomically, so "killed mid-rebuild" is modelled by running the
    /// reindex body and then rolling back instead of committing — the same effect
    /// a crash before `commit()` would have. The version must still read 0, and a
    /// real migration afterward must succeed and decode without undercount.
    #[test]
    fn migration_interrupted_before_commit_never_half_advances() {
        let engine = KvEngine::new(MemoryStore::new());

        // Seed a string index and downgrade its entries to the legacy (no-suffix)
        // layout, so a migration has real work to do.
        let rows = [("active", "u1"), ("active", "u2"), ("inactive", "u3")];
        {
            let tx = engine.begin(false).unwrap();
            tx.create_collection(DEFAULT_CF, "c", &CreateCollectionOptions::default())
                .unwrap();
            tx.create_index_with_options(
                DEFAULT_CF,
                "c",
                "status",
                &IndexOptions { unique: false },
            )
            .unwrap();
            let handle = tx.collection(DEFAULT_CF, "c").unwrap();
            for (status, id) in rows {
                tx.put(&handle, &doc(id, status)).unwrap();
            }
            tx.commit().unwrap();
        }
        {
            let tx = engine.begin(false).unwrap();
            let cf = tx.txn.cf(DEFAULT_CF).unwrap();
            let prefix =
                KeyPrefix::IndexField(Cow::Borrowed("c"), Cow::Borrowed("status")).encode();
            let entries: Vec<(Vec<u8>, Vec<u8>)> = tx
                .txn
                .scan_prefix(&cf, &prefix)
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            for (key, meta) in entries {
                let legacy = key[..key.len() - 4].to_vec();
                tx.txn.delete(&cf, &key).unwrap();
                tx.txn.put(&cf, &legacy, &meta).unwrap();
            }
            tx.commit().unwrap();
        }

        // "Crash" mid-migration: run the reindex body, then roll back.
        {
            let tx = engine.begin(false).unwrap();
            tx.run_index_encoding_migration().unwrap();
            tx.rollback().unwrap();
        }

        // The version must NOT have advanced (the bump was in the rolled-back txn).
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(
                tx.index_encoding_version().unwrap(),
                0,
                "version half-advanced after an interrupted migration"
            );
            tx.rollback().unwrap();
        }

        // A real migration on the next open succeeds and stamps the version.
        engine.migrate_index_encoding().unwrap();
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(tx.index_encoding_version().unwrap(), INDEX_ENCODING_VERSION);
            // And the index decodes with the right count (no undercount).
            let handle = tx.collection(DEFAULT_CF, "c").unwrap();
            let active = bson::Bson::String("active".to_string());
            let got = tx
                .scan_index(&handle, "status", IndexRange::Eq(&active), false)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len();
            assert_eq!(got, 2);
            tx.rollback().unwrap();
        }
    }
}
