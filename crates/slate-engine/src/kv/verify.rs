//! Integrity verification.
//!
//! A read-only walk of one collection that recomputes the index entries the
//! records *should* have and diffs them against what is stored, surfacing any
//! drift between the three cross-structure invariants the engine maintains:
//!
//! 1. **record → `i` index** — every indexed value of every record has its `i`
//!    entry, with matching metadata (type byte + TTL).
//! 2. **record → `u` unique-slot** — every unique-indexed value has its `u`
//!    slot, owned by that record's `doc_id`.
//! 3. **record → TTL** — the TTL path is just an `i` entry, so it is covered by
//!    (1); a record carrying a TTL must have its TTL `i` entry.
//!
//! Records are the source of truth (the same premise the encoding migration
//! relies on), so the diff is computed *from* the records: anything an `i`/`u`
//! scan turns up that no record asked for is an **orphan**; anything a record
//! asks for that the scan is missing is a **missing** entry.
//!
//! This is the read-only half of the durability story. A `repair` that rebuilds
//! the index entries from the records reuses the migration's reindex path; it is
//! also the assertion oracle the crash-test harness calls after each kill.

use std::borrow::Cow;
use std::collections::HashMap;

use slate_store::{Store, Transaction};

use crate::encoding::index_record::unique_entries_from_document;
use crate::encoding::{IndexRecord, Key, KeyPrefix, Record};
use crate::error::EngineError;
use crate::traits::{Catalog, Engine, EngineTransaction};

use super::KvEngine;
use super::transaction::KvTransaction;

/// A single integrity problem found by [`verify`](KvEngine::verify).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityIssue {
    /// A record asks for an `i` index entry that is not in the store.
    MissingIndexEntry {
        field: String,
        /// The record's `_id`, rendered for display.
        doc_id: String,
    },
    /// An `i` index entry exists that no live record accounts for.
    OrphanIndexEntry { field: String, doc_id: String },
    /// An `i` index entry's metadata (type byte / TTL) differs from what the
    /// record implies.
    IndexMetadataMismatch { field: String, doc_id: String },
    /// A record asks for a `u` unique-slot that is not in the store.
    MissingUniqueSlot { field: String, doc_id: String },
    /// A `u` unique-slot exists that no live record accounts for.
    OrphanUniqueSlot { field: String },
    /// A `u` unique-slot is owned by a different `doc_id` than the record that
    /// holds its value (a slot-ownership violation).
    UniqueSlotOwnerMismatch {
        field: String,
        /// The `doc_id` the record expects to own the slot.
        expected: String,
        /// The `doc_id` the stored slot actually names.
        found: String,
    },
    /// A stored record could not be decoded as BSON.
    UndecodableRecord { doc_id: String },
}

/// The result of an integrity check over one collection.
///
/// `ok()` is the headline; the `issues` list enumerates every drift found, and
/// the counts give a quick scale of what was walked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntegrityReport {
    /// Live records walked.
    pub records_checked: u64,
    /// `i` index entries walked.
    pub index_entries_checked: u64,
    /// `u` unique-slots walked.
    pub unique_slots_checked: u64,
    /// Every problem found, in no particular order.
    pub issues: Vec<IntegrityIssue>,
}

impl IntegrityReport {
    /// Whether the collection is internally consistent (no issues).
    pub fn ok(&self) -> bool {
        self.issues.is_empty()
    }
}

impl<S: Store> KvEngine<S> {
    /// Walk a collection's records and index structures in a read-only
    /// transaction, returning an [`IntegrityReport`].
    ///
    /// Pure read path — opens its own read transaction and never writes. Safe to
    /// run on a live database (it observes a consistent snapshot).
    pub fn verify(&self, cf: &str, collection: &str) -> Result<IntegrityReport, EngineError> {
        let txn = self.begin(true)?;
        let report = txn.verify_collection(cf, collection)?;
        txn.rollback()?;
        Ok(report)
    }

    /// Rebuild a collection's `i` and `u` index entries from its records, the
    /// authoritative source of truth, in one atomic write transaction.
    ///
    /// Reuses the encoding migration's reindex path for the `i` entries and
    /// re-derives the `u` slots from the records, so a [`verify`](Self::verify)
    /// run afterwards reports a clean collection. A failure rolls back, leaving
    /// the original (possibly drifted) entries in place rather than a partial
    /// rebuild.
    pub fn repair(&self, cf: &str, collection: &str) -> Result<(), EngineError> {
        let txn = self.begin(false)?;
        txn.repair_collection(cf, collection)?;
        txn.commit()?;
        Ok(())
    }
}

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    /// The verify body (see [`KvEngine::verify`]). Read-only; takes no commit.
    pub(crate) fn verify_collection(
        &self,
        cf: &str,
        collection: &str,
    ) -> Result<IntegrityReport, EngineError> {
        let handle = self.collection(cf, collection)?;
        let cf_handle = self.txn.cf(cf)?;

        // Indexed paths that drive `i` entries: the user indexes plus the TTL
        // path (whose `i` entries are how expiry sweeps find expired docs).
        let mut index_paths: Vec<String> = handle.indexes().to_vec();
        let ttl_path = handle.ttl_path().to_string();
        if !index_paths.iter().any(|p| p == &ttl_path) {
            index_paths.push(ttl_path);
        }
        let unique_paths: Vec<String> = handle.unique_indexes().to_vec();

        let mut report = IntegrityReport::default();

        // ── Pass 1: walk records, build the *expected* entry maps ──────────
        //
        // `expected_index`: i_key -> metadata (the record-derived truth).
        // `expected_unique`: u_key -> owning doc_id value (length-prefixed).
        // No owner side-map is kept: a leftover (missing) `i` key embeds its own
        // length-prefixed doc_id, and a leftover `u` key's mapped value *is* the
        // owning doc_id — so the issue messages recover it from the key/value
        // directly rather than caching an owned `String` per entry.
        let mut expected_index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut expected_unique: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        let rec_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
        for result in self.txn.scan_prefix(&cf_handle, &rec_prefix)? {
            let (key_bytes, value_bytes) = result?;
            let Some(Key::Record(_, doc_id)) = Key::decode(&key_bytes) else {
                continue;
            };
            report.records_checked += 1;

            let record = match Record::from_bytes(value_bytes) {
                Ok(rec) => rec,
                Err(_) => {
                    report.issues.push(IntegrityIssue::UndecodableRecord {
                        doc_id: doc_id.to_string(),
                    });
                    continue;
                }
            };
            let ttl = record.ttl_millis();
            let doc = match record.doc() {
                Ok(doc) => doc,
                Err(_) => {
                    report.issues.push(IntegrityIssue::UndecodableRecord {
                        doc_id: doc_id.to_string(),
                    });
                    continue;
                }
            };

            for entry in IndexRecord::from_document(collection, &index_paths, doc, &doc_id, ttl) {
                let (key, meta) = entry.into_parts();
                expected_index.insert(key, meta);
            }

            for (key, value) in
                unique_entries_from_document(collection, &unique_paths, doc, &doc_id)
            {
                expected_unique.insert(key, value);
            }
        }

        // ── Pass 2: walk stored `i` entries, diff against expected ─────────
        //
        // One prefix scan per indexed field keeps each field's issues labelled.
        for field in &index_paths {
            let prefix =
                KeyPrefix::IndexField(Cow::Borrowed(collection), Cow::Borrowed(field)).encode();
            for result in self.txn.scan_prefix(&cf_handle, &prefix)? {
                let (key_bytes, metadata) = result?;
                report.index_entries_checked += 1;
                match expected_index.remove(&key_bytes) {
                    Some(expected_meta) => {
                        if expected_meta != metadata {
                            let doc_id = stored_index_doc_id(&key_bytes, &metadata);
                            report.issues.push(IntegrityIssue::IndexMetadataMismatch {
                                field: field.clone(),
                                doc_id,
                            });
                        }
                    }
                    None => {
                        let doc_id = stored_index_doc_id(&key_bytes, &metadata);
                        report.issues.push(IntegrityIssue::OrphanIndexEntry {
                            field: field.clone(),
                            doc_id,
                        });
                    }
                }
            }
        }
        // Anything left in `expected_index` was never found in the store. The
        // field and the owning doc_id are both recoverable from the key+metadata
        // (the value), so no owner side-map is needed.
        for (key, meta) in expected_index {
            let field = field_of_index_key(&key).unwrap_or_else(|| "<unknown>".to_string());
            let doc_id = stored_index_doc_id(&key, &meta);
            report
                .issues
                .push(IntegrityIssue::MissingIndexEntry { field, doc_id });
        }

        // ── Pass 3: walk stored `u` slots, diff against expected ───────────
        for field in &unique_paths {
            let prefix =
                KeyPrefix::UniqueIndexField(Cow::Borrowed(collection), Cow::Borrowed(field))
                    .encode();
            for result in self.txn.scan_prefix(&cf_handle, &prefix)? {
                let (key_bytes, value) = result?;
                report.unique_slots_checked += 1;
                match expected_unique.remove(&key_bytes) {
                    Some(expected_value) => {
                        if expected_value != value {
                            report.issues.push(IntegrityIssue::UniqueSlotOwnerMismatch {
                                field: field.clone(),
                                expected: render_doc_id(&expected_value),
                                found: render_doc_id(&value),
                            });
                        }
                    }
                    None => {
                        report.issues.push(IntegrityIssue::OrphanUniqueSlot {
                            field: field.clone(),
                        });
                    }
                }
            }
        }
        // Anything left in `expected_unique` is a missing slot. The field comes
        // from the key; the owning doc_id is the mapped value (length-prefixed).
        for (key, value) in expected_unique {
            let field = Key::decode_unique_index(&key)
                .map(|(_, f, _)| f.to_string())
                .unwrap_or_else(|| "<unknown>".to_string());
            let doc_id = render_doc_id(&value);
            report
                .issues
                .push(IntegrityIssue::MissingUniqueSlot { field, doc_id });
        }

        Ok(report)
    }

    /// The repair body (see [`KvEngine::repair`]). Rebuilds `i` then `u` entries.
    pub(crate) fn repair_collection(&self, cf: &str, collection: &str) -> Result<(), EngineError> {
        // `i` entries (user indexes + TTL) — reuse the migration reindex path,
        // which is the proven rebuild-from-records routine.
        self.reindex_collection_indexes(cf, collection)?;

        // `u` slots — rebuild from records too. Drop the existing slots first so
        // an orphaned slot cannot survive, then re-derive from the live records.
        let cf_handle = self.txn.cf(cf)?;
        let handle = self.collection(cf, collection)?;
        let unique_paths: Vec<String> = handle.unique_indexes().to_vec();
        for field in &unique_paths {
            let prefix =
                KeyPrefix::UniqueIndexField(Cow::Borrowed(collection), Cow::Borrowed(field))
                    .encode();
            self.delete_prefix(&cf_handle, &prefix)?;
        }
        if !unique_paths.is_empty() {
            let rec_prefix = KeyPrefix::Record(Cow::Borrowed(collection)).encode();
            let records: Vec<(Vec<u8>, Vec<u8>)> = self
                .txn
                .scan_prefix(&cf_handle, &rec_prefix)?
                .collect::<Result<_, _>>()?;
            for (key_bytes, value_bytes) in &records {
                let Some(Key::Record(_, doc_id)) = Key::decode(key_bytes) else {
                    continue;
                };
                let record = Record::from_bytes(value_bytes.clone())?;
                let doc = record.doc()?;
                for (u_key, u_val) in
                    unique_entries_from_document(collection, &unique_paths, doc, &doc_id)
                {
                    self.txn.put(&cf_handle, &u_key, &u_val)?;
                }
            }
        }

        Ok(())
    }
}

/// Render a stored index entry's `doc_id` for an issue message, falling back to
/// a placeholder if the boundary can't be resolved.
fn stored_index_doc_id(key_bytes: &[u8], metadata: &[u8]) -> String {
    IndexRecord::from_pair(key_bytes.to_vec(), metadata.to_vec())
        .and_then(|r| r.doc_id().map(|id| id.to_string()))
        .unwrap_or_else(|| "<unknown>".to_string())
}

/// The field name embedded in an `i` index key, if it parses.
fn field_of_index_key(key: &[u8]) -> Option<String> {
    crate::encoding::key::parse_index_collection_field(key).map(|(_, field)| field.to_string())
}

/// Render a length-prefixed `doc_id` value (a `u` slot's stored owner).
fn render_doc_id(value: &[u8]) -> String {
    crate::encoding::bson_value::BsonValue::parse_length_prefixed(value)
        .map(|(bv, _)| bv.to_string())
        .unwrap_or_else(|| "<unknown>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_CF;
    use crate::encoding::bson_value::BsonValue;
    use crate::traits::IndexOptions;
    use bson::spec::ElementType;
    use slate_store::MemoryStore;
    use std::borrow::Cow;

    fn engine() -> KvEngine<MemoryStore> {
        KvEngine::new(MemoryStore::new())
    }

    /// Seed collection `c` with a non-unique `name` index, a unique `email`
    /// index, and `n` documents.
    fn seeded(engine: &KvEngine<MemoryStore>, n: usize) {
        let tx = engine.begin(false).unwrap();
        tx.create_collection(DEFAULT_CF, "c", &Default::default())
            .unwrap();
        tx.create_index(DEFAULT_CF, "c", "name").unwrap();
        tx.create_index_with_options(DEFAULT_CF, "c", "email", &IndexOptions { unique: true })
            .unwrap();
        let handle = tx.collection(DEFAULT_CF, "c").unwrap();
        for i in 0..n {
            let doc = bson::rawdoc! {
                "_id": format!("u{i}"),
                "name": format!("name-{}", i % 3),
                "email": format!("u{i}@example.com"),
            };
            tx.put(&handle, &doc).unwrap();
        }
        tx.commit().unwrap();
    }

    fn str_value(s: &str) -> BsonValue<'static> {
        BsonValue {
            tag: ElementType::String,
            bytes: Cow::Owned(s.as_bytes().to_vec()),
        }
    }

    #[test]
    fn verify_clean_collection_reports_ok() {
        let engine = engine();
        seeded(&engine, 10);

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(report.ok(), "expected clean, got {:?}", report.issues);
        assert_eq!(report.records_checked, 10);
        // 10 name + 10 email `i` entries (no ttl field → no TTL `i` entries).
        assert_eq!(report.index_entries_checked, 20);
        assert_eq!(report.unique_slots_checked, 10);
    }

    #[test]
    fn verify_empty_collection_is_ok() {
        let engine = engine();
        let tx = engine.begin(false).unwrap();
        tx.create_collection(DEFAULT_CF, "c", &Default::default())
            .unwrap();
        tx.create_index(DEFAULT_CF, "c", "name").unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(report.ok());
        assert_eq!(report.records_checked, 0);
    }

    #[test]
    fn verify_detects_missing_index_entry() {
        let engine = engine();
        seeded(&engine, 5);

        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let prefix = KeyPrefix::IndexField(Cow::Borrowed("c"), Cow::Borrowed("name")).encode();
        let first = tx
            .txn
            .scan_prefix(&cf, &prefix)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        tx.txn.delete(&cf, &first).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(|i| matches!(
                i,
                IntegrityIssue::MissingIndexEntry { field, .. } if field == "name"
            )),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn verify_detects_orphan_index_entry() {
        let engine = engine();
        seeded(&engine, 5);

        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let value = str_value("ghost");
        let doc_id = str_value("does-not-exist");
        let key = Key::encode_index_key("c", "name", &value, &doc_id);
        tx.txn.put(&cf, &key, &[ElementType::String as u8]).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(|i| matches!(
                i,
                IntegrityIssue::OrphanIndexEntry { field, .. } if field == "name"
            )),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn verify_detects_index_metadata_mismatch() {
        let engine = engine();
        seeded(&engine, 3);

        // Overwrite an existing `name` entry's metadata with a wrong type byte.
        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let prefix = KeyPrefix::IndexField(Cow::Borrowed("c"), Cow::Borrowed("name")).encode();
        let key = tx
            .txn
            .scan_prefix(&cf, &prefix)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        // Use a fixed-width type byte so the boundary still resolves but the
        // metadata differs from the record-derived String tag.
        tx.txn.put(&cf, &key, &[ElementType::Int64 as u8]).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(|i| matches!(
                i,
                IntegrityIssue::IndexMetadataMismatch { field, .. } if field == "name"
            )),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn verify_detects_orphan_unique_slot() {
        let engine = engine();
        seeded(&engine, 3);

        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let mut keyed = vec![ElementType::String as u8];
        keyed.extend_from_slice(b"ghost@example.com");
        let key = Key::encode_unique_index("c", "email", &keyed);
        let mut value = Vec::new();
        str_value("phantom").write_length_prefixed(&mut value);
        tx.txn.put(&cf, &key, &value).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(
                |i| matches!(i, IntegrityIssue::OrphanUniqueSlot { field } if field == "email")
            ),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn verify_detects_missing_unique_slot() {
        let engine = engine();
        seeded(&engine, 3);

        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let prefix =
            KeyPrefix::UniqueIndexField(Cow::Borrowed("c"), Cow::Borrowed("email")).encode();
        let key = tx
            .txn
            .scan_prefix(&cf, &prefix)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        tx.txn.delete(&cf, &key).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(|i| matches!(
                i,
                IntegrityIssue::MissingUniqueSlot { field, .. } if field == "email"
            )),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn verify_detects_unique_slot_owner_mismatch() {
        let engine = engine();
        seeded(&engine, 3);

        let tx = engine.begin(false).unwrap();
        let cf = tx.txn.cf(DEFAULT_CF).unwrap();
        let prefix =
            KeyPrefix::UniqueIndexField(Cow::Borrowed("c"), Cow::Borrowed("email")).encode();
        let key = tx
            .txn
            .scan_prefix(&cf, &prefix)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        let mut wrong = Vec::new();
        str_value("wrong-owner").write_length_prefixed(&mut wrong);
        tx.txn.put(&cf, &key, &wrong).unwrap();
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(!report.ok());
        assert!(
            report.issues.iter().any(|i| matches!(
                i,
                IntegrityIssue::UniqueSlotOwnerMismatch { field, .. } if field == "email"
            )),
            "issues: {:?}",
            report.issues
        );
    }

    #[test]
    fn repair_rebuilds_drifted_indexes_to_clean() {
        let engine = engine();
        seeded(&engine, 8);

        // Corrupt: drop a `name` entry, add an orphan, mangle a `u` slot owner.
        {
            let tx = engine.begin(false).unwrap();
            let cf = tx.txn.cf(DEFAULT_CF).unwrap();

            let name_prefix =
                KeyPrefix::IndexField(Cow::Borrowed("c"), Cow::Borrowed("name")).encode();
            let drop_key = tx
                .txn
                .scan_prefix(&cf, &name_prefix)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .0;
            tx.txn.delete(&cf, &drop_key).unwrap();

            let value = str_value("ghost");
            let doc_id = str_value("nope");
            let orphan = Key::encode_index_key("c", "name", &value, &doc_id);
            tx.txn
                .put(&cf, &orphan, &[ElementType::String as u8])
                .unwrap();

            let u_prefix =
                KeyPrefix::UniqueIndexField(Cow::Borrowed("c"), Cow::Borrowed("email")).encode();
            let u_key = tx
                .txn
                .scan_prefix(&cf, &u_prefix)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .0;
            let mut wrong = Vec::new();
            str_value("wrong").write_length_prefixed(&mut wrong);
            tx.txn.put(&cf, &u_key, &wrong).unwrap();

            tx.commit().unwrap();
        }

        assert!(!engine.verify(DEFAULT_CF, "c").unwrap().ok());

        engine.repair(DEFAULT_CF, "c").unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(report.ok(), "post-repair issues: {:?}", report.issues);
        assert_eq!(report.records_checked, 8);
        assert_eq!(report.index_entries_checked, 16);
        assert_eq!(report.unique_slots_checked, 8);
    }

    /// SplitMix64 — a dependency-free, deterministic PRNG (mirrors the
    /// `numeric_key` fuzz corpus idiom).
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    /// Property/fuzz pass (RFC Thread B2): a random insert/update/delete stream
    /// against an indexed collection, asserting record ↔ `i` ↔ `u` consistency
    /// (via the Thread-C `verify` oracle) after *every* committed mutation.
    ///
    /// The unique `email` index makes this a real test of slot maintenance: each
    /// doc owns email `e{id}`, so an update that rewrites the email frees the old
    /// `u` slot and claims a new one — exactly the cross-structure invariant that
    /// would drift if the diff logic were wrong.
    #[test]
    fn fuzz_mutation_stream_keeps_indexes_consistent() {
        let engine = engine();
        {
            let tx = engine.begin(false).unwrap();
            tx.create_collection(DEFAULT_CF, "c", &Default::default())
                .unwrap();
            tx.create_index(DEFAULT_CF, "c", "bucket").unwrap();
            tx.create_index_with_options(DEFAULT_CF, "c", "email", &IndexOptions { unique: true })
                .unwrap();
            tx.commit().unwrap();
        }

        let mut rng = SplitMix64(0xDEAD_BEEF_CAFE_F00D);
        // A small key space so deletes/updates frequently hit live docs.
        const KEYS: u64 = 12;

        for step in 0..400u64 {
            let tx = engine.begin(false).unwrap();
            let handle = tx.collection(DEFAULT_CF, "c").unwrap();
            let id = rng.next() % KEYS;
            let id_str = format!("k{id}");

            match rng.next() % 3 {
                // Insert-or-replace: a fresh doc for this id. The email is keyed
                // to the id so the unique slot stays owned by exactly this doc.
                0 | 1 => {
                    let bucket = (rng.next() % 5) as i64;
                    let doc = bson::rawdoc! {
                        "_id": id_str.as_str(),
                        "bucket": bucket,
                        "email": format!("e{id}@x.com"),
                    };
                    tx.put(&handle, &doc).unwrap();
                }
                // Delete (no-op if absent).
                _ => {
                    let id_ref = bson::raw::RawBsonRef::String(id_str.as_str());
                    tx.delete(&handle, &id_ref).unwrap();
                }
            }
            tx.commit().unwrap();

            // Oracle: the collection must be internally consistent every step.
            let report = engine.verify(DEFAULT_CF, "c").unwrap();
            assert!(report.ok(), "drift at step {step}: {:?}", report.issues);
        }
    }

    #[test]
    fn verify_with_ttl_index_entries() {
        // A collection whose docs carry a TTL field: the TTL path produces `i`
        // entries, which must verify clean.
        let engine = engine();
        let tx = engine.begin(false).unwrap();
        tx.create_collection(DEFAULT_CF, "c", &Default::default())
            .unwrap();
        let handle = tx.collection(DEFAULT_CF, "c").unwrap();
        for i in 0..4 {
            let doc = bson::rawdoc! {
                "_id": format!("u{i}"),
                "ttl": bson::DateTime::from_millis(1_000_000 + i as i64),
            };
            tx.put(&handle, &doc).unwrap();
        }
        tx.commit().unwrap();

        let report = engine.verify(DEFAULT_CF, "c").unwrap();
        assert!(report.ok(), "issues: {:?}", report.issues);
        assert_eq!(report.records_checked, 4);
        // TTL path yields one `i` entry per doc.
        assert_eq!(report.index_entries_checked, 4);
    }
}
