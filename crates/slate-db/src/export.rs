//! Logical export / import: a manifest-driven, backend-neutral dump and reload.
//!
//! Where [`Database::backup`](crate::Database::backup) copies a backend's *native*
//! files (a RocksDB checkpoint, a redb file copy) and can only be restored by the
//! same backend, a *logical* dump is **catalog + document streams** in the native
//! BSON representation — portable across backends. It is the format for
//! cross-backend migration (redb → RocksDB), dev seeding, and recovery when a
//! physical file is suspect.
//!
//! ## On-disk layout
//!
//! ```text
//! dump/
//!   manifest.bson            ← collection defs: pk_path, ttl_path, indexes (+unique)
//!   <cf>.<collection>.bson   ← one document stream per collection
//! ```
//!
//! A document-stream file is the concatenation of each document's raw BSON bytes.
//! BSON is self-framing — its first four little-endian bytes are the document's
//! total length — so a reader streams document-by-document without a separate
//! framing format, and a writer never has to materialize the whole collection.
//!
//! ## Indexes are rebuilt, not dumped
//!
//! The manifest records *which* fields are indexed, but index entries are never
//! written to the dump. On import the collection is recreated with its index
//! definitions and the documents are re-inserted, so the engine builds correctly
//! encoded index entries for the *target* backend's current encoding version —
//! the same "records are the source of truth" rebuild the index-encoding
//! migration and [`repair`](crate::Database::repair) already rely on. That keeps
//! the dump small and means an import can never carry stale index encodings.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use slate_store::Store;

use crate::Database;
use crate::collection::CollectionConfig;
use crate::cursor::Cursor;
use crate::error::DbError;

/// File name of the manifest within a dump directory.
const MANIFEST_FILE: &str = "manifest.bson";

/// Documents per `insert`/`upsert` batch on import. Bounds peak memory so a
/// multi-GB dump reloads without ever being fully resident.
const IMPORT_BATCH: usize = 1000;

/// How an import resolves a document whose `_id` already exists in the target
/// collection. Defaults to [`Error`](OnCollision::Error), reusing the
/// insert/upsert semantics already in the mutation API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnCollision {
    /// Abort the import (the whole transaction rolls back) on the first
    /// pre-existing `_id`. The default: an import into a fresh database can only
    /// hit this on a genuinely duplicated dump.
    #[default]
    Error,
    /// Replace the existing document with the dumped one (`upsert`).
    Overwrite,
    /// Keep the existing document and skip the dumped one (insert-if-absent).
    Skip,
}

/// Options for [`Database::export`].
#[derive(Debug, Clone, Default)]
pub struct ExportOptions {
    /// Export only this `(cf, collection)`; `None` dumps the whole database.
    pub only: Option<(String, String)>,
}

/// Options for [`Database::import`].
///
/// The default imports every collection ([`only`](Self::only) `None`) and errors
/// on the first `_id` collision ([`OnCollision::Error`]).
#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    /// Import only this collection (by name); `None` imports every collection in
    /// the manifest.
    pub only: Option<String>,
    /// Collision behavior when a document's `_id` already exists.
    pub on_collision: OnCollision,
}

/// Outcome of an [`export`](Database::export): which collections were dumped and
/// how many documents each contributed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportReport {
    /// `(cf, collection, document_count)` for every collection written.
    pub collections: Vec<(String, String, u64)>,
}

impl ExportReport {
    /// Total documents written across every collection.
    pub fn total_documents(&self) -> u64 {
        self.collections.iter().map(|(_, _, n)| *n).sum()
    }
}

/// Outcome of an [`import`](Database::import).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReport {
    /// `(cf, collection, documents_loaded)` for every collection read. With
    /// [`OnCollision::Skip`] the count is documents *actually inserted*, not the
    /// number present in the dump.
    pub collections: Vec<(String, String, u64)>,
}

impl ImportReport {
    /// Total documents loaded across every collection.
    pub fn total_documents(&self) -> u64 {
        self.collections.iter().map(|(_, _, n)| *n).sum()
    }
}

/// One collection's full definition in the manifest. Everything import needs to
/// recreate the collection before reloading its documents — its key paths and
/// the fields it indexes (with the unique subset called out).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDef {
    pub cf: String,
    pub name: String,
    pub pk_path: String,
    pub ttl_path: String,
    /// Every indexed field (including the unique ones and the TTL path's index).
    pub indexes: Vec<String>,
    /// The subset of `indexes` that enforce uniqueness.
    pub unique_indexes: Vec<String>,
}

/// The catalog half of a dump: every collection's definition. Serialized to
/// `manifest.bson`. Versioned so a future format change can be detected rather
/// than misread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version. Bumped only on an incompatible layout change.
    pub version: u32,
    pub collections: Vec<CollectionDef>,
}

/// Current manifest format version.
const MANIFEST_VERSION: u32 = 1;

impl Manifest {
    /// Read and decode a manifest from `dir/manifest.bson`.
    fn read(dir: &Path) -> Result<Self, DbError> {
        let path = dir.join(MANIFEST_FILE);
        let mut bytes = Vec::new();
        File::open(&path)
            .and_then(|mut f| f.read_to_end(&mut bytes))
            .map_err(|e| {
                DbError::InvalidDocument(format!("cannot read {}: {e}", path.display()))
            })?;
        let manifest: Manifest = bson::deserialize_from_slice(&bytes)
            .map_err(|e| DbError::InvalidDocument(format!("malformed manifest: {e}")))?;
        if manifest.version != MANIFEST_VERSION {
            return Err(DbError::InvalidDocument(format!(
                "unsupported dump format version {} (expected {MANIFEST_VERSION})",
                manifest.version
            )));
        }
        Ok(manifest)
    }

    /// Serialize and write the manifest to `dir/manifest.bson`.
    fn write(&self, dir: &Path) -> Result<(), DbError> {
        let bytes = bson::serialize_to_vec(self)?;
        let path = dir.join(MANIFEST_FILE);
        File::create(&path)
            .and_then(|mut f| f.write_all(&bytes))
            .map_err(|e| {
                DbError::InvalidDocument(format!("cannot write {}: {e}", path.display()))
            })?;
        Ok(())
    }
}

/// The document-stream file name for a collection: `<cf>.<collection>.bson`.
fn stream_file_name(cf: &str, collection: &str) -> String {
    format!("{cf}.{collection}.bson")
}

impl<S: Store> Database<S> {
    /// Export the database (or a single collection) to a logical dump under
    /// `dir`, creating `dir` if needed.
    ///
    /// Writes `manifest.bson` (collection definitions) plus one
    /// `<cf>.<collection>.bson` document stream per collection. Documents are read
    /// through a single read-only transaction so the dump is a coherent snapshot,
    /// and streamed document-by-document so the whole database is never resident.
    ///
    /// The dump is backend-neutral: export from a redb `Database`,
    /// [`import`](Self::import) into a RocksDB one.
    pub fn export(
        &self,
        dir: impl AsRef<Path>,
        options: ExportOptions,
    ) -> Result<ExportReport, DbError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| {
            DbError::InvalidDocument(format!("cannot create {}: {e}", dir.display()))
        })?;

        // One read-only transaction backs the whole export, so every collection
        // is read from the same consistent snapshot.
        let txn = self.begin(true)?;

        let targets: Vec<(String, String)> = match &options.only {
            // Clone two short collection-name strings out of the borrowed options
            // to own the single-target list (the alternative borrows `options`
            // for the loop below, which we'd rather not tie its lifetime to).
            Some((cf, name)) => vec![(cf.clone(), name.clone())],
            None => txn.list_collections()?,
        };

        let mut defs = Vec::with_capacity(targets.len());
        let mut report = Vec::with_capacity(targets.len());
        for (cf, name) in &targets {
            let schema = txn.collection_schema(cf, name)?;
            let count = write_collection_stream(dir, &txn, cf, name)?;
            defs.push(CollectionDef {
                cf: schema.cf,
                name: schema.name,
                pk_path: schema.pk_path,
                ttl_path: schema.ttl_path,
                indexes: schema.indexes,
                unique_indexes: schema.unique_indexes,
            });
            // Clone the two name strings into the report; `cf`/`name` are
            // borrowed from `targets` and still needed by later iterations.
            report.push((cf.clone(), name.clone(), count));
        }

        let manifest = Manifest {
            version: MANIFEST_VERSION,
            collections: defs,
        };
        manifest.write(dir)?;

        // Read-only: nothing to commit, release the snapshot.
        let _ = txn.rollback();
        Ok(ExportReport {
            collections: report,
        })
    }

    /// Import a logical dump from `dir` into this database.
    ///
    /// Reads `manifest.bson`, recreates each collection with its full definition
    /// (key paths and indexes, including unique ones), then streams the dumped
    /// documents back in — the engine rebuilds correctly-encoded index entries
    /// for *this* backend as the documents land. The whole import runs in one
    /// write transaction, so any failure (a malformed stream, a unique violation,
    /// an unexpected collision) rolls the entire import back rather than leaving
    /// it half-applied.
    ///
    /// A collection that already exists is reused (its definition is taken as
    /// given, the manifest's is not re-applied); [`ImportOptions::on_collision`]
    /// then governs per-`_id` conflicts. Importing into a fresh database recreates
    /// everything from the manifest.
    pub fn import(
        &self,
        dir: impl AsRef<Path>,
        options: ImportOptions,
    ) -> Result<ImportReport, DbError> {
        let dir = dir.as_ref();
        let manifest = Manifest::read(dir)?;

        let txn = self.begin(false)?;
        let existing: std::collections::HashSet<(String, String)> =
            txn.list_collections()?.into_iter().collect();

        let mut report = Vec::new();
        for def in &manifest.collections {
            if let Some(only) = &options.only
                && &def.name != only
            {
                continue;
            }

            // Clone the names to build a lookup key; the `HashSet` is keyed by
            // owned `(String, String)` and `def` is borrowed and reused below.
            if !existing.contains(&(def.cf.clone(), def.name.clone())) {
                create_collection_from_def(&txn, def)?;
            }

            let count =
                read_collection_stream(dir, &txn, &def.cf, &def.name, options.on_collision)?;
            // Clone the names into the report; `def` is borrowed from the
            // manifest and reused by the remaining iterations.
            report.push((def.cf.clone(), def.name.clone(), count));
        }

        txn.commit()?;
        Ok(ImportReport {
            collections: report,
        })
    }
}

/// Recreate a collection from its manifest definition: the collection itself,
/// then every index (unique ones as unique). The TTL path's index is auto-created
/// by `create_collection`; creating it again is harmless (`IndexExists` is
/// ignored by `create_collection`, and we skip it here too).
fn create_collection_from_def<S: Store>(
    txn: &crate::database::Transaction<'_, S>,
    def: &CollectionDef,
) -> Result<(), DbError> {
    txn.create_collection(&CollectionConfig {
        // Owned strings: the config takes ownership, and `def` is borrowed and
        // reused for index creation below, so its fields can't be moved out.
        name: def.name.clone(),
        cf: def.cf.clone(),
        pk_path: def.pk_path.clone(),
        ttl_path: def.ttl_path.clone(),
    })?;

    let unique: std::collections::HashSet<&str> =
        def.unique_indexes.iter().map(String::as_str).collect();
    for field in &def.indexes {
        // The TTL index is created by `create_collection`; don't redefine it.
        if field == &def.ttl_path {
            continue;
        }
        if unique.contains(field.as_str()) {
            txn.create_unique_index(&def.cf, &def.name, field)?;
        } else {
            txn.create_index(&def.cf, &def.name, field)?;
        }
    }
    Ok(())
}

/// Stream every document of one collection into its `<cf>.<collection>.bson`
/// file, returning the document count. Reads through the supplied read-only
/// transaction (the export snapshot) so the file is coherent with the manifest.
fn write_collection_stream<S: Store>(
    dir: &Path,
    txn: &crate::database::Transaction<'_, S>,
    cf: &str,
    collection: &str,
) -> Result<u64, DbError> {
    let path: PathBuf = dir.join(stream_file_name(cf, collection));
    let file = File::create(&path)
        .map_err(|e| DbError::InvalidDocument(format!("cannot write {}: {e}", path.display())))?;
    let mut writer = BufWriter::new(file);

    // A full scan with no filter yields every document; iterate raw so each one
    // is written in its native BSON representation with no deserialize round-trip.
    let cursor = txn.find(cf, collection, bson::Document::new(), Default::default())?;
    let mut count = 0u64;
    for doc in cursor.iter_raw()? {
        let doc = doc?;
        writer.write_all(doc.as_bytes()).map_err(|e| {
            DbError::InvalidDocument(format!("cannot write {}: {e}", path.display()))
        })?;
        count += 1;
    }
    writer
        .flush()
        .map_err(|e| DbError::InvalidDocument(format!("cannot flush {}: {e}", path.display())))?;
    Ok(count)
}

/// Read a collection's `<cf>.<collection>.bson` document stream and load it into
/// the target collection within `txn`, returning the number of documents loaded
/// (for [`OnCollision::Skip`], the number actually inserted). Documents are read
/// and inserted in bounded batches so a large dump never fully materializes.
fn read_collection_stream<S: Store>(
    dir: &Path,
    txn: &crate::database::Transaction<'_, S>,
    cf: &str,
    collection: &str,
    on_collision: OnCollision,
) -> Result<u64, DbError> {
    let path = dir.join(stream_file_name(cf, collection));
    let file = File::open(&path)
        .map_err(|e| DbError::InvalidDocument(format!("cannot open {}: {e}", path.display())))?;
    let mut reader = DocStreamReader::new(BufReader::new(file), path);

    let mut count = 0u64;
    let mut batch: Vec<bson::RawDocumentBuf> = Vec::with_capacity(IMPORT_BATCH);
    while let Some(doc) = reader.next_doc()? {
        batch.push(doc);
        if batch.len() >= IMPORT_BATCH {
            count += flush_batch(txn, cf, collection, &mut batch, on_collision)?;
        }
    }
    count += flush_batch(txn, cf, collection, &mut batch, on_collision)?;
    Ok(count)
}

/// Apply one batch of documents under the chosen collision policy and clear it,
/// returning how many were loaded.
fn flush_batch<S: Store>(
    txn: &crate::database::Transaction<'_, S>,
    cf: &str,
    collection: &str,
    batch: &mut Vec<bson::RawDocumentBuf>,
    on_collision: OnCollision,
) -> Result<u64, DbError> {
    if batch.is_empty() {
        return Ok(0);
    }
    let loaded = match on_collision {
        // Plain insert: a pre-existing `_id` surfaces as `DuplicateKey`, which
        // aborts the whole import (the transaction is dropped uncommitted).
        OnCollision::Error => txn.insert_many(cf, collection, batch.drain(..))?.drain()?,
        // Replace whatever is there.
        OnCollision::Overwrite => txn.upsert_many(cf, collection, batch.drain(..))?.drain()?,
        // Insert only those whose `_id` is absent. Done one at a time so a
        // collision skips just that document instead of failing the batch. The
        // duplicate-key error surfaces when the insert cursor is *drained* (the
        // mutation is lazy), so the match is on `insert_one(..).drain()`.
        OnCollision::Skip => {
            let mut loaded = 0u64;
            for doc in batch.drain(..) {
                match txn.insert_one(cf, collection, doc).and_then(Cursor::drain) {
                    Ok(n) => loaded += n,
                    Err(DbError::DuplicateKey(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            loaded
        }
    };
    Ok(loaded)
}

/// A streaming reader over a concatenation of raw BSON documents.
///
/// BSON is self-framing: a document's first four little-endian bytes are its
/// total byte length. The reader peels one length-prefixed document off the
/// stream at a time, so an arbitrarily large dump file is read in bounded memory.
struct DocStreamReader<R: Read> {
    reader: R,
    path: PathBuf,
}

impl<R: Read> DocStreamReader<R> {
    fn new(reader: R, path: PathBuf) -> Self {
        Self { reader, path }
    }

    /// Read the next document, or `None` at a clean end of stream.
    fn next_doc(&mut self) -> Result<Option<bson::RawDocumentBuf>, DbError> {
        // First four bytes: the document length (which includes these four).
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => {
                return Err(DbError::InvalidDocument(format!(
                    "cannot read {}: {e}",
                    self.path.display()
                )));
            }
        }
        let len = i32::from_le_bytes(len_buf);
        if len < 5 {
            return Err(DbError::InvalidDocument(format!(
                "{}: corrupt document length {len}",
                self.path.display()
            )));
        }

        // Read the rest of the document into a buffer that begins with the length
        // prefix, so the bytes form a complete BSON document.
        let total = len as usize;
        let mut buf = vec![0u8; total];
        buf[..4].copy_from_slice(&len_buf);
        self.reader.read_exact(&mut buf[4..]).map_err(|e| {
            DbError::InvalidDocument(format!(
                "{}: truncated document (expected {total} bytes): {e}",
                self.path.display()
            ))
        })?;

        let doc = bson::RawDocumentBuf::from_bytes(buf)
            .map_err(|e| DbError::InvalidDocument(format!("{}: {e}", self.path.display())))?;
        Ok(Some(doc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_CF, DatabaseBuilder};
    use bson::doc;
    use slate_store::MemoryStore;
    use std::str::FromStr;

    fn mem_db() -> Database<MemoryStore> {
        DatabaseBuilder::new().open(MemoryStore::new()).unwrap()
    }

    /// Build a populated source database with two collections, indexes (one
    /// unique), custom pk/ttl paths, and BSON-specific types (ObjectId, DateTime,
    /// Decimal128) to prove the dump is lossless.
    fn populated() -> Database<MemoryStore> {
        let db = mem_db();
        let txn = db.begin(false).unwrap();

        txn.create_collection(&CollectionConfig {
            name: "users".to_string(),
            ..Default::default()
        })
        .unwrap();
        txn.create_index(DEFAULT_CF, "users", "city").unwrap();
        txn.create_unique_index(DEFAULT_CF, "users", "email")
            .unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "users",
            vec![
                doc! { "_id": "1", "name": "ada", "city": "London",
                "email": "ada@x.io", "joined": bson::DateTime::from_millis(1_000),
                "oid": bson::oid::ObjectId::new(),
                "balance": bson::Decimal128::from_str("12.34").unwrap() }, // FromStr in scope
                doc! { "_id": "2", "name": "alan", "city": "London",
                "email": "alan@x.io" },
                doc! { "_id": "3", "name": "grace", "city": "York",
                "email": "grace@x.io" },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();

        // A second collection with a non-default pk/ttl path.
        txn.create_collection(&CollectionConfig {
            name: "events".to_string(),
            pk_path: "key".to_string(),
            ttl_path: "expires".to_string(),
            ..Default::default()
        })
        .unwrap();
        txn.insert_many(
            DEFAULT_CF,
            "events",
            vec![
                doc! { "key": "e1", "kind": "click" },
                doc! { "key": "e2", "kind": "view" },
            ],
        )
        .unwrap()
        .drain()
        .unwrap();

        txn.commit().unwrap();
        db
    }

    /// Gather a collection's documents into a sorted-by-id vec for comparison,
    /// independent of storage order.
    fn dump_docs<S: Store>(
        db: &Database<S>,
        cf: &str,
        collection: &str,
        pk: &str,
    ) -> Vec<bson::Document> {
        let txn = db.begin(true).unwrap();
        let cursor = txn
            .find(cf, collection, doc! {}, Default::default())
            .unwrap();
        let mut docs: Vec<bson::Document> = cursor
            .iter::<bson::Document>()
            .unwrap()
            .map(|d| d.unwrap())
            .collect();
        txn.rollback().unwrap();
        docs.sort_by(|a, b| {
            let ka = a.get(pk).and_then(|v| v.as_str()).unwrap_or("");
            let kb = b.get(pk).and_then(|v| v.as_str()).unwrap_or("");
            ka.cmp(kb)
        });
        docs
    }

    #[test]
    fn round_trip_whole_db_into_fresh() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();

        let report = src.export(dir.path(), ExportOptions::default()).unwrap();
        assert_eq!(report.total_documents(), 5);

        // Import into a brand-new, empty database.
        let dst = mem_db();
        let imported = dst.import(dir.path(), ImportOptions::default()).unwrap();
        assert_eq!(imported.total_documents(), 5);

        // Documents are identical, including BSON-specific types.
        assert_eq!(
            dump_docs(&src, DEFAULT_CF, "users", "_id"),
            dump_docs(&dst, DEFAULT_CF, "users", "_id")
        );
        assert_eq!(
            dump_docs(&src, DEFAULT_CF, "events", "key"),
            dump_docs(&dst, DEFAULT_CF, "events", "key")
        );

        // Catalog is identical (pk/ttl paths and indexes), proving the manifest
        // round-trips the full definition, not just documents.
        let s_users = src
            .begin(true)
            .unwrap()
            .collection_schema(DEFAULT_CF, "users")
            .unwrap();
        let d_users = dst
            .begin(true)
            .unwrap()
            .collection_schema(DEFAULT_CF, "users")
            .unwrap();
        assert_eq!(s_users, d_users);

        let s_events = src
            .begin(true)
            .unwrap()
            .collection_schema(DEFAULT_CF, "events")
            .unwrap();
        let d_events = dst
            .begin(true)
            .unwrap()
            .collection_schema(DEFAULT_CF, "events")
            .unwrap();
        assert_eq!(s_events, d_events);
    }

    #[test]
    fn imported_indexes_are_queryable() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        let dst = mem_db();
        dst.import(dir.path(), ImportOptions::default()).unwrap();

        // The `city` index was rebuilt from the records on import: a query that
        // would use it returns the right rows.
        let txn = dst.begin(true).unwrap();
        let names: Vec<String> = txn
            .query(
                DEFAULT_CF,
                "users",
                "SELECT VALUE c.name FROM c WHERE c.city = 'London' ORDER BY c.name",
            )
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|n| n.unwrap())
            .collect();
        txn.rollback().unwrap();
        assert_eq!(names, vec!["ada".to_string(), "alan".to_string()]);
    }

    #[test]
    fn imported_unique_index_is_enforced() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        let dst = mem_db();
        dst.import(dir.path(), ImportOptions::default()).unwrap();

        // The unique `email` index was recreated: inserting a duplicate fails.
        // The mutation is lazy, so the violation surfaces when the cursor drains.
        let txn = dst.begin(false).unwrap();
        let err = txn
            .insert_one(
                DEFAULT_CF,
                "users",
                doc! { "_id": "99", "email": "ada@x.io" },
            )
            .and_then(|c| c.drain());
        assert!(matches!(err, Err(DbError::UniqueViolation { .. })));
        txn.rollback().unwrap();
    }

    #[test]
    fn export_single_collection() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        let report = src
            .export(
                dir.path(),
                ExportOptions {
                    only: Some((DEFAULT_CF.to_string(), "events".to_string())),
                },
            )
            .unwrap();
        assert_eq!(report.collections.len(), 1);
        assert_eq!(report.total_documents(), 2);

        let dst = mem_db();
        let imported = dst.import(dir.path(), ImportOptions::default()).unwrap();
        assert_eq!(imported.collections.len(), 1);
        assert_eq!(imported.total_documents(), 2);
        assert!(
            dst.list_collections()
                .unwrap()
                .iter()
                .any(|(_, n)| n == "events")
        );
        assert!(
            !dst.list_collections()
                .unwrap()
                .iter()
                .any(|(_, n)| n == "users")
        );
    }

    #[test]
    fn import_single_collection_from_full_dump() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        // Whole-db dump, but import only `events`.
        let dst = mem_db();
        let imported = dst
            .import(
                dir.path(),
                ImportOptions {
                    only: Some("events".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(imported.collections.len(), 1);
        assert_eq!(imported.total_documents(), 2);
        assert!(
            !dst.list_collections()
                .unwrap()
                .iter()
                .any(|(_, n)| n == "users")
        );
    }

    #[test]
    fn import_collision_error_aborts() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        // Target already holds one of the `_id`s.
        let dst = mem_db();
        {
            let txn = dst.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "users".to_string(),
                ..Default::default()
            })
            .unwrap();
            txn.insert_one(DEFAULT_CF, "users", doc! { "_id": "1", "name": "PRE" })
                .unwrap()
                .drain()
                .unwrap();
            txn.commit().unwrap();
        }

        let err = dst.import(
            dir.path(),
            ImportOptions {
                only: Some("users".to_string()),
                on_collision: OnCollision::Error,
            },
        );
        assert!(matches!(err, Err(DbError::DuplicateKey(_))));

        // The whole import rolled back: the pre-existing doc is untouched and no
        // new docs landed.
        assert_eq!(
            dst.begin(true)
                .unwrap()
                .count(DEFAULT_CF, "users", doc! {})
                .unwrap(),
            1
        );
    }

    #[test]
    fn import_collision_overwrite_replaces() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        let dst = mem_db();
        {
            let txn = dst.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "users".to_string(),
                ..Default::default()
            })
            .unwrap();
            txn.insert_one(DEFAULT_CF, "users", doc! { "_id": "1", "name": "PRE" })
                .unwrap()
                .drain()
                .unwrap();
            txn.commit().unwrap();
        }

        dst.import(
            dir.path(),
            ImportOptions {
                only: Some("users".to_string()),
                on_collision: OnCollision::Overwrite,
            },
        )
        .unwrap();

        // `_id` "1" now holds the dumped document (name "ada"), not "PRE".
        let txn = dst.begin(true).unwrap();
        let name: Vec<String> = txn
            .query(
                DEFAULT_CF,
                "users",
                "SELECT VALUE c.name FROM c WHERE c._id = '1'",
            )
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|n| n.unwrap())
            .collect();
        txn.rollback().unwrap();
        assert_eq!(name, vec!["ada".to_string()]);
    }

    #[test]
    fn import_collision_skip_keeps_existing() {
        let src = populated();
        let dir = tempfile::tempdir().unwrap();
        src.export(dir.path(), ExportOptions::default()).unwrap();

        let dst = mem_db();
        {
            let txn = dst.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "users".to_string(),
                ..Default::default()
            })
            .unwrap();
            txn.insert_one(DEFAULT_CF, "users", doc! { "_id": "1", "name": "PRE" })
                .unwrap()
                .drain()
                .unwrap();
            txn.commit().unwrap();
        }

        let report = dst
            .import(
                dir.path(),
                ImportOptions {
                    only: Some("users".to_string()),
                    on_collision: OnCollision::Skip,
                },
            )
            .unwrap();
        // Two of three dumped users were new ("2","3"); "1" was skipped.
        assert_eq!(report.total_documents(), 2);

        let txn = dst.begin(true).unwrap();
        let name: Vec<String> = txn
            .query(
                DEFAULT_CF,
                "users",
                "SELECT VALUE c.name FROM c WHERE c._id = '1'",
            )
            .unwrap()
            .iter_values::<String>()
            .unwrap()
            .map(|n| n.unwrap())
            .collect();
        assert_eq!(txn.count(DEFAULT_CF, "users", doc! {}).unwrap(), 3);
        txn.rollback().unwrap();
        assert_eq!(name, vec!["PRE".to_string()]);
    }

    #[test]
    fn import_rejects_bad_manifest_version() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            version: 999,
            collections: Vec::new(),
        };
        let bytes = bson::serialize_to_vec(&manifest).unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILE), bytes).unwrap();

        let dst = mem_db();
        let err = dst.import(dir.path(), ImportOptions::default());
        assert!(matches!(err, Err(DbError::InvalidDocument(_))));
    }

    #[test]
    fn empty_collection_round_trips() {
        let src = mem_db();
        {
            let txn = src.begin(false).unwrap();
            txn.create_collection(&CollectionConfig {
                name: "empty".to_string(),
                ..Default::default()
            })
            .unwrap();
            txn.commit().unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let report = src.export(dir.path(), ExportOptions::default()).unwrap();
        assert_eq!(report.total_documents(), 0);

        let dst = mem_db();
        dst.import(dir.path(), ImportOptions::default()).unwrap();
        assert!(
            dst.list_collections()
                .unwrap()
                .iter()
                .any(|(_, n)| n == "empty")
        );
        assert_eq!(
            dst.begin(true)
                .unwrap()
                .count(DEFAULT_CF, "empty", doc! {})
                .unwrap(),
            0
        );
    }
}
