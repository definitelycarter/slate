//! On-disk format versioning registry.
//!
//! Slate persists three independent on-disk formats — index entries, record
//! blobs, and the catalog. Each *active* format declares a version stored under
//! a reserved `_sys_` marker (`m\0<format>_version`, whose `m` tag is distinct
//! from every [`Key`](crate::encoding::Key) tag, so the marker is inert to
//! `Key::decode`). On open, [`KvEngine::check_and_migrate_formats`] validates
//! every marker before any transaction is served:
//!
//! - **matches** the binary's supported version — proceed;
//! - **older** on disk — migrate forward (atomic, idempotent on retry), then
//!   stamp the new version;
//! - **newer** on disk — refuse with [`EngineError::UnsupportedFormatVersion`],
//!   naming both versions, never a silent mis-read.
//!
//! This generalises the original index-encoding precedent (see [`super::migrate`])
//! — which versioned exactly one of the three formats — into one contract the
//! catalog now follows too.
//!
//! ## Reserved: the record format
//!
//! The record blob (`encoding/record.rs`) carries a leading *tag* byte
//! (`TAG_NO_TTL` / `TAG_TTL`) that is semantic, not a version. A record format
//! version is **design-reserved**, not implemented: the day a second record
//! layout exists (a compressed body, an added header field), it gains a
//! `Record` variant here, gating how `Record::from_bytes` interprets the tag
//! space — adding no per-record bytes. Until then no marker is stamped, and
//! `from_bytes`'s fail-closed unknown-tag rejection is the backstop. Reserving
//! the seam now means the contract is already in place the day it is needed.

use slate_store::{Store, Transaction};

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

/// Current catalog format version. Bumped whenever the catalog layout
/// (`CollectionMeta`, the index-config value) changes in a way an older binary
/// could not read correctly.
///
/// - `1` — the first versioned catalog. The pre-versioning serde-BSON layout is
///   byte-compatible with it (a new `version` field reads as `0` via
///   `#[serde(default)]`), so the forward migration from `0` is a version stamp,
///   not a blob rewrite.
/// - `2` — adds vector-index config: a new `y`-tagged `_sys_` keyspace holding a
///   self-describing serialized `VectorIndexSpec` per `(cf, collection, field)`,
///   plus a `w`-tagged vector data keyspace in each collection's CF. Purely
///   additive — v1 catalogs carry no `y`/`w` keys, so the forward migration is a
///   version stamp with **no migration code** (there are no persisted databases
///   to migrate; the bump is for honesty under the migrate-or-refuse contract).
pub(crate) const CATALOG_VERSION: u8 = 2;

/// An *active* on-disk format — one that carries a stamped version marker and
/// participates in the open-time check. The record format is reserved (see the
/// module docs) and deliberately absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Format {
    IndexEncoding,
    Catalog,
}

impl Format {
    /// Human-readable name, used in [`EngineError::UnsupportedFormatVersion`].
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Format::IndexEncoding => "index_encoding",
            Format::Catalog => "catalog",
        }
    }

    /// The reserved `_sys_` key holding this format's version. The leading `m`
    /// ("meta") tag is distinct from every `Key` tag, so the marker never
    /// collides with collection/index/function metadata.
    pub(crate) const fn sys_key(self) -> &'static [u8] {
        match self {
            Format::IndexEncoding => b"m\x00index_encoding_version",
            Format::Catalog => b"m\x00catalog_version",
        }
    }

    /// The version this binary writes and supports for the format.
    pub(crate) const fn current_version(self) -> u8 {
        match self {
            Format::IndexEncoding => INDEX_ENCODING_VERSION,
            Format::Catalog => CATALOG_VERSION,
        }
    }
}

/// Refuse cleanly when an on-disk format is newer than this binary supports —
/// opening it could only mis-read. Shared by every format's migrate entry so
/// the "too new → refuse, never proceed" rule is enforced in exactly one place.
pub(crate) fn refuse_if_too_new(format: Format, found: u8) -> Result<(), EngineError> {
    if found > format.current_version() {
        return Err(EngineError::UnsupportedFormatVersion {
            format: format.name(),
            found,
            supported: format.current_version(),
        });
    }
    Ok(())
}

impl<S: Store> KvEngine<S> {
    /// Validate and bring every on-disk format up to date before serving
    /// transactions. Refuses cleanly if any format on disk is newer than this
    /// binary; otherwise migrates each behind-format forward. Run on open, in
    /// place of the old index-encoding-only migration.
    pub fn check_and_migrate_formats(&self) -> Result<(), EngineError> {
        // Index entries first: an un-migrated string index would silently
        // undercount, and the catalog stamp is cheap by comparison.
        self.migrate_index_encoding()?;
        self.migrate_catalog()?;
        Ok(())
    }

    /// Bring the catalog format up to [`CATALOG_VERSION`].
    ///
    /// A no-op (a single read-only peek) when already current. Behind: `v1` is
    /// byte-compatible with the pre-versioning serde layout, so the forward
    /// migration is a version stamp committed atomically — a crash before the
    /// commit rolls back and re-runs on the next open rather than half-advancing.
    /// Newer on disk: refuses with [`EngineError::UnsupportedFormatVersion`].
    pub fn migrate_catalog(&self) -> Result<(), EngineError> {
        // Fast path: avoid opening a write transaction when already current.
        {
            let txn = self.begin(true)?;
            let current = txn.format_version(Format::Catalog)?;
            txn.rollback()?;
            refuse_if_too_new(Format::Catalog, current)?;
            if current == CATALOG_VERSION {
                return Ok(());
            }
        }

        let txn = self.begin(false)?;
        txn.stamp_format_version(Format::Catalog)?;
        txn.commit()?;
        Ok(())
    }
}

impl<'a, S: Store + 'a> KvTransaction<'a, S> {
    /// Read a format's stored version (`0` when the marker is absent, i.e. a
    /// store written before that format was versioned).
    pub(crate) fn format_version(&self, format: Format) -> Result<u8, EngineError> {
        let sys = self.sys_cf()?;
        Ok(self
            .txn
            .get(&sys, format.sys_key())?
            .and_then(|v| v.first().copied())
            .unwrap_or(0))
    }

    /// Stamp a format's marker to the version this binary supports.
    pub(crate) fn stamp_format_version(&self, format: Format) -> Result<(), EngineError> {
        let sys = self.sys_cf()?;
        self.txn
            .put(&sys, format.sys_key(), &[format.current_version()])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_CF;
    use crate::traits::{Catalog, CreateCollectionOptions, Engine, EngineTransaction};
    use slate_store::{MemoryStore, Transaction};

    /// Opening a fresh store stamps every active format to its current version.
    #[test]
    fn fresh_store_stamps_all_formats() {
        let engine = KvEngine::new(MemoryStore::new());
        engine.check_and_migrate_formats().unwrap();

        let tx = engine.begin(true).unwrap();
        assert_eq!(
            tx.format_version(Format::IndexEncoding).unwrap(),
            INDEX_ENCODING_VERSION
        );
        assert_eq!(tx.format_version(Format::Catalog).unwrap(), CATALOG_VERSION);
        tx.rollback().unwrap();
    }

    /// A legacy store (catalog marker absent → reads as 0) is stamped forward to
    /// v1 on open, and its existing collections still load — v1 is byte-compatible
    /// with the pre-versioning layout.
    #[test]
    fn legacy_catalog_stamps_v1_and_still_loads() {
        let engine = KvEngine::new(MemoryStore::new());

        // Create a collection, then strip the catalog marker to simulate a store
        // written before catalog versioning existed.
        {
            let tx = engine.begin(false).unwrap();
            tx.create_collection(DEFAULT_CF, "c", &CreateCollectionOptions::default())
                .unwrap();
            let sys = tx.sys_cf().unwrap();
            tx.txn.delete(&sys, Format::Catalog.sys_key()).unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(tx.format_version(Format::Catalog).unwrap(), 0);
            tx.rollback().unwrap();
        }

        engine.migrate_catalog().unwrap();

        let tx = engine.begin(true).unwrap();
        assert_eq!(tx.format_version(Format::Catalog).unwrap(), CATALOG_VERSION);
        // The collection still loads after the stamp.
        assert!(tx.collection(DEFAULT_CF, "c").is_ok());
        tx.rollback().unwrap();
    }

    /// A catalog written by a *newer* binary (marker ahead of this build) refuses
    /// cleanly rather than risking a silent mis-read.
    #[test]
    fn newer_catalog_refuses_cleanly() {
        let engine = KvEngine::new(MemoryStore::new());
        {
            let tx = engine.begin(false).unwrap();
            let sys = tx.sys_cf().unwrap();
            tx.txn
                .put(&sys, Format::Catalog.sys_key(), &[CATALOG_VERSION + 1])
                .unwrap();
            tx.commit().unwrap();
        }

        let err = engine.check_and_migrate_formats().unwrap_err();
        match err {
            EngineError::UnsupportedFormatVersion {
                format,
                found,
                supported,
            } => {
                assert_eq!(format, "catalog");
                assert_eq!(found, CATALOG_VERSION + 1);
                assert_eq!(supported, CATALOG_VERSION);
            }
            other => panic!("expected UnsupportedFormatVersion, got {other:?}"),
        }
    }

    /// The same refuse-too-new gate applies to the index-encoding format — the
    /// gap the original migration left open (it returned early on `>=`, silently
    /// proceeding over a newer encoding).
    #[test]
    fn newer_index_encoding_refuses_cleanly() {
        let engine = KvEngine::new(MemoryStore::new());
        {
            let tx = engine.begin(false).unwrap();
            let sys = tx.sys_cf().unwrap();
            tx.txn
                .put(
                    &sys,
                    Format::IndexEncoding.sys_key(),
                    &[INDEX_ENCODING_VERSION + 1],
                )
                .unwrap();
            tx.commit().unwrap();
        }

        let err = engine.check_and_migrate_formats().unwrap_err();
        match err {
            EngineError::UnsupportedFormatVersion { format, found, .. } => {
                assert_eq!(format, "index_encoding");
                assert_eq!(found, INDEX_ENCODING_VERSION + 1);
            }
            other => panic!("expected UnsupportedFormatVersion, got {other:?}"),
        }
    }

    /// A catalog migration killed before its commit must not half-advance the
    /// marker — the next open re-runs it cleanly. Modelled by running the stamp
    /// body and rolling back (the same effect a crash before `commit()` has).
    #[test]
    fn catalog_migration_interrupted_before_commit_never_half_advances() {
        let engine = KvEngine::new(MemoryStore::new());

        // Strip the marker so a migration has work to do.
        {
            let tx = engine.begin(false).unwrap();
            let sys = tx.sys_cf().unwrap();
            tx.txn.delete(&sys, Format::Catalog.sys_key()).unwrap();
            tx.commit().unwrap();
        }

        // "Crash" mid-migration: stamp, then roll back instead of committing.
        {
            let tx = engine.begin(false).unwrap();
            tx.stamp_format_version(Format::Catalog).unwrap();
            tx.rollback().unwrap();
        }
        {
            let tx = engine.begin(true).unwrap();
            assert_eq!(
                tx.format_version(Format::Catalog).unwrap(),
                0,
                "catalog version half-advanced after an interrupted migration"
            );
            tx.rollback().unwrap();
        }

        // A real migration on the next open succeeds and stamps the version.
        engine.migrate_catalog().unwrap();
        let tx = engine.begin(true).unwrap();
        assert_eq!(tx.format_version(Format::Catalog).unwrap(), CATALOG_VERSION);
        tx.rollback().unwrap();
    }

    /// New collections carry the current catalog version in their metadata.
    #[test]
    fn new_collection_meta_carries_catalog_version() {
        let engine = KvEngine::new(MemoryStore::new());
        let tx = engine.begin(false).unwrap();
        tx.create_collection(DEFAULT_CF, "c", &CreateCollectionOptions::default())
            .unwrap();
        let meta = tx.load_collection_meta(DEFAULT_CF, "c").unwrap();
        assert_eq!(meta.version, CATALOG_VERSION);
        tx.rollback().unwrap();
    }
}
