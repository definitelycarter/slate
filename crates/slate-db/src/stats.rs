//! Catalog & storage statistics — the `stats()` introspection surface.
//!
//! Answers "how big is this?" for a host with no operational window into the
//! store it embeds: per-collection document and index-entry counts (and an
//! approximate distinct-value cardinality per index), and a per-database roll-up.
//!
//! ## Exact vs approximate
//!
//! These counts are computed by *scanning* the collection and its indexes, so
//! they are **exact** as of the read snapshot — not maintained counters, so no
//! write-path cost and no drift to reconcile. The trade-off is that `stats()` is
//! O(rows + index entries), so it is an introspection call, not a hot path. The
//! [`approximate`](CollectionStats::approximate) flag is wired now (always
//! `false` for the scan-based counts) so a future maintained-counter fast path
//! can flip it to `true` without an API change.
//!
//! On-disk size is intentionally left out of this surface for now: it is a
//! backend property (RocksDB SST size, redb file size) that needs per-backend
//! plumbing through the store trait. The `disk_size_bytes` slot is reserved as an
//! `Option` so adding it later is non-breaking; it is `None` today.

use serde::Serialize;

/// Statistics for a single collection, as of a read snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollectionStats {
    /// Column family the collection lives in.
    pub cf: String,
    /// Collection name.
    pub name: String,
    /// Live (non-expired) document count.
    pub document_count: u64,
    /// Per-index entry and cardinality stats, one per declared index.
    pub indexes: Vec<IndexStats>,
    /// Whether any count above is an estimate rather than exact. Always `false`
    /// for the current scan-based implementation; reserved for a future
    /// maintained-counter fast path.
    pub approximate: bool,
}

/// Statistics for a single index on a collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexStats {
    /// Indexed field path (e.g. `"age"`).
    pub field: String,
    /// Total live index entries (one per document value; multikey indexes emit
    /// several per document).
    pub entry_count: u64,
    /// Number of distinct indexed values — the index's approximate cardinality.
    /// Computed exactly from the scan here, but named "cardinality" because a
    /// future estimating backend may approximate it.
    pub cardinality: u64,
}

/// Database-wide statistics: a per-collection breakdown and rolled-up totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatabaseStats {
    /// Per-collection stats, in catalog order.
    pub collections: Vec<CollectionStats>,
    /// Sum of `document_count` across all collections.
    pub total_documents: u64,
    /// Approximate on-disk size in bytes, when the backend can report it.
    /// `None` for `MemoryStore` and pending per-backend plumbing.
    pub disk_size_bytes: Option<u64>,
}

impl DatabaseStats {
    /// Build a database roll-up from per-collection stats, summing documents and
    /// carrying the (optional) on-disk size.
    pub(crate) fn from_collections(
        collections: Vec<CollectionStats>,
        disk_size_bytes: Option<u64>,
    ) -> Self {
        let total_documents = collections.iter().map(|c| c.document_count).sum();
        Self {
            collections,
            total_documents,
            disk_size_bytes,
        }
    }
}
