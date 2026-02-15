# Wide-Column Storage Design

## Context

Slate is shifting from a record-as-blob model to a wide-column model inspired by Pinterest's Rockstorewidecolumn. Records are not the source of truth — they're materialized from upstream datasources (APIs, databases, etc.) via batch syncs. Slate acts as a local cache for fast querying and grid display.

This design enables:
- Column-level TTL (different fields refresh at different rates)
- Partial row reads (only fetch columns the grid is displaying)
- Secondary indexes with TTL-based expiry
- Efficient reassignment of records between users

## Key Encoding

### Data Columns

Each column is its own RocksDB key, with `expires_at` as part of the key:

```
{datasource}:{record_id}:{column_name}:{expires_at}
```

Examples:
```
accounts:acct_001:email:1739600000    -> "adam@..."
accounts:acct_001:name:1739600000     -> "Adam"
accounts:acct_001:score:1739550000    -> 42
accounts:acct_001:owner:1739600000    -> "u:adam"
```

All columns for a row are colocated in RocksDB's sorted key space.

Values are the raw serialized `Value` — no envelope needed since TTL is in the key.

### TTL in the Key (Unified Model)

Both data columns and index entries encode `expires_at` in the key. This gives a consistent model everywhere:

- **On write**: just write a new key with the new TTL. Don't delete the old one.
- **On read**: scan prefix (without the TTL suffix), take the entry with the latest `expires_at` that hasn't expired.
- **On compaction**: RocksDB's compaction filter parses `expires_at` from the key, drops expired entries automatically. No value deserialization needed.

Updates produce duplicate keys temporarily. For example, refreshing an email column:

```
accounts:acct_001:email:1739600000    -> "adam@..."       (old, expiring soon)
accounts:acct_001:email:1739700000    -> "adam@new.com"   (new, fresh)
```

On read, take the latest. On compaction, the old one gets cleaned up.

### Secondary Indexes

Indexes map field values to record IDs, with TTL in the key:

```
__idx:{field}:{value}:{datasource}:{record_id}:{expires_at}
```

Examples:
```
__idx:owner:u:adam:accounts:acct_001:1739600000  -> ""
__idx:owner:u:adam:accounts:acct_002:1739600000  -> ""
__idx:status:active:accounts:acct_001:1739600000 -> ""
```

Same model as data columns — write new entries on refresh, old ones expire and compact away.

## Query Flow

### With Index (e.g. "all accounts for user adam")

```
1. scan_prefix("__idx:owner:u:adam:accounts")
2. Parse record IDs from keys, skip expired entries
3. Dedupe record IDs (HashSet) — duplicates from TTL refresh overlap
4. For each record ID, point-read the requested display columns
5. Apply any additional filters
6. Sort and paginate
7. Return partial rows with freshness metadata
```

### Without Index (full datasource scan)

```
1. scan_prefix("accounts")
2. Walk row-by-row (columns are colocated)
3. For each row, read filter columns first
4. If filter matches, read display columns
5. Skip columns not needed (no deserialization)
6. Sort and paginate
```

### Query Request Shape

```json
{
  "prefixes": ["accounts"],
  "columns": ["name", "email", "score"],
  "filter": {
    "logical": "And",
    "children": [
      { "Condition": { "field": "status", "operator": "Eq", "value": { "String": "active" } } }
    ]
  },
  "sort": [{ "field": "name", "direction": "Asc" }],
  "take": 200
}
```

Note: `columns` controls what's returned. Filter and sort columns are read internally but not necessarily returned.

### Query Response Shape

```json
{
  "rows": [
    {
      "row_key": "acct_001",
      "columns": {
        "name":  { "value": { "String": "Adam" }, "fresh": true },
        "email": { "value": { "String": "..." }, "fresh": true },
        "score": { "value": { "Int": 42 }, "fresh": false }
      }
    }
  ]
}
```

## Column-Level TTL

Different columns can have different TTL thresholds because they come from different upstream sources:

- `name` from Source A (refreshed daily) — 12h TTL
- `score` from Source B (near-realtime) — 30m TTL

On read, `expires_at` is checked against current time. Stale data is still returned (not omitted) but marked `fresh: false`. The grid UI can visually indicate staleness.

Filtering operates on last-known values regardless of staleness — records don't disappear from views because a TTL expired.

## TTL-Based Compaction

RocksDB's custom compaction filter handles physical cleanup. Since `expires_at` is in the key for both data columns and indexes, the compaction filter never needs to read values:

- Parse `expires_at` from the key suffix
- If expired (`expires_at < now`), drop the entry
- **Deduplication**: if multiple entries exist for the same key prefix (same datasource:record:column or same index reference), keep only the one with the latest `expires_at`, drop the rest

Expired data is automatically cleaned up during RocksDB's normal background compaction — no separate cleanup jobs.

## Write Path

Writes are batch-oriented (bulk syncs from upstream). A single record write produces:

```
WriteBatch {
    put("accounts:acct_001:email:{expires_at}",  "adam@...")
    put("accounts:acct_001:name:{expires_at}",   "Adam")
    put("accounts:acct_001:owner:{expires_at}",  "u:adam")
    put("__idx:owner:u:adam:accounts:acct_001:{expires_at}", "")
    put("__idx:status:active:accounts:acct_001:{expires_at}", "")
}
```

All keys written atomically via RocksDB `WriteBatch`.

## Reassignment

When a record moves from user A to user B:

1. Write new index entry: `__idx:owner:u:bob:accounts:acct_001:{new_expires_at}`
2. Write new column: `accounts:acct_001:owner:{new_expires_at}` -> `"u:bob"`
3. Old index entry `__idx:owner:u:adam:accounts:acct_001:{old_expires_at}` expires and compacts away
4. Old column `accounts:acct_001:owner:{old_expires_at}` also expires and compacts away

No deletes needed — old entries self-expire. During the overlap window, reads take the latest `expires_at` and verify the actual `owner` column value to dedupe.

## Indexed Fields

Which fields get indexed is defined per datasource. On write, the API automatically maintains index entries for indexed fields. Adding a new indexed field to an existing datasource requires a backfill/re-index of existing records.

## Trade-offs & Pitfalls

### Write Amplification
A record with 10 columns + 2 indexed fields = 12 RocksDB keys per write (vs 1 today). Acceptable for batch-oriented writes.

### Atomicity
Column writes + index writes must use `WriteBatch` for atomicity. A crash mid-write could leave orphaned index entries or data without indexes.

### Reassignment Race Condition
During the TTL overlap window, a record may appear for both old and new owners in index scans. Mitigated by verifying the actual `owner` column value on read.

### Sort Performance
Sorting on a field requires reading that column for every matched row, even if it's not in the display columns. No regression from today (full records were loaded anyway), but worth noting.

### Schema Changes
Adding a new field to a datasource: existing records return null for it (fine). Adding a new indexed field: existing records have no index entries, requiring a backfill operation.

### Record Deletion
Deleting a record requires deleting all column keys + all index keys. Either scan `{datasource}:{record_id}:*` to find all columns, or use datasource metadata to know which columns/indexes exist.

## Comparison to Current Slate Model

| | Current (Record-as-blob) | Wide-Column |
|---|---|---|
| Storage | 1 key per record | 1 key per column |
| Read | Full record always | Partial row (projection) |
| Write | 1 put per record | N puts per record (WriteBatch) |
| TTL | None | Per-column |
| Indexes | None | Secondary indexes with TTL |
| Filtering | Full scan + in-memory filter | Index lookup or scan with early skip |
| Reassignment | Update 1 key | Write new index, old expires |

## Store Layer: Raw Byte Interface

The store trait (`slate-store`) should have no dependency on `Record`, `Value`, or any application-level types. It operates purely on byte tuples — the db layer handles serialization.

### Current Store API (to be replaced)

```rust
fn insert(&mut self, record: Record) -> Result<(), StoreError>;
fn get_by_id(&self, id: &str) -> Result<Option<Record>, StoreError>;
fn scan_prefix(&self, prefix: &str) -> Result<Iterator<Record>, StoreError>;
```

### New Store API

```rust
fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
fn put_batch(&mut self, entries: &[(&[u8], &[u8])]) -> Result<(), StoreError>;
fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, StoreError>;
fn delete(&mut self, key: &[u8]) -> Result<(), StoreError>;
fn scan_prefix(&self, prefix: &[u8]) -> Result<Iterator<(Box<[u8]>, Box<[u8]>)>, StoreError>;
```

- `put_batch` maps directly to RocksDB's `WriteBatch` for atomic multi-key writes. The db layer uses this when writing a record's columns + index entries in one shot.
- Return types use `Box<[u8]>` rather than `Vec<u8>` — this matches what RocksDB returns natively, avoiding an unnecessary copy/allocation.

### Layer Responsibilities

- **Store** (`slate-store`): raw byte key-value operations, no knowledge of columns, records, or indexes
- **DB** (`slate-db`): key encoding/decoding, `Cell` serialization/deserialization, index maintenance, query execution

This clean separation means the store could be backed by anything (RocksDB, an in-memory B-tree, etc.) without leaking application concerns.

## References

- [Pinterest: Building a new wide column database using RocksDB](https://medium.com/pinterest-engineering/building-pinterests-new-wide-column-database-using-rocksdb-f5277ee4e3d2)
