# Roadmap

## Collect Node (Plan Materialization Barrier)

### Concept

A `Collect` plan node that drains its child stream and emits the result as a single
materialized batch. This makes materialization points explicit in the plan tree instead
of hidden inside executor implementations.

```
Sort                    IndexMerge(And)
  Collect                 Collect            ← materialized into a set
    Scan                    IndexScan(status)
                          IndexScan(user_id) ← streamed, probed against set
```

### Motivation

Today, `Sort` and `IndexMerge` secretly materialize their inputs internally. The plan
tree shows `Sort(Scan)` which looks like a streaming pipeline, but the executor collects
everything into a `Vec` before sorting. With `Collect` as an explicit node, the plan
tree is honest about where memory grows.

### Current nodes that collect internally

- **`IndexMerge`** — collects both `lhs` and `rhs` into `Vec`s for set intersection/union
- **`Sort`** — collects all records into a `Vec` to sort in memory
- **`Distinct`** — iterates source eagerly to build a dedup set

### Asymmetric IndexMerge (And)

Postgres-style optimization: for `And`, collect only one side into a hash set, then
stream the other side and probe against it. This avoids materializing both sides.
For `Or`, both sides still need collection (full union).

### Benefits

- **Plan legibility** — materialization is visible in the plan tree
- **Reusable** — any node needing a materialized input wraps its child in `Collect`
- **EXPLAIN** — `Collect` nodes tell the user where memory grows
- **Future stages** — natural boundary for spill-to-disk, distributed execution, caching

### Performance note

This is primarily a **composability win**, not a performance win. The same work happens
either way. The real performance opportunity is the asymmetric `IndexMerge(And)` path,
which is an algorithm change that could be implemented with or without `Collect` as a
plan node.

---

## Dedicated TTL Keyspace

### Problem

TTL is currently stored in two places: the record envelope prefix (for O(1) expiry checks
on reads) and as a regular index (`i:ttl:{millis}:{id}`) for purge scans. The regular
index format is keyed by value, not by ID — so there's no way to answer "what's the TTL
for document X?" without reading the record envelope.

This forces the covered projection path to do a `txn.get` on the record key just to check
TTL, adding ~53% overhead to what was previously a zero-read path. It also means TTL
occupies space in the regular index keyspace alongside user-queryable fields.

### Approach

Replace the regular TTL index with a dedicated keyspace: `t:{id}` → `[8-byte LE millis]`.

- **O(1) point lookup by ID** — covered `IndexScan` checks `txn.get(ttl_key(id))`, reads
  8 bytes, compares. No record read needed.
- **Purge** — scan prefix `t:`, check each millis < now, delete record + indexes.
  Same efficiency as the current `IndexScan(Lt(now))` approach.
- **Record envelope simplified** — TTL check can use the dedicated keyspace instead of
  the envelope prefix, or both can coexist for defense-in-depth.

### Impact

- Restores covered projections to zero-record-read performance (~77% faster than non-covered)
- Removes TTL from the regular index keyspace (cleaner separation of system vs user data)
- Write overhead: one extra `txn.put` per TTL document (the `t:{id}` entry)

### Files affected

- `encoding.rs` — add `ttl_key(id)` helper
- `insert_record.rs`, `upsert.rs` — write `t:{id}` entry alongside record
- `delete.rs` — delete `t:{id}` entry
- `index_scan.rs` — covered path checks `t:{id}` instead of record envelope
- `engine.rs` — `purge_expired` scans `t:` prefix instead of TTL index
- `planner.rs` — `FlushExpired` uses dedicated scan instead of `IndexScan`
- Remove auto-added `ttl` index from `ensure_indexes`

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
