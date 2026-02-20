# Roadmap

## Streaming Mutations

### Problem

`delete_many`, `update_many`, `merge_many`, `delete_one`, `update_one`, and `replace_one`
all call `self.find()`, which materializes every matching document into a `Vec<RawDocumentBuf>`
before performing any writes. For a `delete_many` matching 10k records, that's 10k full documents
in memory just to extract IDs and run per-record write helpers.

### Step 1: Transaction writes take `&self` + `&Self::Cf`

Currently `put`, `delete`, and `put_batch` take `&mut self` and a `cf: &str` name. Neither is
necessary — all three backends buffer writes without mutating read state, and the CF is already
resolved by the time we write.

**Trait change:**

```rust
// Before
fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
fn delete(&mut self, cf: &str, key: &[u8]) -> Result<(), StoreError>;

// After
fn put(&self, cf: &Self::Cf, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
fn delete(&self, cf: &Self::Cf, key: &[u8]) -> Result<(), StoreError>;
```

**Per-backend:**

| Backend | Current write path | Change needed |
|---|---|---|
| RocksDB | `self.cf_handle(cf)` (mut for cache) → `txn.put_cf()` | Use `&Self::Cf` directly. Drop `cf_handle()` cache for writes. `&self`. |
| Memory | `self.ensure_cf_mut(cf)` → `Arc::make_mut` | Use `RefCell` or `RwLock` for snapshot mutation. `&self`. |
| redb | `txn.open_table(def)` → `table.insert()` | Already works with `&self` on inner txn. Just change signature. |

This unlocks interleaved reads and writes — the executor can stream `&self` reads while the
caller (or executor) does `&self` writes in the same loop. No collecting required.

**Files to modify:**

- `crates/slate-store/src/store.rs` — trait signatures
- `crates/slate-store/src/rocks/transaction.rs` — use `&Self::Cf` directly
- `crates/slate-store/src/memory/transaction.rs` — interior mutability for snapshot writes
- `crates/slate-store/src/redb_store/transaction.rs` — signature change
- `crates/slate-db/src/database.rs` — update all write call sites to pass CF handle

### Step 2: Mutation nodes in the plan tree

With `&self` writes, mutation operations become composable pipeline nodes — each one is a
streaming pass that adds a side effect and passes `(id, raw_value)` downstream.

**New PlanNode variants:**

```rust
PlanNode::DeleteIndex   // delete old index keys for each record flowing through
PlanNode::Delete        // delete the record key
PlanNode::Update        // read old doc, merge fields, write new doc
PlanNode::Merge         // same as Update but partial merge semantics
PlanNode::Replace       // write replacement doc (full overwrite)
PlanNode::InsertIndex   // write new index keys for each record flowing through
```

**Composed pipelines:**

```
// delete_many: clean up indexes, delete record
Delete
  └── DeleteIndex
        └── Filter → ReadRecord → IndexScan/Scan

// update_many: clean up old indexes, merge doc, write new indexes
InsertIndex
  └── Update
        └── DeleteIndex
              └── Filter → ReadRecord → IndexScan/Scan

// replace_one: clean up old indexes, replace doc, write new indexes
InsertIndex
  └── Replace
        └── DeleteIndex
              └── Filter → ReadRecord → IndexScan/Scan

// reindex (future): rebuild indexes for all records
InsertIndex
  └── DeleteIndex
        └── Scan → ReadRecord
```

Each node is single-responsibility:

- **`DeleteIndex`**: reads old doc bytes, deletes index keys for indexed fields. Passes `(id, raw_value)` through.
- **`Delete`**: deletes the record data key. Passes `(id, raw_value)` through (or yields count).
- **`Update` / `Merge`**: reads old doc, merges update fields, writes new doc. Passes `(id, new_raw_value)` downstream so `InsertIndex` sees the new values.
- **`Replace`**: writes replacement doc. Passes `(id, new_raw_value)` downstream.
- **`InsertIndex`**: writes index keys for the current `raw_value`. Terminal — consumes the stream.

The executor streams one record at a time through the full pipeline. No materialization,
no collecting. `O(1)` memory regardless of how many records match.

### Step 3: Planner builds mutation pipelines

```rust
pub fn plan_delete(collection: &str, indexed_fields: &[String], filter: &FilterGroup) -> PlanNode {
    let source = build_filtered_source(collection, indexed_fields, filter);
    PlanNode::Delete {
        input: Box::new(PlanNode::DeleteIndex {
            indexed_fields: indexed_fields.to_vec(),
            input: Box::new(source),
        }),
    }
}

pub fn plan_update(collection: &str, indexed_fields: &[String], filter: &FilterGroup) -> PlanNode {
    let source = build_filtered_source(collection, indexed_fields, filter);
    PlanNode::InsertIndex {
        indexed_fields: indexed_fields.to_vec(),
        input: Box::new(PlanNode::Update {
            input: Box::new(PlanNode::DeleteIndex {
                indexed_fields: indexed_fields.to_vec(),
                input: Box::new(source),
            }),
        }),
    }
}
```

### Step 4: ExecutionResult enum

Following [toydb's pattern](https://github.com/erikgrinaker/toydb/blob/main/src/sql/execution/executor.rs#L73),
`execute()` returns a typed `ExecutionResult` enum instead of forcing everything through `RawIter`.
Each variant carries exactly what that operation produces.

```rust
pub enum ExecutionResult<'a> {
    /// Query rows: find, find_one
    Rows(RawIter<'a>),
    /// Count result (streamed, not materialized)
    Count(u64),
    /// Distinct values
    Distinct(RawBson),
    /// Mutation results
    Delete { count: u64 },
    Update { matched: u64, modified: u64 },
    Replace { matched: u64, modified: u64 },
}
```

Database methods match on the variant and map to the public result types:

```rust
pub fn delete_many(&mut self, collection: &str, filter: &FilterGroup) -> Result<DeleteResult, DbError> {
    let sys = self.txn.cf(SYS_CF)?;
    let indexed_fields = self.catalog.list_indexes(&self.txn, &sys, collection)?;
    let cf = self.txn.cf(collection)?;
    let plan = planner::plan_delete(collection, &indexed_fields, filter);
    match executor::execute(&self.txn, &cf, &plan)? {
        ExecutionResult::Delete { count } => Ok(DeleteResult { deleted: count }),
        _ => unreachable!(),
    }
}

pub fn find(&mut self, collection: &str, query: &Query) -> Result<Vec<RawDocumentBuf>, DbError> {
    // ...
    match executor::execute(&self.txn, &cf, &plan)? {
        ExecutionResult::Rows(iter) => iter.map(|r| /* ... */).collect(),
        _ => unreachable!(),
    }
}
```

Queries yield rows, distinct yields values, mutations yield counts. Each path is
type-safe — no deserializing BSON result documents, no pretending everything is a row iterator.

### Scope

| Method | Current | After |
|---|---|---|
| `delete_many` | `find()` → `Vec<RawDocumentBuf>` → loop delete | `Delete → DeleteIndex → Filter → ReadRecord → Scan` |
| `delete_one` | `find(take=1)` → Vec → delete first | Same pipeline, executor stops after 1 |
| `update_many` | `find()` → `Vec<RawDocumentBuf>` → loop merge | `InsertIndex → Update → DeleteIndex → Filter → ReadRecord → Scan` |
| `update_one` | `find(take=1)` → Vec → merge first | Same pipeline, executor stops after 1 |
| `replace_one` | `find(take=1)` → Vec → replace first | `InsertIndex → Replace → DeleteIndex → Filter → ReadRecord → Scan` |

### Files to modify

- `crates/slate-store/src/store.rs` — write signatures to `&self` + `&Self::Cf`
- `crates/slate-store/src/{rocks,memory,redb_store}/transaction.rs` — implement new signatures
- `crates/slate-db/src/planner.rs` — add mutation plan builders, new `PlanNode` variants
- `crates/slate-db/src/executor.rs` — execute mutation nodes (streaming writes)
- `crates/slate-db/src/database.rs` — thin wrappers calling planner + executor

### Notes

- Step 1 (`&self` writes) is a prerequisite for Step 2. Can be landed independently.
- The read pipeline (`Scan`, `IndexScan`, `ReadRecord`, `Filter`) is unchanged.
- `insert_many`, `upsert_many`, `merge_many` (bulk by-ID ops) don't filter — unaffected for now.
- `count()` already streams from the executor without materializing.
- Composable nodes mean future operations (reindex, TTL purge) reuse the same building blocks.

---

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
