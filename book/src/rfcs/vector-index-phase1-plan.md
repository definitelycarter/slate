# Vector Index — Phase 1 Implementation Plan (scoped)

> Companion to [vector-index.md](./vector-index.md). This is the spike output: the
> RFC's "Current state" claims validated against the real code, an ordered build
> plan, and the decisions that gate coding. Branch: `worktree-vector-flat-index`.

Phase 1 = exact, brute-force flat index: `VECTORDISTANCE` (shipped) + a
`doc_id → vector bytes` keyspace, queried as a bounded top-k over a range scan, with
pre-filter-into-brute-force via the existing secondary indexes.

## "Current state" — validated, with one correction

- **`VECTORDISTANCE` is shipped, wired, tested, and SQL-reachable today.**
  `crates/slate-eval/src/functions/vectordistance.rs` implements cosine (default),
  dotproduct, euclidean (`Metric` + `measure()`, `:61-96`); arity 2-or-3, optional
  case-insensitive metric arg, unknown-metric is a hard error; non-array / unequal-
  length / non-numeric → `Undefined`; pure Rust (wasm/iOS-clean). Dispatched at
  `functions/mod.rs:239`. `ORDER BY VECTORDISTANCE(...) ... TOP k` already runs
  end-to-end as `Scan → Sort → Limit` (TOP folds to `limit` in
  `slate-sql/src/parser.rs:174-179`; ORDER BY expr survives lowering). 11 unit tests;
  **no Cosmos golden yet** (add it).
- **`IndexSpec { path, unique }` has no kind** — confirmed (`slate-engine/src/traits.rs:451-456`).
  Index config persists in `_sys_` as a **single uniqueness byte** (`kv/catalog.rs:320-322`
  write, `:62-81` read); compound-vs-single is *inferred* from the joined identity,
  not stored. Adding a vector kind is a real catalog change.
- **`CATALOG_VERSION = 1`** with migrate-or-refuse already in place
  (`kv/formats.rs:55`, `check_and_migrate_formats`); the doc comment says to bump it
  when the index-config value changes. The vector config is exactly what this seam
  exists to absorb.
- **`scan_range` exists on all three backends** (`slate-store/src/store.rs:120-125`;
  memory/rocks/redb), already used by `scan_index`.
- ⚠️ **Correction to the RFC:** `scan_range` is on the low-level **store** trait, but
  the **`EngineTransaction`** trait the executor/planner see (`traits.rs:138-228`)
  exposes only `scan` / `scan_index` / `scan_compound_index` — **no raw range scan**.
  Phase 1 therefore needs a **new engine-trait method** bridging to `scan_range`. Also:
  there is **no top-k operator** today — `Sort` is an unbounded blocking buffer
  (`nodes/sort.rs:30-46`), `Limit` is a separate `skip().take()` adapter, and the
  `IndexScan.limit` field is never populated by the planner. The bounded heap is
  genuinely new code, not a tweak.

## Build order (bottom-up: catalog → engine → planner → executor → SQL/parity)

1. **Catalog: vector index kind.** Define `VectorIndexSpec { path: String, dims: u32,
   metric, dtype }` and add `vector_indexes: Vec<VectorIndexSpec>` to
   `CollectionHandleInner` (`traits.rs:35`), parallel to `indexes`/`unique_indexes` so
   the hot secondary path is untouched; also thread it through `IndexOptions`
   (`:460`). Persist it as a **self-describing serialized value** (mirror the existing
   collection-config serialization for `pk_path`/`ttl_path`) in the index-config value
   (`catalog.rs:320-322` write / `:62-81` read). **Bump `CATALOG_VERSION` constant**
   (`formats.rs:55`) for honesty but write **no migration code** (no persisted DBs to
   migrate). Build `vector_indexes` when loading the handle (`catalog.rs:125-133`,
   `:163-171`) so it reaches the planner. Update `drop_collection` / `drop_index`
   (`catalog.rs:208-260`, `:380-403`) to delete the new vector prefix.
2. **Engine: keyspace + write maintenance.** Add `KeyPrefix::Vector(collection, field)`
   / `Key::Vector(collection, field, doc_id)` (`encoding/key.rs`). The document's BSON
   array stays canonical; the index holds a **derived, packed little-endian `f32` blob**.
   Thread vector-field packing into the `put` / `put_nx` / `delete` index-diff path
   (`kv/transaction.rs:431-497`): on write, extract each indexed vector field, pack to
   `f32` bytes, write `(doc_id → bytes)`; on overwrite/delete, remove the old entry. One
   user insert → engine maintains the copy (same diff-on-write pattern as secondary
   indexes); handle multiple vector fields per doc (iterate `vector_indexes`). Backfill
   from records at index-create time, mirroring `create_compound_index_with_options`
   (`catalog.rs:324-374`).
3. **Engine: scan method.** Add `fn scan_vectors(&self, handle, field) ->
   Iterator<(doc_id, Vec<f32>)>` to `EngineTransaction` (`traits.rs` ~`:186`), implemented
   on `KvTransaction` over the `Key::Vector` prefix via `self.txn.scan_range`. (Required
   — the executor can't reach `scan_range` directly.)
4. **Planner: metadata + node.** Add `vector_indexes` to `CollectionMeta`
   (`sargable.rs:34-47`; populate in `database.rs:639-664`). Add `Node::VectorTopK {
   collection, field, query: Expression, metric, k, source: Option<Box<Node>> }`
   (`plan.rs` ~`:186`). In `lower.rs:407-440`, recognise `ORDER BY VECTORDISTANCE(c.<f>,
   <q>[, m]) ... LIMIT k` where `<f>` has a flat vector index, and emit `VectorTopK`
   (with the sargable WHERE pre-filter as its candidate `source`) **in place of**
   `Sort+Limit`.
5. **Executor: top-k node.** New `nodes/vector_topk.rs` + arm in `lib.rs:247-449`
   (mirror `IndexScan`, `:252-267`). Bounded `BinaryHeap` of size k. **Factor
   `Metric::measure` out of `vectordistance.rs`** so the node and the function share
   the exact math (no divergence). Respect metric sense (cosine/dotproduct DESC,
   euclidean ASC). Emit doc-ids → existing `KeyLookup` fetches docs.
6. **SQL/DDL.** `TOP k` needs no parser change. Extend `CREATE INDEX` to accept vector
   config (dims/metric/dtype) — wire to the new `IndexOptions` fields (the slate-sql /
   slate-query DDL path; scope when reached).
7. **Tests + parity.** Unit tests per layer; a Cosmos golden-replay for `ORDER BY
   VECTORDISTANCE ... TOP k` (cosmos-parity skill); bench flat scan at 10K–100K ×
   768/1536 dims to confirm brute force suffices before any ANN talk.

## Decisions — RESOLVED (2026-06-25)

1. **Config value format → self-describing struct.** Serialize a small `VectorIndexSpec`
   (mirror however collection-level config like `pk_path`/`ttl_path` is already
   serialized in the catalog), NOT a hand-rolled tag-byte scheme — the catalog is cold
   (read once at open) and this config grows across phases (quantization dtype, later
   ANN params). The on-disk value is just the serialized form of the in-memory spec.
2. **No migration code.** No persisted databases exist yet, so there is nothing to
   migrate. Still bump the `CATALOG_VERSION` constant (`formats.rs:55`) for honesty, but
   write **zero** migration logic — migrate-or-refuse has no old format to handle.
3. **Storage → dedicated packed-`f32` keyspace (Option A).** The document keeps its BSON
   array as the **canonical source of truth**; the index holds a *derived, packed,
   little-endian `f32` blob* — exactly like every secondary index is a derived copy of a
   record field. One user-facing insert; the engine maintains the index copy on the
   write path (and rebuilds it from records on backfill/import). The BSON array is *not*
   a scan-friendly float layout (typed, non-contiguous), so a packed copy is needed
   regardless — which is why we store it rather than parse-per-query. Packed bytes (not
   BSON) also make Phase 2 quantization a width change, with the config's `dtype` as the
   decode schema.
4. **Multiple vector indexes per collection → first-class.** Vector indexes are
   per-field, so `CollectionHandleInner` carries `vector_indexes: Vec<VectorIndexSpec>`
   (parallel to `indexes`/`unique_indexes`, leaving the hot secondary path untouched),
   keyed per `(collection, field)` in the catalog. A doc may hold several embeddings
   (e.g. `image_embedding` 512-d + `text_embedding` 1536-d, each with its own
   dims/metric/dtype). A query routes to the right index by the field named in the
   `VECTORDISTANCE` call. Cosmos-aligned (its vector embedding policy is a list).

**Defaults (carry as-is; flag only if forced):**

- **Pre-filter wiring** — `VectorTopK` *consumes* a candidate id-stream from the existing
  sargable lowering (simplest); the `IndexMerge(And)` route (`sargable.rs:609-622`) is a
  later option.
- **dims validation** — hard error on `len != dims` at insert (match Cosmos).
- **Metric** — the flat index is used only when the `VECTORDISTANCE` call's metric
  matches the index's declared metric; a mismatch falls back to a correct full scan
  (never a wrong result).

## What's already done (this morning)

`VECTORDISTANCE` (the function-first slice) is shipped on `main`. Phase 1 from here is
the *index* — steps 1–5 above — turning the full-scan `Sort+Limit` into a flat-scan
top-k seek.
