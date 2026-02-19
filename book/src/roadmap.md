# Roadmap

## Loader Implementations

The `Loader` trait is defined and tested with `NoopLoader` and `FakeLoader`. Real implementations are TODO:

### gRPC Loader

Implement a gRPC streaming loader using the protobuf contract:

```protobuf
syntax = "proto3";
package slate.loader;

service Loader {
  rpc Load(LoadRequest) returns (stream LoadResponse);
}

message LoadRequest {
  string collection = 1;
  string key = 2;
  map<string, string> metadata = 3;
}

message LoadResponse {
  bytes document = 1;  // BSON-encoded document
}
```

Each `LoadResponse` → `bson::from_slice` → `bson::Document`. The trait already returns a streaming iterator, so the gRPC stream maps naturally.

### REST Loader

Implement an HTTP loader that calls an external endpoint, receives a JSON array, and converts each item to `bson::Document`.

### Datasource CRD

Configure loader endpoint, TTL, and forwarded headers per collection:

```yaml
loader:
  endpoint: grpc://prospect-loader:50051
  ttl: 3600
  forwardHeaders:
    - Authorization
    - X-SF-Token
```

## Platform Adapters

`ListHttp` uses raw `http::Request`/`http::Response`. Thin adapter binaries are needed for each platform:

- **Knative adapter** — small binary with HTTP server, forwards to `ListHttp::handle()`
- **Lambda adapter** — wraps `ListHttp` with `lambda_http::run()`

## Kubernetes Operator

- **List CRD** → operator creates one Knative Service per list, injects ListConfig as ConfigMap
- **Datasource CRD** → configures loader endpoint, TTL, forwarded headers
- **Gateway API / Kourier** → routes `/lists/{id}/*` to correct Knative Service

## Cross-Type Numeric Coercion

Stored `Double` values don't match against `Bson::Int64` or `Bson::Int32` filter values (and vice versa). A frontend sending `{"value": 100000}` (JSON integer → `Bson::Int64`) won't match a stored `Double(100000.0)`. Add coercion arms to `raw_values_eq` and `raw_compare_values`:

- `(RawBsonRef::Double, Bson::Int64)` — cast int to f64 and compare
- `(RawBsonRef::Double, Bson::Int32)` — cast int to f64 and compare
- `(RawBsonRef::Int64, Bson::Double)` — cast int to f64 and compare
- `(RawBsonRef::Int32, Bson::Double)` — cast int to f64 and compare

## Array Element Matching

Filtering on array fields (e.g. `triggers.items eq "renewal_due"`) currently returns no results because `RawBsonRef::Array` falls through to the default `false` arm. Add implicit element-wise matching (like MongoDB's behavior without `$elemMatch`):

- `(RawBsonRef::Array(arr), query_val)` — iterate array elements, return true if *any* element satisfies the comparison
- Applies to `raw_values_eq` (eq) and `raw_compare_values` (gt, gte, lt, lte)
- Sorting on array fields should remain unsupported (no meaningful scalar ordering)

## ~~Distinct Values~~ (Done)

Implemented. `DistinctQuery { field, filter, sort }` with full-stack support:
- `slate-query` — `DistinctQuery` type
- `slate-db` — `PlanNode::Distinct`, `plan_distinct()`, `raw_get_path_values()` for array flattening, `execute_distinct()` with linear dedup
- `slate-server` — `Request::Distinct` / `Response::Values`
- `slate-client` — `Client::distinct()`
- `slate-lists` — `POST /distinct` endpoint with filter merging

## Unified Raw Tier (RawValue Pipeline)

### Done

The raw tier has been rearchitected from rigid `Cow<[u8]>` bytes to `RawValue<'a>` — a Cow-like enum over `RawBsonRef<'a>` (borrowed, zero-copy from MemoryStore) and `RawBson` (owned, from RocksDB or index entries). The pipeline item type is `(String, Option<RawValue<'a>>)` throughout.

**Completed work:**

- **`RawValue<'a>` enum** — replaces `Cow<[u8]>` as the pipeline item. Carries documents, scalars, or any BSON type. `as_document()` gives `&RawDocument` for field access; `to_bson()` materializes for projection; `into_owned()` drops the lifetime for IndexMerge collection.
- **Filter and Sort on `RawValue`** — Filter calls `val.as_document()` → `raw_matches_group()`. Sort calls `val.as_document()` → `raw_get_path()` for sort key extraction. Both work on `&RawDocument` views without allocation.
- **Conditional ReadRecord** — the planner skips `ReadRecord` for index-covered projections (IndexScan Eq where all projected columns match the indexed field). `IndexScan` carries the matched value as `RawValue::Owned(RawBson)`, and `Projection` constructs documents directly from the scalar — no document fetch, no deserialization. Benchmarked at ~5x faster for narrow projections on indexed fields.
- **ID-tier nodes in raw tier** — `execute_raw_node` delegates `Scan`, `IndexScan`, `IndexMerge` to `execute_id_node`, so ID-tier nodes can appear directly in the raw tier when ReadRecord is skipped.
- **Lazy IndexScan** — `from_fn` iterator that pulls index entries on demand, stops at limit or group boundary. No intermediate `Vec` allocation.
- **`raw_compare_field_values` / `raw_compare_two_values`** — raw-tier comparison on `RawBsonRef` with cross-int coercion (Int32/Int64).

### Remaining

Two pieces of duplication remain, plus the `ExtractField` plan node idea:

**1. Eliminate `compare_bson_values` duplication**

`exec::compare_bson_values(a: &Bson, b: &Bson)` mirrors `raw_compare_two_values(a: &RawBsonRef, b: &RawBsonRef)` — same match arms, same logic, different types. It's only called from `execute_distinct` to sort the final `Vec<Bson>`. Fix: either convert `Bson` → `RawBsonRef` before comparing (if cheap), or have Distinct sort before materializing to `Bson`.

**2. Distinct inline sort → reuse pipeline Sort**

`execute_distinct` collects `Vec<Bson>`, deduplicates with linear `contains()`, then sorts with `compare_bson_values`. This is a separate code path from the pipeline's `Sort` node. Ideally Distinct would work on `RawBsonRef` values and reuse the same comparison logic. The plan would become:

```
Distinct(dedup)
  → Sort(direction)
    → ExtractField("field")
      → Filter
        → ReadRecord
          → Scan
```

But this requires Distinct to operate on `RawBsonRef` values rather than owned `Bson`, which ties into the ExtractField question below.

**3. `ExtractField` plan node (open question)**

Today Sort inlines field extraction — it calls `raw_get_path(doc, &sort.field)` inside `sort_by`. A separate `ExtractField` node would make Sort generic over any `RawBsonRef`:

```
Sort(direction=Asc)
  → ExtractField("score")
    → ReadRecord → Scan
```

Sort just compares `RawBsonRef` values — it doesn't know or care where they came from. This is clean for single-field sort but awkward for multi-field sort — Sort would need the original document to extract secondary keys.

Options for multi-field sort:
- **(a)** Sort receives full documents and extracts fields internally (current approach, pragmatic)
- **(b)** `ExtractField` emits a composite sort key alongside the original data
- **(c)** Only use `ExtractField` for single-field sort; multi-field keeps current inline extraction

Given that multi-field sort already works well with inline extraction, option **(c)** may be the pragmatic choice — `ExtractField` benefits Distinct and single-field Sort without complicating multi-field Sort.

## BSON Type in Index Values

Index keys currently store encoded values without type information. When reading a value back from an index (e.g. for index-covered projections), we have to coerce or guess the BSON type. Storing the BSON element type byte alongside the encoded value would let us reconstruct the exact `RawBson` variant on read — no coercion needed, correct types round-trip through the index.

## Staleness / TTL

Currently the loader fires when `count == 0`. Add TTL-based staleness:
- Track last-loaded timestamp per key
- If data exists but is older than TTL → reload
- TTL comes from the Datasource CRD config

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
