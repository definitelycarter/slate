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

## Unified Raw Tier (RawBsonRef Pipeline)

The executor currently has three rigid tiers with different item types:
- **ID tier** — `(String, Option<Cow<[u8]>>)` — record IDs
- **Raw tier** — `(String, Cow<[u8]>)` — record ID + full document bytes
- **Document tier** — `bson::Document` — materialized documents

This forces every query to fetch full document bytes via `ReadRecord`, even when the answer is already available from the index or filter. It also forces duplication — Sort needs to extract fields from raw bytes, but Distinct sorts owned `Bson` values, requiring a parallel `compare_bson_values` function that mirrors `raw_compare_two_values`.

### Goal

Replace the raw tier's item type with `RawBsonRef`. Every node in the pipeline receives and emits `RawBsonRef` values — which can be documents, scalars, arrays, or any BSON type. Nodes operate generically on whatever they receive:

- **Sort** — compares two `RawBsonRef` values directly via `raw_compare_field_values`. No longer needs to know if it's sorting documents or scalar values. One code path, no duplication.
- **Filter** — if it receives a `RawBsonRef::Document`, it can extract fields and evaluate predicates. If it receives a scalar, it compares directly.
- **Distinct** — reuses Sort naturally since both operate on `RawBsonRef`.
- **Projection** — converts `RawBsonRef::Document` to `bson::Document` at the end.

### Conditional ReadRecord

Today `ReadRecord` is a mandatory boundary between the ID tier and raw tier. In the new model it becomes conditional — the planner injects it only when downstream nodes need fields that aren't already available.

**Covered query** — all needed fields are available from the index/filter:
```
-- select score from accounts where score = 5 (score is indexed)
Projection
  → IndexScan(column="score", value=5)
```
No ReadRecord. IndexScan emits `RawBsonRef::Int32(5)` directly. This is what MongoDB calls a "covered query."

**Uncovered query** — downstream needs fields not in the index:
```
-- select score, name from accounts where score = 5 (score is indexed)
Projection(columns=["score", "name"])
  → ReadRecord
    → IndexScan(column="score", value=5)
```
Planner sees Projection needs `name` which isn't available from IndexScan, so it injects ReadRecord to fetch full document bytes.

**ReadRecord as a conditional noop** — when injected, ReadRecord checks: "do I already have a full document flowing through?" If yes, pass through. If no, fetch from store. This makes it safe to inject conservatively.

### Field Extraction as a Plan Node

Today Sort inlines field extraction — it calls `raw_get_path(doc, &sort.field)` inside `sort_by`. In the new model, field extraction is a separate plan node:

```
-- sort by score
Sort(direction=Asc)
  → ExtractField("score")
    → ReadRecord
      → Scan
```

`ExtractField` takes a `RawBsonRef::Document`, calls `raw_get_path`, and emits the field's `RawBsonRef`. Sort just compares `RawBsonRef` values — it doesn't know or care where they came from.

For multi-field sort, the item would carry the sort key alongside the original data — something like `(RawBsonRef, Cow<[u8]>)` as sort key + original bytes. Or the sort node could receive documents and extract fields internally (current behavior) as a pragmatic choice for multi-field sorts.

### Key Benefits

1. **No duplicated comparison logic** — one `raw_compare_field_values` works for Sort, Distinct, and any future operation that needs to compare values.
2. **Covered queries** — skip record fetches entirely when the index provides all needed data. Significant performance win for narrow projections on indexed fields.
3. **Composable pipeline** — new operations (aggregation, grouping) fit naturally because every node speaks the same type.
4. **Distinct reuses Sort** — no need for a separate `compare_bson_values` or special-cased sort in `execute_distinct`. The plan becomes `Distinct(dedup) → Sort → ExtractField → Filter → ReadRecord → Scan`.

### Migration Path

This is a significant rearchitecture. Suggested approach:
1. Change `RawIter` item type from `(String, Cow<[u8]>)` to a struct that carries both the record ID and a `RawBsonRef` (which may be a full document or a scalar).
2. Update `ReadRecord` to emit `RawBsonRef::Document` instead of raw bytes.
3. Update Filter and Sort to work on `RawBsonRef`.
4. Add `ExtractField` plan node for field extraction.
5. Make ReadRecord conditional in the planner — analyze downstream nodes to determine if record fetch is needed.
6. Remove `compare_bson_values` duplication and `execute_distinct`'s inline sort logic.

### Open Questions

- **Lifetime management** — `RawBsonRef` borrows from the underlying bytes. When the pipeline item type is `RawBsonRef<'a>`, we need the bytes to live long enough. Today `Cow<'a, [u8]>` handles this — the struct wrapping `RawBsonRef` would also need to own or borrow the backing bytes.
- **Multi-field sort** — if Sort only receives a single extracted field value, how does it access secondary sort keys? Options: (a) Sort receives the full document and extracts internally (current approach, pragmatic), (b) sort key is a tuple of extracted values, (c) ExtractField emits a composite key.
- **Filter on non-document** — if a scalar flows through and Filter needs a field, it can't extract one. The planner must ensure ReadRecord is injected before Filter when the input might not be a document. This is a planner responsibility, not a runtime check.

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
