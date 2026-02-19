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

## Distinct Values

Support selecting distinct values from a field at any document depth — top-level scalars, sub-document paths, or array elements. Example: `distinct("triggers.items")` returns all unique trigger names across the collection.

- **`slate-query`** — `DistinctQuery { field: String, filter: Option<FilterGroup> }`
- **`slate-db/exec.rs`** — scan documents, resolve dot-path to leaf, flatten arrays (collect each element individually), deduplicate. Use raw BSON bytes comparison for dedup to avoid `Bson` ordering issues.
- **`slate-client` / `slate-server`** — new protocol message (request: field + optional filter, response: `Vec<Bson>`)
- **`slate-lists/http.rs`** — `POST /distinct` with `{"field": "triggers.items", "filters": ...}`

Useful for populating filter dropdowns in the UI — e.g. distinct statuses, distinct priorities, distinct trigger names.

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
