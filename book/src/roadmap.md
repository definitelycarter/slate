# Roadmap

## Indexed Sort

Currently, `Sort` materializes the entire filtered set before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means deserializing ~50k records just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~124ms to something closer to the ~10ms range.

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort, but it would reduce unnecessary index I/O.

## Multi-Tenant Deployment

Each tenant gets an isolated stack (API + DB). Tenant key is an opaque string — consumers map it to their own concepts (business unit, dept, etc). The DB is ephemeral in-memory — CRDs are the source of truth, data is backfilled lazily from upstream on first request. No PVCs needed.

### K8s Architecture

- **Tenant CRD** → tenant operator provisions pods and services
- **Datasource CRD** → configures loader endpoint, TTL, and forwarded headers
- **List CRD** → list operator pushes query configs (columns, sort, filters, page size) into tenant DB
- **Gateway** → routes requests by tenant key header to the correct API service

### Lists API

Read-only API served by each tenant's API instance:

- `GET /v1/lists` — all list configs
- `GET /v1/lists/{id}` — single list config
- `GET /v1/lists/{id}/data` — execute query, return records. Lazily backfills from upstream if DB is empty.

### Loader (data pipeline)

Slate doesn't know about upstream systems. Consumers implement a **loader** — a gRPC service that streams records back when called. Slate's list API checks staleness (via TTL on the datasource) and calls the loader to refresh when needed.

**Datasource CRD config:**

```yaml
loader:
  endpoint: grpc://prospect-loader:50051
  ttl: 3600
  forwardHeaders:
    - Authorization
    - X-SF-Token
```

**Flow:** `GET /v1/lists/{id}/data` → check staleness → call loader → stream records → `write_batch` → serve response.

Auth context (headers, tokens) is forwarded from the incoming request to the loader via `metadata`. Slate is a passthrough — the consumer's loader handles upstream auth, cross-source dependencies, transforms, etc.

**Protobuf contract:**

```protobuf
syntax = "proto3";
package slate.loader;

service Loader {
  rpc Load(LoadRequest) returns (stream LoadResponse);
}

message LoadRequest {
  string datasource_id = 1;
  map<string, string> metadata = 2;  // forwarded headers, auth context
}

message LoadResponse {
  string record_id = 1;
  bytes document = 2;  // BSON-encoded document
}
```

**Conversion:** `LoadResponse` → `bson::Document` (via `bson::from_slice` on the `document` bytes) → `write_batch`. The loader serializes each record as BSON, which Slate deserializes directly into a `bson::Document`. Slate buffers N records from the stream and batch-writes them.

## Test Coverage

### REST API tests (`slate-api`)

No tests exist for route handlers. Cover list endpoints, error responses (404, 400), and JSON serialization.

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
