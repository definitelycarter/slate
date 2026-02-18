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
