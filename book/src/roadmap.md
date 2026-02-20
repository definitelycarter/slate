# Roadmap

## Platform Adapters

`ListHttp` uses raw `http::Request`/`http::Response`. Thin adapter binaries are needed for each platform:

- **Knative adapter** — small binary with HTTP server, forwards to `ListHttp::handle()`
- **Lambda adapter** — wraps `ListHttp` with `lambda_http::run()`

## Kubernetes Operator

- **List CRD** → operator creates one Knative Service per list, injects ListConfig as ConfigMap
- **Gateway API / Kourier** → routes `/lists/{id}/*` to correct Knative Service

## Test Coverage

### Error paths

Test behavior for: malformed queries, missing collections, invalid filter operators.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
