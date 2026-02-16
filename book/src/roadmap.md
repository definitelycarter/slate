# Roadmap

## Indexed Sort

Currently, `Sort` materializes the entire filtered set before sorting in memory. For `status='active' + sort contacts_count + take(200)`, this means deserializing ~50k records just to return 200 rows.

If the sort field is indexed, we can walk the index in sorted order instead of materializing. Sort + take(200) becomes an index range scan that stops after 200 qualifying rows — O(k) instead of O(n).

This would turn queries like `sort + take(200)` from ~140ms to something closer to the ~10ms range.

## Count/Limit Pushdown on IndexScan

For queries like `status='active' take(200)` without sort, the IndexScan currently yields all matching record IDs from the index (~50k), then Limit takes 200. The IndexScan could accept an optional limit and stop walking the index after enough matches, avoiding the full index traversal.

This is less impactful than indexed sort, but it would reduce unnecessary index I/O.

## No Index on TTL Fields

Reject field configs that have both `indexed: true` and `ttl_seconds: Some(...)` at `save_datasource` time. An expired column would leave stale index entries pointing to logically-gone values — either requiring background cleanup or read-time filtering that defeats the purpose of the index. Fail loud on write rather than dealing with it at query time.

## Multi-Tenant Deployment

Each tenant gets an isolated stack (API + DB). Tenant key is an opaque string — consumers map it to their own concepts (business unit, dept, etc). The DB is ephemeral — CRDs are the source of truth, data is backfilled lazily from upstream on first request.

### K8s Architecture

- **Tenant CRD** → tenant operator provisions pods, services, volumes
- **Datasource CRD** → tenant operator connects via TCP, creates datasource schema
- **List CRD** → list operator pushes query configs (columns, sort, filters, page size) into tenant DB
- **Gateway** → routes requests by tenant key header to the correct API service

### Lists API

Read-only API served by each tenant's API instance:

- `GET /v1/lists` — all list configs
- `GET /v1/lists/{id}` — single list config
- `GET /v1/lists/{id}/data` — execute query, return records. Lazily backfills from upstream if DB is empty.

### Data Pipeline (needs design)

The `/data` endpoint triggers upstream fetches when the DB is cold. Upstream sources include Redshift (via pg driver) and Salesforce (via REST API). Key challenges:

- **Cross-source dependencies** — Salesforce runs first to get account IDs, Redshift uses those as filters
- **Per-request auth** — Salesforce calls need user-specific credentials passed through from the request
- **Field mappings** — upstream fields map to datasource fields (e.g. `Contacts__c.totalCount` → `contacts_count`)
- **Unstructured transforms** — insight values are arbitrary JSON (primitives, arrays, objects) that need coercion into typed fields
- **Selective fetching** — different tenants pull different subsets of insights from the same upstream tables

Config-driven approach works for *what* to fetch and *where* to map it. The *how* (ordering, auth, transforms) likely needs pluggable pipeline steps.

## Test Coverage

### REST API tests (`slate-api`)

No tests exist for route handlers. Cover datasource CRUD endpoints, error responses (404, 400), and JSON serialization.

### Index maintenance on updates

Verify that when an indexed value changes (e.g. status "active" → "rejected"), the old index entry is deleted and the new one is created. Also verify index cleanup on record deletion.

### Error paths

Test behavior for: writing to a nonexistent datasource, querying a deleted datasource, writing unknown column names, malformed queries.

### Encoding edge cases

Negative ints in index keys, empty strings, special characters in record IDs, very long values.

### Concurrent write conflicts

Unit tests (not just bench) for optimistic transaction conflict detection and correct rollback behavior.

### ClientPool

Test connection pooling: checkout/return, behavior under contention, handling of dropped connections.
