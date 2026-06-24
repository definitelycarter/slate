# RFC: SQL Query Surface

> **Status: partially implemented.** Extracted from the roadmap; the
> [roadmap](../roadmap.md) tracks status at a glance.

## Concept

A SQL-like query language for aggregation and complex reads, inspired by CosmosDB's SQL
dialect. This is a query surface — it compiles down to the same plan tree nodes that the
filter/find API uses, plus new aggregation nodes.

```sql
SELECT c.status, COUNT(1) AS total, AVG(c.score) AS avg_score
FROM users c
WHERE c.active = true
GROUP BY c.status
ORDER BY total DESC
```

## Why SQL

SQL is universally understood. Offering a SQL surface for aggregation queries lowers the
learning curve — users don't need to learn a custom pipeline DSL. The document model stays
BSON; SQL is just the query language.

## Sub-document joins (CosmosDB-style)

CosmosDB supports `JOIN` within a single document's sub-arrays, not across collections.
This is a natural fit for an embedded document DB:

```sql
SELECT c.name, t.tag
FROM users c
JOIN t IN c.tags
WHERE t.tag = "rust"
```

This flattens the `tags` array, producing one row per element. No cross-collection joins,
no foreign keys — just array unwinding expressed in SQL syntax.

## Aggregation functions

- **`COUNT`**, **`SUM`**, **`AVG`**, **`MIN`**, **`MAX`** — standard aggregates
- **`GROUP BY`** — groups by one or more fields, produces one output row per group
- **`HAVING`** — filter on aggregate results (post-group)
- **`ARRAY_AGG`** / **`COLLECT`** — gather grouped values into an array

## Execution

Aggregation introduces new plan nodes:

- **`GroupBy`** — materializes input, groups by key fields, computes aggregates
- **`Having`** — post-group filter (operates on aggregate outputs)
- **`ArrayUnwind`** — flattens a sub-array for `JOIN ... IN` syntax

These compose with existing nodes (`Filter`, `Sort`, `Limit`, `Projection`, `IndexScan`).

## Parser

A lightweight SQL parser (hand-written recursive descent or `sqlparser-rs`) that emits the
existing plan tree. The SQL surface is purely additive — the filter/find API continues to
work unchanged.

## Status & not-yet-implemented

Most of the above has shipped — `SELECT`/`VALUE`/`*`/tabular, `WHERE`, `GROUP BY`,
`HAVING`, `ORDER BY`, `OFFSET`/`LIMIT`, `JOIN … IN`, the aggregates (`COUNT`/`SUM`/
`AVG`/`MIN`/`MAX` plus `ARRAY_AGG`/`COLLECT`), and subqueries (see the
[SQL Reference](../sql-support.md) and [Function Reference](../functions.md)).
Newly shipped this slice:

- **`HAVING`** — a post-group filter, lowered to a `Filter` over the `Aggregate`
  output (between aggregation and `ORDER BY`/projection). Like `SELECT`, it may
  reference only group keys and aggregates; an aggregate that appears only in
  `HAVING` is still computed.
- **`ARRAY_AGG`/`COLLECT`** — an aggregate that gathers each group's defined
  values into an array (synonyms; empty group → `[]`).
- **`DOCUMENTID`** scalar function — returns the configured pk value. Desugared at
  plan time to `<alias>.<pk_path>` (the pk path is a catalog fact known only to
  the planner), so it indexes/point-reads exactly like a direct pk reference.

Remaining gaps:

- `RAND()` is done — a fresh `[0, 1)` draw per call from an injected source (see
  the [SQL Reference](../sql-support.md#non-deterministic-functions)).
- **Full-text search** — `FULLTEXTCONTAINS`/`…ALL`/`…ANY`, `FULLTEXTSCORE`, `RRF`,
  `ORDER BY RANK`: needs a full-text index + BM25 scoring.
- **Vector** — `VECTORDISTANCE`: needs a vector index.
- **Spatial index** — the `ST_*` functions are implemented; a spatial index is not
  (see the [Spatial Index RFC](./spatial-index.md)).
