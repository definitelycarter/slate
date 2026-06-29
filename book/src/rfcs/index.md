# RFC Index

A high-level overview of every RFC in this directory and its current status.
Each row links to the full document. The [roadmap](../roadmap.md) tracks the same
status at a glance; this table is organised by theme.

**Status key:**

- ✅ **Shipped** — implemented and on `main`.
- 🟢 **Mostly shipped** — core landed; later phases/parts deferred.
- 🟡 **Partial** — some pieces shipped, substantial work remains.
- 📋 **Proposed** — designed, not yet built.
- 🔬 **Spike / Notes / Reference** — research, a recommendation, or background material; not a build commitment.

## Indexing

| RFC | Status | Summary |
|-----|--------|---------|
| [Index Sargability](./index-sargability.md) | ✅ Shipped | Unified recogniser for sargable predicates; A/B/C increments all complete. |
| [Unified Numeric Index Key](./unified-numeric-index-key.md) | ✅ Shipped | Collapse numeric types to one f64-sortable key to restore Eq/Range selectivity. |
| [Index Key Value/Doc-Id Boundary](./index-key-boundary.md) | ✅ Shipped | Deterministic string-key boundary via a trailing u32 value-length suffix. |
| [Compound Indexes](./compound-indexes.md) | ✅ Shipped | Multi-field indexes (scalar components), programmatic API, unique variants. |
| [Unique Indexes](./unique-indexes.md) | ✅ Shipped | Single-field unique indexes with dual key format and in-snapshot enforcement. |
| [Vector Index & `VECTORDISTANCE`](./vector-index.md) | ✅ Shipped | `VECTORDISTANCE` (cosine/dot/euclidean) + flat exact index, `float16`/`int8` quantization with exact rescore, filtered pre-search. Phases 1–2 complete; ANN (Phase 3) explicitly uncommitted, not pending. |
| [Covering Index Scans & Engine-Level Recheck](./covering-index-scans.md) | 🟢 Mostly shipped | Skip doc fetches when the index covers all referenced fields; Part A + Part B phases 1–2 done, phase 3 (covered aggregate) deferred. |
| [Index Intersection Strategy](./index-intersection-strategy.md) | 🟢 Mostly shipped | Galloping skip-merge for AND-ed equality scans; Phase 1 done, Door B (cost-based selection) deferred. |
| [Spatial Index](./spatial-index.md) | 🔬 Spike | Geohash/S2 candidate-scan + exact recheck to accelerate `ST_DISTANCE`/`ST_WITHIN`; recommendation only, not built. |
| [Multikey (Array) Indexes](./multikey-indexes.md) | 📋 Proposed | Multikey unique indexes enforcing no shared array elements across documents. |
| [Partial Indexes](./partial-indexes.md) | 📋 Proposed | Index only a filtered subset of documents to cut index size and write amplification. |

### Vector companions

| Document | Status | Summary |
|-----|--------|---------|
| [Vector Index — Phase 1 Plan](./vector-index-phase1-plan.md) | ✅ Executed | Spike output + ordered build plan for the flat index — fully carried out; the feature shipped. Kept as a historical record. |
| [Vector Index — Research Notes](./vector-index-research-notes.md) | 🔬 Reference | Background survey of vector-search systems, ANN families, and design trade-offs (84 cited sources). Permanent reference, not a feature. |

## Query & Execution

| RFC | Status | Summary |
|-----|--------|---------|
| [Decimal128 in Query Evaluation](./decimal128-evaluation.md) | ✅ Shipped | Decimal128 evaluates/compares in the f64 number tower; stored bytes untouched. |
| [Execution Context](./execution-context.md) | ✅ Shipped | Bundle per-query capabilities (params/rand/clock/watch/UDF) into an env; steps 1–2 shipped, 3–4 landed with Native Functions. |
| [Observability & Introspection](./observability-and-introspection.md) | 🟢 Mostly shipped | Tracing, `EXPLAIN ANALYZE`, and a stats API; A/B/C shipped, D (rows-examined) + on-disk size deferred. |
| [Resource Limits & Safety Valves](./resource-limits-and-safety-valves.md) | 🟢 Mostly shipped | Per-query deadline + materialization cap; A + B done, C (input-size) + D (rows-examined) deferred. |
| [SQL Query Surface](./sql-query-surface.md) | 🟡 Partial | SQL-like surface for aggregation/joins/complex reads over the shared plan tree. |
| [Collect Node](./collect-node.md) | 📋 Proposed | Explicit plan node that drains child streams into materialized batches for composability. |

## Storage & Durability

| RFC | Status | Summary |
|-----|--------|---------|
| [Raw BSON Robustness](./rawbson-robustness.md) | ✅ Shipped | Harden the raw BSON scanner against truncation with bounds checks + typed errors (`slate-rawbson`). |
| [Durability & Crash Safety](./durability-and-crash-safety.md) | ✅ Shipped | Durability knob with levels, crash-test harness, verify/repair integrity ops. |
| [On-Disk Format Versioning](./on-disk-format-versioning.md) | ✅ Shipped | Unify version tracking + migration across index, record, and catalog formats (v1). |
| [Logical Export / Import](./logical-export-import.md) | ✅ Shipped | Manifest-driven export/import for cross-backend migration and logical recovery (BSON path). |
| [Transaction & Concurrency Contract](./transaction-concurrency-contract.md) | 🟢 Mostly shipped | Pin isolation guarantees, `DbError::Conflict`, retry helper; parts 1–3 done, savepoints deferred. |
| [Encryption at Rest](./encryption-at-rest.md) | 🟡 Partial | OS-level encryption adopted as the documented at-rest story; backend crypto deferred. |
| [MemoryStore Persistence](./memorystore-persistence.md) | 📋 Proposed | Write-behind persistence for MemoryStore with pluggable flush backends. |

## Constraints & Logic

| RFC | Status | Summary |
|-----|--------|---------|
| [Native Functions](./native-functions.md) | ✅ Shipped | Role-typed native Rust hooks — UDFs, validators, triggers — replacing the scripted VM (`slate-vm` deleted). |
| [User-Defined Logic](./user-defined-logic.md) | 🟡 Partial | Umbrella for user-defined logic; triggers/validators/UDFs shipped native (via Native Functions); computed fields, key extractors, partial-index filters, and transform pipelines remain. |

## Public API

| RFC | Status | Summary |
|-----|--------|---------|
| [Public API Ergonomics (Collection handle)](./db-api-cleanup.md) | ✅ Shipped | Reorganise the API around `collection(name)` handle builders with lazy execution + terminal consumption. |

## Change Feeds & Reactivity

| RFC | Status | Summary |
|-----|--------|---------|
| [Change Detection (Watch Queries)](./watch-queries.md) | ✅ Shipped | Change-event core with BSON/SQL filters, callback + cursor delivery, set-transition recasting. |
