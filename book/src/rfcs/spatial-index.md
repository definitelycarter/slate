# RFC: Spatial Index

> **Status:** design spike — recommendation only, no engine code.
> **Scope:** accelerate the `ST_DISTANCE` point-radius and `ST_WITHIN(polygon)`
> predicates, which currently full-scan.

## Problem

The `ST_*` family is implemented in `slate-eval` (`functions/geo/`): a pure-Rust,
wasm-safe geometry model (`Geometry::{Point, LineString, Polygon, …}` over
`[lng, lat]` coords) with `ST_DISTANCE` on the WGS84 ellipsoid (Vincenty inverse
geodesic, haversine fallback near the antipode), `ST_AREA` on the authalic
sphere, and planar `ST_WITHIN`/`ST_INTERSECTS`/`ST_ISVALID`. What is missing is a
way to *find* matching documents without touching all of them.

A predicate like

```sql
SELECT * FROM places c
WHERE ST_DISTANCE(c.loc, {"type":"Point","coordinates":[-122.4,37.8]}) < 1000
```

parses to `Binary { Lt, Function { "ST_DISTANCE", [c.loc, point] }, 1000 }`. The
planner's sargability check (`slate-planner/src/sargable.rs`: `as_atom` →
`path_of`) only recognises the shape `alias.field <cmp> literal`; a function call
wrapping the field returns `None`, so the predicate is never sargable. It falls
through to `Node::Scan` + `Node::Filter`, and **every document in the collection
is decoded and run through the geodesic distance computation**. `ST_WITHIN(c.loc,
$polygon)` is the same: a full scan with a per-row point-in-polygon test.

The goal is to recognise these two predicates and serve them as a
**candidate-cell scan + exact recheck**, mirroring the post-filter discipline the
engine already uses everywhere: an index produces a conservative *superset* of
candidate ids cheaply, then the original predicate is re-evaluated exactly on
each candidate to drop false positives (the numeric byte-range scan already does
this — it sweeps a byte range that can pull in other types and post-filters with
`compare_bson`).

## Candidate schemes

Three families, judged against slate's one and only index mechanism: an **ordered
KV** where the *sort order is the index*, and every secondary entry is

```
i\0{collection}\0{field}\0{sortable_value_bytes}{doc_id_lp}   value: [type][ttl?]
```

range-scanned by a prefix + byte-range bounds (see the Unique/Compound Indexes
sections of the [roadmap](../roadmap.md) for the existing layouts). The decisive
question for each scheme is: **does a query region reduce to a small set of
contiguous, range-scannable key intervals?**

| Scheme | Index key | Region → key ranges? | New machinery | Notes |
|---|---|---|---|---|
| **Geohash prefix** | base32 string (a Z-order curve) | Yes — each cell is one prefix range | ~none (reuses the **string** index path) | non-uniform cells by latitude; boundary discontinuities need neighbour cells |
| **S2 cell covering** | u64 cell id (Hilbert curve over a cube) | Yes — each cell is **one** contiguous `[range_min, range_max]` interval | reuses the **i64-sortable** encoding, but needs an S2 port/dep | near-uniform cells, tighter coverings, hierarchical |
| **R-tree** | bounding boxes in a balanced tree | **No** — wants its own paged, rebalancing tree | a whole second index structure | best for arbitrary polygons + kNN, but fights the ordered-KV model |

### Geohash prefix

A geohash interleaves latitude and longitude bits and base32-encodes them, so a
shared string prefix means spatial proximity and a cell at level *L* is exactly
"all keys beginning with this *L*-character prefix." This is a Z-order
(Morton) space-filling curve.

It maps onto slate with **almost no new code**: a geohash is a string, and the
string index path already exists. Store each point's geohash as an ordinary
string index entry; a query region becomes a set of prefix ranges, each a
`Range { lower: prefix, upper: prefix⁺ }` over the existing string keyspace.
Weaknesses — cells shrink toward the poles, and two nearby points can land in
different cells across a cell boundary (worst near the equator / prime meridian)
— are real but only cost *recall of the covering*, never correctness: the fix is
to include the cell's 8 neighbours in the covering, which just widens an
already-conservative candidate set.

### S2 cell covering

S2 projects the sphere onto the six faces of a cube and lays a Hilbert curve over
each face, yielding a 64-bit cell id. Two properties make it the strongest
*technical* fit: cells are near-uniform in area (the cube projection plus a
quadratic transform hold the size ratio to ~2× versus geohash's large
latitude distortion), and — critically — **a parent cell's children occupy one
contiguous id interval** `[cell.range_min(), cell.range_max()]`. So `S2RegionCoverer`
turns any cap or polygon into a handful of cell ids, and each cell is a *single*
`Range` scan over the i64-sortable keyspace the engine already encodes — no
neighbour fan-out, tighter coverings, fewer scans.

The cost is a dependency: S2 is non-trivial, and pulling in the `s2` crate (or
vendoring a covering subset) cuts against the geo module's deliberate
pure-Rust / wasm-safe / no-proprietary-library stance. That tension is the main
reason it is the *upgrade*, not the first cut.

### R-tree

An R-tree stores minimum bounding rectangles in a balanced tree with overlapping
nodes, splitting and merging on write. It is excellent for arbitrary polygon
geometries and native nearest-neighbour ordering — and it is **the wrong shape
for this engine**. It is not a sortable-key structure: there is no single key
whose order is the index. Persisting one means managing node pages as opaque KV
blobs and implementing splits/rebalancing on top of the store — a second index
engine beside the ordered KV, not an encoding into it. `rstar` is in-memory only;
an on-disk R-tree is a PostGIS-GiST-scale project. Defer unless polygon-valued
geometries and kNN ordering become first-class requirements.

## How it maps onto the sortable-key index

Both space-filling-curve schemes share one shape, so the engine work is the same:

1. **Storage.** Add a spatial index *kind* (today an index carries only `path` +
   `unique: bool`; this is the first index that needs a real kind tag). For each
   document, extract the point and store its **maximum-precision** cell key
   (a long geohash, or a leaf S2 id) as a single index entry through the existing
   secondary-index path. Storing full precision and *querying by prefix-range*
   lets the query pick its covering level per-query instead of baking a level
   into storage. Index maintenance reuses `IndexDiff` unchanged — one entry per
   document, written/removed on insert/update/delete like any other index.

2. **Covering.** A query region (a *cap* of radius *r* for distance, the polygon
   itself for `ST_WITHIN`) is reduced to a small set of cells. For geohash that
   is a set of prefixes (+ neighbours); for S2 a set of `[range_min, range_max]`
   intervals. This is pure geometry and belongs next to the existing geo module
   in `slate-eval`.

3. **Scan.** Each covering cell is one range scan. The cleanest first cut reuses
   nodes that already exist: emit an `IndexMerge(Or)` over one `IndexScan` per
   cell, which unions and de-dups the candidate ids for free, then `KeyLookup`
   fetches the documents and the residual `Filter` does the exact recheck:

   ```
   Filter   ST_DISTANCE(c.loc, $p) < 1000      ← exact recheck (existing Vincenty/WGS84)
     KeyLookup                                  ← fetch full docs by id
       IndexMerge(Or)                           ← union candidate ids across cells
         IndexScan(loc#geo, range "9q8y")       ← one prefix/interval scan per covering cell
         IndexScan(loc#geo, range "9q8z")
         IndexScan(loc#geo, range "9q8v")
         …
   ```

   Reusing `IndexMerge(Or)` means **no new executor node is strictly required for
   v1**; the covering is the only genuinely new code on the read path. A dedicated
   multi-range `SpatialScan { field, region }` node (planner stays geometry-free,
   executor expands the covering) is the tidier long-term shape and gives EXPLAIN
   something honest to print — but it is an optimisation of the above, not a
   prerequisite.

## What it accelerates, and how the planner recognises it

Two predicate shapes, both currently full-scan:

- **Point-radius** — `ST_DISTANCE(<path>, <const point>) < <const>` (and `<=`).
  Cover the cap of radius *r* around the point; recheck with the exact
  `ST_DISTANCE`.
- **Polygon containment** — `ST_WITHIN(<path>, <const polygon>)`. Cover the
  polygon; recheck with the exact `within()`.

Recognition is a new sargability rule alongside `as_atom`, e.g.
`as_spatial_predicate()`, that pattern-matches `Function { "ST_DISTANCE",
[path, geometry-literal] } <cmp> numeric-literal` and `Function { "ST_WITHIN",
[path, polygon-literal] }`, confirms a spatial index exists on `path`, and — on
a match — emits the covering scan with the **original `ST_*` predicate retained
as the residual `Filter`**. This is exactly the existing
`(scan_node, residual_filter)` contract `plan_source` already returns; the
residual plumbing (`residual_excluding` → `Node::Filter`) is untouched. With no
spatial index present, nothing matches and the predicate falls through to today's
full-scan path — purely additive.

`ST_INTERSECTS` is accelerable by the same covering and is a natural follow-up;
v1 starts with the two predicates the roadmap already calls out.

## Accuracy: conservative superset + exact recheck

The index must be **invisible to results** — it changes speed, never which rows
come back. That holds iff the candidate set is a *conservative superset* of the
true matches: every document the full-scan filter would return must appear in the
covering. False positives are free (the recheck drops them); a false **negative**
is a wrong answer.

This is where the existing WGS84 / ellipsoid notes matter. `ST_DISTANCE` measures
on the WGS84 ellipsoid (Vincenty), and a covering will almost certainly be
computed on a cheaper model (a sphere, or S2's spherical cells) — the two
disagree by ~0.3–0.5% (≈5 km at 1900 km). The index lives in that approximate,
cell-quantised world; correctness survives only because the **recheck is the same
exact function the full scan would have run**, and the covering is deliberately
generous. Concretely, cover a cap of radius

```
r_cover = r · (1 + ε_ellipsoid) + cell_diagonal
```

so that a true match sitting just inside *r* on the ellipsoid can never fall in a
cell the covering omitted. For `ST_WITHIN`, an `S2RegionCoverer`/geohash covering
of a polygon is already a superset of its interior; keep the covering's edge
interpretation a superset of the planar edges the stored `within()` uses (or just
cover the polygon's bounding region generously). The discipline is identical to
the numeric byte-range scan that sweeps in cross-type entries and leans on
`compare_bson` to post-filter: **superset, then recheck**. The index can be as
sloppy as we like as long as it never drops a true positive — which is also why
geohash's accuracy weaknesses are tolerable.

## Cosmos, for reference only

Cosmos auto-indexes GeoJSON and supports spatial indexes (`spatialIndexes` in the
indexing policy, by path and geometry type) backed by a hierarchical grid /
quadtree tessellation with a precision — i.e. the same *cell-covering + recheck*
family this design sits in, so the approach is well-trodden. But Cosmos is a
*validation oracle, not a spec*: it tells us whether a document *matches* a
predicate, not how to build the index. And because the index is
invisible to results by construction, there is nothing about the *mechanism* for
the oracle to validate beyond "same rows as a full scan" — which the recheck
guarantees. We pick the scheme that fits slate's ordered-KV model, not Cosmos's.

## Recommendation — geohash for v1, S2 as the upgrade

Recommend **geohash prefix for v1**. It is the honest fit for this codebase: it
maps onto the *existing string index* with near-zero new encoding, it is
trivially pure-Rust and wasm-safe (a few dozen lines: bit-interleave + base32) in
keeping with the geo module's stated values, and the candidate-scan + recheck
structure means its weaknesses (latitude distortion, boundary discontinuities,
looser coverings) only ever cost *speed*, never correctness — they widen an
already-conservative candidate set that the exact recheck cleans up.

Name **S2 cell covering as the documented v2** for when covering tightness and
cell uniformity become the bottleneck: its one-interval-per-cell property is a
strictly better fit for the range-scan model and reuses the i64-sortable
encoding, at the cost of a non-trivial dependency to reconcile with the
pure-Rust stance. **R-tree is explicitly out** unless polygon-valued geometries
and native kNN ordering become first-class — it does not encode into the ordered
KV.

## Minimal v1 scope

- **Index kind.** Add a spatial kind to index metadata (the first index that
  needs more than `unique: bool`); store one full-precision geohash entry per
  document via the existing secondary-index path. `IndexDiff` maintenance is
  unchanged.
- **Point-valued fields only.** One geohash per document — no fan-out. Matches
  Cosmos's point auto-indexing and the spatial corpus. Non-point stored
  geometries (a covering per document, multikey-style fan-out) are deferred.
- **Two predicates.** `ST_DISTANCE(path, const) < const` / `<=` and
  `ST_WITHIN(path, const polygon)`, via a new `as_spatial_predicate()` recogniser.
- **Covering in `slate-eval/geo`.** Cap→cells and polygon→cells, conservatively
  inflated per the accuracy section. Pure-Rust, shared with the executor.
- **Scan via existing nodes.** `IndexMerge(Or)` of per-cell range scans →
  `KeyLookup` → residual `Filter` (the exact `ST_*` recheck). No new executor
  node required.
- **Cost guard.** If the covering explodes (huge radius, whole-globe polygon),
  fall back to the full scan — a covering with more cells than a scan has
  documents is a loss.

## Open questions

- **Covering level / precision.** Fixed per index, or chosen per query from the
  radius / polygon extent? Storing max precision and choosing the query level by
  prefix length keeps it per-query, but the selectivity-vs-cell-count knob still
  needs a heuristic.
- **Multi-range representation.** Ship v1 on `IndexMerge(Or)` and add a dedicated
  multi-range `SpatialScan` node later, or build the node up front for cleaner
  EXPLAIN and less union overhead?
- **Crate boundaries.** The covering is geometry code that the *planner* (or
  executor) needs. Emit an abstract `SpatialScan { field, region }` the executor
  expands (keeps `slate-planner` geometry-free), or lower to `IndexMerge(Or)` at
  plan time (needs the planner to reach the geo covering code)?
- **Antimeridian / poles.** Coverings that wrap ±180° longitude or include a pole
  need explicit handling; the conservative-superset rule must survive the wrap.
- **Non-point geometries.** Indexing LineStrings/Polygons means one document under
  many covering cells (multikey fan-out + candidate de-dup) — interaction with the
  `[]` multikey path and with `ST_INTERSECTS`.
- **S2 dependency.** If/when we adopt S2, do we take the `s2` crate or vendor a
  minimal RegionCoverer to preserve the wasm-safe, dependency-light posture?
