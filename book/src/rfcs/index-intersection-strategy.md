# RFC: Index Intersection Strategy (skip-merge for AND)

> **Status: accepted (spike gate passed); phase 1 in progress.** Phase 1 is a
> *stats-free* skip-merge for the intersection of two-or-more **equality** index
> scans. Cost-based index selection (a cardinality/statistics catalog) is an
> explicit non-goal here — see
> [Two doors](#two-doors-and-why-we-take-the-stats-free-one). The spike confirmed
> the doc-id ordering precondition and benched the skew win + balanced
> non-regression on all three backends — see
> [Spike outcome](#spike-outcome-gate-passed).

## Problem

When two indexed equality predicates are AND-ed, the planner uses **both** indexes
and intersects them. The intersection reads the *larger* index range in full, even
when the *other* predicate is far more selective. Confirmed against the engine:

```
slate(next_best_actions)> .explain
  select nba.user.id as user_id, nba.priority_classification, nba.triggers
  from nba
  where nba.user.id = 'user_whale_a' and nba.status = 'active'
    and not is_null(nba.priority_classification)
  order by nba.priority_classification desc

Project {user_id: nba.user.id, priority_classification: …, triggers: …}
  Sort nba.priority_classification DESC
    Filter NOT is_null(nba.priority_classification)
      KeyLookup default_cf.next_best_actions
        IndexMerge AND
          IndexScan default_cf.next_best_actions.status = "active" forward   ← large
          IndexScan default_cf.next_best_actions.user.id = "user_whale_a" forward  ← selective
```

`status = "active"` matches a large fraction of the collection; `user.id =
"user_whale_a"` matches one user's rows. The intersection result, the `KeyLookup`,
the residual `Filter`, and the `Sort` are all tiny (a whale's active, classified
NBAs). **The only avoidable cost in the whole plan is reading all of `status =
"active"`** — and today we read it, materialise it, and throw almost all of it away.

## Current state

Confirmed by source.

**The planner is greedy — it uses every usable index.** `plan_source` loops over
`meta.indexes` and pushes *each* field that yields a sargable equality into the
access set, with no ranking or selectivity (`crates/slate-planner/src/sargable.rs:135-140`):

```rust
for field in &meta.indexes {
    if let Some((access, used)) = field_index_scan(&conjuncts, &consumed, alias, field) {
        accesses.push(access);          // every indexed field, unconditionally
        consumed.extend(used);
    }
}
```

Multiple accesses are folded left-associatively into an `IndexMerge(And)` tree
(`sargable.rs:160-167` lowering, `merge_sources` at `sargable.rs:454-469`). Because
`status` precedes `user.id` in `meta.indexes`, `status` becomes the **left** child.

**The executor's `And` is blocking and reads both sides fully**
(`crates/slate-executor/src/nodes/index_merge.rs:47-65`): it drains the right child
into a `HashSet`, then `collect()`s the *left* child into a `Vec` and filters it by
membership. So `status = "active"` (the left child) is fully scanned **and**
materialised into a `Vec`, then ~entirely discarded. There is no early termination,
no skip, and no streaming.

**There is no cost model anywhere.** No cardinality, selectivity, histogram, row
count, or estimate exists in the planner — `explain.rs:13` says so outright ("there
are no cost estimates or row counts, just the shape the planner settled on"). The
choice to intersect both indexes is purely structural.

Net cost of an `IndexMerge(And)` today ≈ **the larger of the two scans** (plus
buffering it). The selectivity asymmetry that makes the merge wasteful is invisible
to the planner.

## The cost shape (and when this *doesn't* matter)

Two plans for `A = a AND B = b`, both indexed:

- **Intersect both** (today): cost ≈ `max(|A|, |B|)` index entries read + buffer.
- **Drive off the smaller + recheck the other**: cost ≈ `min(|A|, |B|)` index
  entries + a `KeyLookup` of those docs, with the other predicate re-applied as the
  already-existing residual `Filter`.

Intersection wins when *both* sides are selective but their intersection is smaller
than either — it avoids random `KeyLookup`s of rows that fail the second predicate.
Driving off the smaller side wins under **skew**, which is the case above:
`user.id = whale` is selective, `status = active` is not.

**This may not bite your workload.** The whole premise is "`status = active` ≫ a
whale's rows." If `active` is not actually a large fraction (e.g. NBAs churn out of
`active` quickly), the second scan is cheap and this is premature optimisation. The
fix is gated on a bench (below) that demonstrates the skew win on a representative
corpus before any code lands.

## Two doors (and why we take the stats-free one)

The observation reads like "we need cost estimation." There are two ways to act on
it, and the general one is the one to *avoid* first.

**Door B — cost-based index selection.** Maintain per-index cardinality / distinct-
value stats, estimate equality selectivity as `1/distinct`, and pick a subset of
indexes (often just one) + residual recheck. General — it also informs Eq-vs-Range
driver choice, OR planning, and the intersect-vs-drive decision. But it needs a
statistics catalog *and stat maintenance on write*, and — decisively — **the
motivating example is the poster child for why uniform stats mislead.**
`user_whale_a` is the heavy tail; a `1/distinct` estimate badly under-counts a
whale. (It still gets the *ordering* right here — a whale's rows ≪ all active rows —
so it is usable for "which side drives," but it is fragile and it is a lot of
machinery for a skew problem.) **Door B is an explicit non-goal of this RFC.**

**Door A — a better intersection algorithm, with no stats at all.** Both equality
ranges are already sorted by doc-id (proof below), and the store already exposes the
`scan_range` seek primitive. So intersect them with a **galloping skip-merge** whose
cost is bounded by the *smaller* input regardless of which side that is — no
cardinality estimate, no catalog, robust to the whale. **This RFC builds Door A.**

## The doctrine

> Intersect equality index scans by a **galloping skip-merge over their shared
> doc-id order**, bounded by the smallest input — never by materialising and hashing
> the largest. The intersection is invisible to results (it changes speed, never
> which rows return); the residual recheck and `KeyLookup` are untouched. It applies
> exactly when every AND-ed part is an **equality** scan (so each stream is doc-id-
> sorted); any range/prefix part falls back to today's hash merge.

## Why it is sound on this substrate

The skip-merge needs one thing: both index streams emit doc-ids in the **same total
order**. They do, for equality scans.

1. **Key layout.** An index key is
   `i\0{collection}\0{field}\0{value_bytes}{doc_id_lp}` — value bytes immediately
   followed by the length-prefixed doc-id, the boundary recovered from the type tag
   (`IndexEntry::from_raw`, `crates/slate-engine/src/traits.rs:256-277`). The store
   sorts keys bytewise.
2. **An `Eq` scan fixes `value_bytes`.** Within `field = v`, every entry shares the
   same `value_bytes`, so the residual sort key is exactly `{doc_id_lp}`, compared
   bytewise. **Equality scans are therefore doc-id-sorted.**
3. **Both fields append the identical `doc_id_lp` encoding.** So two equality
   streams on different fields are sorted by the *same* bytewise order on the doc-id
   suffix. A sort-merge intersection across them is correct.
4. **The seek primitive exists.** `Transaction::scan_range(range: RangeBounds<Vec<u8>>,
   reverse)` (`crates/slate-store/src/store.rs:69-74`) is implemented on all three
   backends. To advance `field = v` to the first entry with `doc_id ≥ d`, seek to
   the byte key `i\0{coll}\0{field}\0{value_bytes(v)}{encode(d)}`. (`scan_range`
   already landed on the store trait + all three backends; the separate engine-side
   `scan_index` refactor to *use* it for range over-scan is independent — this RFC
   adds a sibling seek path and does not depend on it.)

**Galloping removes the balanced-case regression.** A naïve one-entry-at-a-time
leapfrog can do many seeks when the two sides are interleaved, and a seek costs more
than a sequential `next()` on every backend. Galloping (exponential probe, then
binary-narrow) makes the cost `O(Σ log gap)`: ≈ linear when the streams interleave
(gaps ≈ 1), and `min · log(max/min)` under skew. So it is never asymptotically worse
than today's hash merge and is dramatically better under skew — the only residual
risk is the per-seek constant factor in the balanced case, which the spike benches.

**Comparison must be on raw doc-id bytes, not decoded BSON.** The streams are sorted
by bytewise `doc_id_lp`; the merge must compare that same byte slice (decoded-value
comparison may not match byte order). The cursor abstraction must expose the raw
doc-id bytes for the merge to compare on.

## Scope: which merges qualify

- **Phase 1 — AND of ≥2 equality scans.** Scalar `Eq` (and, pending spike
  confirmation, multikey `.[]` `Eq`, whose fixed matched-value also yields doc-id
  order). Both children doc-id-sorted ⇒ skip-merge applies. **Bonus:** the merge's
  monotone advance emits each doc-id once, subsuming the multikey dedup the current
  path needs the `IndexMerge` HashSet for.
- **Out of phase 1 — any `Range`/`StringPrefix`/`Full` part.** A range scan's value
  bytes vary, so its stream is ordered by `value_bytes` *then* doc-id — **not**
  doc-id-sorted — so a doc-id skip-merge is unsound without first sorting. Mixed
  `Eq AND Range` falls back to today's hash `IndexMerge(And)`. (Driving off the `Eq`
  and rechecking the range as residual is the natural follow-up, but it is a
  *selectivity* decision — Door B territory — so it is deferred.)
- **Unchanged — `IndexMerge(Or)` (union).** The user's issue is AND-only; OR
  buffering is a separate question and is untouched.

## Design

Three pieces; the residual/recheck plumbing and `KeyLookup` are untouched.

**1. Engine: a seekable index cursor.** The engine owns key encoding, so — exactly
as `StringPrefix` lowers byte bounds inside the engine rather than letting the
planner fabricate non-UTF-8 BSON — the executor must not hand-build index-key bytes.
Add a cursor seam over `scan_index`'s machinery:

```rust
/// A forward (or reverse) cursor over one equality range, seekable by doc-id.
trait IndexCursor<'a> {
    /// Current entry, or None at end. Exposes raw doc-id bytes for byte-order merge.
    fn peek(&self) -> Option<Result<&IndexEntry, EngineError>>;
    /// Advance to the first entry whose doc-id ≥ `doc_id` (galloping internally),
    /// staying within this cursor's fixed value range. Builds the byte key and
    /// scan_ranges; preserves the same TTL-expiry filtering scan_index applies.
    fn seek(&mut self, doc_id: &[u8]) -> Result<(), EngineError>;
    fn advance(&mut self) -> Result<(), EngineError>;
}

fn open_index_cursor<'a>(handle, field, value, reverse) -> Result<impl IndexCursor<'a>, _>;
```

Internally `seek` reuses the same `i\0{coll}\0{field}\0{value_bytes}` prefix
resolution `scan_index` already computes, appends `encode(doc_id)`, and issues
`scan_range`. Expiry filtering stays inside the engine (entries are TTL-checked
internally, `scan_index` doc).

**2. Planner: emit an intersection node when every AND part is an equality.**
`merge_sources` (`sargable.rs:454-469`), when handed `LogicalOp::And` and every part
is an `IndexAccess::Scan { range: Eq, .. }` or `Multikey`, emits a single
`Node::IndexIntersect { collection, parts: Vec<{ field, value }> }` instead of the
left-associative `IndexMerge(And)` fold. Any non-equality part ⇒ today's fold,
unchanged. (`IndexAccess` already distinguishes `Scan`/`Multikey`/`Merge`, so this
is a shape test on the collected accesses — `sargable.rs:40-59`.)

**3. Executor: galloping zig-zag.** Open one cursor per part; repeatedly take the max
current doc-id across cursors, `seek` every other cursor to `≥` it; when all equal,
emit and `advance` all by one. Output is a **streaming, deduped** doc-id iterator
feeding the existing `KeyLookup` (`key_lookup.rs`) — replacing the blocking
buffer-and-hash of `index_merge.rs:47-65` for this case.

`.explain` gains an `IndexIntersect` line so the new shape is observable (mirror the
`IndexMerge` arm at `explain.rs:131-138`).

## Spike outcome (gate passed)

The spike resolved each precondition against `file:line` and benched the Q3 gate
on all three backends. **Verdict: GO.** Summary below; these double as the
implementation contract for phase 1.

**Q1 — doc-id total order & exact seek (SOUND).** The index key is
`i\0{coll}\0{field}\0{value_bytes}{doc_id_lp}`, where `doc_id_lp` is appended by
`doc_id.write_length_prefixed(buf)` (`encoding/key.rs:253-254`) as
`[tag][len_be16][bytes]` (`encoding/bson_value.rs:205-210`); the doc-id starts at
`value_start + end_offset(n-1)` (`IndexEntry::doc_id`, `traits.rs:464-481`). An
`Eq` scan fixes `value_bytes` (`resolve_index_scan`'s `Eq` arm,
`kv/transaction.rs:99-115`), so within a stream the only varying suffix is
`doc_id_lp` and **equality streams are doc-id-sorted** by its bytewise order.
Two equality streams share that order because both append the identical encoding.
The seek **reuses the raw `doc_id_lp` bytes** taken straight from the other
cursor's current entry — no decode→re-encode — so the seek key
`Included(value_prefix ++ doc_id_lp)` is byte-identical to the targeted entry and
lands exactly (or on the first greater entry when the target isn't present). Note
doc-ids are *not* projected onto the f64 key (`bson_value.rs:276`), which raw-byte
reuse sidesteps. *Decision:* add `IndexEntry::doc_id_bytes() -> &[u8]` and have
the merge compare those raw slices (bytewise `Ord` = store order), **never
decoded BSON**.

**Q2 — multikey (SOUND).** A `.[]` `Eq` stream is doc-id-sorted exactly like a
scalar `Eq`. Repeated array elements encode to the **identical key** (no element
ordinal, `key.rs:235-255`) and **collapse** in the store, so a single `.[]` `Eq`
cursor yields each doc-id at most once — verified by an engine probe
(`tags: ["db","db"]` → id once). Neutralising the existing `dedup_ids`
(`sargable.rs:662-674`) left the duplicate-element tests
(`array_contains_index.rs:104-130`) green, confirming dedup is not load-bearing
here. *Decision:* the merge advances **strictly past** each emitted doc-id, so
`IndexIntersect` needs no downstream `Distinct`; a *lone* multikey scan (not ≥2
parts) still routes through `dedup_ids` unchanged, so the `MultikeyEq` fix is
untouched.

**Q3 — galloping constant factor (THE GATE → PASS).** Throwaway store-level bench
across memory/redb/rocks: hash-merge (today) vs naive always-seek leapfrog vs
galloping (`next()` up to a small limit, then one `scan_range` seek for a large
gap). A fresh seek costs `R` = 3.5–7.5× a sequential `next()`.

| | skew (50k ∩ 500) | balanced (25k ∩ 17k, interleaved) |
|---|---|---|
| **memory** | gallop **7.1× faster** | gallop **−35%**; leapfrog **5.5× slower** |
| **redb** | gallop **5.5× faster** | gallop **−27%**; leapfrog **6.5× slower** |
| **rocks** | gallop **6.3× faster** | gallop **−13%**; leapfrog **4.6× slower** |

Skew win is real and large on every backend; the balanced case does **not**
regress under galloping (it's faster — it skips building/probing the hash set).
Naive leapfrog regresses balanced 4.6–6.5× (it pays `R` per step), which is
exactly why the design must **gallop**, not leapfrog. `GALLOP_LIMIT ≈ 8` worked
well; it is a tunable knob, revisit with the real step-5 numbers.

**Q4 — N-way (RESOLVED: N-way).** The zig-zag generalises to N cursors: take the
max current doc-id, seek every other cursor to `≥` it, emit when all equal and
advance all. Terminates (each step advances ≥1 cursor strictly forward) and stays
min-bounded. Prefer **N-way over pairwise** — it never materialises an
intermediate `A∩B`. The planner already builds a flat parts list
(`IndexAccess::Merge` flattens same-op, `sargable.rs:608-620`), so
`Node::IndexIntersect { parts }` drops in.

**Q5 — cursor lifetime (RESOLVED).** The cursor borrows the txn for `'a`
(`&'a txn`, `&'a cf`) and holds an open `scan_range` iterator; `seek` drops and
re-opens it (still `'a`). This composes with the executor's `ValueIter<'a>`; the
prototype compiled and ran with exactly this shape.

## Non-goals

- **Cost-based index selection / a statistics catalog (Door B).** Deferred. Revisit
  only if a workload needs the Eq-vs-Range *driver* choice that genuinely requires
  cardinality — and prefer measuring before building a catalog the whale would fool.
- **Range/prefix intersection skip-merge.** Needs a different order or an explicit
  sort; out of scope.
- **`IndexMerge(Or)` / union changes.** Separate concern.
- **Same-field collapse.** Not relevant — this is a cross-field intersection.

## Sequencing

1. **Spike — DONE (gate passed).** Confirmed the doc-id ordering precondition
   (Q1–Q2), resolved N-way + lifetime (Q4–Q5), and benched the skew *and*
   balanced cases (Q3) on all three backends. **Gate met: skew win 5.5–7.1×,
   balanced non-regressing under galloping.** See
   [Spike outcome](#spike-outcome-gate-passed).
2. **Engine seekable index cursor** — `open_index_cursor` + galloping `seek`,
   reusing `scan_index`'s prefix resolution and expiry filtering. Tests: seek lands
   exactly, expiry honoured, reverse direction.
3. **`Node::IndexIntersect` + planner emission** — `merge_sources` shape test; the
   non-equality fallback path keeps today's `IndexMerge(And)`. `.explain` line.
4. **Galloping executor + tests** — skew, balanced, 3-way, multikey-with-duplicate-
   element, empty-intersection, disjoint, single-survivor. Assert identical results
   to the hash-merge path (the intersection is invisible to results).
5. **Bench + docs** — run the affected harness (the **bench** skill) against the
   hash-merge baseline; if headline numbers move, sync `README.md` and
   `book/src/benchmarks.md`; add a roadmap entry per the **docs** skill.

## Recommendation

Build **Door A, phase 1**: a stats-free galloping skip-merge for the intersection of
all-equality AND, emitted as a dedicated `IndexIntersect` node and bounded by the
smallest input. Keep **Door B (cost-based selection)** an explicit non-goal — the
whale is exactly the input that breaks uniform stats, and the skip-merge gets the win
without estimating anything. **Gate the work on the spike's bench**: if `status =
active` is not actually large relative to a whale on a representative corpus, the
current hash merge is already cheap and this should not ship.
