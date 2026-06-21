# Plan Scenarios

The planner chooses a query's source — an `IndexScan`, an `IndexMerge`, or a full `Scan` — from the filter shape and the collection's indexes. These worked examples show the plan chosen for each common filter (including the known fallbacks), plus a full pipeline and how `LIMIT` is placed. See [Querying](./querying.md) for the query model.

The following scenarios show how the planner chooses a scan source (sargability and index selection) for different filter combinations. All examples assume:

```
indexed_fields: ["user_id", "status"]
```

> **Reading these trees.** Every `find` plan is wrapped in a top-level `Project(c)` (the identity projection) over a single inline-bound alias; both are omitted below to focus on source selection. `KeyLookup` fetches documents for an ID stream produced by an `IndexScan`/`IndexMerge`. v2 uses **every** applicable index — multiple indexed equalities intersect via `IndexMerge(And)` rather than the planner picking one — and a sargable equality that fully covers its index is *consumed* (no residual recheck).

---

### 1. No Filter

**Query:** `find({})`

```
Scan
```

Full scan — reads every document. No filter node needed.

---

### 2. Single Indexed Eq

**Query:** `find({ status = "active" })`

```
KeyLookup
  └── IndexScan(status = "active")
```

The `Eq` on an indexed field becomes an `IndexScan`; `KeyLookup` fetches the matched documents. The equality is *consumed* — a scalar index returns only documents whose `status` equals `"active"`, so no residual recheck is needed.

---

### 3. Single Non-Indexed Condition

**Query:** `find({ score > 50 })`

```
Filter(score > 50)
  └── Scan
```

No index available — full scan with a filter. Each document is read as raw bytes and `score > 50` is evaluated lazily; documents that fail are never deserialized.

---

### 4. AND — Indexed + Non-Indexed

**Query:** `find({ status = "active" AND score > 50 })`

```
Filter(score > 50)
  └── KeyLookup
        └── IndexScan(status = "active")
```

The `Eq` on `status` becomes an `IndexScan` (consumed). `score > 50` can't use an index, so it stays a residual `Filter`. The index narrows the candidate set; the filter evaluates the rest lazily.

---

### 5. AND — Multiple Indexed Fields → IndexMerge(And)

**Query:** `find({ status = "active" AND user_id = "abc" })`

```
KeyLookup
  └── IndexMerge(And)
        ├── IndexScan(status = "active")
        └── IndexScan(user_id = "abc")
```

Both fields are indexed, so v2 scans **both** indexes and intersects their ID sets with `IndexMerge(And)` — a smaller candidate set than either index alone. Both equalities are consumed, so there's no residual filter. (Unlike a pick-one-index strategy, v2 applies every index that helps.)

---

### 6. AND — Multiple Indexed + Non-Indexed

**Query:** `find({ user_id = "abc" AND status = "active" AND score > 50 AND name ~ "alice" })`

```
Filter(score > 50 AND REGEXMATCH(name, "alice"))
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Both indexed equalities push into the `IndexMerge(And)` and are consumed. The non-indexable conditions — `score > 50` and the regex on `name` — remain as the residual filter, evaluated lazily on the intersected candidates.

---

### 7. OR — Both Branches Indexed

**Query:** `find({ user_id = "abc" OR status = "active" })`

```
Filter(user_id = "abc" OR status = "active")
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(user_id = "abc")
              └── IndexScan(status = "active")
```

Every OR branch is an indexed equality, so the planner builds an `IndexMerge(Or)` — a union of the two index scans' ID sets. The full OR is kept as a residual recheck (not consumed): an index union can over-return, so the filter re-confirms each candidate.

---

### 8. OR — One Branch Not Indexed

**Query:** `find({ status = "active" OR name = "test" })`

```
Filter(status = "active" OR name = "test")
  └── Scan
```

`name` is not indexed. If **any** OR branch lacks an indexed equality, the whole OR falls back to a full `Scan` — a disjunction needs every branch to contribute, so one unindexed branch forces scanning everything.

---

### 9. OR — Same Field, Multiple Values (IN-style)

**Query:** `find({ user_id = 1 OR user_id = 2 })`

```
Filter(user_id = 1 OR user_id = 2)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(user_id = 1)
              └── IndexScan(user_id = 2)
```

Effectively an `IN` query. Each value gets its own `IndexScan`, unioned via `IndexMerge(Or)`. Common for multi-tenant access control — "show records for user 1 or user 2." (Each `{user_id: v}` lowers to the implicit-equality idiom `user_id = v OR ARRAY_CONTAINS(user_id, v)`; the planner recognizes the whole idiom as one indexed equality, so the union still forms.)

---

### 10. OR — Three Values (Left-Associative Fold)

**Query:** `find({ user_id = 1 OR user_id = 2 OR user_id = 3 })`

```
Filter(user_id = 1 OR user_id = 2 OR user_id = 3)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

Multiple OR branches fold into a left-associative tree: `((1 Or 2) Or 3)`. Each `IndexMerge(Or)` unions its children's ID sets.

---

### 11. AND with Nested OR — Intersect the Eq with the OR-Merge

**Query:** `find({ user_id = "abc" AND (status = "active" OR status = "archived") })`

```
Filter(status = "active" OR status = "archived")
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(user_id = "abc")
              └── IndexMerge(Or)
                    ├── IndexScan(status = "active")
                    └── IndexScan(status = "archived")
```

Both halves are indexable: the `user_id` equality is consumed as an `IndexScan`, and the fully-indexable OR sub-group becomes an `IndexMerge(Or)`. v2 intersects them with `IndexMerge(And)`, narrowing to documents that satisfy both. The OR sub-group is kept as a residual recheck (an index union can over-return).

---

### 12. AND with Nested OR — No Other Indexed Conjunct

**Query:** `find({ (status = "active" OR status = "archived") AND score > 50 })`

Indexed fields: `["status"]`

```
Filter((status = "active" OR status = "archived") AND score > 50)
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexScan(status = "active")
              └── IndexScan(status = "archived")
```

The fully-indexable OR sub-group becomes an `IndexMerge(Or)` (kept as a recheck). `score > 50` isn't indexable, so the residual filter carries both the OR recheck and `score > 50`, evaluated lazily on the narrowed candidates.

---

### 13. OR with Nested ANDs — Falls Back to Scan (known gap)

**Query:** `find({ (user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending") })`

```
Filter((user_id = "abc" AND status = "active") OR (user_id = "xyz" AND status = "pending"))
  └── Scan
```

Each OR branch is itself a multi-field **AND**. The OR-branch planner recognizes indexed atoms and single-field equalities, but does not yet plan a conjunction branch (pick a branch's best index), so the disjunction can't be indexed and falls back to a `Scan`. **This is a known gap** — see [Roadmap](roadmap.md). The intended plan unions a per-branch `IndexScan` (e.g. on `user_id`) via `IndexMerge(Or)`.

---

### 14. OR with Partial Index Per Branch — Falls Back to Scan (known gap)

**Query:** `find({ (user_id = "abc" AND score > 50) OR status = "active" })`

```
Filter((user_id = "abc" AND score > 50) OR status = "active")
  └── Scan
```

The second branch (`status = "active"`) is indexable on its own, but the first branch is an AND (`user_id = "abc" AND score > 50`) — the same conjunction-branch gap as scenario 13 — so the whole OR scans. Once OR branches can be planned as conjunctions, this becomes an `IndexMerge(Or)` of the `user_id` and `status` scans.

---

### 15. OR with Unindexed Branch — Fallback to Scan

**Query:** `find({ (user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo") })`

```
Filter((user_id = "abc" OR status = "active") OR (count > 5 OR name = "foo"))
  └── Scan
```

The branch `(count > 5 OR name = "foo")` has no indexed equality. Any unindexed OR branch poisons the whole disjunction, so the query falls back to `Scan` with the full predicate as the residual filter.

---

### 16. Complex — Multi-Level Nesting

**Query:** `find({ (user_id = 1 OR user_id = 2 OR user_id = 3) AND status = "active" AND (count > 5 OR name = "foo") })`

```
Filter((user_id = 1 OR user_id = 2 OR user_id = 3) AND (count > 5 OR name = "foo"))
  └── KeyLookup
        └── IndexMerge(And)
              ├── IndexScan(status = "active")
              └── IndexMerge(Or)
                    ├── IndexMerge(Or)
                    │     ├── IndexScan(user_id = 1)
                    │     └── IndexScan(user_id = 2)
                    └── IndexScan(user_id = 3)
```

The AND group has three children:
1. `(user_id = 1 OR user_id = 2 OR user_id = 3)` — fully-indexable OR sub-group → `IndexMerge(Or)`
2. `status = "active"` — indexed equality → `IndexScan` (consumed)
3. `(count > 5 OR name = "foo")` — not indexable → residual

v2 intersects every indexable source: the `status` scan and the `user_id` `IndexMerge(Or)` are combined with `IndexMerge(And)`. The unindexable `(count > 5 OR name = "foo")` and the `user_id` OR recheck remain the residual filter.

If `status` were **not** indexed, the `user_id` OR sub-group is the only indexable source, so it becomes the whole source and everything else is residual:

```
Filter((user_id = 1 OR ...) AND status = "active" AND (count > 5 OR name = "foo"))
  └── KeyLookup
        └── IndexMerge(Or)
              ├── IndexMerge(Or)
              │     ├── IndexScan(user_id = 1)
              │     └── IndexScan(user_id = 2)
              └── IndexScan(user_id = 3)
```

---

### 17. Fully Unindexed

**Query:** `find({ score > 50 AND name ~ "alice" })`

```
Filter(score > 50 AND REGEXMATCH(name, "alice"))
  └── Scan
```

No indexed fields, no equality — full scan. Both conditions are evaluated lazily on raw bytes; rejected documents are never fully deserialized.

---

### 18. Projection Over an Index Scan

**Query:** `find({ status = "active", columns: ["status"] })`

```
Project([_id, status])
  └── KeyLookup
        └── IndexScan(status = "active")
```

`IndexScan` yields document IDs, `KeyLookup` fetches the documents, and `Project` builds `{ _id, status }` from them.

> **Covered-index projection is future work in v2.** Even when every projected column is the indexed field, v2 still fetches the document via `KeyLookup` — it does not yet serve a projection directly from the index entry's value. The deferred optimization (and the approaches to extend it to multiple columns — composite indexes, or secondary index lookups) is tracked in the [Roadmap](roadmap.md).

---

### 19. Array Element Matching

**Query:** `find({ tags = "renewal_due" })` (where `tags` is an array like `["active", "renewal_due"]`)

```
Filter(tags = "renewal_due" OR ARRAY_CONTAINS(tags, "renewal_due"))
  └── Scan
```

The Mongo `{tags: v}` form lowers to the implicit-equality idiom `tags = v OR ARRAY_CONTAINS(tags, v)`: it matches when `tags` equals `v` *or* is an array containing `v`. The evaluator iterates array elements, delegating each to the shared scalar comparison (so cross-type coercion works within elements) — matching MongoDB's behavior without `$elemMatch`.

For an **explicit** multikey path (`tags.[]`, `items.[].sku`), the front-end emits a `MultikeyEq` the planner can match to a `.[]` index. Sorting on array fields has no meaningful scalar ordering and is unsupported.

---

## Full Pipeline Example

**Query:** `find({ filter: status = "active" AND score > 50, sort: score DESC, skip: 10, take: 5, columns: ["name", "score"] })`

```
Limit(skip: 10, take: 5)
  └── Project({_id, name, score})
        └── Sort(score DESC)
              └── Filter(score > 50)
                    └── KeyLookup
                          └── IndexScan(status = "active")
```

Execution flow (data flows bottom-to-top):

1. **IndexScan** — iterates the `status = "active"` index, yields document IDs (the equality is consumed — no `status` recheck)
2. **KeyLookup** — batch-fetches raw BSON bytes via `multi_get`
3. **Filter** — evaluates `score > 50` by reading just the `score` field from raw bytes; rejected documents are skipped with zero deserialization cost
4. **Sort** — collects survivors, reads `score` from raw bytes, sorts in memory
5. **Project** — copies `name` and `score` (plus `_id`) into a `RawDocumentBuf` via `append()`
6. **Limit** — skips 10, takes 5

`Project` sits below `Limit`, so v2 projects the sorted rows and then limits. Documents that fail the filter at step 3 never reach steps 4–6.

## Limit Placement

For document streams (find queries), `Limit` is lazy `skip()` + `take()` on the iterator; for the `RawBson::Array` of a distinct query, it slices array elements directly. It always sits at the top of the pipeline (above `Project`); whether work is saved depends on whether a `Sort` is present.

### Scenario A: Limit with Sort

**Query:** `find({ status = "active", sort: score DESC, take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── Sort(score DESC)
              └── KeyLookup
                    └── IndexScan(status = "active")
```

Limit can't take before Sort orders the rows, so all matching documents enter Sort and Limit takes 200 from the sorted result. Cost: O(n log n) on the full matched set. (`Project(c)` is the identity projection for `find`.)

### Scenario B: Limit without Sort

**Query:** `find({ status = "active", take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── KeyLookup
              └── IndexScan(status = "active")
```

No Sort: the stream stops after 200 documents pass through. Remaining index entries and raw bytes are never touched — much cheaper than Scenario A.

### Scenario C: Limit without Sort, with Filter

**Query:** `find({ score > 50, take: 200 })`

```
Limit(take: 200)
  └── Project(c)
        └── Filter(score > 50)
              └── Scan
```

Documents stream through Filter one at a time; Limit stops after 200 pass. Failures don't count toward the limit — the scan continues until 200 qualify (or the collection is exhausted).

