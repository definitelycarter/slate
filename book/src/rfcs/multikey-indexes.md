# RFC: Multikey (Array) Indexes

> **Status: proposed.** Extracted from the roadmap; the [roadmap](../roadmap.md)
> tracks status at a glance.

## Concept

Array fan-out is already supported for regular (`i`) indexes via `[]` path segments — `tags.[]` writes one index entry per element of `tags`, so `{ tags: "renewal_due" }` matches a document with `tags: ["active", "renewal_due"]`. A multikey index is the formalization of that fan-out as a first-class index kind.

## Motivation

Regular multikey already works for filtering. The open work is **multikey unique**: enforcing that no two documents share *any* array element (MongoDB semantics). With the per-value `u`-key scheme this nearly falls out — each element would claim its own `u` slot — but it is a distinct and surprising semantic surface, so unique indexes currently **reject** `[]` paths rather than enable it implicitly (see [Unique Indexes](./unique-indexes.md)).

## Work

- Allow `unique` on `[]` paths, fanning out one `u` entry per array element.
- Define and document the cross-element collision semantics, including a single document with duplicate elements (claims one slot, not a self-collision).
- Decide the interaction with sparse (empty array → no entries) and with compound multikey (MongoDB restricts to at most one multikey field per compound index).
