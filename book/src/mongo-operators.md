# Mongo Query & Update Operators

Slate's Mongo-style surface (`find`, `update_*`, `delete_*`) accepts BSON filter
and update documents and **translates them to the same expression IR the SQL
front-end produces** — so a `find` filter is planned, indexed, and executed
exactly like the equivalent `WHERE`. This chapter is the operator reference; for
the query model and plan shapes, see [Querying](./querying.md), and for the scalar
functions some operators desugar to, see the [Function Reference](./functions.md).

Field names are **dot-notation paths** (`"address.city"`); an explicit array path
uses `.[]` (`"tags.[]"`), described under [Array fields](#array-fields).

## Query operators (filters)

The filter examples below are **live** — each `slate-find` cell runs against the
playground's `products` and `families` collections (the first line names the
collection). Edit and run them; see the [Playground](./playground.md) for the
dataset shapes.

### Implicit equality — `{ field: value }`

A bare field/value pair matches a scalar field equal to `value` **or** an array
field that contains it — so the same filter works whether the field holds a scalar
or an array:

```slate-find
products
{ "category": "Electronics" }
```
That's `c.category = "Electronics" OR ARRAY_CONTAINS(c.category, "Electronics")`. A
filter on the string-array field `tags` uses the very same form:

```slate-find
products
{ "tags": "office" }
```
As a special case, `{ "field": null }` also matches documents where the field is
**absent** (missing *or* explicitly null), matching Mongo.

### `$eq`

Explicit form of implicit equality (same `= OR ARRAY_CONTAINS` semantics):

```slate-find
products
{ "category": { "$eq": "Furniture" } }
```

### Comparison — `$gt`, `$gte`, `$lt`, `$lte`

```slate-find
products
{ "price": { "$gt": 25, "$lt": 400 } }
```
Multiple operators in one sub-document AND together (`price > 25 AND price < 400`).
Comparisons are **type-bracketed** like SQL — a number bound only matches numbers,
a string bound only strings. (Not available on explicit `.[]` array paths.)

### `$exists`

```slate-find
families
{ "isRegistered": { "$exists": true } }
```
→ `IS_DEFINED(c.isRegistered)`. The negated form
`{ "field": { "$exists": false } }` is `NOT IS_DEFINED(c.field)`. The value must be
a boolean. (Not available on explicit `.[]` array paths.)

### `$regex` / `$options`

```slate-find
products
{ "name": { "$regex": "^Premium", "$options": "i" } }
```
→ `REGEXMATCH(c.name, "(?i)^Premium")`. The options string (`i`, `m`, `s`, `x`) is
folded into an inline regex flag group. `$regex` is exclusive — it cannot be
combined with other operators in the same sub-document.

### Logical — `$and`, `$or`

Each takes an array of sub-filters:

```slate-find
products
{ "$or": [ { "category": "Furniture" }, { "price": { "$lt": 30 } } ] }
```
Multiple fields at the top level are an **implicit AND**:

```slate-find
products
{ "category": "Electronics", "inStock": true }
```
→ `category = "Electronics" AND inStock = true`. When both branches of an `$or` are
indexed equalities the planner builds an `IndexMerge`; see [Querying](./querying.md).

### Array fields

An explicit multikey path (`"tags.[]"`, `"items.[].sku"`) tests **array
membership** and compiles to a form the planner can match against a `.[]` index:

```slate-find
products
{ "tags.[]": "office" }
```
matches any document whose `tags` array contains `"office"`. On an explicit `.[]`
path only equality is supported — range and `$exists` are not.

### Unsupported operators

Only the operators above are implemented. Others — including `$in`, `$nin`,
`$ne`, `$not`, `$nor`, `$elemMatch`, `$type`, `$size`, `$mod` — currently return a
translation error rather than silently mismatching. (`$in` over an indexed field is
expressible in SQL today as an `IN (…)` list, which the planner turns into an
`IndexMerge`; a Mongo `$in` is on the roadmap.)

## Update operators

`update_one` / `update_many` take an update document. Each operator reduces to
"write an expression to a path"; a **bare (non-`$`) field is an implicit `$set`**.
The primary-key field cannot be mutated (the planner rejects it). Unknown `$`
operators error.

### `$set`

```json
{ "$set": { "status": "archived", "meta.reviewed": true } }
```
Writes each value to its (dot-notation) path, creating intermediate fields as
needed.

### `$unset`

```json
{ "$unset": { "tempFlag": "" } }
```
Removes the field (assigns `undefined`).

### `$inc`

```json
{ "$inc": { "views": 1, "score": -2 } }
```
Type-preserving numeric increment — desugars to [`INC(c.field, delta)`](./functions.md#inccurrent-delta).
Unlike SQL `+` (which widens to double), `$inc` keeps the field's integer type, and
a missing field starts from zero.

### `$push` / `$lpush`

```json
{ "$push":  { "tags": "new" } }
{ "$lpush": { "queue": "first" } }
```
Append (`$push` → [`RPUSH`](./functions.md#rpusharr-value)) or prepend (`$lpush` →
[`LPUSH`](./functions.md#lpusharr-value)) a value to an array; a missing array
becomes a single-element array.

### `$pop`

```json
{ "$pop": { "history": 1 } }
```
Removes the last element of the array — desugars to [`POP(c.field)`](./functions.md#poparr).

### `$rename`

```json
{ "$rename": { "fullName": "name" } }
```
Renames a field by setting the new path from the old value, then unsetting the old
field.
