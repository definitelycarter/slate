# Playground

The query blocks on this page are **live**. They run real slate queries in your
browser — the engine is compiled to WebAssembly and seeded with the sample
dataset below. Edit any query and press **Run** (or `Ctrl`/`Cmd`+`Enter`) to see
the result.

Everything runs client-side against an in-memory database; there is no backend
and nothing leaves the page. Each page load starts from a fresh copy of the
dataset, so you can't break anything — reload to reset.

> The playground needs to fetch the WebAssembly module over HTTP, so it only
> activates when the book is served (e.g. `mdbook serve`), not when an
> `.html` file is opened directly from disk. If the module can't load, the
> blocks below stay readable as plain examples.

## How a query names its collection

slate follows the Cosmos model: a query's `FROM` clause binds a **row alias**,
and the container is chosen separately. The playground bridges the two by taking
the collection from the first name in `FROM`. So:

```text
SELECT c.name FROM products c WHERE c.price > 100
            row alias ─┘  └─ collection the playground runs against
```

means "run against the `products` collection, binding each document to `c`".

## The dataset

Two collections are seeded, mirroring the shapes used by slate's Cosmos parity
suite.

### `products` — flat catalog documents

| field      | type            | example                                  |
| ---------- | --------------- | ---------------------------------------- |
| `_id`      | string (pk)     | `"prod-001"`                             |
| `category` | string (indexed)| `"Electronics"`                          |
| `name`     | string          | `"Premium Laptop"`                       |
| `price`    | number          | `1299.99`                                |
| `inStock`  | bool            | `true`                                   |
| `tags`     | array\<string\> | `["computer", "productivity"]`           |

Five products across three categories (`Electronics`, `Furniture`,
`Stationery`).

### `families` — nested documents

| field          | type                | notes                                  |
| -------------- | ------------------- | -------------------------------------- |
| `_id`          | string (pk)         | `"AndersenFamily"`                     |
| `lastName`     | string (indexed)    | `"Andersen"`                           |
| `isRegistered` | bool                |                                        |
| `address`      | object              | `{ state, county, city }` sub-object   |
| `parents`      | array\<object\>     | `[{ firstName }, …]`                   |
| `children`     | array\<object\>     | each may have its own `pets` array     |
| `tags`         | array\<string\>     |                                        |

## Starter queries

### Scan and project

Read every product, keeping just three fields:

```slate-sql
SELECT c.name, c.price, c.inStock FROM products c
```

### Indexed equality

`category` is indexed, so an equality filter on it is served by an index scan
rather than a full collection scan:

```slate-sql
SELECT VALUE c.name FROM products c WHERE c.category = "Electronics"
```

(`SELECT VALUE` projects a bare value — here a list of strings — instead of a
document.)

### Order by

Sort the catalog by price, most expensive first:

```slate-sql
SELECT c.name, c.price FROM products c ORDER BY c.price DESC
```

### Function calls

slate ships the Cosmos scalar functions — `UPPER`, `LOWER`, `LENGTH`,
`STARTSWITH`, `CONTAINS`, `ARRAY_LENGTH`, `ARRAY_CONTAINS`, and more. Here we
upper-case the category and count each product's tags:

```slate-sql
SELECT c.name, UPPER(c.category) AS cat, ARRAY_LENGTH(c.tags) AS tagCount
FROM products c
ORDER BY ARRAY_LENGTH(c.tags) DESC
```

Functions work in `WHERE`, too:

```slate-sql
SELECT VALUE c.name FROM products c WHERE STARTSWITH(c.name, "Premium")
```

### Group by

Aggregate per category with `COUNT` and `AVG`:

```slate-sql
SELECT c.category, COUNT(1) AS n, AVG(c.price) AS avgPrice
FROM products c
GROUP BY c.category
ORDER BY c.category ASC
```

## Working with nested documents

Filter on a sub-object field (`families` is indexed on `lastName`, but here we
filter on `address.state`):

```slate-sql
SELECT VALUE f.lastName FROM families f WHERE f.address.state = "WA"
```

`JOIN` unwinds an array into one row per element — here, one row per child:

```slate-sql
SELECT f.lastName, ch.firstName, ch.grade
FROM families f
JOIN ch IN f.children
ORDER BY ch.grade ASC
```

## The `find` API

slate also has a document-filter `find` API (the same surface the Swift and wasm
bindings expose). A `find` cell is a **collection name** on the first line
followed by a JSON filter:

```slate-find
products
{ "category": "Electronics" }
```

An empty filter matches everything:

```slate-find
families
{}
```

## Non-deterministic sources (the wasm host)

Two SQL functions can't be answered from the data alone: `GETCURRENTTIMESTAMP()`
needs a clock, and `RAND()` needs a random source. The native build supplies
both from the OS, but those defaults sit behind slate-db's `runtime` feature,
which is deliberately turned **off** in the wasm build so the `getrandom`/OS
entropy machinery never reaches `wasm32-unknown-unknown`. Instead the wasm host
— exactly the one running this playground — injects both from JavaScript when it
opens the database:

```rust
use slate_db::DatabaseBuilder;
use slate_store::MemoryStore;

let db = DatabaseBuilder::new()
    // Clock: a *static* value captured once per transaction (epoch millis).
    .with_clock(|| js_sys::Date::now() as i64)
    // Rand: a *callable* — a fresh f64 in [0, 1) on every RAND() call.
    .with_rand(|| js_sys::Math::random())
    .open(MemoryStore::new())?;
```

The two differ in shape, and you can see it in a single query. The clock is the
same for every row (it's threaded in once, as the `$now` parameter), while
`RAND()` is drawn afresh per call:

```slate-sql
SELECT c.name, RAND() AS roll, GETCURRENTTIMESTAMP() AS asOf
FROM products c
```

`asOf` is identical down every row; `roll` is a different number each time. A
bare `SELECT VALUE RAND()` works too — a single draw:

```slate-sql
SELECT VALUE RAND() FROM products c
```
