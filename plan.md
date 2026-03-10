# SQL Query Surface — Implementation Plan

## Overview

Add a SQL query language that compiles down to the existing plan tree. This is purely additive — the BSON filter/find API stays unchanged. SQL is a new front-end that produces the same `Statement` values the planner already consumes.

## Architecture

```
SQL string
  → slate-sql: parse (sqlparser-rs) → SQL AST
  → slate-sql: compile → Statement + Expression tree
  → slate-db: Planner::plan(statement) → Plan<Cf>
  → slate-db: Executor::execute(plan) → RawIter
```

`slate-sql` is a new crate that depends on `slate-db` (for `Statement`, `Expression`, `FindOptions`, etc.) and `sqlparser-rs` (for parsing). It produces `Statement` values that feed directly into the existing planner.

## Why `sqlparser-rs` over hand-rolled

- Battle-tested parser covering the full SQL grammar — handles string escaping, operator precedence, nested expressions, and dozens of edge cases that a hand-rolled parser would need to reimplement.
- ~200KB compile-time dependency, zero runtime overhead (it just produces an AST).
- The Slate-specific work is in the **compiler** (AST → Statement/Expression), not the parser. A hand-rolled parser adds maintenance burden with no architectural benefit.

## Phased Approach

### Phase 1: Basic SELECT (compiles to existing plan nodes)

**Scope:** Parse and execute SQL queries that map 1:1 to `Statement::Find`.

```sql
SELECT name, email FROM users WHERE status = 'active' AND age > 21 ORDER BY name ASC LIMIT 10 OFFSET 5
SELECT * FROM users WHERE name LIKE '^john'
```

**Work:**

1. **Create `crates/slate-sql/`** with dependency on `sqlparser` and `slate-db`
2. **SQL-to-Expression compiler** — walk the `sqlparser::ast::Expr` tree and produce `Expression` variants:
   - `Expr::BinaryOp { op: Eq, .. }` → `Expression::Eq`
   - `Expr::BinaryOp { op: Gt/GtEq/Lt/LtEq, .. }` → `Expression::Gt/Gte/Lt/Lte`
   - `Expr::BinaryOp { op: And/Or, .. }` → `Expression::And/Or`
   - `Expr::Like { .. }` → `Expression::Regex`
   - `Expr::IsNull` / `Expr::IsNotNull` → `Expression::Exists`
3. **SQL-to-Statement compiler** — from a parsed `SELECT` statement, build `Statement::Find`:
   - `FROM <table>` → collection name (and optional cf via schema prefix like `cf.collection`)
   - `WHERE` → Expression tree
   - `ORDER BY` → `Vec<Sort>`
   - `LIMIT` / `OFFSET` → `take` / `skip`
   - `SELECT <columns>` → projection
4. **Public API** — `slate_sql::compile(sql: &str) -> Result<CompiledQuery, SqlError>` where `CompiledQuery` holds the collection/cf and the Statement fields needed to call `txn.find()`
5. **Integration in `slate-db`** — add a `txn.query_sql(sql: &str, options)` convenience method that calls the compiler and executes

**What this does NOT include:** aggregation, GROUP BY, JOINs, subqueries, INSERT/UPDATE/DELETE via SQL.

### Phase 2: Aggregation (new plan nodes)

**Scope:** COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING.

**Work:**

1. **New plan nodes in `slate-db`:**
   - `Node::GroupBy { source, keys: Vec<String>, aggregates: Vec<Aggregate> }` — materializes input, groups by key fields, computes aggregates
   - `Node::Having { source, predicate: Expression }` — post-group filter
   - `Aggregate` enum: `Count`, `Sum(String)`, `Avg(String)`, `Min(String)`, `Max(String)`, `ArrayAgg(String)`

2. **New statement variant:** `Statement::Aggregate { cf, collection, predicate, group_by, aggregates, having, sort, skip, take }`

3. **Executor implementations** for GroupBy and Having nodes

4. **Extend SQL compiler** to recognize aggregate functions and GROUP BY/HAVING clauses, producing `Statement::Aggregate`

### Phase 3: Sub-document JOINs

**Scope:** CosmosDB-style `JOIN t IN c.tags` — array unwinding within a single document.

**Work:**

1. **`Node::ArrayUnwind { source, path: String, alias: String }`** — for each input document, expand the array at `path` into one row per element
2. **Extend SQL compiler** to handle `JOIN <alias> IN <path>` syntax
3. This is document-internal, not cross-collection — no foreign key lookups

## Crate Structure

```
crates/slate-sql/
├── Cargo.toml          # depends on sqlparser, slate-db, bson
└── src/
    ├── lib.rs          # public API: compile(), SqlError
    ├── compiler.rs     # sqlparser AST → Statement/Expression
    ├── expr.rs         # Expr → Expression conversion
    └── error.rs        # SqlError type
```

## Key Design Decisions

1. **`slate-sql` depends on `slate-db`**, not the other way around. The SQL surface is an optional add-on. Users who don't need SQL don't pull in `sqlparser`.

2. **No new `Expression` variants needed for Phase 1.** The existing Expression enum (Eq, Gt, Gte, Lt, Lte, And, Or, Regex, Exists) covers standard SQL WHERE clauses. SQL `IN (...)` can desugar to `Or(vec![Eq(...), Eq(...), ...])`. SQL `BETWEEN` desugars to `And(vec![Gte(...), Lte(...)])`.

3. **Collection families (cf):** SQL queries target a collection name. The default cf is `DEFAULT_CF`. An explicit cf can use dot-prefix syntax: `SELECT * FROM myns.users` where `myns` is the cf and `users` is the collection.

4. **BSON type mapping:** SQL literals map to BSON types: string → `Bson::String`, integer → `Bson::Int64`, float → `Bson::Double`, true/false → `Bson::Boolean`, null → `Bson::Null`.

5. **LIKE → Regex:** SQL `LIKE '%pattern%'` compiles to `Expression::Regex` with `%` → `.*` and `_` → `.` translation. This reuses the existing regex evaluation in the executor.

## Testing Strategy

- Unit tests in `slate-sql` for SQL → Expression conversion (each operator, edge cases)
- Unit tests for SQL → Statement compilation (projection, sort, limit/offset)
- Integration tests that open a `Database<MemoryStore>`, insert documents, and query via SQL
- Error cases: invalid SQL, unsupported features (subqueries, cross-collection joins), type mismatches
