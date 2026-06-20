//! In-memory `SELECT VALUE` execution engine.
//!
//! Pipeline: build a base row per source document → apply each `JOIN ... IN`
//! as an array-unwind cross product → filter by `WHERE` → project `SELECT
//! VALUE` (omitting undefined rows) → `ORDER BY` → `OFFSET`/`LIMIT`.
//!
//! This is deliberately storage-free: it operates over a `&[Bson]` slice so we
//! can validate language semantics without the planner.

use std::cmp::Ordering;

use bson::{Bson, Document};

use crate::error::{Result, SqlError};
use slate_ast::{FromSource, OrderByItem, Query, SortDirection};
use slate_eval::Value;
use slate_eval::eval::{self, Env};

/// One binding in a row: either a borrow into the source slice (the base
/// document) or an owned value (a join-unwound array element).
enum Bound<'a> {
    Borrowed(&'a Bson),
    Owned(Bson),
}

impl Bound<'_> {
    fn as_ref(&self) -> &Bson {
        match self {
            Bound::Borrowed(b) => b,
            Bound::Owned(b) => b,
        }
    }
}

type Row<'a> = Vec<(&'a str, Bound<'a>)>;

/// Execute `query` over `docs` with no parameters.
pub fn execute(query: &Query, docs: &[Bson]) -> Result<Vec<Bson>> {
    let params = Document::new();
    execute_with_params(query, docs, &params)
}

/// Execute `query` over `docs`, resolving `@name` parameters from `params`.
pub fn execute_with_params(query: &Query, docs: &[Bson], params: &Document) -> Result<Vec<Bson>> {
    let base_alias = match &query.from.source {
        FromSource::ImplicitContainer { alias } => alias.as_str(),
        // An array source only appears inside a subquery, which this standalone
        // in-memory executor doesn't run (the planner/executor path does).
        FromSource::Array { .. } => {
            return Err(SqlError::Eval {
                message: "FROM <alias> IN <array> is only valid inside a subquery".into(),
            });
        }
    };

    // Base rows: one per source document.
    let mut rows: Vec<Row> = docs
        .iter()
        .map(|d| vec![(base_alias, Bound::Borrowed(d))])
        .collect();

    // Joins: array-unwind cross product, evaluated left to right.
    for join in &query.from.joins {
        let mut next: Vec<Row> = Vec::new();
        for row in &rows {
            let binds = bindings(row);
            let env = Env::new(&binds, params);
            if let Value::Defined(Bson::Array(items)) = eval::eval(&join.array, &env)? {
                for item in items {
                    let mut new_row = dup_row(row);
                    new_row.push((join.alias.as_str(), Bound::Owned(item)));
                    next.push(new_row);
                }
            }
            // Non-array / undefined → no matches (INNER JOIN drops the row).
        }
        rows = next;
    }

    // Filter + project, carrying ORDER BY keys alongside each output value.
    // Resolve the projection to one value expression (cheap, once per query in
    // this in-memory convenience engine; the storage path lowers it directly).
    let projection = query.select.clone().into_value_expr(base_alias);
    let mut projected: Vec<(Vec<Value>, Bson)> = Vec::new();
    for row in &rows {
        let binds = bindings(row);
        let env = Env::new(&binds, params);

        if let Some(pred) = &query.filter {
            // Keep the row only when the predicate is *exactly* true; undefined
            // and false are both excluded.
            if !matches!(eval::eval(pred, &env)?, Value::Defined(Bson::Boolean(true))) {
                continue;
            }
        }

        let value = match eval::eval(&projection, &env)? {
            Value::Defined(b) => b,
            // `SELECT VALUE <undefined>` omits the row entirely.
            Value::Undefined => continue,
        };

        let keys = order_keys(&query.order_by, &env)?;
        projected.push((keys, value));
    }

    // ORDER BY.
    if !query.order_by.is_empty() {
        let directions: Vec<SortDirection> = query.order_by.iter().map(|o| o.direction).collect();
        projected.sort_by(|a, b| order_cmp(&a.0, &b.0, &directions));
    }

    // OFFSET / LIMIT.
    let start = query.offset.unwrap_or(0) as usize;
    let mut out: Vec<Bson> = projected.into_iter().skip(start).map(|(_, v)| v).collect();
    if let Some(limit) = query.limit {
        out.truncate(limit as usize);
    }
    Ok(out)
}

// ── Row helpers ─────────────────────────────────────────────────

fn bindings<'a>(row: &'a Row<'a>) -> Vec<(&'a str, &'a Bson)> {
    row.iter().map(|(n, b)| (*n, b.as_ref())).collect()
}

/// Duplicate a row for the join cross product. Borrowed bindings stay borrows;
/// owned bindings are cloned because each output row needs its own copy of the
/// materialized join element.
fn dup_row<'a>(row: &Row<'a>) -> Row<'a> {
    row.iter()
        .map(|(n, b)| {
            let bound = match b {
                Bound::Borrowed(r) => Bound::Borrowed(*r),
                Bound::Owned(v) => Bound::Owned(v.clone()),
            };
            (*n, bound)
        })
        .collect()
}

fn order_keys(items: &[OrderByItem], env: &Env) -> Result<Vec<Value>> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        keys.push(eval::eval(&item.expr, env)?);
    }
    Ok(keys)
}

// ── ORDER BY comparison ─────────────────────────────────────────

fn order_cmp(a: &[Value], b: &[Value], directions: &[SortDirection]) -> Ordering {
    for ((av, bv), dir) in a.iter().zip(b).zip(directions) {
        let ord = eval::order_values(av, bv);
        let ord = match dir {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::bson;

    fn run(sql: &str, docs: &[Bson]) -> Vec<Bson> {
        crate::query(sql, docs).unwrap()
    }

    fn people() -> Vec<Bson> {
        vec![
            bson!({ "name": "ada", "age": 36, "tags": ["math", "logic"] }),
            bson!({ "name": "alan", "age": 41, "tags": ["computing"] }),
            bson!({ "name": "grace", "age": 44, "tags": [] }),
        ]
    }

    #[test]
    fn select_value_scalar() {
        let out = run("SELECT VALUE c.name FROM c", &people());
        assert_eq!(out, vec![bson!("ada"), bson!("alan"), bson!("grace")]);
    }

    #[test]
    fn where_filter() {
        let out = run("SELECT VALUE c.name FROM c WHERE c.age >= 41", &people());
        assert_eq!(out, vec![bson!("alan"), bson!("grace")]);
    }

    #[test]
    fn select_value_object_projection() {
        let out = run(
            r#"SELECT VALUE { "n": c.name, "twice": c.age * 2 } FROM c WHERE c.name = "ada""#,
            &people(),
        );
        assert_eq!(out, vec![bson!({ "n": "ada", "twice": 72_i64 })]);
    }

    #[test]
    fn array_unwind_join_flattens() {
        let out = run("SELECT VALUE t FROM c JOIN t IN c.tags", &people());
        // grace has no tags → contributes no rows (inner join).
        assert_eq!(out, vec![bson!("math"), bson!("logic"), bson!("computing")]);
    }

    #[test]
    fn join_with_object_projection() {
        let out = run(
            r#"SELECT VALUE { "who": c.name, "tag": t } FROM c JOIN t IN c.tags WHERE c.name = "ada""#,
            &people(),
        );
        assert_eq!(
            out,
            vec![
                bson!({ "who": "ada", "tag": "math" }),
                bson!({ "who": "ada", "tag": "logic" }),
            ]
        );
    }

    #[test]
    fn order_by_desc_with_limit_offset() {
        let out = run(
            "SELECT VALUE c.name FROM c ORDER BY c.age DESC OFFSET 1 LIMIT 1",
            &people(),
        );
        // ages 44(grace),41(alan),36(ada) → skip grace, take alan
        assert_eq!(out, vec![bson!("alan")]);
    }

    #[test]
    fn select_value_undefined_is_omitted() {
        let out = run("SELECT VALUE c.missing FROM c", &people());
        assert!(out.is_empty());
    }

    #[test]
    fn function_in_projection() {
        let out = run(
            r#"SELECT VALUE UPPER(c.name) FROM c WHERE c.name = "ada""#,
            &people(),
        );
        assert_eq!(out, vec![bson!("ADA")]);
    }

    #[test]
    fn parameterized_predicate() {
        let q = crate::parse("SELECT VALUE c.name FROM c WHERE c.age > @minAge").unwrap();
        let mut params = Document::new();
        params.insert("minAge", 40_i64);
        let out = execute_with_params(&q, &people(), &params).unwrap();
        assert_eq!(out, vec![bson!("alan"), bson!("grace")]);
    }
}
