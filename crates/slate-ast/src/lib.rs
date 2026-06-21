//! `slate-ast` — the shared query AST.
//!
//! The single intermediate representation that every query surface targets:
//! `slate-sql` parses SQL text into it, `slate-query` translates a Mongo-style
//! find into it, `slate-planner` lowers it to a physical plan, and `slate-eval`
//! gives it meaning. It depends only on `bson` and the light `slate-mutation`
//! leaf (whose [`Mutation`] a write [`Statement`] carries) — never on a query
//! *surface* — so all of those share one expression language and one statement
//! type without any front-end depending on a sibling.
//!
//! The shapes here intentionally leave room to grow (see the `Future:` notes)
//! without reshaping existing variants:
//! - [`FromSource`] will gain a `Collection { name, alias }` variant for the
//!   cross-collection-join extension.

use std::collections::BTreeSet;

use bson::{RawBson, RawDocumentBuf};
use slate_mutation::Mutation;

/// A literal scalar value as written in the source.
///
/// Kept distinct from `bson::Bson` so the AST stays `PartialEq` and free of
/// BSON-only variants; the evaluator (`slate-eval`) lowers these to `Bson` at
/// eval time.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

/// A value-producing expression (the core of `SELECT VALUE`, `WHERE`, etc.).
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal written in source text (SQL) — kept BSON-free so it stays
    /// purely syntactic.
    Literal(Literal),
    /// An already-materialized BSON value. Unlike [`Literal`], this preserves
    /// the exact BSON type (e.g. `Int32` vs `Int64`) and can hold non-textual
    /// values (`DateTime`, `ObjectId`). The Mongo front-end emits these so a
    /// filter literal round-trips with the same type it had in the request —
    /// which matters for index bounds, whose key encoding is type-tagged.
    Value(bson::Bson),
    /// A bare name — resolves against the FROM/JOIN bindings (e.g. `c`, `t`).
    Identifier(String),
    /// `@name` query parameter.
    Parameter(String),
    /// `<base>.<field>` member access.
    Member {
        base: Box<Expression>,
        field: String,
    },
    /// `<base>[<index>]` — array index or object-key access.
    Index {
        base: Box<Expression>,
        index: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Binary {
        op: BinOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
    /// `NAME(arg, ...)` — scalar function call.
    Function {
        name: String,
        args: Vec<Expression>,
    },
    /// `{ "k": expr, ... }` object literal.
    Object(Vec<(String, Expression)>),
    /// `[ expr, ... ]` array literal.
    Array(Vec<Expression>),

    // ── Mongo-only constructs ───────────────────────────────────
    //
    // These have no SQL syntax — only the Mongo front-end constructs them, to
    // express semantics Cosmos SQL doesn't share (array-distributing paths,
    // multikey matching). Member access / SQL never traverse arrays, so these
    // are kept distinct rather than overloading `Member`/`Function`.
    /// Resolve a dotted `path` against `base`, **distributing over arrays**: an
    /// array applies the remaining path to each element and flattens one level.
    /// Mongo path semantics, used by `distinct` over array paths.
    PathGet {
        base: Box<Expression>,
        path: Vec<String>,
    },
    /// Explicit multikey-array equality: true when an array reachable at the
    /// `.[]` path `index_path` contains `value`. `index_path` is kept verbatim
    /// (e.g. `"tags.[]"`) so the planner can match it to a multikey index.
    MultikeyEq {
        base: Box<Expression>,
        index_path: String,
        value: Box<Expression>,
    },

    /// A subquery used in scalar position — `(SELECT …)`, `EXISTS (…)`, or
    /// `ARRAY (…)`. The inner [`Query`] ranges over an in-document array (its
    /// `FROM x IN <array>`) and may reference the outer row (correlated). The
    /// planner extracts these out of the surrounding expression into a
    /// correlated-apply node, so the evaluators never see this variant directly.
    Subquery {
        query: Box<Query>,
        kind: SubqueryKind,
    },
}

/// How a [`Expression::Subquery`]'s row stream is reduced to a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryKind {
    /// `(SELECT …)` — the single produced value, or undefined if none.
    Scalar,
    /// `EXISTS (…)` — whether the subquery produced any row.
    Exists,
    /// `ARRAY (…)` — the produced values collected into an array.
    Array,
}

impl Expression {
    /// Add the names of every `@parameter` referenced in this expression
    /// (recursively, without the leading `@`) to `out`.
    pub fn collect_parameters<'a>(&'a self, out: &mut BTreeSet<&'a str>) {
        match self {
            Expression::Parameter(name) => {
                out.insert(name.as_str());
            }
            Expression::Literal(_) | Expression::Value(_) | Expression::Identifier(_) => {}
            Expression::Member { base, .. } | Expression::PathGet { base, .. } => {
                base.collect_parameters(out)
            }
            Expression::Index { base, index } => {
                base.collect_parameters(out);
                index.collect_parameters(out);
            }
            Expression::Unary { expr, .. } => expr.collect_parameters(out),
            Expression::Binary { lhs, rhs, .. } => {
                lhs.collect_parameters(out);
                rhs.collect_parameters(out);
            }
            Expression::MultikeyEq { base, value, .. } => {
                base.collect_parameters(out);
                value.collect_parameters(out);
            }
            Expression::Function { args, .. } | Expression::Array(args) => {
                for e in args {
                    e.collect_parameters(out);
                }
            }
            Expression::Object(fields) => {
                for (_, e) in fields {
                    e.collect_parameters(out);
                }
            }
            // A subquery may reference outer `@params`; descend into it.
            Expression::Subquery { query, .. } => query.collect_parameters(out),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// A complete query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: SelectClause,
    /// `SELECT DISTINCT …` — deduplicate the projected result rows by value
    /// (an array value counts as one whole value, matching Cosmos).
    pub distinct: bool,
    /// `FROM …` — `None` for a FROM-less query (`SELECT VALUE 1`), which Cosmos
    /// evaluates exactly once over a single implicit row. `SELECT *` is invalid
    /// without a `FROM` (rejected by the front-end).
    pub from: Option<FromClause>,
    pub filter: Option<Expression>,
    /// `GROUP BY <expr>, …` — empty when absent. Rows are collapsed into one per
    /// distinct tuple of these expressions, and the `SELECT` may then reference
    /// only these expressions or aggregates.
    pub group_by: Vec<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

impl Query {
    /// The names of every `@parameter` referenced anywhere in the query — the
    /// projection, `JOIN … IN`/`FROM … IN` array expressions, `WHERE`,
    /// `GROUP BY`, `ORDER BY`, and any nested subqueries — with the leading `@`
    /// stripped, deduplicated and sorted. Used to validate that a caller supplied
    /// a value for each referenced parameter.
    pub fn parameter_names(&self) -> BTreeSet<&str> {
        let mut out = BTreeSet::new();
        self.collect_parameters(&mut out);
        out
    }

    /// Add every referenced `@parameter` name to `out` (recursing into nested
    /// subqueries). Shared by [`parameter_names`](Self::parameter_names) and the
    /// subquery arm of [`Expression::collect_parameters`].
    pub fn collect_parameters<'a>(&'a self, out: &mut BTreeSet<&'a str>) {
        match &self.select {
            SelectClause::Value(e) => e.collect_parameters(out),
            SelectClause::Star => {}
            SelectClause::Projections(items) => {
                for it in items {
                    it.expr.collect_parameters(out);
                }
            }
        }
        if let Some(from) = &self.from {
            if let FromSource::Array { array, .. } = &from.source {
                array.collect_parameters(out);
            }
            for join in &from.joins {
                join.array.collect_parameters(out);
            }
        }
        if let Some(filter) = &self.filter {
            filter.collect_parameters(out);
        }
        for key in &self.group_by {
            key.collect_parameters(out);
        }
        for item in &self.order_by {
            item.expr.collect_parameters(out);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectClause {
    /// `SELECT VALUE <expr>` — yields exactly one value per surviving row.
    Value(Expression),
    /// `SELECT *` — yields the whole bound row (the identity projection).
    Star,
    /// `SELECT <expr> [AS <key>], ...` — yields a document of the projected
    /// fields. Each key is resolved by the front-end (the last path segment of
    /// a member access, an explicit `AS`, or a positional `$N` for an unnamed
    /// computed column). Unlike a Mongo find projection, nothing is auto-added
    /// (no primary key) and member values are not trimmed — `SELECT c.address`
    /// yields `{ "address": <the whole sub-document> }`.
    Projections(Vec<SelectItem>),
}

/// One column of a tabular `SELECT` projection: a resolved output `key` and the
/// expression producing its value.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub key: String,
    pub expr: Expression,
}

impl SelectClause {
    /// Resolve the projection to the single value expression each surviving row
    /// produces: `VALUE e` → `e`; `*` → the row identity (`alias`); a tabular
    /// list → an object literal `{ key: expr, ... }`. `alias` is the `FROM`
    /// alias, used only by `*`.
    pub fn into_value_expr(self, alias: &str) -> Expression {
        match self {
            SelectClause::Value(expr) => expr,
            SelectClause::Star => Expression::Identifier(alias.to_string()),
            SelectClause::Projections(items) => {
                Expression::Object(items.into_iter().map(|it| (it.key, it.expr)).collect())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub source: FromSource,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromSource {
    /// `FROM <alias>` — the alias binds to each document of the container
    /// chosen out-of-band by the caller. There is no collection name in the
    /// SQL text (matching Cosmos).
    ImplicitContainer { alias: String },
    /// `FROM <alias> IN <array>` — the alias binds to each element of an array
    /// expression, with no container scan. Used by subqueries, whose source is
    /// an in-document array (correlated) or a literal (uncorrelated).
    Array { alias: String, array: Expression },
    /// `FROM <base>.<path> <alias>` — scope iteration to a sub-path of each
    /// container document: `alias` binds to `base.path` (the whole sub-value,
    /// object or array — no unwinding), one row per document, dropping documents
    /// where the path is undefined. `base` names the container root.
    Subroot {
        base: String,
        path: Vec<String>,
        alias: String,
    },
    // Future: `Collection { name: String, alias: String }` for cross-collection joins.
}

/// `JOIN <alias> IN <array_expr>` — intra-document array unwind.
///
/// This is the Cosmos-compatible join: `array` is evaluated against the
/// current row's bindings and each element produces a new row with `alias`
/// bound to it.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub alias: String,
    pub array: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: SortDirection,
}

/// A single operation to plan — the shared input that every query surface
/// targets and `slate-planner` lowers to a physical plan. `slate-sql` parses
/// SQL text into one; `slate-query` translates a Mongo request into one. Having
/// reads *and* writes share this one type is what lets a single fallible
/// `plan()` serve both surfaces (and keeps them from drifting).
///
/// A statement says *what* to do, free of catalog state: the target collection,
/// its indexes, and its triggers/validators are supplied separately at plan time
/// (so the same statement plans identically wherever it runs). Filter-bearing
/// writes carry the [`Query`] that selects their target documents; the planner
/// lowers that to the read source and wraps it with the write op.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A read: `SELECT …` (SQL) or a Mongo `find`.
    Query(Query),
    /// Insert each document (a primary key is generated when absent).
    Insert { docs: Vec<RawBson> },
    /// Insert-or-(replace|merge) each document by primary key.
    Upsert {
        docs: Vec<RawBson>,
        mode: UpsertMode,
    },
    /// Update the documents `query` selects by applying `mutation`.
    Update { query: Query, mutation: Mutation },
    /// Replace the documents `query` selects with `replacement` (pk preserved).
    Replace {
        query: Query,
        replacement: RawDocumentBuf,
    },
    /// Delete the documents `query` selects.
    Delete { query: Query },
    /// Distinct values of `alias`.`field` among documents matching `predicate`,
    /// flattening array values one level (Mongo `distinct` semantics, which —
    /// unlike SQL `SELECT DISTINCT` — treats each array element as a value).
    Distinct {
        alias: String,
        field: String,
        predicate: Option<Expression>,
        sort: Option<SortDirection>,
        skip: Option<u64>,
        take: Option<u64>,
    },
}

/// How an upsert writes over an existing document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertMode {
    /// Overwrite the existing document entirely (preserving its primary key).
    Replace,
    /// Field-merge the new document into the existing one.
    Merge,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(n: &str) -> Expression {
        Expression::Parameter(n.into())
    }

    #[test]
    fn collect_parameters_walks_nested_expressions() {
        // (@a + c.x) OR F(@b, [@c, @a]) — three distinct names, `a` repeated.
        let expr = Expression::Binary {
            op: BinOp::Or,
            lhs: Box::new(Expression::Binary {
                op: BinOp::Add,
                lhs: Box::new(param("a")),
                rhs: Box::new(Expression::Member {
                    base: Box::new(Expression::Identifier("c".into())),
                    field: "x".into(),
                }),
            }),
            rhs: Box::new(Expression::Function {
                name: "F".into(),
                args: vec![param("b"), Expression::Array(vec![param("c"), param("a")])],
            }),
        };
        let mut out = BTreeSet::new();
        expr.collect_parameters(&mut out);
        assert_eq!(out.into_iter().collect::<Vec<_>>(), vec!["a", "b", "c"]);
    }

    #[test]
    fn parameter_names_covers_all_clauses() {
        let q = Query {
            select: SelectClause::Value(param("sel")),
            distinct: false,
            from: Some(FromClause {
                source: FromSource::ImplicitContainer { alias: "c".into() },
                joins: vec![Join {
                    alias: "t".into(),
                    array: param("arr"),
                }],
            }),
            filter: Some(param("flt")),
            group_by: vec![param("grp")],
            order_by: vec![OrderByItem {
                expr: param("ord"),
                direction: SortDirection::Asc,
            }],
            offset: None,
            limit: None,
        };
        assert_eq!(
            q.parameter_names().into_iter().collect::<Vec<_>>(),
            vec!["arr", "flt", "grp", "ord", "sel"]
        );
    }
}
