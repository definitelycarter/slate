//! `slate-ast` — the shared query AST.
//!
//! The single intermediate representation that every query surface targets:
//! `slate-sql` parses SQL text into it, `slate-query` translates a Mongo-style
//! find into it, `slate-planner` lowers it to a physical plan, and `slate-eval`
//! gives it meaning. Keeping it a dependency-free leaf is what lets all of those
//! share one expression language without anyone depending on a sibling surface.
//!
//! The shapes here intentionally leave room to grow (see the `Future:` notes)
//! without reshaping existing variants:
//! - [`FromSource`] will gain a `Collection { name, alias }` variant for the
//!   cross-collection-join extension.

use std::collections::BTreeSet;

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
pub enum ScalarExpr {
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
        base: Box<ScalarExpr>,
        field: String,
    },
    /// `<base>[<index>]` — array index or object-key access.
    Index {
        base: Box<ScalarExpr>,
        index: Box<ScalarExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<ScalarExpr>,
    },
    Binary {
        op: BinOp,
        lhs: Box<ScalarExpr>,
        rhs: Box<ScalarExpr>,
    },
    /// `NAME(arg, ...)` — scalar function call.
    Function {
        name: String,
        args: Vec<ScalarExpr>,
    },
    /// `{ "k": expr, ... }` object literal.
    Object(Vec<(String, ScalarExpr)>),
    /// `[ expr, ... ]` array literal.
    Array(Vec<ScalarExpr>),

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
        base: Box<ScalarExpr>,
        path: Vec<String>,
    },
    /// Explicit multikey-array equality: true when an array reachable at the
    /// `.[]` path `index_path` contains `value`. `index_path` is kept verbatim
    /// (e.g. `"tags.[]"`) so the planner can match it to a multikey index.
    MultikeyEq {
        base: Box<ScalarExpr>,
        index_path: String,
        value: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    /// Add the names of every `@parameter` referenced in this expression
    /// (recursively, without the leading `@`) to `out`.
    pub fn collect_parameters<'a>(&'a self, out: &mut BTreeSet<&'a str>) {
        match self {
            ScalarExpr::Parameter(name) => {
                out.insert(name.as_str());
            }
            ScalarExpr::Literal(_) | ScalarExpr::Value(_) | ScalarExpr::Identifier(_) => {}
            ScalarExpr::Member { base, .. } | ScalarExpr::PathGet { base, .. } => {
                base.collect_parameters(out)
            }
            ScalarExpr::Index { base, index } => {
                base.collect_parameters(out);
                index.collect_parameters(out);
            }
            ScalarExpr::Unary { expr, .. } => expr.collect_parameters(out),
            ScalarExpr::Binary { lhs, rhs, .. } => {
                lhs.collect_parameters(out);
                rhs.collect_parameters(out);
            }
            ScalarExpr::MultikeyEq { base, value, .. } => {
                base.collect_parameters(out);
                value.collect_parameters(out);
            }
            ScalarExpr::Function { args, .. } | ScalarExpr::Array(args) => {
                for e in args {
                    e.collect_parameters(out);
                }
            }
            ScalarExpr::Object(fields) => {
                for (_, e) in fields {
                    e.collect_parameters(out);
                }
            }
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
    pub from: FromClause,
    pub filter: Option<ScalarExpr>,
    pub order_by: Vec<OrderByItem>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

impl Query {
    /// The names of every `@parameter` referenced anywhere in the query — the
    /// projection, `JOIN … IN` array expressions, `WHERE`, and `ORDER BY` — with
    /// the leading `@` stripped, deduplicated and sorted. Used to validate that a
    /// caller supplied a value for each referenced parameter.
    pub fn parameter_names(&self) -> BTreeSet<&str> {
        let mut out = BTreeSet::new();
        match &self.select {
            SelectClause::Value(e) => e.collect_parameters(&mut out),
            SelectClause::Star => {}
            SelectClause::Projections(items) => {
                for it in items {
                    it.expr.collect_parameters(&mut out);
                }
            }
        }
        for join in &self.from.joins {
            join.array.collect_parameters(&mut out);
        }
        if let Some(filter) = &self.filter {
            filter.collect_parameters(&mut out);
        }
        for item in &self.order_by {
            item.expr.collect_parameters(&mut out);
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectClause {
    /// `SELECT VALUE <expr>` — yields exactly one value per surviving row.
    Value(ScalarExpr),
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
    pub expr: ScalarExpr,
}

impl SelectClause {
    /// Resolve the projection to the single value expression each surviving row
    /// produces: `VALUE e` → `e`; `*` → the row identity (`alias`); a tabular
    /// list → an object literal `{ key: expr, ... }`. `alias` is the `FROM`
    /// alias, used only by `*`.
    pub fn into_value_expr(self, alias: &str) -> ScalarExpr {
        match self {
            SelectClause::Value(expr) => expr,
            SelectClause::Star => ScalarExpr::Identifier(alias.to_string()),
            SelectClause::Projections(items) => {
                ScalarExpr::Object(items.into_iter().map(|it| (it.key, it.expr)).collect())
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
    pub array: ScalarExpr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: ScalarExpr,
    pub direction: SortDirection,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(n: &str) -> ScalarExpr {
        ScalarExpr::Parameter(n.into())
    }

    #[test]
    fn collect_parameters_walks_nested_expressions() {
        // (@a + c.x) OR F(@b, [@c, @a]) — three distinct names, `a` repeated.
        let expr = ScalarExpr::Binary {
            op: BinOp::Or,
            lhs: Box::new(ScalarExpr::Binary {
                op: BinOp::Add,
                lhs: Box::new(param("a")),
                rhs: Box::new(ScalarExpr::Member {
                    base: Box::new(ScalarExpr::Identifier("c".into())),
                    field: "x".into(),
                }),
            }),
            rhs: Box::new(ScalarExpr::Function {
                name: "F".into(),
                args: vec![param("b"), ScalarExpr::Array(vec![param("c"), param("a")])],
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
            from: FromClause {
                source: FromSource::ImplicitContainer { alias: "c".into() },
                joins: vec![Join {
                    alias: "t".into(),
                    array: param("arr"),
                }],
            },
            filter: Some(param("flt")),
            order_by: vec![OrderByItem {
                expr: param("ord"),
                direction: SortDirection::Asc,
            }],
            offset: None,
            limit: None,
        };
        assert_eq!(
            q.parameter_names().into_iter().collect::<Vec<_>>(),
            vec!["arr", "flt", "ord", "sel"]
        );
    }
}
