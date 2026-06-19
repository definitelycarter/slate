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
//! - [`SelectClause`] will gain a tabular `Projections` variant.
//! - [`FromSource`] will gain a `Collection { name, alias }` variant for the
//!   cross-collection-join extension.

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
    Literal(Literal),
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

#[derive(Debug, Clone, PartialEq)]
pub enum SelectClause {
    /// `SELECT VALUE <expr>` — yields exactly one value per surviving row.
    Value(ScalarExpr),
    // Future: `Projections(Vec<SelectItem>)` for `SELECT a, b AS c`.
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
