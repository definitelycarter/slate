//! Recursive-descent parser with precedence climbing for scalar expressions.
//!
//! Precedence, lowest to highest:
//! `OR` < `AND` < `NOT` < comparison < `+`/`-` < `*`/`/`/`%` < unary `-` <
//! postfix (`.field`, `[index]`) < primary.
//!
//! Comparison is non-associative (a single `a <op> b`), matching SQL.

use crate::error::{Result, SqlError};
use crate::token::Token;
use slate_ast::*;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

/// The output key for an unaliased projection column (Cosmos rule): the last
/// path segment of a member access (or a bare identifier's name), else a
/// positional `$N` for an unnamed computed column.
fn infer_key(expr: &Expression, positional: &mut u32) -> String {
    match expr {
        Expression::Member { field, .. } => field.clone(),
        Expression::Identifier(name) => name.clone(),
        _ => {
            *positional += 1;
            format!("${positional}")
        }
    }
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    // ── Cursor helpers ──────────────────────────────────────────

    fn peek(&self) -> &Token {
        // `tokenize` always appends Eof, so indexing is in-bounds.
        &self.tokens[self.pos]
    }

    fn advance(&mut self) {
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
    }

    /// Take the current token by value (leaving Eof behind) and advance.
    /// Avoids cloning the token's owned `String` payloads.
    fn take(&mut self) -> Token {
        let tok = std::mem::replace(&mut self.tokens[self.pos], Token::Eof);
        self.advance();
        tok
    }

    fn matches(&mut self, t: &Token) -> bool {
        if self.peek() == t {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: &Token) -> Result<()> {
        if self.peek() == t {
            self.advance();
            Ok(())
        } else {
            Err(SqlError::Parse {
                message: format!("expected {:?}, found {:?}", t, self.peek()),
            })
        }
    }

    fn parse_ident(&mut self) -> Result<String> {
        match self.take() {
            Token::Ident(s) => Ok(s),
            other => Err(SqlError::Parse {
                message: format!("expected identifier, found {other:?}"),
            }),
        }
    }

    fn parse_u64(&mut self) -> Result<u64> {
        match self.take() {
            Token::Int(i) if i >= 0 => Ok(i as u64),
            other => Err(SqlError::Parse {
                message: format!("expected a non-negative integer, found {other:?}"),
            }),
        }
    }

    // ── Top-level query ─────────────────────────────────────────

    pub fn parse_query(&mut self) -> Result<Query> {
        let query = self.parse_query_body()?;
        self.expect(&Token::Eof)?;
        Ok(query)
    }

    /// Parse a query body without the trailing end-of-input check — shared by the
    /// top-level [`parse_query`](Self::parse_query) and by subqueries, which end
    /// at `)` rather than at end-of-input.
    fn parse_query_body(&mut self) -> Result<Query> {
        self.expect(&Token::Select)?;
        let distinct = self.matches(&Token::Distinct);
        // `TOP <n>` (Cosmos) caps the result count; it follows DISTINCT and is
        // mutually exclusive with OFFSET/LIMIT (reconciled below).
        let top = if self.matches(&Token::Top) {
            Some(self.parse_u64()?)
        } else {
            None
        };
        let select = self.parse_select()?;

        // The `FROM` clause is optional (Cosmos): a FROM-less query evaluates the
        // `SELECT` exactly once over a single implicit row, e.g. `SELECT VALUE 1`.
        // `SELECT *` is the one form that requires a source to expand.
        let from = if self.matches(&Token::From) {
            Some(self.parse_from()?)
        } else {
            if matches!(select, SelectClause::Star) {
                return Err(SqlError::Parse {
                    message: "SELECT * requires a FROM clause".into(),
                });
            }
            None
        };

        let filter = if self.matches(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.matches(&Token::Group) {
            self.expect(&Token::By)?;
            self.parse_group_by()?
        } else {
            Vec::new()
        };

        // `HAVING <expr>` — a post-aggregation filter. It follows `GROUP BY` and
        // precedes `ORDER BY` (Cosmos/SQL order). The full scalar grammar is
        // accepted; the grounding check (group keys / aggregates only) runs at
        // plan time, alongside the same check on `SELECT`.
        let having = if self.matches(&Token::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.matches(&Token::Order) {
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        let offset = if self.matches(&Token::Offset) {
            Some(self.parse_u64()?)
        } else {
            None
        };

        let limit = if self.matches(&Token::Limit) {
            Some(self.parse_u64()?)
        } else {
            None
        };

        // `TOP` is just a result cap, so it folds into `limit` — but Cosmos
        // rejects mixing it with OFFSET/LIMIT, so guard that first.
        let limit = match top {
            Some(_) if offset.is_some() || limit.is_some() => {
                return Err(SqlError::Parse {
                    message: "TOP cannot be combined with OFFSET or LIMIT".into(),
                });
            }
            Some(t) => Some(t),
            None => limit,
        };

        Ok(Query {
            select,
            distinct,
            from,
            filter,
            group_by,
            having,
            order_by,
            offset,
            limit,
        })
    }

    /// Parse the comma-separated expression list after `GROUP BY`.
    fn parse_group_by(&mut self) -> Result<Vec<Expression>> {
        let mut keys = Vec::new();
        loop {
            keys.push(self.parse_expr()?);
            if !self.matches(&Token::Comma) {
                break;
            }
        }
        Ok(keys)
    }

    /// Parse the `SELECT` clause: `VALUE <expr>` | `*` | a projection list
    /// `<expr> [AS <key>], ...`.
    fn parse_select(&mut self) -> Result<SelectClause> {
        if self.matches(&Token::Value) {
            let expr = self.parse_expr()?;
            // `SELECT VALUE <expr>` yields a bare value stream with no key, so an
            // alias is meaningless — Cosmos rejects it, and so do we (with a
            // clearer message than the downstream end-of-input failure).
            if self.peek() == &Token::As {
                return Err(SqlError::Parse {
                    message: "SELECT VALUE does not take an AS alias".into(),
                });
            }
            return Ok(SelectClause::Value(expr));
        }
        if self.matches(&Token::Star) {
            return Ok(SelectClause::Star);
        }

        let mut items: Vec<SelectItem> = Vec::new();
        let mut positional = 0u32; // counter for unnamed computed columns ($N)
        loop {
            let expr = self.parse_expr()?;
            let key = if self.matches(&Token::As) {
                self.parse_ident()?
            } else {
                infer_key(&expr, &mut positional)
            };
            if items.iter().any(|it| it.key == key) {
                return Err(SqlError::Parse {
                    message: format!("duplicate projected key '{key}'; use AS to disambiguate"),
                });
            }
            items.push(SelectItem { key, expr });
            if !self.matches(&Token::Comma) {
                break;
            }
        }
        Ok(SelectClause::Projections(items))
    }

    fn parse_from(&mut self) -> Result<FromClause> {
        let first = self.parse_ident()?;
        // Forms:
        //   FROM x IN <array>          — array source (a subquery's source)
        //   FROM <base>.<path> <a>     — subroot: iterate a sub-path of each doc
        //   FROM <container> AS <a>    — container with an explicit alias
        //   FROM <container> <a>       — container with an alias (no AS)
        //   FROM <alias>               — bare alias bound to the container
        // The container is chosen out-of-band (matching Cosmos, where the FROM
        // container name need not match the queried container), so when a name
        // and an alias are both present we keep only the alias.
        let source = if self.peek() == &Token::Dot {
            // Subroot path source: `FROM base.p1.p2 [AS] alias`. The alias is
            // optional; without one Cosmos still iterates the sub-path, so we
            // default the binding to the last path segment.
            let mut path = Vec::new();
            while self.matches(&Token::Dot) {
                path.push(self.parse_ident()?);
            }
            let alias = if self.matches(&Token::As) || matches!(self.peek(), Token::Ident(_)) {
                self.parse_ident()?
            } else {
                // `path` is non-empty (we consumed at least one `.segment`).
                path.last().cloned().unwrap_or_else(|| first.clone())
            };
            FromSource::Subroot {
                base: first,
                path,
                alias,
            }
        } else if self.matches(&Token::In) {
            FromSource::Array {
                alias: first,
                array: self.parse_expr()?,
            }
        } else if self.matches(&Token::As) || matches!(self.peek(), Token::Ident(_)) {
            FromSource::ImplicitContainer {
                alias: self.parse_ident()?,
            }
        } else {
            FromSource::ImplicitContainer { alias: first }
        };

        let mut joins = Vec::new();
        while self.matches(&Token::Join) {
            let alias = self.parse_ident()?;
            self.expect(&Token::In)?;
            let array = self.parse_expr()?;
            joins.push(Join { alias, array });
        }

        Ok(FromClause { source, joins })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByItem>> {
        self.expect(&Token::By)?;
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let direction = if self.matches(&Token::Desc) {
                SortDirection::Desc
            } else {
                self.matches(&Token::Asc); // optional, default ASC
                SortDirection::Asc
            };
            items.push(OrderByItem { expr, direction });
            if !self.matches(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    // ── Expressions (precedence climbing) ───────────────────────

    fn parse_expr(&mut self) -> Result<Expression> {
        self.parse_coalesce()
    }

    /// `a ?? b` — the coalesce operator, lowest precedence (below `OR`) and
    /// right-associative (`a ?? b ?? c` = `a ?? (b ?? c)`). Desugars to
    /// `IIF(IS_DEFINED(a), a, b)`, so it coalesces only on *undefined* (a defined
    /// `null` is returned as-is), matching Cosmos.
    fn parse_coalesce(&mut self) -> Result<Expression> {
        let lhs = self.parse_or()?;
        if self.matches(&Token::Coalesce) {
            let rhs = self.parse_coalesce()?;
            Ok(coalesce(lhs, rhs))
        } else {
            Ok(lhs)
        }
    }

    fn parse_or(&mut self) -> Result<Expression> {
        let mut lhs = self.parse_and()?;
        while self.matches(&Token::Or) {
            let rhs = self.parse_and()?;
            lhs = binary(BinOp::Or, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Expression> {
        let mut lhs = self.parse_not()?;
        while self.matches(&Token::And) {
            let rhs = self.parse_not()?;
            lhs = binary(BinOp::And, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Expression> {
        if self.matches(&Token::Not) {
            let expr = self.parse_not()?;
            Ok(Expression::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expression> {
        let lhs = self.parse_additive()?;

        // `IN (…)` and `BETWEEN … AND …`, each optionally negated with `NOT`.
        // An infix `NOT` at this position can only introduce one of these two,
        // so it is safe to consume speculatively.
        let negated = if self.peek() == &Token::Not {
            self.advance();
            true
        } else {
            false
        };
        match self.peek() {
            Token::In => {
                self.advance();
                let expr = self.parse_in_list(lhs)?;
                return Ok(maybe_not(negated, expr));
            }
            Token::Between => {
                self.advance();
                let expr = self.parse_between(lhs)?;
                return Ok(maybe_not(negated, expr));
            }
            Token::Like => {
                self.advance();
                let expr = self.parse_like(lhs)?;
                return Ok(maybe_not(negated, expr));
            }
            other if negated => {
                return Err(SqlError::Parse {
                    message: format!("expected IN, BETWEEN, or LIKE after NOT, found {other:?}"),
                });
            }
            _ => {}
        }

        let op = match self.peek() {
            Token::Eq => BinOp::Eq,
            Token::Neq => BinOp::Neq,
            Token::Lt => BinOp::Lt,
            Token::Lte => BinOp::Lte,
            Token::Gt => BinOp::Gt,
            Token::Gte => BinOp::Gte,
            _ => return Ok(lhs),
        };
        self.advance();
        let rhs = self.parse_additive()?;
        Ok(binary(op, lhs, rhs))
    }

    /// Desugar `lhs IN (a, b, …)` into `lhs = a OR lhs = b OR …`. Reusing the
    /// equality path means the planner indexes it as an `IndexMerge(Or)` for
    /// free. The LHS is cloned into each disjunct because the OR-of-equalities
    /// encoding inherently repeats it; the operands are small AST nodes.
    fn parse_in_list(&mut self, lhs: Expression) -> Result<Expression> {
        self.expect(&Token::LParen)?;
        if self.peek() == &Token::RParen {
            return Err(SqlError::Parse {
                message: "IN requires at least one value".into(),
            });
        }
        let mut disjunction: Option<Expression> = None;
        loop {
            let item = self.parse_expr()?;
            let eq = binary(BinOp::Eq, lhs.clone(), item);
            disjunction = Some(match disjunction {
                Some(prev) => binary(BinOp::Or, prev, eq),
                None => eq,
            });
            if !self.matches(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        disjunction.ok_or_else(|| SqlError::Parse {
            message: "IN requires at least one value".into(),
        })
    }

    /// Desugar `lhs BETWEEN lo AND hi` into `lhs >= lo AND lhs <= hi` —
    /// inclusive on both ends, as in Cosmos. The bounds parse at additive
    /// precedence (the same level as the operands of `=`/`<`), so the middle
    /// `AND` is the BETWEEN separator rather than boolean conjunction.
    fn parse_between(&mut self, lhs: Expression) -> Result<Expression> {
        let lower = self.parse_additive()?;
        self.expect(&Token::And)?;
        let upper = self.parse_additive()?;
        let ge = binary(BinOp::Gte, lhs.clone(), lower);
        let le = binary(BinOp::Lte, lhs, upper);
        Ok(binary(BinOp::And, ge, le))
    }

    /// Desugar `lhs LIKE '<pattern>' [ESCAPE '<c>']` into
    /// `REGEXMATCH(lhs, '<anchored regex>')`. The pattern is translated rather
    /// than passed through: only the SQL wildcards (`%`, `_`, `[…]`, `[^…]`)
    /// become regex constructs, and every other character — including regex
    /// metacharacters — is escaped, so a literal `.` or `(` in the pattern stays
    /// literal. The pattern (and `ESCAPE`) must be string literals.
    fn parse_like(&mut self, lhs: Expression) -> Result<Expression> {
        let pattern = match self.take() {
            Token::Str(s) => s,
            other => {
                return Err(SqlError::Parse {
                    message: format!("LIKE pattern must be a string literal, found {other:?}"),
                });
            }
        };
        let escape = if self.matches(&Token::Escape) {
            match self.take() {
                Token::Str(s) => {
                    let mut chars = s.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => Some(c),
                        _ => {
                            return Err(SqlError::Parse {
                                message: "ESCAPE expects a single-character string".into(),
                            });
                        }
                    }
                }
                other => {
                    return Err(SqlError::Parse {
                        message: format!("ESCAPE expects a string literal, found {other:?}"),
                    });
                }
            }
        } else {
            None
        };
        let regex = like_to_regex(&pattern, escape);
        Ok(Expression::Function {
            name: "REGEXMATCH".into(),
            args: vec![lhs, Expression::Literal(Literal::Str(regex))],
        })
    }

    fn parse_additive(&mut self) -> Result<Expression> {
        let mut lhs = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let rhs = self.parse_multiplicative()?;
            lhs = binary(op, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_multiplicative(&mut self) -> Result<Expression> {
        let mut lhs = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let rhs = self.parse_unary()?;
            lhs = binary(op, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<Expression> {
        if self.peek() == &Token::Minus {
            self.advance();
            let expr = self.parse_unary()?;
            Ok(Expression::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> Result<Expression> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek() {
                Token::Dot => {
                    self.advance();
                    let field = self.parse_ident()?;
                    expr = Expression::Member {
                        base: Box::new(expr),
                        field,
                    };
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    self.expect(&Token::RBracket)?;
                    expr = Expression::Index {
                        base: Box::new(expr),
                        index: Box::new(index),
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expression> {
        match self.take() {
            Token::Int(i) => Ok(Expression::Literal(Literal::Int(i))),
            Token::Float(f) => Ok(Expression::Literal(Literal::Float(f))),
            Token::Str(s) => Ok(Expression::Literal(Literal::Str(s))),
            Token::True => Ok(Expression::Literal(Literal::Bool(true))),
            Token::False => Ok(Expression::Literal(Literal::Bool(false))),
            Token::Null => Ok(Expression::Literal(Literal::Null)),
            Token::Param(p) => Ok(Expression::Parameter(p)),
            Token::LParen => {
                // `(SELECT …)` is a scalar subquery; otherwise a grouped expr.
                if self.peek() == &Token::Select {
                    let query = self.parse_query_body()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expression::Subquery {
                        query: Box::new(query),
                        kind: SubqueryKind::Scalar,
                    })
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(expr)
                }
            }
            Token::LBrace => self.parse_object(),
            Token::LBracket => self.parse_array(),
            Token::Ident(name) => {
                // `udf.NAME(args)` — a user-defined function call. The `udf`
                // namespace (Cosmos syntax) is reserved: a leading `udf` followed
                // by `.` is always a UDF call, never member access. The function
                // name keeps its case (UDF names are case-sensitive); only the
                // `udf` keyword is matched case-insensitively, like EXISTS/ARRAY.
                if name.eq_ignore_ascii_case("udf") && self.peek() == &Token::Dot {
                    self.advance(); // consume `.`
                    let fn_name = self.parse_ident()?;
                    self.expect(&Token::LParen)?;
                    let args = self.parse_call_args()?;
                    return Ok(Expression::Udf {
                        name: fn_name,
                        args,
                    });
                }
                // `EXISTS (SELECT …)` / `ARRAY (SELECT …)` are subquery forms;
                // otherwise these are ordinary identifiers/function calls.
                let upper = name.to_ascii_uppercase();
                if (upper == "EXISTS" || upper == "ARRAY") && self.peek() == &Token::LParen {
                    self.advance(); // consume `(`
                    if self.peek() == &Token::Select {
                        let query = self.parse_query_body()?;
                        self.expect(&Token::RParen)?;
                        let kind = if upper == "EXISTS" {
                            SubqueryKind::Exists
                        } else {
                            SubqueryKind::Array
                        };
                        return Ok(Expression::Subquery {
                            query: Box::new(query),
                            kind,
                        });
                    }
                    // Not a subquery — a normal call (`(` already consumed).
                    let args = self.parse_call_args()?;
                    return Ok(Expression::Function { name, args });
                }
                if self.peek() == &Token::LParen {
                    self.advance();
                    let args = self.parse_call_args()?;
                    Ok(Expression::Function { name, args })
                } else {
                    // `NaN`/`Infinity` are numeric literals in Cosmos, not
                    // identifiers (`undefined` stays an identifier — it resolves
                    // to the undefined value). Case-sensitive, matching Cosmos.
                    Ok(match name.as_str() {
                        "NaN" => Expression::Literal(Literal::Float(f64::NAN)),
                        "Infinity" => Expression::Literal(Literal::Float(f64::INFINITY)),
                        _ => Expression::Identifier(name),
                    })
                }
            }
            other => Err(SqlError::Parse {
                message: format!("unexpected token in expression: {other:?}"),
            }),
        }
    }

    /// Parse the argument list after a consumed `(`.
    fn parse_call_args(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();
        if self.peek() != &Token::RParen {
            loop {
                args.push(self.parse_expr()?);
                if !self.matches(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect(&Token::RParen)?;
        Ok(args)
    }

    /// Parse the body after a consumed `{`.
    fn parse_object(&mut self) -> Result<Expression> {
        let mut fields = Vec::new();
        if self.peek() != &Token::RBrace {
            loop {
                let key = match self.take() {
                    Token::Str(s) => s,
                    Token::Ident(s) => s,
                    other => {
                        return Err(SqlError::Parse {
                            message: format!("expected object key, found {other:?}"),
                        });
                    }
                };
                self.expect(&Token::Colon)?;
                let value = self.parse_expr()?;
                fields.push((key, value));
                if !self.matches(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(Expression::Object(fields))
    }

    /// Parse the body after a consumed `[`.
    fn parse_array(&mut self) -> Result<Expression> {
        let mut items = Vec::new();
        if self.peek() != &Token::RBracket {
            loop {
                items.push(self.parse_expr()?);
                if !self.matches(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(Expression::Array(items))
    }
}

fn binary(op: BinOp, lhs: Expression, rhs: Expression) -> Expression {
    Expression::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

/// Desugar `a ?? b` to `IIF(IS_DEFINED(a), a, b)`. `a` is duplicated because it
/// is both the test and the result and the AST has no node sharing; this is a
/// one-time parse-time clone of the left subexpression.
fn coalesce(lhs: Expression, rhs: Expression) -> Expression {
    let is_defined = Expression::Function {
        name: "IS_DEFINED".into(),
        args: vec![lhs.clone()],
    };
    Expression::Function {
        name: "IIF".into(),
        args: vec![is_defined, lhs, rhs],
    }
}

/// Wrap `expr` in a logical `NOT` when `negated`, else return it unchanged.
fn maybe_not(negated: bool, expr: Expression) -> Expression {
    if negated {
        Expression::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        }
    } else {
        expr
    }
}

/// Translate a SQL `LIKE` pattern into an anchored regular expression.
///
/// Only the SQL wildcards are given meaning — `%` → `.*`, `_` → `.`, and a
/// `[…]`/`[^…]` set maps onto a regex character class. Every other character is
/// treated as a literal: regex metacharacters are backslash-escaped so a pattern
/// like `"a.b("` matches only the literal text `a.b(`. An `escape` character, if
/// supplied, makes the character that follows it literal (so `%` can be matched
/// as itself). The result is wrapped in `^…$` because `LIKE` matches the whole
/// string.
fn like_to_regex(pattern: &str, escape: Option<char>) -> String {
    let mut out = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            // The next character is a literal, whatever it is.
            match chars.next() {
                Some(next) => push_regex_literal(&mut out, next),
                None => push_regex_literal(&mut out, c),
            }
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                // Collect the bracket body up to the closing `]`. A `[…]` set
                // maps directly onto a regex class (`[a-f]`, `[^abc]`, `[%]`),
                // so wildcard characters inside it are already literal.
                let mut inner = String::new();
                let mut closed = false;
                for nc in chars.by_ref() {
                    if nc == ']' {
                        closed = true;
                        break;
                    }
                    inner.push(nc);
                }
                if closed {
                    out.push('[');
                    for ch in inner.chars() {
                        if ch == '\\' {
                            out.push_str("\\\\");
                        } else {
                            out.push(ch);
                        }
                    }
                    out.push(']');
                } else {
                    // Unterminated `[` — treat it and the rest as literals.
                    push_regex_literal(&mut out, '[');
                    for ch in inner.chars() {
                        push_regex_literal(&mut out, ch);
                    }
                }
            }
            _ => push_regex_literal(&mut out, c),
        }
    }
    out.push('$');
    out
}

/// Append `c` to a regex, backslash-escaping it when it is a metacharacter so it
/// matches only itself.
fn push_regex_literal(out: &mut String, c: char) {
    if r"\.^$*+?()[]{}|".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokenize;

    fn parse(src: &str) -> Query {
        let mut p = Parser::new(tokenize(src).unwrap());
        p.parse_query().unwrap()
    }

    fn parse_err(src: &str) -> SqlError {
        let mut p = Parser::new(tokenize(src).unwrap());
        p.parse_query().unwrap_err()
    }

    #[test]
    fn minimal_select_value() {
        let q = parse("SELECT VALUE c FROM c");
        assert!(matches!(q.select, SelectClause::Value(Expression::Identifier(ref a)) if a == "c"));
        let from = q.from.as_ref().unwrap();
        assert!(matches!(
            from.source,
            FromSource::ImplicitContainer { ref alias } if alias == "c"
        ));
        assert!(from.joins.is_empty());
        assert!(q.filter.is_none());
    }

    #[test]
    fn from_container_with_alias() {
        // `FROM <container> <alias>` and `FROM <container> AS <alias>` keep the
        // alias (the container name is external and only a label).
        for sql in [
            "SELECT VALUE e.name FROM employees e",
            "SELECT VALUE e.name FROM employees AS e",
        ] {
            let q = parse(sql);
            assert!(
                matches!(q.from.as_ref().unwrap().source,
                    FromSource::ImplicitContainer { ref alias } if alias == "e"),
                "{sql}"
            );
        }
        // Bare alias still works.
        assert!(matches!(
            parse("SELECT VALUE c.x FROM c").from.as_ref().unwrap().source,
            FromSource::ImplicitContainer { ref alias } if alias == "c"
        ));
    }

    #[test]
    fn from_is_optional() {
        // `SELECT VALUE <expr>` with no FROM (Cosmos evaluates it once).
        let q = parse("SELECT VALUE 1 + 1");
        assert!(q.from.is_none());
        assert!(matches!(q.select, SelectClause::Value(_)));

        // A projection list with no FROM is also valid (wraps into an object).
        let q = parse("SELECT 1 AS a, 2 AS b");
        assert!(q.from.is_none());
        assert!(matches!(q.select, SelectClause::Projections(ref items) if items.len() == 2));
    }

    #[test]
    fn select_star_requires_from() {
        // `SELECT *` is the one form that needs a source to expand.
        assert!(matches!(parse_err("SELECT *"), SqlError::Parse { .. }));
        // With a FROM it parses fine.
        assert!(parse("SELECT * FROM c").from.is_some());
    }

    #[test]
    fn member_and_index_paths() {
        let q = parse("SELECT VALUE c.tags[0].name FROM c");
        // ((c.tags)[0]).name
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        match expr {
            Expression::Member { base, field } => {
                assert_eq!(field, "name");
                assert!(matches!(*base, Expression::Index { .. }));
            }
            other => panic!("expected Member, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_precedence() {
        // 1 + 2 * 3  =>  Add(1, Mul(2,3))
        let q = parse("SELECT VALUE 1 + 2 * 3 FROM c");
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        match expr {
            Expression::Binary {
                op: BinOp::Add,
                rhs,
                ..
            } => assert!(matches!(*rhs, Expression::Binary { op: BinOp::Mul, .. })),
            other => panic!("expected Add at root, got {other:?}"),
        }
    }

    #[test]
    fn boolean_precedence() {
        // a OR b AND c => Or(a, And(b,c))
        let q = parse("SELECT VALUE c FROM c WHERE a OR b AND c");
        match q.filter {
            Some(Expression::Binary {
                op: BinOp::Or, rhs, ..
            }) => assert!(matches!(*rhs, Expression::Binary { op: BinOp::And, .. })),
            other => panic!("expected Or at root, got {other:?}"),
        }
    }

    #[test]
    fn function_call() {
        let q = parse("SELECT VALUE UPPER(c.name) FROM c");
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        match expr {
            Expression::Function { name, args } => {
                assert_eq!(name, "UPPER");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected Function, got {other:?}"),
        }
    }

    #[test]
    fn object_literal_projection() {
        let q = parse(r#"SELECT VALUE { "n": c.name, tag: t } FROM c JOIN t IN c.tags"#);
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        assert!(matches!(expr, Expression::Object(ref f) if f.len() == 2));
        let from = q.from.as_ref().unwrap();
        assert_eq!(from.joins.len(), 1);
        assert_eq!(from.joins[0].alias, "t");
    }

    #[test]
    fn array_unwind_join() {
        let q = parse("SELECT VALUE t FROM c JOIN t IN c.tags");
        let from = q.from.as_ref().unwrap();
        assert_eq!(from.joins.len(), 1);
        assert!(
            matches!(&from.joins[0].array, Expression::Member { field, .. } if field == "tags")
        );
    }

    #[test]
    fn udf_call() {
        let q = parse("SELECT VALUE udf.double(c.x) FROM c");
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        match expr {
            Expression::Udf { name, args } => {
                assert_eq!(name, "double");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected Udf, got {other:?}"),
        }
    }

    #[test]
    fn udf_keyword_case_insensitive_name_preserved() {
        // The `udf` keyword matches case-insensitively (like EXISTS/ARRAY); the
        // function name keeps its case, and multiple args parse.
        let q = parse("SELECT VALUE UDF.MyFunc(c.a, c.b, 5) FROM c");
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        match expr {
            Expression::Udf { name, args } => {
                assert_eq!(name, "MyFunc");
                assert_eq!(args.len(), 3);
            }
            other => panic!("expected Udf, got {other:?}"),
        }
    }

    #[test]
    fn udf_as_field_is_member_not_namespace() {
        // `udf` is the UDF namespace only as a *leading* identifier; after a `.`
        // it is an ordinary field name (`c.udf` is member access, not a call).
        let q = parse("SELECT VALUE c.udf FROM c");
        let SelectClause::Value(expr) = q.select else {
            panic!("expected SELECT VALUE")
        };
        assert!(matches!(expr, Expression::Member { ref field, .. } if field == "udf"));
    }

    #[test]
    fn order_offset_limit() {
        let q = parse("SELECT VALUE c FROM c ORDER BY c.a DESC, c.b OFFSET 5 LIMIT 10");
        assert_eq!(q.order_by.len(), 2);
        assert_eq!(q.order_by[0].direction, SortDirection::Desc);
        assert_eq!(q.order_by[1].direction, SortDirection::Asc);
        assert_eq!(q.offset, Some(5));
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parameter_in_predicate() {
        let q = parse("SELECT VALUE c FROM c WHERE c.age > @minAge");
        match q.filter {
            Some(Expression::Binary { rhs, .. }) => {
                assert!(matches!(*rhs, Expression::Parameter(ref p) if p == "minAge"))
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn select_star() {
        assert!(matches!(
            parse("SELECT * FROM c").select,
            SelectClause::Star
        ));
    }

    #[test]
    fn tabular_projection_infers_keys() {
        // last path segment for member access; AS overrides; `$N` for computed.
        let SelectClause::Projections(items) =
            parse("SELECT c.name, c.address.city, c.age + 1, c.x AS y FROM c").select
        else {
            panic!("expected tabular projections");
        };
        let keys: Vec<&str> = items.iter().map(|it| it.key.as_str()).collect();
        assert_eq!(keys, vec!["name", "city", "$1", "y"]);
    }

    #[test]
    fn duplicate_projected_key_errors() {
        // `c.address.city` and `c.work.city` both infer the key "city".
        assert!(matches!(
            parse_err("SELECT c.address.city, c.work.city FROM c"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn trailing_garbage_errors() {
        // `FROM c garbage` is now a valid container+alias; use trailing tokens
        // after an otherwise-complete query instead.
        assert!(matches!(
            parse_err("SELECT VALUE c FROM c LIMIT 5 garbage"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn in_list_desugars_to_or_of_equalities() {
        // c.status IN (1, 2, 3)  =>  (status = 1 OR status = 2) OR status = 3
        let q = parse("SELECT VALUE c FROM c WHERE c.status IN (1, 2, 3)");
        let Some(Expression::Binary {
            op: BinOp::Or,
            lhs,
            rhs,
        }) = q.filter
        else {
            panic!("expected a top-level OR");
        };
        // The last disjunct is `status = 3`.
        let Expression::Binary {
            op: BinOp::Eq,
            rhs: eq_rhs,
            ..
        } = *rhs
        else {
            panic!("expected equality as the last disjunct");
        };
        assert!(matches!(*eq_rhs, Expression::Literal(Literal::Int(3))));
        // The earlier values fold left into another OR.
        assert!(matches!(*lhs, Expression::Binary { op: BinOp::Or, .. }));
    }

    #[test]
    fn between_desugars_to_inclusive_range() {
        // c.age BETWEEN 18 AND 65  =>  age >= 18 AND age <= 65
        let q = parse("SELECT VALUE c FROM c WHERE c.age BETWEEN 18 AND 65");
        let Some(Expression::Binary {
            op: BinOp::And,
            lhs,
            rhs,
        }) = q.filter
        else {
            panic!("expected a top-level AND");
        };
        let Expression::Binary {
            op: BinOp::Gte,
            rhs: lo,
            ..
        } = *lhs
        else {
            panic!("expected >= as the lower bound");
        };
        let Expression::Binary {
            op: BinOp::Lte,
            rhs: hi,
            ..
        } = *rhs
        else {
            panic!("expected <= as the upper bound");
        };
        assert!(matches!(*lo, Expression::Literal(Literal::Int(18))));
        assert!(matches!(*hi, Expression::Literal(Literal::Int(65))));
    }

    #[test]
    fn not_in_and_not_between_wrap_in_not() {
        let q = parse("SELECT VALUE c FROM c WHERE c.status NOT IN (1, 2)");
        let Some(Expression::Unary {
            op: UnaryOp::Not,
            expr,
        }) = q.filter
        else {
            panic!("expected NOT around the desugared IN");
        };
        assert!(matches!(*expr, Expression::Binary { op: BinOp::Or, .. }));

        let q = parse("SELECT VALUE c FROM c WHERE c.age NOT BETWEEN 1 AND 2");
        let Some(Expression::Unary {
            op: UnaryOp::Not,
            expr,
        }) = q.filter
        else {
            panic!("expected NOT around the desugared BETWEEN");
        };
        assert!(matches!(*expr, Expression::Binary { op: BinOp::And, .. }));
    }

    #[test]
    fn empty_in_list_errors() {
        assert!(matches!(
            parse_err("SELECT VALUE c FROM c WHERE c.status IN ()"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn between_respects_outer_boolean_precedence() {
        // `age BETWEEN 1 AND 2 OR c.x = 9` is `(age>=1 AND age<=2) OR x=9` — the
        // trailing OR is boolean, not the BETWEEN separator.
        let q = parse("SELECT VALUE c FROM c WHERE c.age BETWEEN 1 AND 2 OR c.x = 9");
        assert!(matches!(
            q.filter,
            Some(Expression::Binary { op: BinOp::Or, .. })
        ));
    }

    #[test]
    fn like_translates_wildcards_and_escapes_metacharacters() {
        // The documented wildcards, plus regex metacharacters that must stay
        // literal. https://learn.microsoft.com/en-us/cosmos-db/query/like
        assert_eq!(like_to_regex("%driver%", None), "^.*driver.*$");
        assert_eq!(like_to_regex("fruit%", None), "^fruit.*$");
        assert_eq!(like_to_regex("%Road", None), "^.*Road$");
        assert_eq!(like_to_regex("a.b(", None), r"^a\.b\($");
        assert_eq!(like_to_regex("%SO[t-z]PS%", None), "^.*SO[t-z]PS.*$");
        assert_eq!(like_to_regex("%SO[^abc]PS%", None), "^.*SO[^abc]PS.*$");
        // Bracket-literal forms.
        assert_eq!(like_to_regex("20-30[%]", None), "^20-30[%]$");
        assert_eq!(like_to_regex("[_]n", None), "^[_]n$");
        assert_eq!(like_to_regex("[[]", None), "^[[]$");
        assert_eq!(like_to_regex("]", None), r"^\]$");
        // ESCAPE makes the following `%` a literal.
        assert_eq!(like_to_regex("%20^%%", Some('^')), "^.*20%.*$");
    }

    #[test]
    fn like_desugars_to_regexmatch() {
        let q = parse(r#"SELECT VALUE c FROM c WHERE c.name LIKE "a%""#);
        let Some(Expression::Function { name, args }) = q.filter else {
            panic!("expected a REGEXMATCH call");
        };
        assert_eq!(name, "REGEXMATCH");
        assert_eq!(args.len(), 2);
        assert!(matches!(args[1], Expression::Literal(Literal::Str(ref r)) if r == "^a.*$"));
    }

    #[test]
    fn like_with_escape_clause() {
        let q = parse(r#"SELECT VALUE c FROM c WHERE c.x LIKE "%20^%%" ESCAPE "^""#);
        let Some(Expression::Function { name, args }) = q.filter else {
            panic!("expected a REGEXMATCH call");
        };
        assert_eq!(name, "REGEXMATCH");
        assert!(matches!(args[1], Expression::Literal(Literal::Str(ref r)) if r == "^.*20%.*$"));
    }

    #[test]
    fn not_like_wraps_in_not() {
        let q = parse(r#"SELECT VALUE c FROM c WHERE c.name NOT LIKE "a%""#);
        let Some(Expression::Unary {
            op: UnaryOp::Not,
            expr,
        }) = q.filter
        else {
            panic!("expected NOT around the desugared LIKE");
        };
        assert!(matches!(*expr, Expression::Function { ref name, .. } if name == "REGEXMATCH"));
    }

    #[test]
    fn like_requires_string_literal_pattern() {
        assert!(matches!(
            parse_err("SELECT VALUE c FROM c WHERE c.name LIKE 5"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn group_by_clause_parses() {
        let q = parse("SELECT c.kind, COUNT(c.tags) FROM c GROUP BY c.kind");
        assert_eq!(q.group_by.len(), 1);
        assert!(matches!(q.group_by[0], Expression::Member { ref field, .. } if field == "kind"));
    }

    #[test]
    fn group_by_multiple_keys() {
        let q = parse("SELECT VALUE c FROM c GROUP BY c.a, c.b");
        assert_eq!(q.group_by.len(), 2);
    }

    #[test]
    fn no_group_by_is_empty() {
        assert!(parse("SELECT VALUE c FROM c").group_by.is_empty());
    }

    #[test]
    fn having_clause_parses_after_group_by() {
        let q = parse("SELECT c.kind, COUNT(1) AS n FROM c GROUP BY c.kind HAVING COUNT(1) > 2");
        assert_eq!(q.group_by.len(), 1);
        // HAVING is a comparison whose lhs is the COUNT aggregate.
        let Some(Expression::Binary {
            op: BinOp::Gt, lhs, ..
        }) = q.having
        else {
            panic!("expected a HAVING comparison, got {:?}", q.having);
        };
        assert!(matches!(*lhs, Expression::Function { ref name, .. } if name == "COUNT"));
    }

    #[test]
    fn having_sits_between_group_by_and_order_by() {
        // The full clause order parses end to end.
        let q = parse(
            "SELECT c.kind, SUM(c.n) AS total FROM c \
             GROUP BY c.kind HAVING SUM(c.n) > 10 ORDER BY total DESC LIMIT 5",
        );
        assert!(q.having.is_some());
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn no_having_is_none() {
        assert!(
            parse("SELECT c.kind FROM c GROUP BY c.kind")
                .having
                .is_none()
        );
    }

    #[test]
    fn array_agg_and_collect_parse_as_function_calls() {
        // Both surface as ordinary function calls in the AST; the planner routes
        // them to the aggregation node.
        let q = parse("SELECT VALUE ARRAY_AGG(c.tag) FROM c");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Function { ref name, .. }) if name == "ARRAY_AGG"
        ));
        let q = parse("SELECT VALUE COLLECT(c.tag) FROM c");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Function { ref name, .. }) if name == "COLLECT"
        ));
    }

    #[test]
    fn documentid_parses_as_function_call() {
        // DOCUMENTID is a plain function in the AST; the planner desugars it.
        let q = parse("SELECT VALUE DOCUMENTID(c) FROM c");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Function { ref name, ref args })
                if name == "DOCUMENTID" && args.len() == 1
        ));
    }

    #[test]
    fn scalar_subquery_parses() {
        let q = parse("SELECT VALUE (SELECT VALUE COUNT(1) FROM t IN c.tags) FROM c");
        let SelectClause::Value(Expression::Subquery { kind, query }) = q.select else {
            panic!("expected a scalar subquery");
        };
        assert_eq!(kind, SubqueryKind::Scalar);
        // The inner query's FROM is an array source.
        assert!(
            matches!(query.from.as_ref().unwrap().source, FromSource::Array { ref alias, .. } if alias == "t")
        );
    }

    #[test]
    fn exists_and_array_subqueries_parse() {
        let q = parse("SELECT VALUE c FROM c WHERE EXISTS (SELECT VALUE t FROM t IN c.tags)");
        assert!(matches!(
            q.filter,
            Some(Expression::Subquery {
                kind: SubqueryKind::Exists,
                ..
            })
        ));
        let q = parse("SELECT VALUE ARRAY(SELECT VALUE t FROM t IN c.tags) FROM c");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Subquery {
                kind: SubqueryKind::Array,
                ..
            })
        ));
    }

    #[test]
    fn from_array_source_parses() {
        let q = parse("SELECT VALUE x FROM x IN [1, 2, 3]");
        assert!(
            matches!(q.from.as_ref().unwrap().source, FromSource::Array { ref alias, .. } if alias == "x")
        );
    }

    #[test]
    fn exists_without_subquery_is_a_normal_function() {
        // `EXISTS(...)` not followed by SELECT stays an ordinary function call.
        let q = parse("SELECT VALUE EXISTS(c.x) FROM c");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Function { ref name, .. }) if name == "EXISTS"
        ));
    }

    #[test]
    fn from_subroot_path_source() {
        let q = parse("SELECT * FROM employees.employment e");
        match &q.from.as_ref().unwrap().source {
            FromSource::Subroot { base, path, alias } => {
                assert_eq!(base, "employees");
                assert_eq!(path, &vec!["employment".to_string()]);
                assert_eq!(alias, "e");
            }
            other => panic!("expected Subroot, got {other:?}"),
        }
        // Multi-segment path, AS alias.
        let q = parse("SELECT VALUE x FROM c.a.b AS x");
        assert!(matches!(
            &q.from.as_ref().unwrap().source,
            FromSource::Subroot { path, alias, .. } if path == &["a", "b"] && alias == "x"
        ));
        // No alias → defaults to the last path segment.
        let q = parse("SELECT * FROM c.emp");
        assert!(matches!(
            &q.from.as_ref().unwrap().source,
            FromSource::Subroot { alias, .. } if alias == "emp"
        ));
    }

    #[test]
    fn select_value_rejects_an_alias() {
        // A VALUE stream has no key, so `AS` is invalid — matching Cosmos.
        assert!(
            parse_err("SELECT VALUE c.x AS y FROM c")
                .to_string()
                .contains("VALUE")
        );
    }

    #[test]
    fn coalesce_desugars_to_iif() {
        // `a ?? b` → IIF(IS_DEFINED(a), a, b).
        let q = parse("SELECT VALUE c.x ?? c.y FROM c");
        let SelectClause::Value(Expression::Function { name, args }) = &q.select else {
            panic!("expected a function");
        };
        assert_eq!(name, "IIF");
        assert_eq!(args.len(), 3);
        assert!(matches!(&args[0], Expression::Function { name, .. } if name == "IS_DEFINED"));
    }

    #[test]
    fn coalesce_is_lower_precedence_than_arithmetic() {
        // `2 + c.x ?? 3` parses as `(2 + c.x) ?? 3`, so the IIF result branch is
        // the addition — not `2 + (c.x ?? 3)`.
        let q = parse("SELECT VALUE 2 + c.x ?? 3 FROM c");
        let SelectClause::Value(Expression::Function { name, args }) = &q.select else {
            panic!("expected IIF");
        };
        assert_eq!(name, "IIF");
        assert!(matches!(
            &args[1],
            Expression::Binary { op: BinOp::Add, .. }
        ));
    }

    #[test]
    fn lone_question_mark_is_a_lex_error() {
        assert!(crate::lexer::tokenize("SELECT VALUE 1 ?").is_err());
    }

    #[test]
    fn select_top_folds_into_limit() {
        let q = parse("SELECT TOP 2 VALUE c.x FROM c");
        assert_eq!(q.limit, Some(2));
        assert_eq!(q.offset, None);
    }

    #[test]
    fn top_follows_distinct() {
        // `DISTINCT TOP` is the valid order (Cosmos rejects `TOP DISTINCT`).
        let q = parse("SELECT DISTINCT TOP 3 VALUE c.x FROM c");
        assert!(q.distinct);
        assert_eq!(q.limit, Some(3));
        assert!(
            parse_err("SELECT TOP 3 DISTINCT VALUE c.x FROM c")
                .to_string()
                .to_lowercase()
                .contains("distinct")
        );
    }

    #[test]
    fn top_conflicts_with_offset_limit() {
        assert!(
            parse_err("SELECT TOP 2 VALUE c.x FROM c LIMIT 1")
                .to_string()
                .contains("TOP")
        );
        assert!(
            parse_err("SELECT TOP 2 VALUE c.x FROM c OFFSET 1 LIMIT 1")
                .to_string()
                .contains("TOP")
        );
    }

    #[test]
    fn top_is_reserved_so_it_is_not_an_alias() {
        // `AS top` is invalid in Cosmos (TOP is reserved); we match that.
        assert!(
            parse_err("SELECT c.x AS top FROM c")
                .to_string()
                .contains("identifier")
        );
    }

    #[test]
    fn select_distinct_sets_the_flag() {
        assert!(parse("SELECT DISTINCT VALUE c.x FROM c").distinct);
        assert!(parse("SELECT DISTINCT c.x, c.y FROM c").distinct);
        assert!(!parse("SELECT VALUE c.x FROM c").distinct);
    }

    #[test]
    fn nan_and_infinity_are_float_literals() {
        // Cosmos treats `NaN`/`Infinity` as numeric literals, not identifiers.
        let q = parse("SELECT VALUE NaN");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Literal(Literal::Float(f))) if f.is_nan()
        ));
        let q = parse("SELECT VALUE Infinity");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Literal(Literal::Float(f))) if f.is_infinite() && f > 0.0
        ));
        // `undefined` stays an identifier (it resolves to the undefined value).
        let q = parse("SELECT VALUE undefined");
        assert!(matches!(
            q.select,
            SelectClause::Value(Expression::Identifier(ref n)) if n == "undefined"
        ));
    }
}
