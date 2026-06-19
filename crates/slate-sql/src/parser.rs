//! Recursive-descent parser with precedence climbing for scalar expressions.
//!
//! Precedence, lowest to highest:
//! `OR` < `AND` < `NOT` < comparison < `+`/`-` < `*`/`/`/`%` < unary `-` <
//! postfix (`.field`, `[index]`) < primary.
//!
//! Comparison is non-associative (a single `a <op> b`), matching SQL.

use crate::ast::*;
use crate::error::{Result, SqlError};
use crate::token::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
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
        self.expect(&Token::Select)?;
        self.expect(&Token::Value)?; // v1: only `SELECT VALUE`
        let select = SelectClause::Value(self.parse_expr()?);

        self.expect(&Token::From)?;
        let from = self.parse_from()?;

        let filter = if self.matches(&Token::Where) {
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

        self.expect(&Token::Eof)?;

        Ok(Query {
            select,
            from,
            filter,
            order_by,
            offset,
            limit,
        })
    }

    fn parse_from(&mut self) -> Result<FromClause> {
        let alias = self.parse_ident()?;
        let source = FromSource::ImplicitContainer { alias };

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

    fn parse_expr(&mut self) -> Result<ScalarExpr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<ScalarExpr> {
        let mut lhs = self.parse_and()?;
        while self.matches(&Token::Or) {
            let rhs = self.parse_and()?;
            lhs = binary(BinOp::Or, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<ScalarExpr> {
        let mut lhs = self.parse_not()?;
        while self.matches(&Token::And) {
            let rhs = self.parse_not()?;
            lhs = binary(BinOp::And, lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<ScalarExpr> {
        if self.matches(&Token::Not) {
            let expr = self.parse_not()?;
            Ok(ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<ScalarExpr> {
        let lhs = self.parse_additive()?;
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

    fn parse_additive(&mut self) -> Result<ScalarExpr> {
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

    fn parse_multiplicative(&mut self) -> Result<ScalarExpr> {
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

    fn parse_unary(&mut self) -> Result<ScalarExpr> {
        if self.peek() == &Token::Minus {
            self.advance();
            let expr = self.parse_unary()?;
            Ok(ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> Result<ScalarExpr> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek() {
                Token::Dot => {
                    self.advance();
                    let field = self.parse_ident()?;
                    expr = ScalarExpr::Member {
                        base: Box::new(expr),
                        field,
                    };
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    self.expect(&Token::RBracket)?;
                    expr = ScalarExpr::Index {
                        base: Box::new(expr),
                        index: Box::new(index),
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<ScalarExpr> {
        match self.take() {
            Token::Int(i) => Ok(ScalarExpr::Literal(Literal::Int(i))),
            Token::Float(f) => Ok(ScalarExpr::Literal(Literal::Float(f))),
            Token::Str(s) => Ok(ScalarExpr::Literal(Literal::Str(s))),
            Token::True => Ok(ScalarExpr::Literal(Literal::Bool(true))),
            Token::False => Ok(ScalarExpr::Literal(Literal::Bool(false))),
            Token::Null => Ok(ScalarExpr::Literal(Literal::Null)),
            Token::Param(p) => Ok(ScalarExpr::Parameter(p)),
            Token::LParen => {
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::LBrace => self.parse_object(),
            Token::LBracket => self.parse_array(),
            Token::Ident(name) => {
                if self.peek() == &Token::LParen {
                    self.advance();
                    let args = self.parse_call_args()?;
                    Ok(ScalarExpr::Function { name, args })
                } else {
                    Ok(ScalarExpr::Identifier(name))
                }
            }
            other => Err(SqlError::Parse {
                message: format!("unexpected token in expression: {other:?}"),
            }),
        }
    }

    /// Parse the argument list after a consumed `(`.
    fn parse_call_args(&mut self) -> Result<Vec<ScalarExpr>> {
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
    fn parse_object(&mut self) -> Result<ScalarExpr> {
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
        Ok(ScalarExpr::Object(fields))
    }

    /// Parse the body after a consumed `[`.
    fn parse_array(&mut self) -> Result<ScalarExpr> {
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
        Ok(ScalarExpr::Array(items))
    }
}

fn binary(op: BinOp, lhs: ScalarExpr, rhs: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
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
        assert!(matches!(q.select, SelectClause::Value(ScalarExpr::Identifier(ref a)) if a == "c"));
        assert!(matches!(
            q.from.source,
            FromSource::ImplicitContainer { ref alias } if alias == "c"
        ));
        assert!(q.from.joins.is_empty());
        assert!(q.filter.is_none());
    }

    #[test]
    fn member_and_index_paths() {
        let q = parse("SELECT VALUE c.tags[0].name FROM c");
        // ((c.tags)[0]).name
        let SelectClause::Value(expr) = q.select;
        match expr {
            ScalarExpr::Member { base, field } => {
                assert_eq!(field, "name");
                assert!(matches!(*base, ScalarExpr::Index { .. }));
            }
            other => panic!("expected Member, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_precedence() {
        // 1 + 2 * 3  =>  Add(1, Mul(2,3))
        let q = parse("SELECT VALUE 1 + 2 * 3 FROM c");
        let SelectClause::Value(expr) = q.select;
        match expr {
            ScalarExpr::Binary {
                op: BinOp::Add,
                rhs,
                ..
            } => assert!(matches!(*rhs, ScalarExpr::Binary { op: BinOp::Mul, .. })),
            other => panic!("expected Add at root, got {other:?}"),
        }
    }

    #[test]
    fn boolean_precedence() {
        // a OR b AND c => Or(a, And(b,c))
        let q = parse("SELECT VALUE c FROM c WHERE a OR b AND c");
        match q.filter {
            Some(ScalarExpr::Binary {
                op: BinOp::Or, rhs, ..
            }) => assert!(matches!(*rhs, ScalarExpr::Binary { op: BinOp::And, .. })),
            other => panic!("expected Or at root, got {other:?}"),
        }
    }

    #[test]
    fn function_call() {
        let q = parse("SELECT VALUE UPPER(c.name) FROM c");
        let SelectClause::Value(expr) = q.select;
        match expr {
            ScalarExpr::Function { name, args } => {
                assert_eq!(name, "UPPER");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected Function, got {other:?}"),
        }
    }

    #[test]
    fn object_literal_projection() {
        let q = parse(r#"SELECT VALUE { "n": c.name, tag: t } FROM c JOIN t IN c.tags"#);
        let SelectClause::Value(expr) = q.select;
        assert!(matches!(expr, ScalarExpr::Object(ref f) if f.len() == 2));
        assert_eq!(q.from.joins.len(), 1);
        assert_eq!(q.from.joins[0].alias, "t");
    }

    #[test]
    fn array_unwind_join() {
        let q = parse("SELECT VALUE t FROM c JOIN t IN c.tags");
        assert_eq!(q.from.joins.len(), 1);
        assert!(
            matches!(&q.from.joins[0].array, ScalarExpr::Member { field, .. } if field == "tags")
        );
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
            Some(ScalarExpr::Binary { rhs, .. }) => {
                assert!(matches!(*rhs, ScalarExpr::Parameter(ref p) if p == "minAge"))
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn missing_value_keyword_errors() {
        // v1 only supports SELECT VALUE.
        assert!(matches!(
            parse_err("SELECT c FROM c"),
            SqlError::Parse { .. }
        ));
    }

    #[test]
    fn trailing_garbage_errors() {
        assert!(matches!(
            parse_err("SELECT VALUE c FROM c garbage"),
            SqlError::Parse { .. }
        ));
    }
}
