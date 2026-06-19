//! Lexical tokens.

/// A single lexical token. Keywords are case-insensitive at the lexer level;
/// identifiers preserve their original casing.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── Literals ────────────────────────────────────────────────
    Int(i64),
    Float(f64),
    Str(String),
    Ident(String),
    /// `@name` query parameter.
    Param(String),

    // ── Keywords ────────────────────────────────────────────────
    Select,
    Value,
    From,
    Where,
    Join,
    In,
    Order,
    By,
    Asc,
    Desc,
    Offset,
    Limit,
    And,
    Or,
    Not,
    True,
    False,
    Null,

    // ── Punctuation ─────────────────────────────────────────────
    Dot,
    Comma,
    Colon,
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,

    // ── Operators ───────────────────────────────────────────────
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,

    /// End-of-input sentinel — always the final token.
    Eof,
}
