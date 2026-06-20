//! Hand-written lexer: source text → `Vec<Token>`.
//!
//! Single forward pass over `char_indices`, with at most one character of
//! lookahead (enough for the two-character operators `<=`, `>=`, `<>`, `!=`).
//! The returned vector always ends with [`Token::Eof`].

use std::iter::Peekable;
use std::str::CharIndices;

use crate::error::{Result, SqlError};
use crate::token::Token;

type Cursor<'a> = Peekable<CharIndices<'a>>;

/// Tokenize `src` into a vector terminated by [`Token::Eof`].
pub fn tokenize(src: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut cur = src.char_indices().peekable();

    while let Some(&(i, c)) = cur.peek() {
        if c.is_whitespace() {
            cur.next();
            continue;
        }
        match c {
            '0'..='9' => tokens.push(lex_number(&mut cur, i)?),
            '\'' | '"' => tokens.push(lex_string(&mut cur, i)?),
            '@' => {
                cur.next();
                let name = lex_ident_str(&mut cur);
                if name.is_empty() {
                    return Err(SqlError::Lex {
                        message: "empty parameter name after '@'".into(),
                        at: i,
                    });
                }
                tokens.push(Token::Param(name));
            }
            c if is_ident_start(c) => {
                let word = lex_ident_str(&mut cur);
                tokens.push(keyword_or_ident(word));
            }
            _ => tokens.push(lex_symbol(&mut cur, i)?),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

// ── Identifiers & keywords ──────────────────────────────────────

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn lex_ident_str(cur: &mut Cursor) -> String {
    let mut s = String::new();
    while let Some(&(_, c)) = cur.peek() {
        if is_ident_continue(c) {
            s.push(c);
            cur.next();
        } else {
            break;
        }
    }
    s
}

fn keyword_or_ident(word: String) -> Token {
    match word.to_ascii_lowercase().as_str() {
        "select" => Token::Select,
        "value" => Token::Value,
        "as" => Token::As,
        "from" => Token::From,
        "where" => Token::Where,
        "join" => Token::Join,
        "in" => Token::In,
        "between" => Token::Between,
        "like" => Token::Like,
        "escape" => Token::Escape,
        "group" => Token::Group,
        "order" => Token::Order,
        "by" => Token::By,
        "asc" => Token::Asc,
        "desc" => Token::Desc,
        "offset" => Token::Offset,
        "limit" => Token::Limit,
        "and" => Token::And,
        "or" => Token::Or,
        "not" => Token::Not,
        "true" => Token::True,
        "false" => Token::False,
        "null" => Token::Null,
        _ => Token::Ident(word),
    }
}

// ── Numbers ─────────────────────────────────────────────────────

fn lex_number(cur: &mut Cursor, start: usize) -> Result<Token> {
    let mut s = String::new();
    let mut is_float = false;

    take_digits(cur, &mut s);

    // Fractional part.
    if let Some(&(_, '.')) = cur.peek() {
        is_float = true;
        s.push('.');
        cur.next();
        if !take_digits(cur, &mut s) {
            return Err(SqlError::Lex {
                message: "expected digits after '.' in number".into(),
                at: start,
            });
        }
    }

    // Exponent.
    if let Some(&(_, e)) = cur.peek() {
        if e == 'e' || e == 'E' {
            is_float = true;
            s.push('e');
            cur.next();
            if let Some(&(_, sign)) = cur.peek() {
                if sign == '+' || sign == '-' {
                    s.push(sign);
                    cur.next();
                }
            }
            if !take_digits(cur, &mut s) {
                return Err(SqlError::Lex {
                    message: "expected digits in number exponent".into(),
                    at: start,
                });
            }
        }
    }

    if is_float {
        s.parse::<f64>()
            .map(Token::Float)
            .map_err(|e| SqlError::Lex {
                message: format!("invalid float literal: {e}"),
                at: start,
            })
    } else {
        s.parse::<i64>().map(Token::Int).map_err(|e| SqlError::Lex {
            message: format!("invalid integer literal: {e}"),
            at: start,
        })
    }
}

/// Consume a run of ASCII digits into `s`; returns whether any were consumed.
fn take_digits(cur: &mut Cursor, s: &mut String) -> bool {
    let mut any = false;
    while let Some(&(_, c)) = cur.peek() {
        if c.is_ascii_digit() {
            s.push(c);
            cur.next();
            any = true;
        } else {
            break;
        }
    }
    any
}

// ── Strings ─────────────────────────────────────────────────────

fn lex_string(cur: &mut Cursor, start: usize) -> Result<Token> {
    let quote = match cur.next() {
        Some((_, c)) => c,
        None => {
            return Err(SqlError::Lex {
                message: "unterminated string".into(),
                at: start,
            });
        }
    };

    let mut s = String::new();
    loop {
        match cur.next() {
            Some((_, c)) if c == quote => return Ok(Token::Str(s)),
            Some((_, '\\')) => match cur.next() {
                Some((_, e)) => s.push(unescape(e)),
                None => {
                    return Err(SqlError::Lex {
                        message: "unterminated escape in string".into(),
                        at: start,
                    });
                }
            },
            Some((_, c)) => s.push(c),
            None => {
                return Err(SqlError::Lex {
                    message: "unterminated string".into(),
                    at: start,
                });
            }
        }
    }
}

fn unescape(e: char) -> char {
    match e {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        other => other, // \\ \' \" and anything else: pass through literally
    }
}

// ── Operators & punctuation ─────────────────────────────────────

fn lex_symbol(cur: &mut Cursor, at: usize) -> Result<Token> {
    let (i, c) = match cur.next() {
        Some(pair) => pair,
        None => {
            return Err(SqlError::Lex {
                message: "unexpected end of input".into(),
                at,
            });
        }
    };

    let tok = match c {
        '.' => Token::Dot,
        ',' => Token::Comma,
        ':' => Token::Colon,
        '(' => Token::LParen,
        ')' => Token::RParen,
        '{' => Token::LBrace,
        '}' => Token::RBrace,
        '[' => Token::LBracket,
        ']' => Token::RBracket,
        '+' => Token::Plus,
        '-' => Token::Minus,
        '*' => Token::Star,
        '/' => Token::Slash,
        '%' => Token::Percent,
        '=' => Token::Eq,
        '<' => match cur.peek() {
            Some(&(_, '=')) => {
                cur.next();
                Token::Lte
            }
            Some(&(_, '>')) => {
                cur.next();
                Token::Neq
            }
            _ => Token::Lt,
        },
        '>' => match cur.peek() {
            Some(&(_, '=')) => {
                cur.next();
                Token::Gte
            }
            _ => Token::Gt,
        },
        '!' => match cur.peek() {
            Some(&(_, '=')) => {
                cur.next();
                Token::Neq
            }
            _ => {
                return Err(SqlError::Lex {
                    message: "unexpected '!' (did you mean '!='?)".into(),
                    at: i,
                });
            }
        },
        other => {
            return Err(SqlError::Lex {
                message: format!("unexpected character '{other}'"),
                at: i,
            });
        }
    };
    Ok(tok)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(src: &str) -> Vec<Token> {
        tokenize(src).unwrap()
    }

    #[test]
    fn keywords_are_case_insensitive() {
        assert_eq!(
            lex("SeLeCt vAlUe"),
            vec![Token::Select, Token::Value, Token::Eof]
        );
    }

    #[test]
    fn identifiers_preserve_case() {
        assert_eq!(
            lex("myAlias"),
            vec![Token::Ident("myAlias".into()), Token::Eof]
        );
    }

    #[test]
    fn numbers_int_and_float() {
        assert_eq!(
            lex("42 3.14 1e3 2.5e-2"),
            vec![
                Token::Int(42),
                Token::Float(3.14),
                Token::Float(1000.0),
                Token::Float(0.025),
                Token::Eof
            ]
        );
    }

    #[test]
    fn strings_single_and_double_quoted() {
        assert_eq!(lex("'hi'"), vec![Token::Str("hi".into()), Token::Eof]);
        assert_eq!(lex("\"hi\""), vec![Token::Str("hi".into()), Token::Eof]);
    }

    #[test]
    fn string_escapes() {
        assert_eq!(
            lex(r"'a\nb\'c'"),
            vec![Token::Str("a\nb'c".into()), Token::Eof]
        );
    }

    #[test]
    fn two_char_operators() {
        assert_eq!(
            lex("<= >= <> != ="),
            vec![
                Token::Lte,
                Token::Gte,
                Token::Neq,
                Token::Neq,
                Token::Eq,
                Token::Eof
            ]
        );
    }

    #[test]
    fn parameter() {
        assert_eq!(
            lex("@minAge"),
            vec![Token::Param("minAge".into()), Token::Eof]
        );
    }

    #[test]
    fn full_query_shape() {
        let toks = lex("SELECT VALUE c.name FROM c WHERE c.age >= 21");
        assert_eq!(toks.first(), Some(&Token::Select));
        assert_eq!(toks.last(), Some(&Token::Eof));
        assert!(toks.contains(&Token::Gte));
        assert!(toks.contains(&Token::Dot));
    }

    #[test]
    fn unterminated_string_errors() {
        let err = tokenize("'oops").unwrap_err();
        assert!(matches!(err, SqlError::Lex { .. }));
    }

    #[test]
    fn unexpected_char_errors() {
        let err = tokenize("a ~ b").unwrap_err();
        assert!(matches!(err, SqlError::Lex { .. }));
    }
}
