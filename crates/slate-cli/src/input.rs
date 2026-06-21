//! Line buffering for multi-line SQL input.
//!
//! Meta-commands (dot-prefixed) and blank lines are single-line: they complete
//! the instant Enter is pressed. A SQL statement, by contrast, may span several
//! lines and is only complete once a line ends with `;` — until then the shell
//! shows a continuation prompt and keeps reading. The terminating `;` is
//! stripped before the statement reaches the parser.
//!
//! Terminator detection is deliberately simple — the last non-whitespace
//! character of the accumulated buffer. A `;` inside a string literal at the
//! very end of a line would terminate early; that trade-off keeps the buffering
//! free of a SQL tokenizer, and a stray case is recoverable with Ctrl-C.

/// Accumulates input lines into a single complete statement.
#[derive(Debug, Default)]
pub struct InputBuffer {
    buf: String,
}

/// What [`InputBuffer::push`] decided about the line just read.
#[derive(Debug, PartialEq, Eq)]
pub enum Feed {
    /// A complete statement, ready to parse and run (terminator already stripped).
    Ready(String),
    /// The statement is still open; read another line at the continuation prompt.
    More,
}

impl InputBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a statement is currently open, so the caller shows the
    /// continuation prompt rather than the primary one.
    pub fn is_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// Discard any partially-typed statement (e.g. on Ctrl-C).
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Feed one raw input line. Returns [`Feed::Ready`] with the statement to
    /// run, or [`Feed::More`] when the statement is still open.
    pub fn push(&mut self, line: &str) -> Feed {
        // A fresh statement: a blank line or a dot-command is always single-line
        // and runs immediately; anything else is SQL that may span lines.
        if self.buf.is_empty() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('.') {
                return Feed::Ready(line.to_string());
            }
        } else {
            // Continuation lines join with a newline so the original layout is
            // preserved for the parser and any error spans it reports.
            self.buf.push('\n');
        }
        self.buf.push_str(line);

        if ends_statement(&self.buf) {
            let stmt = std::mem::take(&mut self.buf);
            Feed::Ready(strip_terminator(&stmt))
        } else {
            Feed::More
        }
    }
}

/// Whether the accumulated buffer ends a statement: its last non-whitespace
/// character is the `;` terminator.
fn ends_statement(buf: &str) -> bool {
    buf.trim_end().ends_with(';')
}

/// Strip the trailing `;` terminator (and surrounding whitespace) from a
/// complete statement.
fn strip_terminator(stmt: &str) -> String {
    let trimmed = stmt.trim_end();
    trimmed
        .strip_suffix(';')
        .unwrap_or(trimmed)
        .trim_end()
        .to_string()
}

/// The history entry for a completed statement. SQL gets its `;` terminator
/// re-appended — [`InputBuffer`] strips it before parsing, but a recalled entry
/// should run as-is rather than re-open at the `...>` prompt. Meta-commands take
/// no terminator and are stored verbatim.
pub fn history_entry(statement: &str) -> String {
    if statement.trim_start().starts_with('.') {
        statement.to_string()
    } else {
        format!("{statement};")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_command_completes_on_one_line() {
        let mut b = InputBuffer::new();
        assert_eq!(b.push(".seed"), Feed::Ready(".seed".to_string()));
        assert!(!b.is_pending());
    }

    #[test]
    fn blank_line_completes_immediately() {
        let mut b = InputBuffer::new();
        assert_eq!(b.push("   "), Feed::Ready("   ".to_string()));
        assert!(!b.is_pending());
    }

    #[test]
    fn single_line_sql_needs_a_terminator() {
        let mut b = InputBuffer::new();
        // Without `;` the statement stays open.
        assert_eq!(b.push("SELECT * FROM c"), Feed::More);
        assert!(b.is_pending());
        // The terminating line completes it, with the `;` stripped.
        assert_eq!(
            b.push("WHERE c.age > 30;"),
            Feed::Ready("SELECT * FROM c\nWHERE c.age > 30".to_string())
        );
        assert!(!b.is_pending());
    }

    #[test]
    fn terminator_on_the_first_line_completes_at_once() {
        let mut b = InputBuffer::new();
        assert_eq!(
            b.push("SELECT * FROM c;"),
            Feed::Ready("SELECT * FROM c".to_string())
        );
        assert!(!b.is_pending());
    }

    #[test]
    fn trailing_whitespace_after_terminator_is_trimmed() {
        let mut b = InputBuffer::new();
        assert_eq!(b.push("SELECT 1 ;   "), Feed::Ready("SELECT 1".to_string()));
    }

    #[test]
    fn clear_cancels_a_pending_statement() {
        let mut b = InputBuffer::new();
        assert_eq!(b.push("SELECT * FROM c"), Feed::More);
        assert!(b.is_pending());
        b.clear();
        assert!(!b.is_pending());
        // After cancelling, a dot-command runs cleanly again.
        assert_eq!(b.push(".help"), Feed::Ready(".help".to_string()));
    }

    #[test]
    fn history_entry_re_terminates_sql_but_not_dot_commands() {
        // SQL is stored with the `;` back so a recalled entry runs as-is.
        assert_eq!(history_entry("SELECT * FROM c"), "SELECT * FROM c;");
        assert_eq!(history_entry("SELECT *\nFROM c"), "SELECT *\nFROM c;");
        // Meta-commands are verbatim — no terminator.
        assert_eq!(history_entry(".schema sample"), ".schema sample");
    }

    #[test]
    fn dot_prefix_only_matters_at_statement_start() {
        // A `.` inside an open SQL statement is just content (e.g. `c.name`),
        // not a meta-command.
        let mut b = InputBuffer::new();
        assert_eq!(b.push("SELECT VALUE"), Feed::More);
        assert_eq!(
            b.push("c.name FROM c;"),
            Feed::Ready("SELECT VALUE\nc.name FROM c".to_string())
        );
    }
}
