//! Tab-completion for the shell.
//!
//! The completer suggests three things by cursor context: meta-command names
//! (when the line starts with `.`), collection names (after commands that take
//! a collection), and the active collection's indexed fields (after commands
//! that take a field).
//!
//! It reads from a cheap [`CompletionState`] snapshot rather than touching the
//! database during a keypress — the REPL loop refreshes that snapshot between
//! prompts. The decision itself lives in the pure [`complete`] function so it
//! can be unit-tested without a terminal.

use std::cell::RefCell;
use std::rc::Rc;

use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

/// Meta-command names offered for completion (without the leading dot).
const COMMAND_NAMES: &[&str] = &[
    "help",
    "quit",
    "exit",
    "collections",
    "use",
    "create",
    "drop",
    "seed",
    "insert",
    "update",
    "replace",
    "delete",
    "count",
    "distinct",
    "index",
    "unique-index",
    "drop-index",
    "indexes",
    "schema",
    "backup",
    "explain",
];

/// A snapshot of what the completer needs, refreshed by the REPL between
/// prompts so a keypress never runs a transaction.
#[derive(Debug, Default)]
pub struct CompletionState {
    /// Names of all collections.
    pub collections: Vec<String>,
    /// Indexed field paths of the active collection.
    pub fields: Vec<String>,
}

/// What kind of argument a meta-command takes, for completion purposes.
enum ArgKind {
    Collection,
    Field,
}

/// Classify the argument a meta-command (with or without its leading dot) takes.
fn arg_kind(cmd: &str) -> Option<ArgKind> {
    let name = cmd.strip_prefix('.').unwrap_or(cmd).to_ascii_lowercase();
    match name.as_str() {
        "use" | "drop" | "schema" => Some(ArgKind::Collection),
        "distinct" | "index" | "unique-index" | "drop-index" => Some(ArgKind::Field),
        _ => None,
    }
}

/// The dot-prefixed command names, for first-word completion.
fn command_names() -> Vec<String> {
    COMMAND_NAMES.iter().map(|c| format!(".{c}")).collect()
}

/// Decide the completion for `line` at byte cursor `pos`. Returns the start
/// offset of the word being completed and the matching candidates.
///
/// The first word completes against meta-command names when it starts with `.`;
/// a later word completes against collection or field names depending on the
/// command. SQL keywords are intentionally not completed.
pub fn complete(line: &str, pos: usize, state: &CompletionState) -> (usize, Vec<String>) {
    let head = &line[..pos];
    // The current word starts just after the last whitespace before the cursor.
    let word_start = head.rfind(char::is_whitespace).map(|i| i + 1).unwrap_or(0);
    let word = &head[word_start..];
    let first = head[..word_start].split_whitespace().next();

    // Choose the candidate pool as a borrowed slice; only the matches are owned.
    let from_commands;
    let pool: &[String] = match first {
        // First word: only dot-commands are completed.
        None if word.starts_with('.') => {
            from_commands = command_names();
            &from_commands
        }
        None => return (word_start, Vec::new()),
        // Argument of a known command.
        Some(cmd) => match arg_kind(cmd) {
            Some(ArgKind::Collection) => &state.collections,
            Some(ArgKind::Field) => &state.fields,
            None => return (word_start, Vec::new()),
        },
    };

    let matches = pool
        .iter()
        .filter(|c| c.starts_with(word))
        .cloned()
        .collect();
    (word_start, matches)
}

/// A rustyline [`Helper`] that completes from a shared [`CompletionState`].
pub struct SlateCompleter {
    state: Rc<RefCell<CompletionState>>,
}

impl SlateCompleter {
    pub fn new(state: Rc<RefCell<CompletionState>>) -> Self {
        Self { state }
    }
}

impl Completer for SlateCompleter {
    // `String` candidates display and insert the same text — exactly what we
    // want, and it avoids building `Pair`s.
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<String>), ReadlineError> {
        Ok(complete(line, pos, &self.state.borrow()))
    }
}

// The remaining `Helper` supertraits use their default (no-op) behaviour.
impl Hinter for SlateCompleter {
    type Hint = String;
}
impl Highlighter for SlateCompleter {}
impl Validator for SlateCompleter {}
impl Helper for SlateCompleter {}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> CompletionState {
        CompletionState {
            collections: vec!["sample".to_string(), "people".to_string()],
            fields: vec!["city".to_string(), "age".to_string()],
        }
    }

    #[test]
    fn completes_dot_command_names() {
        let (start, matches) = complete(".sch", 4, &CompletionState::default());
        assert_eq!(start, 0);
        assert!(matches.contains(&".schema".to_string()));
        assert!(!matches.iter().any(|m| m == ".insert"));
    }

    #[test]
    fn completes_collection_names_after_use() {
        let (start, matches) = complete(".use sa", 7, &state());
        assert_eq!(start, 5);
        assert_eq!(matches, vec!["sample".to_string()]);
    }

    #[test]
    fn completes_collection_for_schema() {
        let (_, matches) = complete(".schema pe", 10, &state());
        assert_eq!(matches, vec!["people".to_string()]);
    }

    #[test]
    fn completes_indexed_fields_for_distinct() {
        let (_, matches) = complete(".distinct ci", 12, &state());
        assert_eq!(matches, vec!["city".to_string()]);
    }

    #[test]
    fn bare_sql_is_not_completed() {
        let (_, matches) = complete("SEL", 3, &state());
        assert!(matches.is_empty());
    }

    #[test]
    fn unknown_command_arguments_are_not_completed() {
        // `.count` takes a JSON filter, not a collection or field name.
        let (_, matches) = complete(".count sa", 9, &state());
        assert!(matches.is_empty());
    }
}
