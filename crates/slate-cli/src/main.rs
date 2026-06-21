//! `slate` — an interactive shell for a Slate database.
//!
//! Defaults to an ephemeral in-memory database; pass `--rocksdb <path>` or
//! `--redb <path>` (with the matching cargo feature) to open a persistent one.
//! Lines starting with `.` are meta-commands (`.help` lists them); everything
//! else is run as CosmosDB-style SQL against the current collection.

use std::cell::RefCell;
use std::path::PathBuf;
use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustyline::Editor;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use slate_db::{Database, DatabaseBuilder};
use slate_store::{BackupStore, Store};

use slate_cli::format::fmt_duration;
use slate_cli::{
    Command, CompletionState, Feed, InputBuffer, Output, Session, SlateCompleter, history_entry,
};

const USAGE: &str = "\
slate — interactive Slate shell

USAGE:
    slate [--rocksdb <path> | --redb <path>]

OPTIONS:
    --rocksdb <path>   open a persistent RocksDB-backed database (requires the
                       `rocksdb` feature)
    --redb <path>      open a persistent redb-backed database (requires the
                       `redb` feature)
    -h, --help         print this help

With no options, an in-memory database is used (discarded on exit).
";

const HELP: &str = "\
Commands:
    .help                       show this help
    .quit | .exit               leave the shell
    .collections                list collections
    .use <name>                 set the active collection
    .create <name>              create a collection (and make it active)
    .drop <name>                drop a collection
    .seed                       load a small sample collection
    .insert <json>              insert a document, or an array of documents
    .update <filter> <update>   update matching documents (Mongo operators)
    .replace <filter> <json>    replace the first matching document (no merge)
    .delete <filter>            delete matching documents
    .count [filter]             count documents (optionally filtered)
    .distinct <field> [filter]  distinct values of a field
    .index <field>              create an index on a field
    .unique-index <field>       create a unique index on a field
    .drop-index <field>         drop an index on a field
    .indexes                    list indexes on the active collection
    .schema [collection]        show key paths, indexes, and document count
    .backup <dir>               back up the database (rocksdb/redb only)

Anything else is run as SQL against the active collection, where `c` is the
row. End a statement with `;` — it may span multiple lines (`...>` continues it,
Ctrl-C cancels it):
    SELECT * FROM c;
    SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age DESC;
    SELECT c.city, COUNT(1) AS n FROM c GROUP BY c.city;
";

fn main() -> ExitCode {
    match real_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

enum Backend {
    Memory,
    Rocks(String),
    Redb(String),
}

fn real_main() -> Result<(), String> {
    let mut backend = Backend::Memory;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(());
            }
            "--rocksdb" => {
                let path = args.next().ok_or("--rocksdb requires a path")?;
                backend = Backend::Rocks(path);
            }
            "--redb" => {
                let path = args.next().ok_or("--redb requires a path")?;
                backend = Backend::Redb(path);
            }
            other => return Err(format!("unknown argument `{other}`\n\n{USAGE}")),
        }
    }

    match backend {
        Backend::Memory => run_memory(),
        Backend::Rocks(path) => run_rocks(path),
        Backend::Redb(path) => run_redb(path),
    }
}

// ── Backend openers (feature-gated) ─────────────────────────────

#[cfg(feature = "memory")]
fn run_memory() -> Result<(), String> {
    let db = DatabaseBuilder::new()
        .open(slate_store::MemoryStore::new())
        .map_err(|e| e.to_string())?;
    eprintln!("slate: in-memory database (changes are discarded on exit)");
    run(db)
}

#[cfg(not(feature = "memory"))]
fn run_memory() -> Result<(), String> {
    Err("in-memory backend not compiled in — rebuild with `--features memory`".into())
}

#[cfg(feature = "rocksdb")]
fn run_rocks(path: String) -> Result<(), String> {
    let store =
        slate_store::RocksStore::open(std::path::Path::new(&path)).map_err(|e| e.to_string())?;
    let db = DatabaseBuilder::new()
        .open(store)
        .map_err(|e| e.to_string())?;
    eprintln!("slate: RocksDB database at {path}");
    run(db)
}

#[cfg(not(feature = "rocksdb"))]
fn run_rocks(_path: String) -> Result<(), String> {
    Err("rocksdb backend not compiled in — rebuild with `--features rocksdb`".into())
}

#[cfg(feature = "redb")]
fn run_redb(path: String) -> Result<(), String> {
    let store =
        slate_store::RedbStore::open(std::path::Path::new(&path)).map_err(|e| e.to_string())?;
    let db = DatabaseBuilder::new()
        .open(store)
        .map_err(|e| e.to_string())?;
    eprintln!("slate: redb database at {path}");
    run(db)
}

#[cfg(not(feature = "redb"))]
fn run_redb(_path: String) -> Result<(), String> {
    Err("redb backend not compiled in — rebuild with `--features redb`".into())
}

// ── REPL loop ───────────────────────────────────────────────────

fn run<S: Store + BackupStore>(db: Database<S>) -> Result<(), String> {
    let mut session = Session::new(db);
    let mut rl: Editor<SlateCompleter, DefaultHistory> =
        Editor::new().map_err(|e| e.to_string())?;

    // The completer reads a snapshot the loop refreshes between prompts, so a
    // keypress never runs a transaction.
    let completions = Rc::new(RefCell::new(CompletionState::default()));
    rl.set_helper(Some(SlateCompleter::new(Rc::clone(&completions))));

    let history = history_path();
    if let Some(path) = &history {
        load_history(&mut rl, path);
    }

    eprintln!("Type `.help` for commands, `.quit` to exit. End SQL with `;`.\n");

    let mut buffer = InputBuffer::new();
    loop {
        // Refresh completion data at the primary prompt (best-effort — stale or
        // missing suggestions must never break the shell).
        if !buffer.is_pending() {
            let mut state = completions.borrow_mut();
            state.collections = session.collection_names().unwrap_or_default();
            state.fields = session.active_index_fields().unwrap_or_default();
        }

        // A statement-in-progress gets the continuation prompt; otherwise the
        // primary prompt reflects the active collection.
        let prompt = if buffer.is_pending() {
            "   ...> ".to_string()
        } else {
            match session.current() {
                Some(name) => format!("slate({name})> "),
                None => "slate> ".to_string(),
            }
        };
        match rl.readline(&prompt) {
            Ok(line) => {
                let statement = match buffer.push(&line) {
                    Feed::Ready(stmt) => stmt,
                    Feed::More => continue, // keep buffering at the `...>` prompt
                };
                if !statement.trim().is_empty() {
                    let _ = rl.add_history_entry(history_entry(&statement));
                }
                let command = match Command::parse(&statement) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        eprintln!("error: {e}");
                        continue;
                    }
                };
                let start = Instant::now();
                let result = session.execute(command);
                let elapsed = start.elapsed();
                match result {
                    Ok(Output::Quit) => break,
                    Ok(output) => print_output(&output, elapsed),
                    Err(e) => eprintln!("error: {e}"),
                }
            }
            // Ctrl-C: cancel any partially-typed statement and return to a fresh
            // prompt (a no-op when nothing is buffered).
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                continue;
            }
            Err(ReadlineError::Eof) => break, // Ctrl-D: leave
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        }
    }

    // Persist history on every exit path — `.quit`, Ctrl-D, and the error break
    // all fall through to here.
    if let Some(path) = &history
        && let Err(e) = rl.save_history(path)
    {
        eprintln!("warning: could not save history to {}: {e}", path.display());
    }
    Ok(())
}

/// Location of the persistent REPL history file (`~/.slate_history`), or `None`
/// when `HOME` is unset — in which case history is simply not persisted.
fn history_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".slate_history"))
}

/// Load prior history into the editor, creating the file on first run. A broken
/// or unreadable history file must never stop the shell, so failures here only
/// warn.
fn load_history<H: rustyline::Helper>(
    rl: &mut rustyline::Editor<H, rustyline::history::DefaultHistory>,
    path: &std::path::Path,
) {
    if !path.exists()
        && let Err(e) = std::fs::File::create(path)
    {
        eprintln!(
            "warning: could not create history file {}: {e}",
            path.display()
        );
        return;
    }
    if let Err(e) = rl.load_history(path) {
        eprintln!(
            "warning: could not load history from {}: {e}",
            path.display()
        );
    }
}

fn print_output(output: &Output, elapsed: Duration) {
    let took = fmt_duration(elapsed);
    match output {
        Output::Empty | Output::Quit => {}
        Output::Help => print!("{HELP}"),
        Output::Message(msg) => println!("{msg}"),
        Output::Affected(n) => println!("({n} affected, {took})"),
        Output::Count(n) => println!("{n} ({took})"),
        Output::Rows(rows) => {
            for row in rows {
                println!("{row}");
            }
            println!(
                "({} row{}, {took})",
                rows.len(),
                if rows.len() == 1 { "" } else { "s" }
            );
        }
        Output::Collections(pairs) => {
            if pairs.is_empty() {
                println!("(no collections)");
            } else {
                for (cf, name) in pairs {
                    println!("{cf}.{name}");
                }
            }
        }
        Output::Indexes(fields) => {
            if fields.is_empty() {
                println!("(no indexes)");
            } else {
                for field in fields {
                    println!("{field}");
                }
            }
        }
        Output::Schema(s) => {
            println!("collection `{}`", s.collection);
            println!("  pk:    {}", s.pk_path);
            println!("  ttl:   {}", s.ttl_path);
            println!("  count: {}", s.count);
            if s.indexes.is_empty() {
                println!("  indexes: (none)");
            } else {
                println!("  indexes:");
                for ix in &s.indexes {
                    let mark = if ix.unique { "  [unique]" } else { "" };
                    println!("    {}{mark}", ix.field);
                }
            }
        }
    }
}
