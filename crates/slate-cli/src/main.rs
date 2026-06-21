//! `slate` — an interactive shell for a Slate database.
//!
//! Defaults to an ephemeral in-memory database; pass `--rocksdb <path>` or
//! `--redb <path>` (with the matching cargo feature) to open a persistent one.
//! Lines starting with `.` are meta-commands (`.help` lists them); everything
//! else is run as CosmosDB-style SQL against the current collection.

use std::process::ExitCode;

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use slate_db::{Database, DatabaseBuilder};
use slate_store::Store;

use slate_cli::{Command, Output, Session};

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
    .delete <filter>            delete matching documents
    .count [filter]             count documents (optionally filtered)
    .index <field>              create an index on a field
    .indexes                    list indexes on the active collection

Anything else is run as SQL against the active collection, where `c` is the row:
    SELECT * FROM c
    SELECT VALUE c.name FROM c WHERE c.age > 40 ORDER BY c.age DESC
    SELECT c.city, COUNT(1) AS n FROM c GROUP BY c.city
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

fn run<S: Store>(db: Database<S>) -> Result<(), String> {
    let mut session = Session::new(db);
    let mut rl = DefaultEditor::new().map_err(|e| e.to_string())?;

    eprintln!("Type `.help` for commands, `.quit` to exit.\n");

    loop {
        let prompt = match session.current() {
            Some(name) => format!("slate({name})> "),
            None => "slate> ".to_string(),
        };
        match rl.readline(&prompt) {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());
                let command = match Command::parse(&line) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        eprintln!("error: {e}");
                        continue;
                    }
                };
                match session.execute(command) {
                    Ok(Output::Quit) => break,
                    Ok(output) => print_output(&output),
                    Err(e) => eprintln!("error: {e}"),
                }
            }
            Err(ReadlineError::Interrupted) => continue, // Ctrl-C: abandon the line
            Err(ReadlineError::Eof) => break,            // Ctrl-D: leave
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn print_output(output: &Output) {
    match output {
        Output::Empty | Output::Quit => {}
        Output::Help => print!("{HELP}"),
        Output::Message(msg) => println!("{msg}"),
        Output::Affected(n) => println!("{n} document(s) affected"),
        Output::Count(n) => println!("{n}"),
        Output::Rows(rows) => {
            for row in rows {
                println!("{row}");
            }
            println!(
                "({} row{})",
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
    }
}
