//! Runs parsed [`Command`]s against a live database.
//!
//! Each command runs in its own auto-committed transaction — the right default
//! for an interactive probe (every statement is durable immediately, and there
//! is no open transaction to leak across the prompt loop). The session tracks a
//! *current collection* so SQL (`FROM c`) and the document commands know which
//! container to target without naming it every time.
//!
//! `execute` returns a semantic [`Output`] rather than printing, so it can be
//! driven and asserted on in tests with no terminal.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use bson::Bson;
use serde_json::{Value, json};

use slate_db::{
    CollectionConfig, CollectionStats, DEFAULT_CF, Database, DatabaseStats, DistinctOptions,
    ExportOptions, ImportOptions,
};
use slate_store::{BackupStore, Store};

use crate::command::Command;
use crate::format;

/// The result of running a [`Command`]. The frontend renders it; tests inspect
/// it directly.
#[derive(Debug, PartialEq)]
pub enum Output {
    /// Nothing to show (blank line).
    Empty,
    /// A status line, e.g. "created `users`".
    Message(String),
    /// A write affected `n` documents.
    Affected(u64),
    /// A bulk file load: `count` documents loaded into `collection`.
    Loaded { count: u64, collection: String },
    /// A count result.
    Count(u64),
    /// Rendered query result rows (already JSON-formatted).
    Rows(Vec<String>),
    /// A rendered query plan tree (from `.explain` / `.explain analyze`).
    Plan(String),
    /// A rendered size/cardinality statistics report (from `.stats`).
    Stats(String),
    /// `(cf, name)` pairs for every collection.
    Collections(Vec<(String, String)>),
    /// Indexed fields of the current collection.
    Indexes(Vec<String>),
    /// A collection's schema (key paths, indexes, document count).
    Schema(SchemaReport),
    /// Show the help text.
    Help,
    /// Leave the shell.
    Quit,
}

/// One indexed field in a [`SchemaReport`], flagged unique or not.
#[derive(Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub field: String,
    pub unique: bool,
}

/// A collection's schema for `.schema`: its key paths, indexes, and live
/// document count.
#[derive(Debug, PartialEq, Eq)]
pub struct SchemaReport {
    pub collection: String,
    pub pk_path: String,
    pub ttl_path: String,
    pub indexes: Vec<IndexEntry>,
    pub count: u64,
}

/// An interactive session over one [`Database`].
pub struct Session<S: Store> {
    db: Database<S>,
    current: Option<String>,
}

/// Stringify any displayable error — the shell surfaces errors as text.
fn es<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// Render one collection's stats as an indented text block.
fn render_collection_stats(s: &CollectionStats) -> String {
    let mut out = String::new();
    let approx = if s.approximate { " (approximate)" } else { "" };
    out.push_str(&format!("collection `{}.{}`\n", s.cf, s.name));
    out.push_str(&format!("  documents: {}{approx}\n", s.document_count));
    if s.indexes.is_empty() {
        out.push_str("  indexes: (none)");
    } else {
        out.push_str("  indexes:");
        for ix in &s.indexes {
            out.push_str(&format!(
                "\n    {} — {} entries, {} distinct",
                ix.field, ix.entry_count, ix.cardinality
            ));
        }
    }
    out
}

/// Render database-wide stats: a per-collection breakdown plus totals.
fn render_database_stats(s: &DatabaseStats) -> String {
    let mut out = String::new();
    if s.collections.is_empty() {
        out.push_str("(no collections)\n");
    } else {
        for c in &s.collections {
            out.push_str(&render_collection_stats(c));
            out.push('\n');
        }
    }
    out.push_str(&format!("total documents: {}", s.total_documents));
    match s.disk_size_bytes {
        Some(bytes) => out.push_str(&format!("\ndisk size: {bytes} bytes")),
        None => out.push_str("\ndisk size: (unavailable)"),
    }
    out
}

// The session requires `BackupStore` (not just `Store`) so `.backup` can reach
// the existing `Database::backup` passthrough. This costs nothing: every store
// the CLI opens — memory, rocksdb, redb — implements `BackupStore`, and the
// memory backend's impl returns a clear "not supported" error we surface as-is.
impl<S: BackupStore> Session<S> {
    pub fn new(db: Database<S>) -> Self {
        Self { db, current: None }
    }

    /// The collection SQL and document commands currently target, if any.
    pub fn current(&self) -> Option<&str> {
        self.current.as_deref()
    }

    /// Every collection name, for tab-completion.
    pub fn collection_names(&self) -> Result<Vec<String>, String> {
        Ok(self
            .db
            .list_collections()
            .map_err(es)?
            .into_iter()
            .map(|(_, name)| name)
            .collect())
    }

    /// Indexed field paths of the active collection, for tab-completion. Empty
    /// when no collection is selected.
    pub fn active_index_fields(&self) -> Result<Vec<String>, String> {
        let Some(collection) = self.current.as_deref() else {
            return Ok(Vec::new());
        };
        let txn = self.db.begin(true).map_err(es)?;
        let fields = txn.list_indexes(DEFAULT_CF, collection).map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(fields)
    }

    /// Run one command.
    pub fn execute(&mut self, cmd: Command) -> Result<Output, String> {
        match cmd {
            Command::Empty => Ok(Output::Empty),
            Command::Help => Ok(Output::Help),
            Command::Quit => Ok(Output::Quit),
            Command::ListCollections => self.list_collections(),
            Command::Use(name) => self.use_collection(name),
            Command::Create(name) => self.create(name),
            Command::Drop(name) => self.drop(name),
            Command::Insert(value) => self.insert(value),
            Command::Update { filter, update } => self.update(filter, update),
            Command::Replace {
                filter,
                replacement,
            } => self.replace(filter, replacement),
            Command::Delete { filter } => self.delete(filter),
            Command::Distinct { field, filter } => self.distinct(field, filter),
            Command::CreateIndex(fields) => self.create_index(fields),
            Command::CreateUniqueIndex(fields) => self.create_unique_index(fields),
            Command::DropIndex(fields) => self.drop_index(fields),
            Command::ListIndexes => self.list_indexes(),
            Command::Count(filter) => self.count(filter),
            Command::Schema(name) => self.schema(name),
            Command::Seed => self.seed(),
            Command::SeedFile { path, collection } => self.seed_file(path, collection),
            Command::Backup(dest) => self.backup(dest),
            Command::Export(dir) => self.export(dir),
            Command::Import(dir) => self.import(dir),
            Command::Explain(query) => self.explain(&query),
            Command::ExplainAnalyze(query) => self.explain_analyze(&query),
            Command::Stats(name) => self.stats(name),
            Command::Sql(sql) => self.sql(&sql),
        }
    }

    /// The collection commands operate on, or an error prompting the user to
    /// select one.
    fn require_collection(&self) -> Result<&str, String> {
        self.current
            .as_deref()
            .ok_or_else(|| "no collection selected — use `.use <name>` or `.create <name>`".into())
    }

    fn list_collections(&self) -> Result<Output, String> {
        let pairs = self.db.list_collections().map_err(es)?;
        Ok(Output::Collections(pairs))
    }

    fn use_collection(&mut self, name: String) -> Result<Output, String> {
        let exists = self
            .db
            .list_collections()
            .map_err(es)?
            .iter()
            .any(|(_, n)| n == &name);
        if !exists {
            return Err(format!(
                "collection `{name}` does not exist — create it with `.create {name}`"
            ));
        }
        let msg = format!("using `{name}`");
        self.current = Some(name);
        Ok(Output::Message(msg))
    }

    fn create(&mut self, name: String) -> Result<Output, String> {
        let config = CollectionConfig {
            name,
            ..Default::default()
        };
        let txn = self.db.begin(false).map_err(es)?;
        txn.create_collection(&config).map_err(es)?;
        txn.commit().map_err(es)?;

        let name = config.name;
        let msg = format!("created `{name}` (now in use)");
        self.current = Some(name);
        Ok(Output::Message(msg))
    }

    fn drop(&mut self, name: String) -> Result<Output, String> {
        let txn = self.db.begin(false).map_err(es)?;
        txn.drop_collection(DEFAULT_CF, &name).map_err(es)?;
        txn.commit().map_err(es)?;

        if self.current.as_deref() == Some(name.as_str()) {
            self.current = None;
        }
        Ok(Output::Message(format!("dropped `{name}`")))
    }

    fn insert(&self, value: Value) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        let affected = match value {
            Value::Array(items) => {
                let docs = items
                    .into_iter()
                    .map(to_bson_document)
                    .collect::<Result<Vec<_>, _>>()?;
                txn.insert_many(DEFAULT_CF, collection, docs)
                    .map_err(es)?
                    .drain()
                    .map_err(es)?
            }
            object @ Value::Object(_) => txn
                .insert_one(DEFAULT_CF, collection, to_bson_document(object)?)
                .map_err(es)?
                .drain()
                .map_err(es)?,
            _ => return Err("insert expects a JSON object or array of objects".into()),
        };
        txn.commit().map_err(es)?;
        Ok(Output::Affected(affected))
    }

    fn update(&self, filter: Value, update: Value) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        let affected = txn
            .update_many(DEFAULT_CF, collection, filter, update)
            .map_err(es)?
            .drain()
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Affected(affected))
    }

    fn replace(&self, filter: Value, replacement: Value) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        let affected = txn
            .replace_one(DEFAULT_CF, collection, filter, replacement)
            .map_err(es)?
            .drain()
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Affected(affected))
    }

    fn delete(&self, filter: Value) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        let affected = txn
            .delete_many(DEFAULT_CF, collection, filter)
            .map_err(es)?
            .drain()
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Affected(affected))
    }

    fn distinct(&self, field: String, filter: Option<Value>) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let filter = filter.unwrap_or_else(|| json!({}));
        let txn = self.db.begin(true).map_err(es)?;
        let result = txn
            .distinct(
                DEFAULT_CF,
                collection,
                &field,
                filter,
                DistinctOptions::default(),
            )
            .map_err(es)?;
        txn.rollback().map_err(es)?;

        // `distinct` yields a BSON array of bare values; render one row per
        // value so the result reads like a query (and gets a row count).
        let rows = match Bson::try_from(result).map_err(es)? {
            Bson::Array(items) => items.into_iter().map(format::render_bson).collect(),
            other => vec![format::render_bson(other)],
        };
        Ok(Output::Rows(rows))
    }

    fn create_index(&self, fields: Vec<String>) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        // A single field uses the plain index API; multiple fields a compound
        // index. (The engine treats single-field as the one-component case, so
        // these converge — the split is just for the clearer canonical API.)
        match fields.as_slice() {
            [single] => txn
                .create_index(DEFAULT_CF, collection, single)
                .map_err(es)?,
            many => txn
                .create_compound_index(DEFAULT_CF, collection, many)
                .map_err(es)?,
        }
        txn.commit().map_err(es)?;
        let kind = if fields.len() == 1 {
            "index"
        } else {
            "compound index"
        };
        Ok(Output::Message(format!(
            "created {kind} on `{}`",
            fields.join(", ")
        )))
    }

    fn create_unique_index(&self, fields: Vec<String>) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        match fields.as_slice() {
            [single] => txn
                .create_unique_index(DEFAULT_CF, collection, single)
                .map_err(es)?,
            many => txn
                .create_unique_compound_index(DEFAULT_CF, collection, many)
                .map_err(es)?,
        }
        txn.commit().map_err(es)?;
        let kind = if fields.len() == 1 {
            "unique index"
        } else {
            "unique compound index"
        };
        Ok(Output::Message(format!(
            "created {kind} on `{}`",
            fields.join(", ")
        )))
    }

    fn drop_index(&self, fields: Vec<String>) -> Result<Output, String> {
        let collection = self.require_collection()?;
        // An index is keyed by its identity: a single field is itself; a compound
        // index joins its components. Reconstruct the identity from the fields.
        let identity = slate_db::join_index_fields(&fields);
        let txn = self.db.begin(false).map_err(es)?;
        txn.drop_index(DEFAULT_CF, collection, &identity)
            .map_err(es)?;
        txn.commit().map_err(es)?;
        let kind = if fields.len() == 1 {
            "index"
        } else {
            "compound index"
        };
        Ok(Output::Message(format!(
            "dropped {kind} on `{}`",
            fields.join(", ")
        )))
    }

    fn list_indexes(&self) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(true).map_err(es)?;
        let indexes = txn.list_indexes(DEFAULT_CF, collection).map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(Output::Indexes(indexes))
    }

    fn schema(&self, name: Option<String>) -> Result<Output, String> {
        let collection = match &name {
            Some(n) => n.as_str(),
            None => self.require_collection()?,
        };
        let txn = self.db.begin(true).map_err(es)?;
        let schema = txn.collection_schema(DEFAULT_CF, collection).map_err(es)?;
        let count = txn.count(DEFAULT_CF, collection, json!({})).map_err(es)?;
        txn.rollback().map_err(es)?;

        // Move each index field into an entry, flagging the ones in the unique
        // set — no clone, since `indexes` and `unique_indexes` are distinct
        // fields of the snapshot.
        let indexes = schema
            .indexes
            .into_iter()
            .map(|field| {
                let unique = schema.unique_indexes.iter().any(|u| u == &field);
                IndexEntry { field, unique }
            })
            .collect();
        Ok(Output::Schema(SchemaReport {
            collection: schema.name,
            pk_path: schema.pk_path,
            ttl_path: schema.ttl_path,
            indexes,
            count,
        }))
    }

    fn count(&self, filter: Option<Value>) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let filter = filter.unwrap_or_else(|| json!({}));
        let txn = self.db.begin(true).map_err(es)?;
        let n = txn.count(DEFAULT_CF, collection, filter).map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(Output::Count(n))
    }

    fn sql(&self, sql: &str) -> Result<Output, String> {
        // A FROM-less query (`SELECT VALUE 1`) reads no container, so it can run
        // with nothing selected. A parse error needs no container either — let
        // the engine surface it below rather than demanding a collection first.
        let needs_collection = matches!(slate_sql::parse(sql), Ok(query) if query.from.is_some());
        let collection = if needs_collection {
            self.require_collection()?
        } else {
            self.current.as_deref().unwrap_or("")
        };
        let txn = self.db.begin(true).map_err(es)?;
        let rows = {
            let cursor = txn.query(DEFAULT_CF, collection, sql).map_err(es)?;
            let mut rows = Vec::new();
            for item in cursor.iter_raw_values().map_err(es)? {
                rows.push(format::render_value(item.map_err(es)?)?);
            }
            rows
        };
        txn.rollback().map_err(es)?;
        Ok(Output::Rows(rows))
    }

    /// Lower a query to its physical plan and render the plan tree, without
    /// running it. Collection resolution matches [`sql`](Self::sql): a FROM-less
    /// query (`SELECT VALUE 1`) needs no active collection, while one with a
    /// `FROM` does.
    fn explain(&self, query: &str) -> Result<Output, String> {
        let needs_collection = matches!(slate_sql::parse(query), Ok(q) if q.from.is_some());
        let collection = if needs_collection {
            self.require_collection()?
        } else {
            self.current.as_deref().unwrap_or("")
        };
        let txn = self.db.begin(true).map_err(es)?;
        let plan = txn.explain(DEFAULT_CF, collection, query).map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(Output::Plan(plan))
    }

    /// Run a query and render its plan annotated with per-node execution stats
    /// (`.explain analyze`). Collection resolution matches [`explain`](Self::explain).
    fn explain_analyze(&self, query: &str) -> Result<Output, String> {
        let needs_collection = matches!(slate_sql::parse(query), Ok(q) if q.from.is_some());
        let collection = if needs_collection {
            self.require_collection()?
        } else {
            self.current.as_deref().unwrap_or("")
        };
        let txn = self.db.begin(true).map_err(es)?;
        let plan = txn
            .explain_analyze(DEFAULT_CF, collection, query)
            .map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(Output::Plan(plan))
    }

    /// Render size/cardinality statistics. `None` targets the whole database; a
    /// name targets one collection.
    fn stats(&self, name: Option<String>) -> Result<Output, String> {
        let txn = self.db.begin(true).map_err(es)?;
        let report = match &name {
            Some(collection) => {
                let s = txn.collection_stats(DEFAULT_CF, collection).map_err(es)?;
                render_collection_stats(&s)
            }
            None => {
                let s = txn.stats().map_err(es)?;
                render_database_stats(&s)
            }
        };
        txn.rollback().map_err(es)?;
        Ok(Output::Stats(report))
    }

    fn seed(&mut self) -> Result<Output, String> {
        const NAME: &str = "sample";
        let exists = self
            .db
            .list_collections()
            .map_err(es)?
            .iter()
            .any(|(_, n)| n == NAME);

        if !exists {
            let txn = self.db.begin(false).map_err(es)?;
            txn.create_collection(&CollectionConfig {
                name: NAME.to_string(),
                ..Default::default()
            })
            .map_err(es)?;
            txn.insert_many(DEFAULT_CF, NAME, seed_docs())
                .map_err(es)?
                .drain()
                .map_err(es)?;
            txn.commit().map_err(es)?;
        }

        self.current = Some(NAME.to_string());
        let note = if exists { " (already present)" } else { "" };
        Ok(Output::Message(format!(
            "collection `{NAME}` ready{note} — now in use; try `SELECT * FROM c;`"
        )))
    }

    /// Bulk-load a dataset file into `collection`, creating the collection if it
    /// does not exist and making it current. The whole load runs in one write
    /// transaction so a bad document (e.g. a duplicate `_id`, or a malformed
    /// line) rolls the import back instead of leaving it half-applied —
    /// returning early drops `txn` uncommitted, which discards every write.
    fn seed_file(&mut self, path: String, collection: String) -> Result<Output, String> {
        let path = Path::new(&path);
        let file = File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        let mut reader = BufReader::new(file);

        let exists = self
            .db
            .list_collections()
            .map_err(es)?
            .iter()
            .any(|(_, n)| n == &collection);

        let txn = self.db.begin(false).map_err(es)?;
        if !exists {
            txn.create_collection(&CollectionConfig {
                name: collection.clone(),
                ..Default::default()
            })
            .map_err(es)?;
        }

        // Insert one bounded batch, draining it so per-document errors (such as a
        // duplicate `_id`) surface here. Scoped before the `commit` below so its
        // borrow of `txn` is released in time.
        let flush = |batch: &[bson::Document]| -> Result<u64, String> {
            if batch.is_empty() {
                return Ok(0);
            }
            txn.insert_many(DEFAULT_CF, &collection, batch.iter())
                .map_err(es)?
                .drain()
                .map_err(es)
        };

        // Auto-detect the format from the first non-whitespace byte: `[` is a
        // single JSON array, `{` is JSONL/NDJSON.
        let count = match detect_format(&mut reader, path)? {
            // An array is one JSON value, so read it whole (it isn't line-oriented),
            // then insert it in bounded batches.
            Some(b'[') => {
                let mut text = String::new();
                reader
                    .read_to_string(&mut text)
                    .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
                let docs = parse_array(&text)?
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        to_bson_document(v).map_err(|e| format!("array element {i}: {e}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut count = 0u64;
                for chunk in docs.chunks(SEED_BATCH) {
                    count += flush(chunk)?;
                }
                count
            }
            // JSONL: stream line by line so a large file is never fully resident —
            // only one batch of parsed documents is held at a time.
            Some(b'{') => {
                let mut count = 0u64;
                let mut batch: Vec<bson::Document> = Vec::with_capacity(SEED_BATCH);
                for (i, line) in reader.lines().enumerate() {
                    let line = line.map_err(|e| {
                        format!("{}: read error on line {}: {e}", path.display(), i + 1)
                    })?;
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(trimmed)
                        .map_err(|e| format!("line {}: invalid JSON: {e}", i + 1))?;
                    if !value.is_object() {
                        return Err(format!("line {}: not a JSON object", i + 1));
                    }
                    let doc =
                        to_bson_document(value).map_err(|e| format!("line {}: {e}", i + 1))?;
                    batch.push(doc);
                    if batch.len() >= SEED_BATCH {
                        count += flush(&batch)?;
                        batch.clear();
                    }
                }
                count += flush(&batch)?;
                if count == 0 {
                    return Err(format!("{}: no documents found", path.display()));
                }
                count
            }
            Some(_) => {
                return Err(format!(
                    "{}: expected a JSON array (starting with `[`) or JSONL (one `{{...}}` per line)",
                    path.display()
                ));
            }
            None => return Err(format!("{}: no JSON documents found", path.display())),
        };

        txn.commit().map_err(es)?;

        // A clone so the name can both become the active collection and be
        // reported back in the output.
        self.current = Some(collection.clone());
        Ok(Output::Loaded { count, collection })
    }

    fn backup(&self, dest: String) -> Result<Output, String> {
        // Online passthrough to the store's physical backup. On the in-memory
        // backend this surfaces a clear "not supported" error rather than
        // pretending to succeed.
        self.db.backup(&dest).map_err(es)?;
        Ok(Output::Message(format!("backed up to `{dest}`")))
    }

    /// Write a logical dump of the whole database to `dir` (BSON manifest +
    /// per-collection document streams). Unlike `.backup` this is backend-neutral:
    /// the dump can be imported into a database on any backend.
    fn export(&self, dir: String) -> Result<Output, String> {
        let report = self.db.export(&dir, ExportOptions::default()).map_err(es)?;
        let docs = report.total_documents();
        let cols = report.collections.len();
        let dplural = if docs == 1 { "" } else { "s" };
        let cplural = if cols == 1 { "" } else { "s" };
        Ok(Output::Message(format!(
            "exported {docs} document{dplural} from {cols} collection{cplural} to `{dir}`"
        )))
    }

    /// Load a logical dump from `dir` into the database, recreating each
    /// collection (with its indexes) from the manifest and reloading its
    /// documents. A pre-existing `_id` aborts the import (the default
    /// collision-is-an-error policy), so an import into a fresh database is safe.
    fn import(&self, dir: String) -> Result<Output, String> {
        let report = self.db.import(&dir, ImportOptions::default()).map_err(es)?;
        let docs = report.total_documents();
        let cols = report.collections.len();
        let dplural = if docs == 1 { "" } else { "s" };
        let cplural = if cols == 1 { "" } else { "s" };
        Ok(Output::Message(format!(
            "imported {docs} document{dplural} into {cols} collection{cplural} from `{dir}`"
        )))
    }
}

/// A small Cosmos-flavoured dataset for `.seed`: nested objects and arrays so
/// functions, GROUP BY, and subqueries all have something to chew on.
fn seed_docs() -> Vec<Value> {
    vec![
        json!({ "_id": "1", "name": "ada", "age": 36, "city": "London",
                "tags": ["math", "logic"] }),
        json!({ "_id": "2", "name": "alan", "age": 41, "city": "London",
                "tags": ["computing"] }),
        json!({ "_id": "3", "name": "grace", "age": 44, "city": "New York",
                "tags": ["compilers", "navy"] }),
        json!({ "_id": "4", "name": "edsger", "age": 52, "city": "Austin",
                "tags": ["algorithms"] }),
    ]
}

/// Documents per `insert_many` batch when loading a file.
const SEED_BATCH: usize = 1000;

/// Coerce a JSON value into a BSON document, interpreting MongoDB **extended
/// JSON** — both canonical (`{"$date":{"$numberLong":"…"}}`) and relaxed
/// (`{"$oid":"…"}`, `{"$date":"…"}`) — so a `mongoexport` dump keeps its real
/// types (dates, ObjectIds, …) instead of being stored as nested `$`-keyed
/// sub-documents. This is what makes `.insert` and `.seed` round-trip Mongo data
/// faithfully; a plain serde serialization would not. Errors on non-objects and
/// on malformed extended JSON (e.g. a `$oid` that isn't 24 hex chars).
fn to_bson_document(value: Value) -> Result<bson::Document, String> {
    match Bson::try_from(value).map_err(es)? {
        Bson::Document(doc) => Ok(doc),
        _ => Err("expected a JSON object".to_string()),
    }
}

/// Peek the first non-whitespace byte of `reader` without consuming it, so the
/// format-specific reader still sees the whole stream (and JSONL line numbers
/// stay accurate). Returns `None` for an empty or whitespace-only file.
///
/// Only the first buffered chunk is inspected — enough for any real dataset,
/// whose opening `[`/`{` sits at the very start.
fn detect_format<R: BufRead>(reader: &mut R, path: &Path) -> Result<Option<u8>, String> {
    let buf = reader
        .fill_buf()
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    Ok(buf.iter().copied().find(|b| !b.is_ascii_whitespace()))
}

/// Parse a whole-file JSON array into its document elements, erroring (with the
/// element index) on anything that is not a JSON object.
fn parse_array(text: &str) -> Result<Vec<Value>, String> {
    let value: Value = serde_json::from_str(text).map_err(|e| format!("invalid JSON: {e}"))?;
    let items = match value {
        Value::Array(items) => items,
        _ => return Err("expected a JSON array of documents".to_string()),
    };
    for (i, item) in items.iter().enumerate() {
        if !item.is_object() {
            return Err(format!("array element {i} is not a JSON object"));
        }
    }
    if items.is_empty() {
        return Err("array contained no documents".to_string());
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slate_db::DatabaseBuilder;
    use slate_store::MemoryStore;

    fn session() -> Session<MemoryStore> {
        let db = DatabaseBuilder::new().open(MemoryStore::new()).unwrap();
        Session::new(db)
    }

    fn run(s: &mut Session<MemoryStore>, line: &str) -> Output {
        s.execute(Command::parse(line).unwrap()).unwrap()
    }

    #[test]
    fn create_sets_current_collection() {
        let mut s = session();
        assert_eq!(s.current(), None);
        let out = run(&mut s, ".create users");
        assert!(matches!(out, Output::Message(_)));
        assert_eq!(s.current(), Some("users"));
    }

    #[test]
    fn from_less_sql_runs_without_a_collection() {
        // `SELECT VALUE 1` needs no container — it works on a fresh session.
        let mut s = session();
        assert_eq!(s.current(), None);
        assert_eq!(
            run(&mut s, "SELECT VALUE 1 + 1"),
            Output::Rows(vec!["2".to_string()])
        );
    }

    #[test]
    fn commands_need_a_collection_first() {
        let mut s = session();
        let err = s.execute(Command::parse(".insert {\"a\":1}").unwrap());
        assert!(err.is_err());
        let err = s.execute(Command::parse("SELECT * FROM c").unwrap());
        assert!(err.is_err());
    }

    #[test]
    fn insert_and_query_roundtrip() {
        let mut s = session();
        run(&mut s, ".create people");
        let out = run(
            &mut s,
            r#".insert [{"_id":"1","name":"ada"},{"_id":"2","name":"alan"}]"#,
        );
        assert_eq!(out, Output::Affected(2));

        let out = run(&mut s, "SELECT VALUE c.name FROM c ORDER BY c.name");
        assert_eq!(
            out,
            Output::Rows(vec!["\"ada\"".to_string(), "\"alan\"".to_string()])
        );
    }

    #[test]
    fn insert_single_object() {
        let mut s = session();
        run(&mut s, ".create people");
        let out = run(&mut s, r#".insert {"_id":"1","name":"ada"}"#);
        assert_eq!(out, Output::Affected(1));
    }

    #[test]
    fn count_with_and_without_filter() {
        let mut s = session();
        run(&mut s, ".create people");
        run(
            &mut s,
            r#".insert [{"_id":"1","active":true},{"_id":"2","active":false},{"_id":"3","active":true}]"#,
        );
        assert_eq!(run(&mut s, ".count"), Output::Count(3));
        assert_eq!(run(&mut s, r#".count {"active":true}"#), Output::Count(2));
    }

    #[test]
    fn update_and_delete() {
        let mut s = session();
        run(&mut s, ".create people");
        run(
            &mut s,
            r#".insert [{"_id":"1","name":"ada","age":36},{"_id":"2","name":"alan","age":41}]"#,
        );
        let out = run(&mut s, r#".update {"name":"ada"} {"$set":{"age":37}}"#);
        assert_eq!(out, Output::Affected(1));

        let out = run(&mut s, "SELECT VALUE c.age FROM c WHERE c.name = 'ada'");
        assert_eq!(out, Output::Rows(vec!["37".to_string()]));

        let out = run(&mut s, r#".delete {"name":"alan"}"#);
        assert_eq!(out, Output::Affected(1));
        assert_eq!(run(&mut s, ".count"), Output::Count(1));
    }

    #[test]
    fn distinct_over_seed_with_and_without_filter() {
        let mut s = session();
        run(&mut s, ".seed");
        match run(&mut s, ".distinct city") {
            Output::Rows(rows) => {
                // London, New York, Austin — three distinct cities.
                assert_eq!(rows.len(), 3);
                assert!(rows.iter().any(|r| r == "\"London\""));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match run(&mut s, r#".distinct city {"age":{"$gt":42}}"#) {
            Output::Rows(rows) => {
                // grace (44, New York) and edsger (52, Austin).
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn replace_swaps_the_whole_document() {
        let mut s = session();
        run(&mut s, ".create people");
        run(&mut s, r#".insert {"_id":"1","name":"ada","age":36}"#);
        let out = run(
            &mut s,
            r#".replace {"_id":"1"} {"_id":"1","name":"ada lovelace"}"#,
        );
        assert_eq!(out, Output::Affected(1));

        // Replace is not a merge: the old `age` field is gone.
        match run(&mut s, "SELECT * FROM c") {
            Output::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0].contains("ada lovelace"));
                assert!(
                    !rows[0].contains("\"age\""),
                    "age should be gone: {}",
                    rows[0]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn unique_index_enforced_and_flagged_in_schema() {
        let mut s = session();
        run(&mut s, ".create people");
        run(
            &mut s,
            r#".insert [{"_id":"1","email":"a@x"},{"_id":"2","email":"b@x"}]"#,
        );
        assert!(matches!(
            run(&mut s, ".unique-index email"),
            Output::Message(_)
        ));
        match run(&mut s, ".schema") {
            Output::Schema(report) => assert!(
                report
                    .indexes
                    .iter()
                    .any(|ix| ix.field == "email" && ix.unique),
                "email index should be unique: {:?}",
                report.indexes
            ),
            other => panic!("expected schema, got {other:?}"),
        }
        // A duplicate email now violates the unique index.
        let dup = s.execute(Command::parse(r#".insert {"_id":"3","email":"a@x"}"#).unwrap());
        assert!(dup.is_err(), "duplicate insert should be rejected");
    }

    #[test]
    fn drop_index_removes_it() {
        let mut s = session();
        run(&mut s, ".create people");
        run(&mut s, ".index city");
        match run(&mut s, ".indexes") {
            Output::Indexes(fields) => assert!(fields.iter().any(|f| f == "city")),
            other => panic!("expected indexes, got {other:?}"),
        }
        run(&mut s, ".drop-index city");
        match run(&mut s, ".indexes") {
            Output::Indexes(fields) => assert!(!fields.iter().any(|f| f == "city")),
            other => panic!("expected indexes, got {other:?}"),
        }
    }

    #[test]
    fn compound_index_via_multi_arg() {
        let mut s = session();
        run(&mut s, ".create orders");
        // Multiple fields create a compound index; its identity joins the
        // components with the field separator (0x01).
        match run(&mut s, ".index status created_at") {
            Output::Message(m) => assert!(m.contains("compound index"), "got {m:?}"),
            other => panic!("expected message, got {other:?}"),
        }
        match run(&mut s, ".indexes") {
            Output::Indexes(fields) => assert!(
                fields.iter().any(|f| f == "status\u{1}created_at"),
                "compound identity missing: {fields:?}"
            ),
            other => panic!("expected indexes, got {other:?}"),
        }
        // A single field is still a plain (non-compound) index.
        match run(&mut s, ".index city") {
            Output::Message(m) => {
                assert!(
                    m.contains("index on") && !m.contains("compound"),
                    "got {m:?}"
                )
            }
            other => panic!("expected message, got {other:?}"),
        }
        // Drop the compound index by naming its component fields in order.
        match run(&mut s, ".drop-index status created_at") {
            Output::Message(m) => assert!(m.contains("compound index"), "got {m:?}"),
            other => panic!("expected message, got {other:?}"),
        }
        match run(&mut s, ".indexes") {
            Output::Indexes(fields) => assert!(
                !fields.iter().any(|f| f == "status\u{1}created_at"),
                "compound index should be gone: {fields:?}"
            ),
            other => panic!("expected indexes, got {other:?}"),
        }
    }

    #[test]
    fn list_collections_and_indexes() {
        let mut s = session();
        run(&mut s, ".create people");
        run(&mut s, ".index name");
        match run(&mut s, ".indexes") {
            Output::Indexes(fields) => assert!(fields.iter().any(|f| f == "name")),
            other => panic!("expected indexes, got {other:?}"),
        }
        match run(&mut s, ".collections") {
            Output::Collections(pairs) => {
                assert!(pairs.iter().any(|(_, n)| n == "people"))
            }
            other => panic!("expected collections, got {other:?}"),
        }
    }

    #[test]
    fn schema_reports_paths_indexes_and_count() {
        let mut s = session();
        run(&mut s, ".create people");
        run(
            &mut s,
            r#".insert [{"_id":"1","email":"a@x"},{"_id":"2","email":"b@x"}]"#,
        );
        run(&mut s, ".index email");
        match run(&mut s, ".schema people") {
            Output::Schema(report) => {
                assert_eq!(report.collection, "people");
                assert_eq!(report.pk_path, "_id");
                assert_eq!(report.ttl_path, "ttl");
                assert_eq!(report.count, 2);
                assert!(
                    report
                        .indexes
                        .iter()
                        .any(|ix| ix.field == "email" && !ix.unique),
                    "expected a non-unique email index: {:?}",
                    report.indexes
                );
            }
            other => panic!("expected schema, got {other:?}"),
        }
    }

    #[test]
    fn schema_without_arg_uses_active_collection() {
        let mut s = session();
        run(&mut s, ".seed");
        match run(&mut s, ".schema") {
            Output::Schema(report) => {
                assert_eq!(report.collection, "sample");
                assert_eq!(report.count, 4);
            }
            other => panic!("expected schema, got {other:?}"),
        }
    }

    #[test]
    fn schema_needs_a_collection() {
        let mut s = session();
        assert!(s.execute(Command::parse(".schema").unwrap()).is_err());
    }

    #[test]
    fn drop_clears_current() {
        let mut s = session();
        run(&mut s, ".create people");
        assert_eq!(s.current(), Some("people"));
        run(&mut s, ".drop people");
        assert_eq!(s.current(), None);
    }

    #[test]
    fn use_unknown_collection_errors() {
        let mut s = session();
        assert!(s.execute(Command::parse(".use nope").unwrap()).is_err());
    }

    #[test]
    fn backup_on_memory_backend_errors_clearly() {
        // The success path needs a persistent backend (verified manually); the
        // memory store rejects backup, and we surface that clearly rather than
        // claiming success.
        let mut s = session();
        let err = s
            .execute(Command::parse(".backup /tmp/slate-backup-test").unwrap())
            .unwrap_err();
        assert!(
            err.contains("backup") || err.contains("in-memory"),
            "unexpected backup error: {err}"
        );
    }

    #[test]
    fn export_then_import_round_trips_between_sessions() {
        // `.export` is logical, so unlike `.backup` it works on the in-memory
        // backend: dump one session and reload it into a fresh, independent one.
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().to_string_lossy().into_owned();

        let mut src = session();
        run(&mut src, ".create people");
        run(&mut src, ".index city");
        run(
            &mut src,
            r#".insert [{"_id":"1","name":"ada","city":"London"},{"_id":"2","name":"alan","city":"London"},{"_id":"3","name":"grace","city":"York"}]"#,
        );

        let out = run(&mut src, &format!(".export {dump}"));
        match out {
            Output::Message(m) => assert!(
                m.contains("exported 3 documents"),
                "unexpected export message: {m}"
            ),
            other => panic!("expected a message, got {other:?}"),
        }

        // Fresh session, empty database — import the dump.
        let mut dst = session();
        let out = run(&mut dst, &format!(".import {dump}"));
        match out {
            Output::Message(m) => assert!(
                m.contains("imported 3 documents"),
                "unexpected import message: {m}"
            ),
            other => panic!("expected a message, got {other:?}"),
        }

        // The documents — and the `city` index — survived the trip.
        run(&mut dst, ".use people");
        assert_eq!(run(&mut dst, ".count"), Output::Count(3));
        match run(
            &mut dst,
            "SELECT VALUE c.name FROM c WHERE c.city = 'London' ORDER BY c.name",
        ) {
            Output::Rows(rows) => {
                assert_eq!(rows, vec!["\"ada\"".to_string(), "\"alan\"".to_string()])
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn import_into_populated_collection_reports_collision() {
        // Default collision policy is error: a clashing `_id` aborts the import.
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().to_string_lossy().into_owned();

        let mut src = session();
        run(&mut src, ".create people");
        run(&mut src, r#".insert {"_id":"1","name":"ada"}"#);
        run(&mut src, &format!(".export {dump}"));

        let mut dst = session();
        run(&mut dst, ".create people");
        run(&mut dst, r#".insert {"_id":"1","name":"other"}"#);
        let err = dst
            .execute(Command::parse(&format!(".import {dump}")).unwrap())
            .unwrap_err();
        assert!(err.contains("duplicate"), "unexpected import error: {err}");
    }

    #[test]
    fn explain_indexed_eq_shows_index_scan_tree() {
        let mut s = session();
        run(&mut s, ".create people");
        run(&mut s, ".index name");
        run(&mut s, r#".insert {"_id":"1","name":"ada"}"#);
        match run(
            &mut s,
            r#".explain SELECT VALUE c.name FROM c WHERE c.name = "ada""#,
        ) {
            Output::Plan(plan) => {
                // The sargable equality on the indexed field plans to an index
                // scan resolved by a key lookup.
                assert!(plan.contains("IndexScan"), "expected an index scan: {plan}");
                assert!(plan.contains("KeyLookup"), "expected a key lookup: {plan}");
            }
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    #[test]
    fn explain_unindexed_predicate_is_a_filtered_scan() {
        let mut s = session();
        run(&mut s, ".create people");
        run(&mut s, r#".insert {"_id":"1","age":36}"#);
        match run(
            &mut s,
            ".explain SELECT VALUE c.age FROM c WHERE c.age = 36",
        ) {
            Output::Plan(plan) => {
                assert!(
                    !plan.contains("IndexScan"),
                    "an unindexed predicate must not index-scan: {plan}"
                );
                assert!(plan.contains("Scan"), "expected a scan: {plan}");
                assert!(
                    plan.contains("Filter c.age = 36"),
                    "expected the residual filter: {plan}"
                );
            }
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    #[test]
    fn explain_from_less_query_needs_no_collection() {
        // Like running `SELECT VALUE 1`, explaining it works on a fresh session.
        let mut s = session();
        match run(&mut s, ".explain SELECT VALUE 1 + 1") {
            Output::Plan(plan) => assert!(!plan.is_empty(), "expected a plan tree"),
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    #[test]
    fn explain_with_from_requires_a_collection() {
        let mut s = session();
        assert!(
            s.execute(Command::parse(".explain SELECT VALUE c.name FROM c").unwrap())
                .is_err()
        );
    }

    #[test]
    fn seed_then_aggregate() {
        let mut s = session();
        run(&mut s, ".seed");
        assert_eq!(s.current(), Some("sample"));
        // GROUP BY produces one row per city.
        match run(
            &mut s,
            "SELECT c.city, COUNT(1) AS n FROM c GROUP BY c.city",
        ) {
            Output::Rows(rows) => assert_eq!(rows.len(), 3),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn seed_is_idempotent() {
        let mut s = session();
        run(&mut s, ".seed");
        run(&mut s, ".seed");
        assert_eq!(run(&mut s, ".count"), Output::Count(4));
    }

    /// Absolute path to a checked-in test fixture.
    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn seed_file_loads_jsonl_and_is_queryable() {
        let mut s = session();
        // Drive it through the parser so the file-stem collection name and the
        // whole-loader path are both exercised end to end.
        let out = s
            .execute(Command::parse(&format!(".seed {}", fixture("movies.jsonl"))).unwrap())
            .unwrap();
        assert_eq!(
            out,
            Output::Loaded {
                count: 3,
                collection: "movies".to_string()
            }
        );
        // The loaded collection becomes active and the documents are visible.
        assert_eq!(s.current(), Some("movies"));
        assert_eq!(run(&mut s, ".count"), Output::Count(3));
        match run(
            &mut s,
            "SELECT VALUE c.title FROM c WHERE c.year < 1940 ORDER BY c.year",
        ) {
            Output::Rows(rows) => assert_eq!(
                rows,
                vec!["\"Metropolis\"".to_string(), "\"Modern Times\"".to_string()]
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn seed_file_loads_json_array_and_is_queryable() {
        let mut s = session();
        let out = s
            .execute(Command::SeedFile {
                path: fixture("cities.json"),
                collection: "cities".to_string(),
            })
            .unwrap();
        assert_eq!(
            out,
            Output::Loaded {
                count: 2,
                collection: "cities".to_string()
            }
        );
        match run(&mut s, "SELECT VALUE c.name FROM c ORDER BY c.name") {
            Output::Rows(rows) => {
                assert_eq!(
                    rows,
                    vec!["\"Austin\"".to_string(), "\"London\"".to_string()]
                )
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn seed_file_streams_multiple_batches() {
        // More documents than one batch, written to a temp JSONL file, to drive
        // the streaming multi-batch path (`SEED_BATCH` is 1000).
        let n = SEED_BATCH * 2 + 5;
        let mut content = String::new();
        for i in 0..n {
            content.push_str(&format!("{{\"_id\":\"{i}\",\"v\":{i}}}\n"));
        }
        let path = std::env::temp_dir().join("slate_seed_batches.jsonl");
        std::fs::write(&path, content).unwrap();

        let mut s = session();
        let out = s
            .execute(Command::SeedFile {
                path: path.to_string_lossy().into_owned(),
                collection: "batched".to_string(),
            })
            .unwrap();
        assert_eq!(
            out,
            Output::Loaded {
                count: n as u64,
                collection: "batched".to_string()
            }
        );
        assert_eq!(run(&mut s, ".count"), Output::Count(n as u64));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn seed_file_line_numbers_account_for_leading_blanks() {
        // Leading blank lines must not shift the reported line number — the bad
        // document sits on line 4 of the file.
        let mut s = session();
        let err = s
            .execute(Command::SeedFile {
                path: fixture("leading_blanks.jsonl"),
                collection: "lb".to_string(),
            })
            .unwrap_err();
        assert!(
            err.contains("line 4"),
            "error should name the true line: {err}"
        );
    }

    #[test]
    fn seed_file_reports_a_malformed_line_position() {
        let mut s = session();
        let err = s
            .execute(Command::SeedFile {
                path: fixture("malformed.jsonl"),
                collection: "broken".to_string(),
            })
            .unwrap_err();
        assert!(err.contains("line 2"), "error should name the line: {err}");
        // The import stopped before touching the database — nothing was created.
        assert_eq!(s.current(), None);
        match run(&mut s, ".collections") {
            Output::Collections(pairs) => {
                assert!(!pairs.iter().any(|(_, n)| n == "broken"))
            }
            other => panic!("expected collections, got {other:?}"),
        }
    }

    #[test]
    fn insert_interprets_extended_json_types() {
        let mut s = session();
        run(&mut s, ".create events");
        run(
            &mut s,
            r#".insert {"_id":"1","at":{"$date":{"$numberLong":"1256616000000"}}}"#,
        );
        match run(&mut s, "SELECT VALUE c.at FROM c") {
            Output::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                // A real BSON DateTime renders back as a relaxed-extjson ISO
                // string, not the canonical nested `$numberLong` it was loaded
                // from — proving the type was interpreted, not stored verbatim.
                assert!(
                    rows[0].contains("2009-10"),
                    "expected an ISO date: {}",
                    rows[0]
                );
                assert!(
                    !rows[0].contains("$numberLong"),
                    "date was not converted: {}",
                    rows[0]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn seed_file_interprets_extended_json() {
        let mut s = session();
        s.execute(Command::parse(&format!(".seed {}", fixture("events.jsonl"))).unwrap())
            .unwrap();
        // The `$oid` `_id`s round-trip as ObjectIds and the `$date`s as real
        // dates, so a date-ordered projection comes back as ISO strings.
        match run(&mut s, "SELECT VALUE c.at FROM c ORDER BY c.at") {
            Output::Rows(rows) => {
                assert_eq!(rows.len(), 2);
                assert!(
                    rows.iter().all(|r| !r.contains("$numberLong")),
                    "dates were not converted: {rows:?}"
                );
                assert!(
                    rows[0].contains("2009-10"),
                    "expected an ISO date: {}",
                    rows[0]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // `_id` is a genuine ObjectId, rendered in relaxed `$oid` form.
        match run(&mut s, "SELECT VALUE c._id FROM c ORDER BY c._id") {
            Output::Rows(rows) => {
                assert!(
                    rows[0].contains("$oid"),
                    "expected an ObjectId: {}",
                    rows[0]
                );
                assert!(rows[0].contains("0123456789abcdef01234567"));
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
}
