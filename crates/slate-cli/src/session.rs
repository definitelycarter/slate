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

use bson::Bson;
use serde_json::{Value, json};

use slate_db::{CollectionConfig, DEFAULT_CF, Database, DistinctOptions};
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
            Command::CreateIndex(field) => self.create_index(field),
            Command::CreateUniqueIndex(field) => self.create_unique_index(field),
            Command::DropIndex(field) => self.drop_index(field),
            Command::ListIndexes => self.list_indexes(),
            Command::Count(filter) => self.count(filter),
            Command::Schema(name) => self.schema(name),
            Command::Seed => self.seed(),
            Command::SeedFile { path, collection } => self.seed_file(path, collection),
            Command::Backup(dest) => self.backup(dest),
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
            Value::Array(items) => txn
                .insert_many(DEFAULT_CF, collection, items)
                .map_err(es)?
                .drain()
                .map_err(es)?,
            object @ Value::Object(_) => txn
                .insert_one(DEFAULT_CF, collection, object)
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

    fn create_index(&self, field: String) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        txn.create_index(DEFAULT_CF, collection, &field)
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Message(format!("created index on `{field}`")))
    }

    fn create_unique_index(&self, field: String) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        txn.create_unique_index(DEFAULT_CF, collection, &field)
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Message(format!(
            "created unique index on `{field}`"
        )))
    }

    fn drop_index(&self, field: String) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        txn.drop_index(DEFAULT_CF, collection, &field).map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Message(format!("dropped index on `{field}`")))
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
    /// transaction so a bad document (e.g. a duplicate `_id`) rolls back the
    /// import instead of leaving it half-applied.
    fn seed_file(&mut self, path: String, collection: String) -> Result<Output, String> {
        let docs = load_documents(std::path::Path::new(&path))?;

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

        // Insert in bounded batches so a huge file isn't one giant `insert_many`,
        // draining each batch to surface per-document errors as they happen.
        let mut count = 0u64;
        for chunk in docs.chunks(SEED_BATCH) {
            count += txn
                .insert_many(DEFAULT_CF, &collection, chunk.iter())
                .map_err(es)?
                .drain()
                .map_err(es)?;
        }
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

/// Read a dataset file and parse it into a list of JSON documents, auto-detecting
/// the format from the first non-whitespace byte: `[` is a single JSON array of
/// documents, `{` is JSONL/NDJSON (one document per line, blank lines skipped —
/// `mongoexport`'s default). On a malformed document the error names its position
/// (array index or 1-based line number) and the load stops rather than importing
/// part of the file.
fn load_documents(path: &std::path::Path) -> Result<Vec<Value>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    match text.trim_start().as_bytes().first() {
        Some(b'[') => parse_array(&text),
        Some(b'{') => parse_jsonl(&text),
        Some(_) => Err(format!(
            "{}: expected a JSON array (starting with `[`) or JSONL (one `{{...}}` per line)",
            path.display()
        )),
        None => Err(format!("{} is empty", path.display())),
    }
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

/// Parse JSONL/NDJSON text into documents, one per non-blank line. Errors name
/// the 1-based line number on a parse failure or a non-object line.
fn parse_jsonl(text: &str) -> Result<Vec<Value>, String> {
    let mut docs = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value =
            serde_json::from_str(line).map_err(|e| format!("line {}: invalid JSON: {e}", i + 1))?;
        if !value.is_object() {
            return Err(format!("line {}: not a JSON object", i + 1));
        }
        docs.push(value);
    }
    if docs.is_empty() {
        return Err("no documents found".to_string());
    }
    Ok(docs)
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
}
