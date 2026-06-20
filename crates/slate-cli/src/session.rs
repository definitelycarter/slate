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

use serde_json::{Value, json};

use slate_db::{CollectionConfig, DEFAULT_CF, Database};
use slate_store::Store;

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
    /// A count result.
    Count(u64),
    /// Rendered query result rows (already JSON-formatted).
    Rows(Vec<String>),
    /// `(cf, name)` pairs for every collection.
    Collections(Vec<(String, String)>),
    /// Indexed fields of the current collection.
    Indexes(Vec<String>),
    /// Show the help text.
    Help,
    /// Leave the shell.
    Quit,
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

impl<S: Store> Session<S> {
    pub fn new(db: Database<S>) -> Self {
        Self { db, current: None }
    }

    /// The collection SQL and document commands currently target, if any.
    pub fn current(&self) -> Option<&str> {
        self.current.as_deref()
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
            Command::Delete { filter } => self.delete(filter),
            Command::CreateIndex(field) => self.create_index(field),
            Command::ListIndexes => self.list_indexes(),
            Command::Count(filter) => self.count(filter),
            Command::Seed => self.seed(),
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

    fn create_index(&self, field: String) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(false).map_err(es)?;
        txn.create_index(DEFAULT_CF, collection, &field)
            .map_err(es)?;
        txn.commit().map_err(es)?;
        Ok(Output::Message(format!("created index on `{field}`")))
    }

    fn list_indexes(&self) -> Result<Output, String> {
        let collection = self.require_collection()?;
        let txn = self.db.begin(true).map_err(es)?;
        let indexes = txn.list_indexes(DEFAULT_CF, collection).map_err(es)?;
        txn.rollback().map_err(es)?;
        Ok(Output::Indexes(indexes))
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
            "collection `{NAME}` ready{note} — now in use; try `SELECT * FROM c`"
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
}
