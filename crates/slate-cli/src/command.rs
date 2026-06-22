//! Input grammar for the shell.
//!
//! A line is either a *meta-command* (dot-prefixed, sqlite-style — `.use`,
//! `.insert`, …) or a SQL statement (anything else, passed verbatim to the
//! engine). Meta-command arguments that are documents are parsed as a stream of
//! JSON values, so `.update {"a":1} {"$set":{"b":2}}` cleanly yields two values
//! even though each contains spaces.

use serde_json::Value;

/// A parsed line of shell input.
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Blank line — does nothing.
    Empty,
    /// Print the help text.
    Help,
    /// Leave the shell.
    Quit,
    /// List every collection.
    ListCollections,
    /// Set the collection that SQL and document commands operate on.
    Use(String),
    /// Create a collection (and make it current).
    Create(String),
    /// Drop a collection.
    Drop(String),
    /// Insert one document (JSON object) or many (JSON array of objects).
    Insert(Value),
    /// Update every document matching `filter` with `update` (Mongo operators).
    Update { filter: Value, update: Value },
    /// Replace the first document matching `filter` with `replacement` (no merge).
    Replace { filter: Value, replacement: Value },
    /// Delete every document matching `filter`.
    Delete { filter: Value },
    /// Distinct values of a field, optionally restricted by `filter`.
    Distinct {
        field: String,
        filter: Option<Value>,
    },
    /// Create an index on a field of the current collection.
    CreateIndex(String),
    /// Create a unique index on a field of the current collection.
    CreateUniqueIndex(String),
    /// Drop an index on a field of the current collection.
    DropIndex(String),
    /// List indexes on the current collection.
    ListIndexes,
    /// Count documents in the current collection, optionally filtered.
    Count(Option<Value>),
    /// Show a collection's schema (key paths, indexes, count). `None` targets
    /// the active collection.
    Schema(Option<String>),
    /// Load a small sample collection and make it current.
    Seed,
    /// Bulk-load documents from a file into `collection` (defaulting to the
    /// file stem) and make that collection current.
    SeedFile { path: String, collection: String },
    /// Back up the database to a directory (persistent backends only).
    Backup(String),
    /// Show the physical plan for a query without running it.
    Explain(String),
    /// A SQL statement to run against the current collection.
    Sql(String),
}

impl Command {
    /// Parse a single line of input. Returns an error string for malformed
    /// meta-commands; unknown non-dot input is treated as SQL and only fails
    /// later, at the engine.
    pub fn parse(line: &str) -> Result<Command, String> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(Command::Empty);
        }
        match trimmed.strip_prefix('.') {
            Some(rest) => parse_meta(rest),
            None => Ok(Command::Sql(trimmed.to_string())),
        }
    }
}

fn parse_meta(rest: &str) -> Result<Command, String> {
    let (head, args) = match rest.split_once(char::is_whitespace) {
        Some((h, a)) => (h, a.trim()),
        None => (rest, ""),
    };
    match head.to_ascii_lowercase().as_str() {
        "help" | "h" | "?" => Ok(Command::Help),
        "quit" | "exit" | "q" => Ok(Command::Quit),
        "collections" | "tables" | "ls" => Ok(Command::ListCollections),
        "use" => Ok(Command::Use(name_arg(args, "use")?)),
        "create" => Ok(Command::Create(name_arg(args, "create")?)),
        "drop" => Ok(Command::Drop(name_arg(args, "drop")?)),
        "index" => Ok(Command::CreateIndex(name_arg(args, "index")?)),
        "unique-index" => Ok(Command::CreateUniqueIndex(name_arg(args, "unique-index")?)),
        "drop-index" => Ok(Command::DropIndex(name_arg(args, "drop-index")?)),
        "indexes" => Ok(Command::ListIndexes),
        "schema" => {
            if args.trim().is_empty() {
                Ok(Command::Schema(None))
            } else {
                Ok(Command::Schema(Some(name_arg(args, "schema")?)))
            }
        }
        "seed" => {
            // No argument keeps the built-in sample loader. Otherwise the whole
            // remaining argument is the dataset path (paths may contain spaces,
            // like `.backup`); the collection defaults to the sanitized file
            // stem since a trailing override can't be told apart from a path
            // that contains spaces.
            let path = args.trim();
            if path.is_empty() {
                Ok(Command::Seed)
            } else {
                let collection = collection_from_path(path);
                Ok(Command::SeedFile {
                    path: path.to_string(),
                    collection,
                })
            }
        }
        "backup" => {
            // Take the whole remaining argument as the path so destinations
            // containing spaces still work.
            let path = args.trim();
            if path.is_empty() {
                return Err(".backup requires a destination path".to_string());
            }
            Ok(Command::Backup(path.to_string()))
        }
        "explain" => {
            // The whole remaining line is the query (like `.backup`'s path) —
            // it contains spaces and its own syntax, so it is not tokenized.
            let query = args.trim();
            if query.is_empty() {
                return Err(".explain requires a query".to_string());
            }
            Ok(Command::Explain(query.to_string()))
        }
        "insert" => {
            let [doc] = exactly(
                parse_json_values(args)?,
                "insert expects exactly one JSON value",
            )?;
            Ok(Command::Insert(doc))
        }
        "update" => {
            let [filter, update] = exactly(
                parse_json_values(args)?,
                "update expects <filter-json> <update-json>",
            )?;
            Ok(Command::Update { filter, update })
        }
        "replace" => {
            let [filter, replacement] = exactly(
                parse_json_values(args)?,
                "replace expects <filter-json> <replacement-json>",
            )?;
            Ok(Command::Replace {
                filter,
                replacement,
            })
        }
        "delete" => {
            let [filter] = exactly(parse_json_values(args)?, "delete expects <filter-json>")?;
            Ok(Command::Delete { filter })
        }
        "distinct" => {
            // `<field> [filter-json]` — the field is the first bare word, the
            // rest (if any) is a single JSON filter.
            let (field, rest) = match args.split_once(char::is_whitespace) {
                Some((f, r)) => (f, r.trim()),
                None => (args, ""),
            };
            if field.is_empty() {
                return Err(".distinct requires a field".to_string());
            }
            let filter = if rest.is_empty() {
                None
            } else {
                let [filter] = exactly(
                    parse_json_values(rest)?,
                    "distinct expects <field> [filter-json]",
                )?;
                Some(filter)
            };
            Ok(Command::Distinct {
                field: field.to_string(),
                filter,
            })
        }
        "count" => {
            let values = parse_json_values(args)?;
            match values.len() {
                0 => Ok(Command::Count(None)),
                1 => {
                    let [filter] = exactly(values, "count expects at most one filter")?;
                    Ok(Command::Count(Some(filter)))
                }
                _ => Err("count expects at most one filter".to_string()),
            }
        }
        other => Err(format!("unknown command `.{other}` (try `.help`)")),
    }
}

/// A single bare-word name argument (collection name or field path).
fn name_arg(args: &str, cmd: &str) -> Result<String, String> {
    let name = args.trim();
    if name.is_empty() {
        return Err(format!(".{cmd} requires a name"));
    }
    if name.split_whitespace().count() != 1 {
        return Err(format!(".{cmd} takes a single name"));
    }
    Ok(name.to_string())
}

/// Derive a collection name from a dataset file path: the file stem with every
/// character outside `[A-Za-z0-9_]` replaced by `_`, so `movies.json` → `movies`
/// and `2024 sales.jsonl` → `2024_sales`. Falls back to `seeded` when the stem is
/// empty or has no usable characters.
fn collection_from_path(path: &str) -> String {
    let stem = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let sanitized: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "seeded".to_string()
    } else {
        sanitized
    }
}

/// Parse zero or more whitespace-separated JSON values.
fn parse_json_values(s: &str) -> Result<Vec<Value>, String> {
    let stream = serde_json::Deserializer::from_str(s).into_iter::<Value>();
    let mut out = Vec::new();
    for value in stream {
        out.push(value.map_err(|e| format!("invalid JSON: {e}"))?);
    }
    Ok(out)
}

/// Convert a `Vec` to a fixed-size array, erroring with `msg` on a length
/// mismatch — avoids panic-prone indexing while keeping the arity explicit.
fn exactly<const N: usize>(values: Vec<Value>, msg: &str) -> Result<[Value; N], String> {
    <[Value; N]>::try_from(values).map_err(|_| msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn blank_is_empty() {
        assert_eq!(Command::parse("   ").unwrap(), Command::Empty);
    }

    #[test]
    fn non_dot_is_sql() {
        assert_eq!(
            Command::parse("SELECT * FROM c").unwrap(),
            Command::Sql("SELECT * FROM c".to_string())
        );
    }

    #[test]
    fn meta_keywords_are_case_insensitive() {
        assert_eq!(Command::parse(".QUIT").unwrap(), Command::Quit);
        assert_eq!(Command::parse(".Help").unwrap(), Command::Help);
    }

    #[test]
    fn aliases() {
        assert_eq!(Command::parse(".q").unwrap(), Command::Quit);
        assert_eq!(Command::parse(".ls").unwrap(), Command::ListCollections);
        assert_eq!(Command::parse(".tables").unwrap(), Command::ListCollections);
    }

    #[test]
    fn use_and_create_take_a_name() {
        assert_eq!(
            Command::parse(".use users").unwrap(),
            Command::Use("users".to_string())
        );
        assert_eq!(
            Command::parse(".create users").unwrap(),
            Command::Create("users".to_string())
        );
        assert!(Command::parse(".use").is_err());
        assert!(Command::parse(".use a b").is_err());
    }

    #[test]
    fn insert_object_and_array() {
        assert_eq!(
            Command::parse(r#".insert {"name":"ada"}"#).unwrap(),
            Command::Insert(json!({"name": "ada"}))
        );
        assert_eq!(
            Command::parse(r#".insert [{"a":1},{"a":2}]"#).unwrap(),
            Command::Insert(json!([{"a": 1}, {"a": 2}]))
        );
        assert!(Command::parse(".insert").is_err());
        assert!(Command::parse(r#".insert {"a":1} {"b":2}"#).is_err());
    }

    #[test]
    fn update_takes_two_documents() {
        assert_eq!(
            Command::parse(r#".update {"name":"ada"} {"$set":{"age":37}}"#).unwrap(),
            Command::Update {
                filter: json!({"name": "ada"}),
                update: json!({"$set": {"age": 37}}),
            }
        );
        assert!(Command::parse(r#".update {"a":1}"#).is_err());
    }

    #[test]
    fn delete_takes_one_document() {
        assert_eq!(
            Command::parse(r#".delete {"name":"ada"}"#).unwrap(),
            Command::Delete {
                filter: json!({"name": "ada"})
            }
        );
        assert!(Command::parse(".delete").is_err());
    }

    #[test]
    fn count_filter_is_optional() {
        assert_eq!(Command::parse(".count").unwrap(), Command::Count(None));
        assert_eq!(
            Command::parse(r#".count {"active":true}"#).unwrap(),
            Command::Count(Some(json!({"active": true})))
        );
    }

    #[test]
    fn index_commands() {
        assert_eq!(
            Command::parse(".index email").unwrap(),
            Command::CreateIndex("email".to_string())
        );
        assert_eq!(Command::parse(".indexes").unwrap(), Command::ListIndexes);
    }

    #[test]
    fn replace_takes_two_documents() {
        assert_eq!(
            Command::parse(r#".replace {"_id":"1"} {"_id":"1","name":"ada"}"#).unwrap(),
            Command::Replace {
                filter: json!({"_id": "1"}),
                replacement: json!({"_id": "1", "name": "ada"}),
            }
        );
        assert!(Command::parse(r#".replace {"a":1}"#).is_err());
    }

    #[test]
    fn distinct_field_and_optional_filter() {
        assert_eq!(
            Command::parse(".distinct city").unwrap(),
            Command::Distinct {
                field: "city".to_string(),
                filter: None,
            }
        );
        assert_eq!(
            Command::parse(r#".distinct city {"age":{"$gt":40}}"#).unwrap(),
            Command::Distinct {
                field: "city".to_string(),
                filter: Some(json!({"age": {"$gt": 40}})),
            }
        );
        assert!(Command::parse(".distinct").is_err());
    }

    #[test]
    fn unique_and_drop_index_take_a_field() {
        assert_eq!(
            Command::parse(".unique-index email").unwrap(),
            Command::CreateUniqueIndex("email".to_string())
        );
        assert_eq!(
            Command::parse(".drop-index email").unwrap(),
            Command::DropIndex("email".to_string())
        );
        assert!(Command::parse(".unique-index").is_err());
        assert!(Command::parse(".drop-index a b").is_err());
    }

    #[test]
    fn schema_arg_is_optional() {
        assert_eq!(Command::parse(".schema").unwrap(), Command::Schema(None));
        assert_eq!(
            Command::parse(".schema users").unwrap(),
            Command::Schema(Some("users".to_string()))
        );
        assert!(Command::parse(".schema a b").is_err());
    }

    #[test]
    fn backup_takes_a_path() {
        assert_eq!(
            Command::parse(".backup /tmp/snap").unwrap(),
            Command::Backup("/tmp/snap".to_string())
        );
        // Paths with spaces are preserved.
        assert_eq!(
            Command::parse(".backup /tmp/my snap").unwrap(),
            Command::Backup("/tmp/my snap".to_string())
        );
        assert!(Command::parse(".backup").is_err());
    }

    #[test]
    fn seed_without_path_loads_the_builtin_sample() {
        assert_eq!(Command::parse(".seed").unwrap(), Command::Seed);
    }

    #[test]
    fn seed_with_path_defaults_collection_to_file_stem() {
        assert_eq!(
            Command::parse(".seed /data/movies.json").unwrap(),
            Command::SeedFile {
                path: "/data/movies.json".to_string(),
                collection: "movies".to_string(),
            }
        );
    }

    #[test]
    fn seed_path_keeps_spaces_and_sanitizes_the_stem() {
        // The whole argument is the path; the derived name replaces non-word
        // characters in the stem (here, the space) with `_`.
        assert_eq!(
            Command::parse(".seed /tmp/2024 sales.jsonl").unwrap(),
            Command::SeedFile {
                path: "/tmp/2024 sales.jsonl".to_string(),
                collection: "2024_sales".to_string(),
            }
        );
    }

    #[test]
    fn explain_takes_the_whole_query() {
        // The remaining line is one query, spaces and all — not tokenized.
        assert_eq!(
            Command::parse(r#".explain SELECT VALUE c.name FROM c WHERE c.name = "x""#).unwrap(),
            Command::Explain(r#"SELECT VALUE c.name FROM c WHERE c.name = "x""#.to_string())
        );
        assert!(Command::parse(".explain").is_err());
    }

    #[test]
    fn unknown_meta_errors() {
        assert!(Command::parse(".frobnicate").is_err());
    }

    #[test]
    fn invalid_json_errors() {
        assert!(Command::parse(".insert {not json}").is_err());
    }
}
