//! `slate-query` — the MongoDB-style query **front-end**.
//!
//! One of two query surfaces (the other being SQL text in `slate-sql`). It owns
//! the find DTOs ([`FindOptions`], [`Sort`]) and translates a find request — a
//! `$`-operator filter document plus those options — into the shared
//! [`slate_ast::Query`], which `slate-planner` lowers like any other query. The
//! filter translation ([`translate_filter`]) is the single definition of what a
//! Mongo filter means, reused by the write APIs that select documents with it.

mod error;
mod query;
mod sort;
mod translate;

pub use error::TranslateError;
pub use query::{DistinctOptions, FindOptions};
pub use sort::{Sort, SortDirection};
pub use translate::{ALIAS, find_to_query, translate_filter};
