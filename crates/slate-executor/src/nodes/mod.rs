//! Per-node executors.
//!
//! Each module exposes an `execute(...)` returning a [`crate::ValueIter`]. The
//! dispatcher in the crate root recurses children first, then hands each node
//! executor its already-built input, so node files deal only in iterators —
//! never `Node`/`Plan`. Two shapes:
//!
//! - **Source** nodes produce a stream from data or storage and take no input
//!   iterator: `Values`, `Scan`, `IndexScan`.
//! - **Transform** nodes take their source iterator(s) and return one: `Bind`,
//!   `Unwind`, `Project`, `Filter`, `Sort`, `Limit`, `KeyLookup`, `Distinct`.
//!   `IndexMerge` is the binary one — two inputs, still iterator-in /
//!   iterator-out. `Sort`/`IndexMerge` are *blocking* (buffer their sources),
//!   so their `execute` is fallible, as are the storage sources.
//!
//! The binding-aware nodes (`Bind`, `Unwind`, `Filter`, `Project`, `Sort`)
//! operate on *environment documents* — see [`env`].

pub(crate) mod aggregate;
pub(crate) mod bind;
pub(crate) mod delete;
pub(crate) mod distinct;
pub(crate) mod env;
pub(crate) mod filter;
pub(crate) mod index_merge;
pub(crate) mod index_scan;
pub(crate) mod insert;
pub(crate) mod key_lookup;
pub(crate) mod limit;
pub(crate) mod mutate;
pub(crate) mod project;
pub(crate) mod replace;
pub(crate) mod scan;
pub(crate) mod sort;
pub(crate) mod subquery;
pub(crate) mod trigger;
pub(crate) mod unwind;
pub(crate) mod upsert;
pub(crate) mod validate;
pub(crate) mod values;

#[cfg(test)]
pub(crate) mod test_support;
