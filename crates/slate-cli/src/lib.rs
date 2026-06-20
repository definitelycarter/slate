//! Core of the `slate` interactive shell, split out from the binary so it can be
//! unit-tested headlessly (and, later, reused behind a wasm playground).
//!
//! Three pieces, deliberately decoupled from any terminal I/O:
//! - [`command`] parses a line of input into a [`Command`].
//! - [`session`] runs a [`Command`] against a live [`slate_db::Database`],
//!   returning a semantic [`Output`] rather than printing.
//! - [`format`] renders BSON result values as human-readable JSON.
//!
//! The binary (`main.rs`) is just argument parsing plus a `rustyline` loop that
//! threads input through these three.

pub mod command;
pub mod format;
pub mod session;

pub use command::Command;
pub use session::{Output, Session};
