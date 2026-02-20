uniffi::setup_scaffolding!();

mod db;
mod error;

pub use db::SlateDatabase;
pub use error::SlateError;
