//! The evaluation value domain.
//!
//! [`Value`] now lives in the standalone `slate-value` crate so the function
//! crates (`slate-udf`, and later `slate-trigger` / `slate-validator`) can speak
//! it without depending on the evaluator. It is re-exported here so
//! `slate_eval::Value` and `slate_eval::value::Value` are unchanged.

pub use slate_value::Value;
