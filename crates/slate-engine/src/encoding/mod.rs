pub mod bson_value;
pub mod index_record;
pub mod key;
pub mod numeric_key;
pub mod record;

pub use index_record::IndexRecord;
pub use key::{Key, KeyPrefix};
pub use record::Record;

// Raw byte-level BSON primitives (skip_bson_value, RawField, for_each_path_value,
// raw_merge) live in the leaf `slate-rawbson` crate; this module imports them
// directly where needed rather than re-exporting.
