mod encoding;
mod error;
mod index_sync;
mod kv;
mod traits;
mod validate;
mod vector;

pub use encoding::key::{join_index_fields, split_index_fields};
pub use error::{EncodingError, EngineError};
pub use kv::{DEFAULT_CF, IntegrityIssue, IntegrityReport, KvEngine};
pub use traits::{
    Catalog, CollectionHandle, CompoundRange, CompoundTail, CreateCollectionOptions, Engine,
    EngineTransaction, FunctionEntry, FunctionKind, IndexEntry, IndexOptions, IndexRange,
    IndexSpec, VectorScanEntry, runtime_tag,
};
pub use vector::{VectorDataType, VectorIndexSpec, VectorMetric};
