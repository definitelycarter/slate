use bson::RawDocumentBuf;
use slate_query::{Sort, SortDirection};

use crate::mutation::Mutation;

use crate::expression::Expression;

pub enum Statement<'a> {
    Find {
        collection: &'a str,
        predicate: Expression,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        projection: Option<Vec<String>>,
    },
    Distinct {
        collection: &'a str,
        field: String,
        predicate: Expression,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    },
    Insert {
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
    Update {
        collection: &'a str,
        predicate: Expression,
        mutation: Mutation,
        limit: Option<usize>,
    },
    Replace {
        collection: &'a str,
        predicate: Expression,
        replacement: RawDocumentBuf,
    },
    Delete {
        collection: &'a str,
        predicate: Expression,
        limit: Option<usize>,
    },
    Merge {
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
    Upsert {
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
}
