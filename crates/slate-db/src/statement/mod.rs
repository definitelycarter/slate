use bson::RawDocumentBuf;
use slate_query::{Sort, SortDirection};

use crate::mutation::Mutation;

use crate::expression::Expression;

pub enum Statement<'a> {
    Find {
        cf: &'a str,
        collection: &'a str,
        predicate: Expression,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        projection: Option<Vec<String>>,
    },
    Distinct {
        cf: &'a str,
        collection: &'a str,
        field: String,
        predicate: Expression,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    },
    Insert {
        cf: &'a str,
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
    Update {
        cf: &'a str,
        collection: &'a str,
        predicate: Expression,
        mutation: Mutation,
        limit: Option<usize>,
    },
    Replace {
        cf: &'a str,
        collection: &'a str,
        predicate: Expression,
        replacement: RawDocumentBuf,
    },
    Delete {
        cf: &'a str,
        collection: &'a str,
        predicate: Expression,
        limit: Option<usize>,
    },
    Merge {
        cf: &'a str,
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
    Upsert {
        cf: &'a str,
        collection: &'a str,
        docs: Vec<RawDocumentBuf>,
    },
}
