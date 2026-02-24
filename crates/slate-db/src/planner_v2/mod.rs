mod expression;
mod parse_filter;
mod plan_node;
mod statement;

pub use expression::{Expression, LogicalOp};
pub use plan_node::{PlanNode, UpsertMode};
pub use statement::Statement;

use std::borrow::Cow;

use slate_query::{Mutation, Sort, SortDirection};
use slate_store::Transaction;

use crate::catalog::Catalog;
use crate::error::DbError;

const SYS_CF: &str = "_sys";

pub struct Planner<'a, T: Transaction> {
    txn: &'a T,
    catalog: &'a Catalog,
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn new(txn: &'a T, catalog: &'a Catalog) -> Self {
        Self { txn, catalog }
    }

    /// Fetch indexed fields for a collection from the catalog.
    fn indexed_fields(&self, collection: &str) -> Result<Vec<String>, DbError> {
        let sys = self.txn.cf(SYS_CF)?;
        self.catalog.list_indexes(self.txn, &sys, collection)
    }

    pub fn plan(&self, collection: &str, statement: Statement) -> Result<PlanNode, DbError> {
        let collection = Cow::Borrowed(collection);
        match statement {
            Statement::Find {
                filter,
                sort,
                skip,
                take,
                columns,
            } => self.plan_find(collection, filter, sort, skip, take, columns),
            Statement::Distinct {
                field,
                filter,
                sort,
                skip,
                take,
            } => self.plan_distinct(collection, field, filter, sort, skip, take),
            Statement::Update {
                filter,
                mutation,
                limit,
            } => self.plan_update(collection, filter, mutation, limit),
            Statement::Replace {
                filter,
                replacement,
            } => self.plan_replace(collection, filter, replacement),
            Statement::Delete { filter, limit } => self.plan_delete(collection, filter, limit),
            Statement::Insert { docs } => self.plan_insert(collection, docs),
            Statement::UpsertMany { docs } => self.plan_upsert(collection, docs),
            Statement::MergeMany { docs } => self.plan_merge(collection, docs),
            Statement::FlushExpired { filter } => self.plan_flush_expired(collection, filter),
        }
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// Parse a RawDocumentBuf filter into an Expression.
    fn parse_filter(doc: &bson::RawDocumentBuf) -> Result<Expression, DbError> {
        parse_filter::parse_filter(doc).map_err(|e| DbError::InvalidQuery(e.to_string()))
    }

    /// Parse an optional filter.
    fn parse_optional_filter(
        doc: Option<&bson::RawDocumentBuf>,
    ) -> Result<Option<Expression>, DbError> {
        match doc {
            Some(d) => Self::parse_filter(d).map(Some),
            None => Ok(None),
        }
    }

    // ── Find ────────────────────────────────────────────────────

    fn plan_find(
        &self,
        collection: Cow<'_, str>,
        filter: Option<bson::RawDocumentBuf>,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;
        let expr = Self::parse_optional_filter(filter.as_ref())?;

        // Step 1: Plan the filter — split into ID-tier node + residual predicate
        let (id_node, residual_filter) = match &expr {
            Some(e) => plan_filter(&indexed_fields, e),
            None => (PlanNode::Scan, None),
        };

        let id_is_scan = matches!(id_node, PlanNode::Scan);
        let has_residual_filter = residual_filter.is_some();

        // Step 2: Covered projection — IndexScan with Eq can yield docs directly
        let no_residual = !has_residual_filter;
        let covered = no_residual
            && matches!(
                &id_node,
                PlanNode::IndexScan {
                    column,
                    filter: Some(Expression::Eq(_, _)),
                    ..
                } if columns.as_ref().is_some_and(|cols|
                    cols.iter().all(|c| c == "_id" || c == column)
                )
            );

        let mut id_node = id_node;
        if covered {
            if let PlanNode::IndexScan {
                covered: ref mut c, ..
            } = id_node
            {
                *c = true;
            }
        }

        let node = if covered {
            id_node
        } else {
            // Scan yields full documents; IndexScan/IndexMerge yield bare IDs
            let node = if id_is_scan {
                id_node
            } else {
                PlanNode::ReadRecord {
                    input: Box::new(id_node),
                }
            };

            match residual_filter {
                Some(expr) => PlanNode::Filter {
                    predicate: expr,
                    input: Box::new(node),
                },
                None => node,
            }
        };

        // Step 3: Sort
        let can_use_indexed_sort = !sort.is_empty()
            && take.is_some()
            && indexed_fields.contains(&sort[0].field)
            && id_is_scan;

        let node = if can_use_indexed_sort && sort.len() == 1 {
            let index_limit = if !has_residual_filter {
                Some(skip.unwrap_or(0) + take.unwrap_or(0))
            } else {
                None
            };
            replace_scan_with_index_order(
                node,
                &sort[0].field,
                sort[0].direction,
                index_limit,
                false,
            )
        } else if can_use_indexed_sort {
            let index_limit = if !has_residual_filter {
                Some(skip.unwrap_or(0) + take.unwrap_or(0))
            } else {
                None
            };
            let node = replace_scan_with_index_order(
                node,
                &sort[0].field,
                sort[0].direction,
                index_limit,
                true,
            );
            PlanNode::Sort {
                sorts: sort.clone(),
                input: Box::new(node),
            }
        } else if !sort.is_empty() {
            PlanNode::Sort {
                sorts: sort.clone(),
                input: Box::new(node),
            }
        } else {
            node
        };

        // Step 4: Limit
        let node = if skip.is_some() || take.is_some() {
            PlanNode::Limit {
                skip: skip.unwrap_or(0),
                take,
                input: Box::new(node),
            }
        } else {
            node
        };

        // Step 5: Projection
        Ok(if covered {
            node
        } else {
            PlanNode::Projection {
                columns,
                input: Box::new(node),
            }
        })
    }

    // ── Distinct ────────────────────────────────────────────────

    fn plan_distinct(
        &self,
        collection: Cow<'_, str>,
        field: String,
        filter: Option<bson::RawDocumentBuf>,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;
        let expr = Self::parse_optional_filter(filter.as_ref())?;

        // Step 1: Plan the filter
        let (id_node, residual_filter) = match &expr {
            Some(e) => plan_filter(&indexed_fields, e),
            None => (PlanNode::Scan, None),
        };

        // Step 2: ReadRecord (only for index paths)
        let node = if matches!(id_node, PlanNode::Scan) {
            id_node
        } else {
            PlanNode::ReadRecord {
                input: Box::new(id_node),
            }
        };

        // Step 3: Residual filter
        let node = match residual_filter {
            Some(expr) => PlanNode::Filter {
                predicate: expr,
                input: Box::new(node),
            },
            None => node,
        };

        // Step 4: Projection — extract the distinct field
        let node = PlanNode::Projection {
            columns: Some(vec![field.clone()]),
            input: Box::new(node),
        };

        // Step 5: Distinct
        let node = PlanNode::Distinct {
            field: field.clone(),
            input: Box::new(node),
        };

        // Step 6: Sort
        let node = match sort {
            Some(dir) => PlanNode::Sort {
                sorts: vec![Sort {
                    field: field.clone(),
                    direction: dir,
                }],
                input: Box::new(node),
            },
            None => node,
        };

        // Step 7: Limit
        let node = match (skip, take) {
            (None, None) => node,
            (skip, take) => PlanNode::Limit {
                skip: skip.unwrap_or(0),
                take,
                input: Box::new(node),
            },
        };

        Ok(node)
    }

    // ── Mutation plan builders ──────────────────────────────────

    fn plan_update(
        &self,
        collection: Cow<'_, str>,
        filter: bson::RawDocumentBuf,
        mutation: Mutation,
        limit: Option<usize>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;
        let expr = Self::parse_filter(&filter)?;
        let source = build_filtered_source(&indexed_fields, &expr, limit);

        let node = PlanNode::DeleteIndex {
            indexed_fields: indexed_fields.clone(),
            input: Box::new(source),
        };

        let node = PlanNode::Update {
            mutation,
            input: Box::new(node),
        };

        Ok(PlanNode::InsertIndex {
            indexed_fields,
            input: Box::new(node),
        })
    }

    fn plan_replace(
        &self,
        collection: Cow<'_, str>,
        filter: bson::RawDocumentBuf,
        replacement: bson::RawDocumentBuf,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;
        let expr = Self::parse_filter(&filter)?;
        let source = build_filtered_source(&indexed_fields, &expr, Some(1));

        let node = PlanNode::DeleteIndex {
            indexed_fields: indexed_fields.clone(),
            input: Box::new(source),
        };

        let node = PlanNode::Replace {
            replacement,
            input: Box::new(node),
        };

        Ok(PlanNode::InsertIndex {
            indexed_fields,
            input: Box::new(node),
        })
    }

    fn plan_delete(
        &self,
        collection: Cow<'_, str>,
        filter: bson::RawDocumentBuf,
        limit: Option<usize>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;
        let expr = Self::parse_filter(&filter)?;
        let source = build_filtered_source(&indexed_fields, &expr, limit);

        let node = PlanNode::DeleteIndex {
            indexed_fields,
            input: Box::new(source),
        };

        Ok(PlanNode::Delete {
            input: Box::new(node),
        })
    }

    fn plan_insert(
        &self,
        collection: Cow<'_, str>,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;

        let node = PlanNode::Values { docs };
        let node = PlanNode::InsertRecord {
            input: Box::new(node),
        };

        Ok(PlanNode::InsertIndex {
            indexed_fields,
            input: Box::new(node),
        })
    }

    fn plan_upsert(
        &self,
        collection: Cow<'_, str>,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;

        let node = PlanNode::Values { docs };
        let node = PlanNode::Upsert {
            mode: UpsertMode::Replace,
            indexed_fields: indexed_fields.clone(),
            input: Box::new(node),
        };

        Ok(PlanNode::InsertIndex {
            indexed_fields,
            input: Box::new(node),
        })
    }

    fn plan_merge(
        &self,
        collection: Cow<'_, str>,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<PlanNode, DbError> {
        let indexed_fields = self.indexed_fields(&collection)?;

        let node = PlanNode::Values { docs };
        let node = PlanNode::Upsert {
            mode: UpsertMode::Merge,
            indexed_fields: indexed_fields.clone(),
            input: Box::new(node),
        };

        Ok(PlanNode::InsertIndex {
            indexed_fields,
            input: Box::new(node),
        })
    }

    fn plan_flush_expired(
        &self,
        collection: Cow<'_, str>,
        filter: bson::RawDocumentBuf,
    ) -> Result<PlanNode, DbError> {
        self.plan_delete(collection, filter, None)
    }
}

// ── Free functions ──────────────────────────────────────────────

/// Build the read source for a mutation: ReadRecord → Filter → Scan/IndexScan.
fn build_filtered_source(
    indexed_fields: &[String],
    filter: &Expression,
    take: Option<usize>,
) -> PlanNode {
    let (id_node, residual_filter) = plan_filter(indexed_fields, filter);

    let node = if matches!(id_node, PlanNode::Scan) {
        id_node
    } else {
        PlanNode::ReadRecord {
            input: Box::new(id_node),
        }
    };

    let node = match residual_filter {
        Some(expr) => PlanNode::Filter {
            predicate: expr,
            input: Box::new(node),
        },
        None => node,
    };

    match take {
        Some(n) => PlanNode::Limit {
            skip: 0,
            take: Some(n),
            input: Box::new(node),
        },
        None => node,
    }
}

/// Replace the Scan node inside a plan subtree with an ordered IndexScan.
fn replace_scan_with_index_order(
    node: PlanNode,
    sort_field: &str,
    direction: SortDirection,
    limit: Option<usize>,
    complete_groups: bool,
) -> PlanNode {
    match node {
        PlanNode::Scan => PlanNode::ReadRecord {
            input: Box::new(PlanNode::IndexScan {
                column: sort_field.to_string(),
                filter: None,
                direction,
                limit,
                complete_groups,
                covered: false,
            }),
        },
        PlanNode::Filter { predicate, input } => PlanNode::Filter {
            predicate,
            input: Box::new(replace_scan_with_index_order(
                *input,
                sort_field,
                direction,
                limit,
                complete_groups,
            )),
        },
        other => other,
    }
}

// ── Expression-based filter planning ────────────────────────────

/// Plan a filter expression, returning an ID-tier node and optional residual.
fn plan_filter(
    indexed_fields: &[String],
    expr: &Expression,
) -> (PlanNode, Option<Expression>) {
    match expr {
        Expression::And(children) => plan_and_children(indexed_fields, children, expr),
        Expression::Or(children) => plan_or_children(indexed_fields, children, expr),
        other => plan_single_condition(indexed_fields, other, expr),
    }
}

/// Plan a single (non-And, non-Or) expression.
fn plan_single_condition(
    indexed_fields: &[String],
    expr: &Expression,
    original: &Expression,
) -> (PlanNode, Option<Expression>) {
    if let Some(node) = try_index_condition(indexed_fields, expr) {
        (node, None)
    } else {
        (PlanNode::Scan, Some(original.clone()))
    }
}

/// Try to convert a single condition into an IndexScan node.
fn try_index_condition(
    indexed_fields: &[String],
    expr: &Expression,
) -> Option<PlanNode> {
    let field = match expr {
        Expression::Eq(f, _)
        | Expression::Gt(f, _)
        | Expression::Gte(f, _)
        | Expression::Lt(f, _)
        | Expression::Lte(f, _) => f,
        _ => return None,
    };

    if !indexed_fields.contains(field) {
        return None;
    }

    Some(PlanNode::IndexScan {
        column: field.clone(),
        filter: Some(expr.clone()),
        direction: SortDirection::Asc,
        limit: None,
        complete_groups: false,
        covered: false,
    })
}

/// Plan an AND group of children.
fn plan_and_children(
    indexed_fields: &[String],
    children: &[Expression],
    original: &Expression,
) -> (PlanNode, Option<Expression>) {
    let best = find_best_and_child(indexed_fields, children);

    match best {
        Some((id_node, consumed_indices)) => {
            let remaining: Vec<Expression> = children
                .iter()
                .enumerate()
                .filter(|(i, _)| !consumed_indices.contains(i))
                .map(|(_, c)| c.clone())
                .collect();

            let residual = if remaining.is_empty() {
                None
            } else if remaining.len() == 1 {
                Some(remaining.into_iter().next().unwrap())
            } else {
                Some(Expression::And(remaining))
            };

            (id_node, residual)
        }
        None => (PlanNode::Scan, Some(original.clone())),
    }
}

/// Find the best AND child to push into the ID tier.
///
/// Priority: Eq > OR sub-groups > range conditions.
fn find_best_and_child(
    indexed_fields: &[String],
    children: &[Expression],
) -> Option<(PlanNode, Vec<usize>)> {
    // Priority 1: Eq conditions (most selective)
    for field in indexed_fields {
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(f, _) = child {
                if f == field {
                    let node = PlanNode::IndexScan {
                        column: f.clone(),
                        filter: Some(child.clone()),
                        direction: SortDirection::Asc,
                        limit: None,
                        complete_groups: false,
                        covered: false,
                    };
                    return Some((node, vec![i]));
                }
            }
        }
    }

    // Priority 2: fully-indexable OR sub-groups
    for (_i, child) in children.iter().enumerate() {
        if let Expression::Or(or_children) = child {
            if let Some(id_node) = try_or_index_merge(indexed_fields, or_children) {
                return Some((id_node, vec![]));
            }
        }
    }

    // Priority 3: range conditions on indexed fields
    for field in indexed_fields {
        let mut lower: Option<(usize, &Expression)> = None;
        let mut upper: Option<(usize, &Expression)> = None;

        for (i, child) in children.iter().enumerate() {
            match child {
                Expression::Gt(f, _) | Expression::Gte(f, _) if f == field => {
                    lower = Some((i, child));
                }
                Expression::Lt(f, _) | Expression::Lte(f, _) if f == field => {
                    upper = Some((i, child));
                }
                _ => {}
            }
        }

        if lower.is_some() || upper.is_some() {
            let (filter_expr, consumed) = match (lower, upper) {
                (Some((li, lower_expr)), Some((ui, upper_expr))) => {
                    // Both bounds → And(lower, upper)
                    let expr = Expression::And(vec![
                        lower_expr.clone(),
                        upper_expr.clone(),
                    ]);
                    (expr, vec![li, ui])
                }
                (Some((li, expr)), None) => (expr.clone(), vec![li]),
                (None, Some((ui, expr))) => (expr.clone(), vec![ui]),
                (None, None) => unreachable!(),
            };

            let node = PlanNode::IndexScan {
                column: field.clone(),
                filter: Some(filter_expr),
                direction: SortDirection::Asc,
                limit: None,
                complete_groups: false,
                covered: false,
            };
            return Some((node, consumed));
        }
    }

    None
}

/// Plan an OR group of children.
fn plan_or_children(
    indexed_fields: &[String],
    children: &[Expression],
    original: &Expression,
) -> (PlanNode, Option<Expression>) {
    match try_or_index_merge(indexed_fields, children) {
        Some(id_node) => (id_node, Some(original.clone())),
        None => (PlanNode::Scan, Some(original.clone())),
    }
}

/// Try to build an IndexMerge(Or) from OR children.
fn try_or_index_merge(
    indexed_fields: &[String],
    children: &[Expression],
) -> Option<PlanNode> {
    let mut id_nodes: Vec<PlanNode> = Vec::new();

    for child in children {
        match child {
            Expression::Eq(field, _) if indexed_fields.iter().any(|f| f == field) => {
                id_nodes.push(PlanNode::IndexScan {
                    column: field.clone(),
                    filter: Some(child.clone()),
                    direction: SortDirection::Asc,
                    limit: None,
                    complete_groups: false,
                    covered: false,
                });
            }
            Expression::And(sub_children) => {
                let (id_node, _residual) =
                    plan_and_children(indexed_fields, sub_children, child);
                if matches!(id_node, PlanNode::Scan) {
                    return None;
                }
                id_nodes.push(id_node);
            }
            Expression::Or(sub_children) => {
                match try_or_index_merge(indexed_fields, sub_children) {
                    Some(node) => id_nodes.push(node),
                    None => return None,
                }
            }
            _ => return None,
        }
    }

    if id_nodes.is_empty() {
        return None;
    }

    if id_nodes.len() == 1 {
        return Some(id_nodes.into_iter().next().unwrap());
    }

    // Fold into binary tree of IndexMerge(Or)
    let mut iter = id_nodes.into_iter();
    let mut result = iter.next().unwrap();
    for node in iter {
        result = PlanNode::IndexMerge {
            logical: LogicalOp::Or,
            lhs: Box::new(result),
            rhs: Box::new(node),
        };
    }

    Some(result)
}
