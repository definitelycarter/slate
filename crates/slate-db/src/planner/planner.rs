use slate_engine::CollectionHandle;
use slate_query::{Sort, SortDirection};

use crate::error::DbError;
use crate::expression::{Expression, LogicalOp};
use crate::statement::Statement;

use super::plan::{IndexScanRange, Node, Plan, ScanDirection};

pub struct Planner<F> {
    resolve: F,
}

impl<Cf: Clone, F: Fn(&str) -> Result<CollectionHandle<Cf>, DbError>> Planner<F> {
    pub fn new(resolve: F) -> Self {
        Self { resolve }
    }

    pub fn plan(&self, stmt: Statement<'_>) -> Result<Plan<Cf>, DbError> {
        match stmt {
            Statement::Find {
                collection,
                predicate,
                sort,
                skip,
                take,
                projection: columns,
            } => self.plan_find(collection, &predicate, sort, skip, take, columns),
            Statement::Distinct {
                collection,
                field,
                predicate,
                sort,
                skip,
                take,
            } => self.plan_distinct(collection, field, &predicate, sort, skip, take),
            Statement::Insert { collection, docs } => self.plan_insert(collection, docs),
            Statement::Update {
                collection,
                predicate,
                mutation,
                limit,
            } => self.plan_update(collection, &predicate, mutation, limit),
            Statement::Replace {
                collection,
                predicate,
                replacement,
            } => self.plan_replace(collection, &predicate, replacement),
            Statement::Delete {
                collection,
                predicate,
                limit,
            } => self.plan_delete(collection, &predicate, limit),
            Statement::Merge { collection, docs } => self.plan_merge(collection, docs),
            Statement::Upsert { collection, docs } => self.plan_upsert(collection, docs),
        }
    }

    // ── Find ────────────────────────────────────────────────────

    fn plan_find(
        &self,
        collection: &str,
        predicate: &Expression,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;
        let (source, residual) = self.plan_source(&handle, predicate);

        let source_is_scan = matches!(source, Node::Scan { .. });
        let has_residual = residual.is_some();

        // Covered index optimization: Eq on indexed field, no residual,
        // and projection only needs _id + the indexed field.
        let covered = !has_residual
            && matches!(
                &source,
                Node::IndexScan {
                    field,
                    range: IndexScanRange::Eq(_),
                    ..
                } if columns.as_ref().is_some_and(|cols|
                    cols.iter().all(|c| c == "_id" || c == field)
                )
            );

        // Mark the IndexScan as covered if applicable.
        let source = if covered {
            match source {
                Node::IndexScan {
                    collection: c,
                    field: f,
                    range: r,
                    direction: d,
                    limit: l,
                    ..
                } => Node::IndexScan {
                    collection: c,
                    field: f,
                    range: r,
                    direction: d,
                    limit: l,
                    covered: true,
                },
                other => other,
            }
        } else {
            source
        };

        // KeyLookup for index paths (unless covered or already a KeyLookup).
        let source_is_key_lookup = matches!(source, Node::KeyLookup { .. });
        let node = if covered || source_is_scan || source_is_key_lookup {
            source
        } else {
            Node::KeyLookup {
                collection: handle.clone(),
                source: Box::new(source),
            }
        };

        // Residual filter.
        let node = match residual {
            Some(expr) => Node::Filter {
                predicate: expr,
                source: Box::new(node),
            },
            None => node,
        };

        // Sort — try to use an index-ordered scan when possible.
        let can_use_indexed_sort = !sort.is_empty()
            && take.is_some()
            && handle.indexes.contains(&sort[0].field)
            && source_is_scan;

        let node = if can_use_indexed_sort && sort.len() == 1 {
            let index_limit = if !has_residual {
                Some(skip.unwrap_or(0) + take.unwrap_or(0))
            } else {
                None
            };
            replace_scan_with_index_order(
                node,
                &handle,
                &sort[0].field,
                sort[0].direction,
                index_limit,
            )
        } else if can_use_indexed_sort {
            // Multi-field sort: use index for first field, then Sort for the rest.
            let index_limit = if !has_residual {
                Some(skip.unwrap_or(0) + take.unwrap_or(0))
            } else {
                None
            };
            let node = replace_scan_with_index_order(
                node,
                &handle,
                &sort[0].field,
                sort[0].direction,
                index_limit,
            );
            Node::Sort {
                sorts: sort,
                source: Box::new(node),
            }
        } else if !sort.is_empty() {
            Node::Sort {
                sorts: sort,
                source: Box::new(node),
            }
        } else {
            node
        };

        // Limit.
        let node = if skip.is_some() || take.is_some() {
            Node::Limit {
                skip: skip.unwrap_or(0),
                take,
                source: Box::new(node),
            }
        } else {
            node
        };

        // Projection (skip if covered — already yields only needed fields).
        let node = if covered {
            node
        } else {
            Node::Projection {
                columns,
                source: Box::new(node),
            }
        };

        Ok(Plan::Find(node))
    }

    // ── Distinct ──────────────────────────────────────────────────

    fn plan_distinct(
        &self,
        collection: &str,
        field: String,
        predicate: &Expression,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;
        let (source, residual) = self.plan_source(&handle, predicate);

        // KeyLookup for index paths (skip if already a KeyLookup).
        let node = if matches!(source, Node::Scan { .. } | Node::KeyLookup { .. }) {
            source
        } else {
            Node::KeyLookup {
                collection: handle.clone(),
                source: Box::new(source),
            }
        };

        // Residual filter.
        let node = match residual {
            Some(expr) => Node::Filter {
                predicate: expr,
                source: Box::new(node),
            },
            None => node,
        };

        // Project to just the distinct field.
        let node = Node::Projection {
            columns: Some(vec![field.clone()]),
            source: Box::new(node),
        };

        // Distinct.
        let node = Node::Distinct {
            field: field.clone(),
            source: Box::new(node),
        };

        // Sort.
        let node = match sort {
            Some(dir) => Node::Sort {
                sorts: vec![Sort {
                    field: field.clone(),
                    direction: dir,
                }],
                source: Box::new(node),
            },
            None => node,
        };

        // Limit.
        let node = match (skip, take) {
            (None, None) => node,
            _ => Node::Limit {
                skip: skip.unwrap_or(0),
                take,
                source: Box::new(node),
            },
        };

        Ok(Plan::Find(node))
    }

    // ── Insert ──────────────────────────────────────────────────

    fn plan_insert(
        &self,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;

        Ok(Plan::Insert {
            collection: handle,
            source: Node::Values(docs),
        })
    }

    // ── Update ──────────────────────────────────────────────────

    fn plan_update(
        &self,
        collection: &str,
        predicate: &Expression,
        mutation: crate::mutation::Mutation,
        limit: Option<usize>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;
        let source = self.plan_read_source(&handle, predicate, limit);

        Ok(Plan::Update {
            collection: handle,
            mutation,
            source,
        })
    }

    // ── Replace ─────────────────────────────────────────────────

    fn plan_replace(
        &self,
        collection: &str,
        predicate: &Expression,
        replacement: bson::RawDocumentBuf,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;
        let source = self.plan_read_source(&handle, predicate, Some(1));

        Ok(Plan::Replace {
            collection: handle,
            replacement,
            source,
        })
    }

    // ── Delete ──────────────────────────────────────────────────

    fn plan_delete(
        &self,
        collection: &str,
        predicate: &Expression,
        limit: Option<usize>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;
        let source = self.plan_read_source(&handle, predicate, limit);

        Ok(Plan::Delete {
            collection: handle,
            source,
        })
    }

    // ── Merge ───────────────────────────────────────────────────

    fn plan_merge(
        &self,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;

        Ok(Plan::Merge {
            collection: handle,
            source: Node::Values(docs),
        })
    }

    // ── Upsert ──────────────────────────────────────────────────

    fn plan_upsert(
        &self,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<Cf>, DbError> {
        let handle = (self.resolve)(collection)?;

        Ok(Plan::Upsert {
            collection: handle,
            source: Node::Values(docs),
        })
    }

    // ── Shared planning helpers ─────────────────────────────────

    /// Build a source that yields full documents for mutation operations:
    /// plan_source → KeyLookup (if index) → Filter (if residual) → Limit.
    fn plan_read_source(
        &self,
        handle: &CollectionHandle<Cf>,
        predicate: &Expression,
        limit: Option<usize>,
    ) -> Node<Cf> {
        let (source, residual) = self.plan_source(handle, predicate);

        let source = if matches!(source, Node::Scan { .. } | Node::KeyLookup { .. }) {
            source
        } else {
            Node::KeyLookup {
                collection: handle.clone(),
                source: Box::new(source),
            }
        };

        let source = match residual {
            Some(expr) => Node::Filter {
                predicate: expr,
                source: Box::new(source),
            },
            None => source,
        };

        match limit {
            Some(n) => Node::Limit {
                skip: 0,
                take: Some(n),
                source: Box::new(source),
            },
            None => source,
        }
    }

    // ── Filter planning ─────────────────────────────────────────

    /// Decide whether to use an index or fall back to a full scan.
    fn plan_source(
        &self,
        handle: &CollectionHandle<Cf>,
        predicate: &Expression,
    ) -> (Node<Cf>, Option<Expression>) {
        // Fast path: _id equality → direct key lookup, no scan needed.
        if let Expression::Eq(field, value) = predicate
            && field == "_id"
        {
            return (Self::id_lookup(handle, value), None);
        }

        match predicate {
            Expression::And(children) => self.plan_and(handle, children, predicate),
            Expression::Or(children) => self.plan_or(handle, children, predicate),
            _ => self.plan_single(handle, predicate),
        }
    }

    /// Try to push a single condition into an IndexScan.
    fn plan_single(
        &self,
        handle: &CollectionHandle<Cf>,
        predicate: &Expression,
    ) -> (Node<Cf>, Option<Expression>) {
        if let Some(node) = self.try_index_scan(handle, predicate) {
            (node, None)
        } else {
            (
                Node::Scan {
                    collection: handle.clone(),
                },
                Some(predicate.clone()),
            )
        }
    }

    /// Plan an AND: find the best indexed child, leave the rest as residual.
    fn plan_and(
        &self,
        handle: &CollectionHandle<Cf>,
        children: &[Expression],
        original: &Expression,
    ) -> (Node<Cf>, Option<Expression>) {
        // Priority 0: _id equality — direct key lookup, always wins.
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(field, value) = child
                && field == "_id"
            {
                let node = Self::id_lookup(handle, value);
                let residual = residual_from_and(children, &[i]);
                return (node, residual);
            }
        }

        // Priority 1: Eq on an indexed field (most selective).
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(field, _) = child
                && handle.indexes.contains(field)
            {
                let node = self.try_index_scan(handle, child).unwrap();
                let residual = residual_from_and(children, &[i]);
                return (node, residual);
            }
        }

        // Priority 2: Fully-indexable OR sub-groups.
        for (i, child) in children.iter().enumerate() {
            if let Expression::Or(or_children) = child
                && let Some(node) = self.try_or_index_merge(handle, or_children)
            {
                let residual = residual_from_and(children, &[i]);
                return (node, residual);
            }
        }

        // Priority 3: Range conditions on an indexed field.
        for field in &handle.indexes {
            let mut lower_idx = None;
            let mut upper_idx = None;

            for (i, child) in children.iter().enumerate() {
                match child {
                    Expression::Gt(f, _) | Expression::Gte(f, _) if f == field => {
                        lower_idx = Some(i);
                    }
                    Expression::Lt(f, _) | Expression::Lte(f, _) if f == field => {
                        upper_idx = Some(i);
                    }
                    _ => {}
                }
            }

            if lower_idx.is_some() || upper_idx.is_some() {
                let lower = lower_idx.map(|i| match &children[i] {
                    Expression::Gt(_, v) => (v.clone(), false),
                    Expression::Gte(_, v) => (v.clone(), true),
                    _ => unreachable!(),
                });
                let upper = upper_idx.map(|i| match &children[i] {
                    Expression::Lt(_, v) => (v.clone(), false),
                    Expression::Lte(_, v) => (v.clone(), true),
                    _ => unreachable!(),
                });

                let consumed: Vec<usize> = [lower_idx, upper_idx].into_iter().flatten().collect();
                let node = Node::IndexScan {
                    collection: handle.clone(),
                    field: field.clone(),
                    range: IndexScanRange::Range { lower, upper },
                    direction: ScanDirection::Forward,
                    limit: None,
                    covered: false,
                };
                let residual = residual_from_and(children, &consumed);
                return (node, residual);
            }
        }

        // No indexed condition found — full scan with whole predicate as residual.
        (
            Node::Scan {
                collection: handle.clone(),
            },
            Some(original.clone()),
        )
    }

    /// Plan an OR: try to build an IndexMerge, fall back to scan.
    /// Always keeps the full OR as a residual — the index merge narrows
    /// candidates but the full predicate must be rechecked after KeyLookup.
    fn plan_or(
        &self,
        handle: &CollectionHandle<Cf>,
        children: &[Expression],
        original: &Expression,
    ) -> (Node<Cf>, Option<Expression>) {
        match self.try_or_index_merge(handle, children) {
            Some(node) => (node, Some(original.clone())),
            None => (
                Node::Scan {
                    collection: handle.clone(),
                },
                Some(original.clone()),
            ),
        }
    }

    /// Try to build an IndexMerge(Or) from OR children.
    /// Returns None if any child cannot be pushed into an index.
    fn try_or_index_merge(
        &self,
        handle: &CollectionHandle<Cf>,
        children: &[Expression],
    ) -> Option<Node<Cf>> {
        let mut nodes: Vec<Node<Cf>> = Vec::new();

        for child in children {
            match child {
                Expression::Eq(field, _) if handle.indexes.contains(field) => {
                    nodes.push(self.try_index_scan(handle, child)?);
                }
                Expression::And(sub_children) => {
                    let (node, _residual) = self.plan_and(handle, sub_children, child);
                    if matches!(node, Node::Scan { .. }) {
                        return None;
                    }
                    nodes.push(node);
                }
                Expression::Or(sub_children) => {
                    nodes.push(self.try_or_index_merge(handle, sub_children)?);
                }
                _ => return None,
            }
        }

        if nodes.is_empty() {
            return None;
        }

        if nodes.len() == 1 {
            return Some(nodes.into_iter().next().unwrap());
        }

        // Fold into binary tree of IndexMerge(Or).
        let mut iter = nodes.into_iter();
        let mut result = iter.next().unwrap();
        for node in iter {
            result = Node::IndexMerge {
                logical: LogicalOp::Or,
                lhs: Box::new(result),
                rhs: Box::new(node),
            };
        }

        Some(result)
    }

    /// Build a KeyLookup over a Values node for a direct _id point read.
    fn id_lookup(handle: &CollectionHandle<Cf>, value: &bson::Bson) -> Node<Cf> {
        let doc = bson::RawDocumentBuf::try_from(&bson::doc! { "_id": value.clone() })
            .expect("_id document is always valid");
        Node::KeyLookup {
            collection: handle.clone(),
            source: Box::new(Node::Values(vec![doc])),
        }
    }

    /// Try to convert a single expression into an IndexScan.
    fn try_index_scan(&self, handle: &CollectionHandle<Cf>, expr: &Expression) -> Option<Node<Cf>> {
        let (field, range) = match expr {
            Expression::Eq(f, v) => (f, IndexScanRange::Eq(v.clone())),
            Expression::Gt(f, v) => (
                f,
                IndexScanRange::Range {
                    lower: Some((v.clone(), false)),
                    upper: None,
                },
            ),
            Expression::Gte(f, v) => (
                f,
                IndexScanRange::Range {
                    lower: Some((v.clone(), true)),
                    upper: None,
                },
            ),
            Expression::Lt(f, v) => (
                f,
                IndexScanRange::Range {
                    lower: None,
                    upper: Some((v.clone(), false)),
                },
            ),
            Expression::Lte(f, v) => (
                f,
                IndexScanRange::Range {
                    lower: None,
                    upper: Some((v.clone(), true)),
                },
            ),
            _ => return None,
        };

        if !handle.indexes.contains(field) {
            return None;
        }

        Some(Node::IndexScan {
            collection: handle.clone(),
            field: field.clone(),
            range,
            direction: ScanDirection::Forward,
            limit: None,
            covered: false,
        })
    }
}

// ── Free functions ──────────────────────────────────────────────

/// Given AND children and a set of consumed indices, build the residual predicate.
fn residual_from_and(children: &[Expression], consumed: &[usize]) -> Option<Expression> {
    let remaining: Vec<Expression> = children
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, c)| c.clone())
        .collect();

    match remaining.len() {
        0 => None,
        1 => Some(remaining.into_iter().next().unwrap()),
        _ => Some(Expression::And(remaining)),
    }
}

/// Replace the Scan node inside a plan subtree with an ordered IndexScan.
fn replace_scan_with_index_order<Cf: Clone>(
    node: Node<Cf>,
    handle: &CollectionHandle<Cf>,
    sort_field: &str,
    direction: SortDirection,
    limit: Option<usize>,
) -> Node<Cf> {
    let scan_dir = match direction {
        SortDirection::Asc => ScanDirection::Forward,
        SortDirection::Desc => ScanDirection::Reverse,
    };

    match node {
        Node::Scan { .. } => Node::KeyLookup {
            collection: handle.clone(),
            source: Box::new(Node::IndexScan {
                collection: handle.clone(),
                field: sort_field.to_string(),
                range: IndexScanRange::Full,
                direction: scan_dir,
                limit,
                covered: false,
            }),
        },
        Node::Filter { predicate, source } => Node::Filter {
            predicate,
            source: Box::new(replace_scan_with_index_order(
                *source, handle, sort_field, direction, limit,
            )),
        },
        other => other,
    }
}
