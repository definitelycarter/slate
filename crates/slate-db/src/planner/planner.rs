use slate_engine::{Catalog, CollectionHandle};
use slate_query::{Sort, SortDirection};

use crate::error::DbError;
use crate::expression::{Expression, LogicalOp};
use crate::hooks::HookSnapshot;
use crate::statement::Statement;

use super::plan::{IndexScanRange, Node, Plan, ScanDirection};

pub struct Planner<'a, T: Catalog> {
    catalog: &'a T,
    snapshot: Option<&'a HookSnapshot>,
}

impl<'a, T: Catalog> Planner<'a, T>
where
    T::Cf: Clone,
{
    pub fn new(catalog: &'a T) -> Self {
        Self {
            catalog,
            snapshot: None,
        }
    }

    pub fn with_snapshot(catalog: &'a T, snapshot: Option<&'a HookSnapshot>) -> Self {
        Self { catalog, snapshot }
    }

    pub fn plan(&self, stmt: Statement<'_>) -> Result<Plan<T::Cf>, DbError> {
        match stmt {
            Statement::Find {
                cf,
                collection,
                predicate,
                sort,
                skip,
                take,
                projection: columns,
            } => self.plan_find(cf, collection, &predicate, sort, skip, take, columns),
            Statement::Distinct {
                cf,
                collection,
                field,
                predicate,
                sort,
                skip,
                take,
            } => self.plan_distinct(cf, collection, field, &predicate, sort, skip, take),
            Statement::Insert { cf, collection, docs } => self.plan_insert(cf, collection, docs),
            Statement::Update {
                cf,
                collection,
                predicate,
                mutation,
                limit,
            } => self.plan_update(cf, collection, &predicate, mutation, limit),
            Statement::Replace {
                cf,
                collection,
                predicate,
                replacement,
            } => self.plan_replace(cf, collection, &predicate, replacement),
            Statement::Delete {
                cf,
                collection,
                predicate,
                limit,
            } => self.plan_delete(cf, collection, &predicate, limit),
            Statement::Merge { cf, collection, docs } => self.plan_merge(cf, collection, docs),
            Statement::Upsert { cf, collection, docs } => self.plan_upsert(cf, collection, docs),
        }
    }

    // ── Find ────────────────────────────────────────────────────

    fn plan_find(
        &self,
        cf: &str,
        collection: &str,
        predicate: &Expression,
        sort: Vec<Sort>,
        skip: Option<usize>,
        take: Option<usize>,
        columns: Option<Vec<String>>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
        let (source, residual) = self.plan_source(&handle, predicate);

        let source_is_scan = matches!(source, Node::Scan { .. });
        let has_residual = residual.is_some();

        // Covered index optimization: Eq on indexed field, no residual,
        // and projection only needs pk + the indexed field.
        let pk = handle.pk_path();
        let covered = !has_residual
            && matches!(
                &source,
                Node::IndexScan {
                    field,
                    range: IndexScanRange::Eq(_),
                    ..
                } if columns.as_ref().is_some_and(|cols|
                    cols.iter().all(|c| c == pk || c == field)
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
            && handle.indexes().contains(&sort[0].field)
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
                collection: handle.clone(),
                columns,
                source: Box::new(node),
            }
        };

        Ok(Plan::Find(node))
    }

    // ── Distinct ──────────────────────────────────────────────────

    fn plan_distinct(
        &self,
        cf: &str,
        collection: &str,
        field: String,
        predicate: &Expression,
        sort: Option<SortDirection>,
        skip: Option<usize>,
        take: Option<usize>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
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
            collection: handle.clone(),
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

    // ── Hook wrapping helpers ───────────────────────────────────

    /// Wrap source with Node::Validate then Node::Trigger (before-action).
    fn wrap_before(&self, cf: &str, coll: &str, action: &str, source: Node<T::Cf>) -> Node<T::Cf> {
        let Some(snap) = self.snapshot else {
            return source;
        };

        let validators = snap.validators_for(cf, coll);
        let source = if !validators.is_empty() {
            Node::Validate {
                validators: validators.to_vec(),
                source: Box::new(source),
            }
        } else {
            source
        };

        let triggers = snap.triggers_for(cf, coll);
        if !triggers.is_empty() {
            Node::Trigger {
                cf: cf.into(),
                action: action.into(),
                hooks: triggers.to_vec(),
                source: Box::new(source),
            }
        } else {
            source
        }
    }

    /// Wrap source with Node::Trigger only (before-action, no validation).
    /// Used for deletes where nothing is being written.
    fn wrap_before_triggers(&self, cf: &str, coll: &str, action: &str, source: Node<T::Cf>) -> Node<T::Cf> {
        let Some(snap) = self.snapshot else {
            return source;
        };

        let triggers = snap.triggers_for(cf, coll);
        if !triggers.is_empty() {
            Node::Trigger {
                cf: cf.into(),
                action: action.into(),
                hooks: triggers.to_vec(),
                source: Box::new(source),
            }
        } else {
            source
        }
    }

    /// Wrap plan with Plan::Trigger (after-action).
    fn wrap_after(&self, cf: &str, coll: &str, action: &str, plan: Plan<T::Cf>) -> Plan<T::Cf> {
        let Some(snap) = self.snapshot else {
            return plan;
        };

        let triggers = snap.triggers_for(cf, coll);
        if !triggers.is_empty() {
            Plan::Trigger {
                cf: cf.into(),
                action: action.into(),
                hooks: triggers.to_vec(),
                plan: Box::new(plan),
            }
        } else {
            plan
        }
    }

    // ── Insert ──────────────────────────────────────────────────

    fn plan_insert(
        &self,
        cf: &str,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
        let source = self.wrap_before(cf, collection, "inserting", Node::Values(docs));
        let plan = Plan::Insert {
            collection: handle,
            source,
        };
        Ok(self.wrap_after(cf, collection, "inserted", plan))
    }

    // ── Update ──────────────────────────────────────────────────

    fn plan_update(
        &self,
        cf: &str,
        collection: &str,
        predicate: &Expression,
        mutation: crate::mutation::Mutation,
        limit: Option<usize>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
        let source = self.plan_read_source(&handle, predicate, limit);
        let source = self.wrap_before(cf, collection, "updating", source);
        let plan = Plan::Update {
            collection: handle,
            mutation,
            source,
        };
        Ok(self.wrap_after(cf, collection, "updated", plan))
    }

    // ── Replace ─────────────────────────────────────────────────

    fn plan_replace(
        &self,
        cf: &str,
        collection: &str,
        predicate: &Expression,
        replacement: bson::RawDocumentBuf,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
        let source = self.plan_read_source(&handle, predicate, Some(1));
        let source = self.wrap_before(cf, collection, "updating", source);
        let plan = Plan::Replace {
            collection: handle,
            replacement,
            source,
        };
        Ok(self.wrap_after(cf, collection, "updated", plan))
    }

    // ── Delete ──────────────────────────────────────────────────

    fn plan_delete(
        &self,
        cf: &str,
        collection: &str,
        predicate: &Expression,
        limit: Option<usize>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;
        let source = self.plan_read_source(&handle, predicate, limit);
        let source = self.wrap_before_triggers(cf, collection, "deleting", source);
        let plan = Plan::Delete {
            collection: handle,
            source,
        };
        Ok(self.wrap_after(cf, collection, "deleted", plan))
    }

    // ── Merge ───────────────────────────────────────────────────

    fn plan_merge(
        &self,
        cf: &str,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;

        // Upsert/merge keeps triggers internal (runtime-conditional actions).
        let hooks = self
            .snapshot
            .map(|s| s.triggers_for(cf, collection).to_vec())
            .unwrap_or_default();
        Ok(Plan::Merge {
            collection: handle,
            hooks,
            source: Node::Values(docs),
        })
    }

    // ── Upsert ──────────────────────────────────────────────────

    fn plan_upsert(
        &self,
        cf: &str,
        collection: &str,
        docs: Vec<bson::RawDocumentBuf>,
    ) -> Result<Plan<T::Cf>, DbError> {
        let handle = self.catalog.collection(cf, collection)?;

        // Upsert/merge keeps triggers internal (runtime-conditional actions).
        let hooks = self
            .snapshot
            .map(|s| s.triggers_for(cf, collection).to_vec())
            .unwrap_or_default();
        Ok(Plan::Upsert {
            collection: handle,
            hooks,
            source: Node::Values(docs),
        })
    }

    // ── Shared planning helpers ─────────────────────────────────

    /// Build a source that yields full documents for mutation operations:
    /// plan_source → KeyLookup (if index) → Filter (if residual) → Limit.
    fn plan_read_source(
        &self,
        handle: &CollectionHandle<T::Cf>,
        predicate: &Expression,
        limit: Option<usize>,
    ) -> Node<T::Cf> {
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
        handle: &CollectionHandle<T::Cf>,
        predicate: &Expression,
    ) -> (Node<T::Cf>, Option<Expression>) {
        // Fast path: pk equality → direct key lookup, no scan needed.
        if let Expression::Eq(field, value) = predicate
            && field == handle.pk_path()
        {
            return (Self::pk_lookup(handle, value), None);
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
        handle: &CollectionHandle<T::Cf>,
        predicate: &Expression,
    ) -> (Node<T::Cf>, Option<Expression>) {
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
        handle: &CollectionHandle<T::Cf>,
        children: &[Expression],
        original: &Expression,
    ) -> (Node<T::Cf>, Option<Expression>) {
        // Priority 0: pk equality — direct key lookup, always wins.
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(field, value) = child
                && field == handle.pk_path()
            {
                let node = Self::pk_lookup(handle, value);
                let residual = residual_from_and(children, &[i]);
                return (node, residual);
            }
        }

        // Priority 1: Eq on an indexed field (most selective).
        for (i, child) in children.iter().enumerate() {
            if let Expression::Eq(field, _) = child
                && handle.indexes().contains(field)
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
        for field in handle.indexes() {
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
        handle: &CollectionHandle<T::Cf>,
        children: &[Expression],
        original: &Expression,
    ) -> (Node<T::Cf>, Option<Expression>) {
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
        handle: &CollectionHandle<T::Cf>,
        children: &[Expression],
    ) -> Option<Node<T::Cf>> {
        let mut nodes: Vec<Node<T::Cf>> = Vec::new();

        for child in children {
            match child {
                Expression::Eq(field, _) if handle.indexes().contains(field) => {
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
                collection: handle.clone(),
                logical: LogicalOp::Or,
                lhs: Box::new(result),
                rhs: Box::new(node),
            };
        }

        Some(result)
    }

    /// Build a KeyLookup over a Values node for a direct pk point read.
    fn pk_lookup(handle: &CollectionHandle<T::Cf>, value: &bson::Bson) -> Node<T::Cf> {
        let mut doc = bson::Document::new();
        doc.insert(handle.pk_path().to_string(), value.clone());
        let raw = bson::RawDocumentBuf::try_from(&doc)
            .expect("pk document is always valid");
        Node::KeyLookup {
            collection: handle.clone(),
            source: Box::new(Node::Values(vec![raw])),
        }
    }

    /// Try to convert a single expression into an IndexScan.
    fn try_index_scan(&self, handle: &CollectionHandle<T::Cf>, expr: &Expression) -> Option<Node<T::Cf>> {
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

        if !handle.indexes().contains(field) {
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
