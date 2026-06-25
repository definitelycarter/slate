//! Rendering a [`Plan`] as a human-readable tree (`EXPLAIN`).
//!
//! [`Plan::explain`] walks the IR top-down and emits one line per node: the
//! operator name followed by the decisions that define it — the index and bounds
//! an `IndexScan` chose, a `Filter`'s predicate, a `Sort`'s keys. Children sit
//! below their parent, indented two spaces per level, so the data flow reads from
//! the leaves up: the source scan is the most-indented line and the root operator
//! is flush left.
//!
//! Expressions render as compact SQL-ish text (`c.age >= 21`, `UPPER(c.name)`)
//! rather than the IR's `Debug`, so the output is something a user can read back
//! against the query they wrote. This is a *logical* rendering — there are no
//! cost estimates or row counts, just the shape the planner settled on.

use bson::Bson;
use slate_ast::{BinOp, Expression, Literal, OrderByItem, SortDirection, SubqueryKind, UnaryOp};

use crate::plan::{
    AggregateExpr, CollectionRef, CompoundScanRange, CompoundScanTail, GroupKey, IndexScanRange,
    LogicalOp, Node, Plan, ScanDirection, UpsertMode,
};
use crate::stats::PlanStats;

impl Plan {
    /// Render this plan as an indented operator tree (see the module docs).
    ///
    /// The string has no trailing newline, so the caller chooses how to
    /// terminate it (the REPL prints it with `println!`).
    pub fn explain(&self) -> String {
        let mut lines = Vec::new();
        let mut ctx = RenderCtx::logical();
        render_plan(self, 0, &mut lines, &mut ctx);
        lines.join("\n")
    }

    /// Render this plan as `EXPLAIN ANALYZE` — the same operator tree as
    /// [`explain`](Self::explain), annotated with per-node *actuals* from a run.
    ///
    /// Each node line gains `rows=N` (rows it emitted) and, for non-source nodes,
    /// `examined=M` (rows that flowed in, i.e. its child's emitted count). The
    /// `stats` must have been collected against *this* plan (see [`PlanStats`]),
    /// so the pre-order node indices line up with this walk.
    pub fn explain_analyze(&self, stats: &PlanStats) -> String {
        let mut lines = Vec::new();
        let mut ctx = RenderCtx::analyze(stats);
        render_plan(self, 0, &mut lines, &mut ctx);
        lines.join("\n")
    }
}

/// Threads the pre-order node index (and, in analyze mode, the collected
/// [`PlanStats`]) through the render walk. The index advances per node in the
/// exact pre-order the executor's analyze walk and `PlanStats::for_plan` use, so
/// a node's counter lines up with its rendered line.
struct RenderCtx<'a> {
    /// Pre-order index of the *next* node to render. Bumped at each `render_node`
    /// entry, before the node's children are visited.
    next_index: usize,
    /// `Some` in analyze mode (annotate with actuals), `None` for logical EXPLAIN.
    stats: Option<&'a PlanStats>,
}

impl<'a> RenderCtx<'a> {
    fn logical() -> Self {
        Self {
            next_index: 0,
            stats: None,
        }
    }

    fn analyze(stats: &'a PlanStats) -> Self {
        Self {
            next_index: 0,
            stats: Some(stats),
        }
    }

    /// Claim the next pre-order index for the node about to be rendered.
    fn claim(&mut self) -> usize {
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    /// In analyze mode, the `rows`/`examined` suffix for the node at `index`
    /// given its child's emitted count (`examined`). Empty string in logical mode.
    fn annotate(&self, index: usize, examined: Option<u64>) -> String {
        match self.stats {
            None => String::new(),
            Some(stats) => match examined {
                Some(examined) => format!(" rows={} examined={}", stats.emitted(index), examined),
                None => format!(" rows={}", stats.emitted(index)),
            },
        }
    }

    /// The emitted count of the node at `index` (its child's `examined`), or 0 in
    /// logical mode.
    fn emitted(&self, index: usize) -> u64 {
        self.stats.map_or(0, |s| s.emitted(index))
    }
}

/// Push one operator line at `depth` (two spaces of indent per level).
fn line(lines: &mut Vec<String>, depth: usize, text: String) {
    lines.push(format!("{}{}", "  ".repeat(depth), text));
}

/// Render a top-level [`Plan`] — a read query is just its node tree; a write
/// names the target collection and then renders the source that feeds it.
///
/// Write/trigger wrappers are not executor *nodes* (they are `Plan` variants), so
/// they claim no pre-order index — only the `Node` tree under them does. This
/// keeps the renderer's index walk aligned with the executor's, which only
/// counts `Node`s.
fn render_plan(plan: &Plan, depth: usize, lines: &mut Vec<String>, ctx: &mut RenderCtx) {
    match plan {
        Plan::Query(node) => render_node(node, depth, lines, ctx),
        Plan::Insert { collection, source } => {
            line(lines, depth, format!("Insert {}", coll(collection)));
            render_node(source, depth + 1, lines, ctx);
        }
        Plan::Delete { collection, source } => {
            line(lines, depth, format!("Delete {}", coll(collection)));
            render_node(source, depth + 1, lines, ctx);
        }
        Plan::Update {
            collection,
            assignments,
            source,
        } => {
            let sets = assignments
                .iter()
                .map(|a| format!("{} = {}", a.path.join("."), expr(&a.value)))
                .collect::<Vec<_>>()
                .join(", ");
            line(
                lines,
                depth,
                format!("Update {} SET {sets}", coll(collection)),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Plan::Replace {
            collection, source, ..
        } => {
            line(lines, depth, format!("Replace {}", coll(collection)));
            render_node(source, depth + 1, lines, ctx);
        }
        Plan::Upsert {
            collection,
            mode,
            source,
            ..
        } => {
            let mode = match mode {
                UpsertMode::Replace => "replace",
                UpsertMode::Merge => "merge",
            };
            line(
                lines,
                depth,
                format!("Upsert {} ({mode})", coll(collection)),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Plan::Trigger { action, plan, .. } => {
            line(lines, depth, format!("Trigger {action}"));
            render_plan(plan, depth + 1, lines, ctx);
        }
    }
}

/// Render a read [`Node`] and its children.
///
/// Each node claims its pre-order index up front (so the index walk stays in
/// lock-step with the executor), then — in analyze mode — annotates its line with
/// `rows=` (its own emitted count) and, where it has a single source child,
/// `examined=` (the child's emitted count, i.e. what flowed in). A node's single
/// child always sits at `my_index + 1` in pre-order.
fn render_node(node: &Node, depth: usize, lines: &mut Vec<String>, ctx: &mut RenderCtx) {
    let index = ctx.claim();
    // The single-source child (when present) is the very next node in pre-order.
    let child_examined = || Some(ctx.emitted(index + 1));
    match node {
        Node::Values(values) => {
            let suffix = ctx.annotate(index, None);
            line(
                lines,
                depth,
                format!("Values ({} rows){suffix}", values.len()),
            );
        }
        Node::Scan { collection } => {
            let suffix = ctx.annotate(index, None);
            line(lines, depth, format!("Scan {}{suffix}", coll(collection)));
        }
        Node::IndexScan {
            collection,
            field,
            range,
            direction,
            limit,
            covering,
        } => {
            let mut text = format!("IndexScan {}.{field} {}", coll(collection), bounds(range));
            text.push(' ');
            text.push_str(match direction {
                ScanDirection::Forward => "forward",
                ScanDirection::Reverse => "reverse",
            });
            if let Some(n) = limit {
                text.push_str(&format!(" limit {n}"));
            }
            if *covering {
                text.push_str(" covering");
            }
            text.push_str(&ctx.annotate(index, None));
            line(lines, depth, text);
        }
        Node::CompoundIndexScan {
            collection,
            field,
            range,
            direction,
            limit,
        } => {
            let mut text = format!(
                "CompoundIndexScan {}.{field} {}",
                coll(collection),
                compound_bounds(range)
            );
            text.push(' ');
            text.push_str(match direction {
                ScanDirection::Forward => "forward",
                ScanDirection::Reverse => "reverse",
            });
            if let Some(n) = limit {
                text.push_str(&format!(" limit {n}"));
            }
            line(lines, depth, text);
        }
        Node::KeyLookup { collection, source } => {
            let suffix = ctx.annotate(index, child_examined());
            line(
                lines,
                depth,
                format!("KeyLookup {}{suffix}", coll(collection)),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Node::IndexMerge {
            logical, lhs, rhs, ..
        } => {
            let op = match logical {
                LogicalOp::And => "AND",
                LogicalOp::Or => "OR",
            };
            // Two arms: "examined" isn't a single child's count, so report only
            // rows emitted for the merge itself.
            let suffix = ctx.annotate(index, None);
            line(lines, depth, format!("IndexMerge {op}{suffix}"));
            render_node(lhs, depth + 1, lines, ctx);
            render_node(rhs, depth + 1, lines, ctx);
        }
        Node::Bind { alias, source } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Bind {alias}{suffix}"));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Unwind {
            alias,
            array,
            source,
        } => {
            let suffix = ctx.annotate(index, child_examined());
            line(
                lines,
                depth,
                format!("Unwind {alias} IN {}{suffix}", expr(array)),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Project {
            expr: e, source, ..
        } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Project {}{suffix}", expr(e)));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Filter {
            predicate, source, ..
        } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Filter {}{suffix}", expr(predicate)));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Sort { keys, source, .. } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Sort {}{suffix}", sort_keys(keys)));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Limit { skip, take, source } => {
            let take = match take {
                Some(n) => n.to_string(),
                None => "all".to_string(),
            };
            let suffix = ctx.annotate(index, child_examined());
            line(
                lines,
                depth,
                format!("Limit skip={skip} take={take}{suffix}"),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Distinct { source, flatten } => {
            let tag = if *flatten { " (flatten)" } else { "" };
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Distinct{tag}{suffix}"));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Aggregate {
            group_keys,
            aggregates,
            source,
            ..
        } => {
            let suffix = ctx.annotate(index, child_examined());
            line(
                lines,
                depth,
                format!(
                    "Aggregate group=[{}] aggregates=[{}]{suffix}",
                    group_list(group_keys),
                    agg_list(aggregates),
                ),
            );
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Trigger { action, source, .. } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Trigger {action}{suffix}"));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Validate { source, .. } => {
            let suffix = ctx.annotate(index, child_examined());
            line(lines, depth, format!("Validate{suffix}"));
            render_node(source, depth + 1, lines, ctx);
        }
        Node::Subquery {
            slot,
            kind,
            subplan,
            source,
        } => {
            // `source` is the row stream that drives the apply; report its count
            // as "examined". `subplan` re-runs per outer row, so its own counter
            // accumulates across runs.
            let suffix = ctx.annotate(index, child_examined());
            line(
                lines,
                depth,
                format!("Subquery {slot} ({}){suffix}", subquery_kind(*kind)),
            );
            render_node(source, depth + 1, lines, ctx);
            render_node(subplan, depth + 1, lines, ctx);
        }
        Node::CurrentRow => {
            let suffix = ctx.annotate(index, None);
            line(lines, depth, format!("CurrentRow{suffix}"));
        }
    }
}

/// `cf.collection`, the container identity the executor resolves.
fn coll(c: &CollectionRef) -> String {
    format!("{}.{}", c.cf, c.collection)
}

/// Render an index scan's bounds: an exact match, a (half-)open range, or the
/// whole index.
fn bounds(range: &IndexScanRange) -> String {
    match range {
        IndexScanRange::Full => "(all)".to_string(),
        IndexScanRange::Eq(v) => format!("= {}", bson(v)),
        IndexScanRange::Range { lower, upper } => {
            let mut parts = Vec::new();
            if let Some((v, inclusive)) = lower {
                parts.push(format!(
                    "{} {}",
                    if *inclusive { ">=" } else { ">" },
                    bson(v)
                ));
            }
            if let Some((v, inclusive)) = upper {
                parts.push(format!(
                    "{} {}",
                    if *inclusive { "<=" } else { "<" },
                    bson(v)
                ));
            }
            if parts.is_empty() {
                "(all)".to_string()
            } else {
                parts.join(", ")
            }
        }
        IndexScanRange::StringPrefix(p) => format!("starts with {p:?}"),
    }
}

/// Render a compound scan's bounds: the equality prefix then the trailing
/// predicate (`[= "active", = X] then >= Y`).
fn compound_bounds(range: &CompoundScanRange) -> String {
    let mut parts: Vec<String> = range
        .eq_prefix
        .iter()
        .map(|v| format!("= {}", bson(v)))
        .collect();
    match &range.tail {
        CompoundScanTail::Unbounded => {}
        CompoundScanTail::Eq(v) => parts.push(format!("= {}", bson(v))),
        CompoundScanTail::Range { lower, upper } => {
            if let Some((v, inclusive)) = lower {
                parts.push(format!(
                    "{} {}",
                    if *inclusive { ">=" } else { ">" },
                    bson(v)
                ));
            }
            if let Some((v, inclusive)) = upper {
                parts.push(format!(
                    "{} {}",
                    if *inclusive { "<=" } else { "<" },
                    bson(v)
                ));
            }
        }
    }
    if parts.is_empty() {
        "(all)".to_string()
    } else {
        format!("[{}]", parts.join(", "))
    }
}

/// `c.age DESC, c.name ASC`.
fn sort_keys(keys: &[OrderByItem]) -> String {
    keys.iter()
        .map(|k| {
            let dir = match k.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            format!("{} {dir}", expr(&k.expr))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `c.city AS $key0, ...` — each group expression with the slot it binds.
fn group_list(keys: &[GroupKey]) -> String {
    keys.iter()
        .map(|k| format!("{} AS {}", expr(&k.expr), k.slot))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `COUNT(1) AS $agg0, ...` — each aggregate with the slot it binds.
fn agg_list(aggs: &[AggregateExpr]) -> String {
    aggs.iter()
        .map(|a| format!("{}({}) AS {}", a.func, expr(&a.arg), a.slot))
        .collect::<Vec<_>>()
        .join(", ")
}

fn subquery_kind(kind: SubqueryKind) -> &'static str {
    match kind {
        SubqueryKind::Scalar => "scalar",
        SubqueryKind::Exists => "exists",
        SubqueryKind::Array => "array",
    }
}

/// Render an expression as compact SQL-ish text.
fn expr(e: &Expression) -> String {
    match e {
        Expression::Literal(lit) => literal(lit),
        Expression::Value(v) => bson(v),
        Expression::Identifier(name) => name.to_string(),
        Expression::Parameter(name) => format!("@{name}"),
        Expression::Member { base, field } => format!("{}.{field}", expr(base)),
        Expression::Index { base, index } => format!("{}[{}]", expr(base), expr(index)),
        Expression::Unary { op, expr: inner } => match op {
            UnaryOp::Not => format!("NOT {}", operand(inner)),
            UnaryOp::Neg => format!("-{}", operand(inner)),
        },
        Expression::Binary { op, lhs, rhs } => {
            format!("{} {} {}", operand(lhs), binop(*op), operand(rhs))
        }
        Expression::Function { name, args } => {
            let args = args.iter().map(expr).collect::<Vec<_>>().join(", ");
            format!("{name}({args})")
        }
        Expression::Object(fields) => {
            let body = fields
                .iter()
                .map(|(k, v)| format!("{k}: {}", expr(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{body}}}")
        }
        Expression::Array(items) => {
            let body = items.iter().map(expr).collect::<Vec<_>>().join(", ");
            format!("[{body}]")
        }
        Expression::PathGet { base, path } => {
            format!("{}.{}", expr(base), path.join("."))
        }
        Expression::MultikeyEq {
            base,
            index_path,
            value,
        } => format!("{}.{index_path} = {}", expr(base), expr(value)),
        Expression::Subquery { kind, .. } => format!("{}(...)", subquery_kind(*kind)),
    }
}

/// Parenthesize a binary/unary operand so nesting stays unambiguous, leaving
/// atoms (identifiers, members, literals) bare.
fn operand(e: &Expression) -> String {
    match e {
        Expression::Binary { .. } | Expression::Unary { .. } => format!("({})", expr(e)),
        _ => expr(e),
    }
}

fn literal(lit: &Literal) -> String {
    match lit {
        Literal::Null => "null".to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Int(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Str(s) => format!("\"{s}\""),
    }
}

/// A BSON literal rendered compactly — `bson::Bson`'s `Display` already quotes
/// strings and prints numbers bare, which is exactly the SQL-ish form we want.
fn bson(b: &Bson) -> String {
    b.to_string()
}

fn binop(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::Neq => "!=",
        BinOp::Lt => "<",
        BinOp::Lte => "<=",
        BinOp::Gt => ">",
        BinOp::Gte => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::RowBinding;
    use slate_ast::{BinOp, Expression};

    fn ident(name: &str) -> Expression {
        Expression::Identifier(name.to_string())
    }

    fn member(base: &str, field: &str) -> Expression {
        Expression::Member {
            base: Box::new(ident(base)),
            field: field.to_string(),
        }
    }

    fn cref() -> CollectionRef {
        CollectionRef {
            cf: "default".to_string(),
            collection: "users".to_string(),
        }
    }

    #[test]
    fn scan_then_filter_then_project_indents_each_level() {
        // Project(c.name) <- Filter(c.age > 21) <- Bind(c) <- Scan
        let plan = Plan::Query(Node::Project {
            expr: member("c", "name"),
            binding: RowBinding::Alias("c".to_string()),
            source: Box::new(Node::Filter {
                predicate: Expression::Binary {
                    op: BinOp::Gt,
                    lhs: Box::new(member("c", "age")),
                    rhs: Box::new(Expression::Literal(Literal::Int(21))),
                },
                binding: RowBinding::Alias("c".to_string()),
                source: Box::new(Node::Bind {
                    alias: "c".to_string(),
                    source: Box::new(Node::Scan { collection: cref() }),
                }),
            }),
        });
        assert_eq!(
            plan.explain(),
            "\
Project c.name
  Filter c.age > 21
    Bind c
      Scan default.users"
        );
    }

    #[test]
    fn index_scan_eq_feeds_key_lookup() {
        // KeyLookup <- IndexScan(name = "x")
        let plan = Plan::Query(Node::KeyLookup {
            collection: cref(),
            source: Box::new(Node::IndexScan {
                collection: cref(),
                field: "name".to_string(),
                range: IndexScanRange::Eq(Bson::String("x".to_string())),
                direction: ScanDirection::Forward,
                limit: None,
                covering: false,
            }),
        });
        assert_eq!(
            plan.explain(),
            "\
KeyLookup default.users
  IndexScan default.users.name = \"x\" forward"
        );
    }

    #[test]
    fn index_scan_renders_range_direction_and_limit() {
        let plan = Plan::Query(Node::IndexScan {
            collection: cref(),
            field: "age".to_string(),
            range: IndexScanRange::Range {
                lower: Some((Bson::Int32(21), true)),
                upper: Some((Bson::Int32(65), false)),
            },
            direction: ScanDirection::Reverse,
            limit: Some(10),
            covering: false,
        });
        assert_eq!(
            plan.explain(),
            "IndexScan default.users.age >= 21, < 65 reverse limit 10"
        );
    }

    #[test]
    fn sort_and_limit_show_keys_and_window() {
        let plan = Plan::Query(Node::Limit {
            skip: 5,
            take: None,
            source: Box::new(Node::Sort {
                keys: vec![
                    OrderByItem {
                        expr: member("c", "age"),
                        direction: SortDirection::Desc,
                    },
                    OrderByItem {
                        expr: member("c", "name"),
                        direction: SortDirection::Asc,
                    },
                ],
                binding: RowBinding::Alias("c".to_string()),
                source: Box::new(Node::Scan { collection: cref() }),
            }),
        });
        assert_eq!(
            plan.explain(),
            "\
Limit skip=5 take=all
  Sort c.age DESC, c.name ASC
    Scan default.users"
        );
    }

    #[test]
    fn aggregate_shows_group_keys_and_aggregates() {
        let plan = Plan::Query(Node::Aggregate {
            group_keys: vec![GroupKey {
                slot: "$key0".to_string(),
                expr: member("c", "city"),
            }],
            aggregates: vec![AggregateExpr {
                func: "COUNT".to_string(),
                arg: Expression::Literal(Literal::Int(1)),
                slot: "$agg0".to_string(),
            }],
            binding: RowBinding::Env,
            source: Box::new(Node::Scan { collection: cref() }),
        });
        assert_eq!(
            plan.explain(),
            "\
Aggregate group=[c.city AS $key0] aggregates=[COUNT(1) AS $agg0]
  Scan default.users"
        );
    }

    #[test]
    fn compound_predicate_parenthesizes_nested_binaries() {
        // (c.a = 1) AND (c.b = 2)
        let pred = Expression::Binary {
            op: BinOp::And,
            lhs: Box::new(Expression::Binary {
                op: BinOp::Eq,
                lhs: Box::new(member("c", "a")),
                rhs: Box::new(Expression::Literal(Literal::Int(1))),
            }),
            rhs: Box::new(Expression::Binary {
                op: BinOp::Eq,
                lhs: Box::new(member("c", "b")),
                rhs: Box::new(Expression::Literal(Literal::Int(2))),
            }),
        };
        let plan = Plan::Query(Node::Filter {
            predicate: pred,
            binding: RowBinding::Alias("c".to_string()),
            source: Box::new(Node::Scan { collection: cref() }),
        });
        assert_eq!(
            plan.explain(),
            "\
Filter (c.a = 1) AND (c.b = 2)
  Scan default.users"
        );
    }

    #[test]
    fn function_call_renders_args() {
        let plan = Plan::Query(Node::Project {
            expr: Expression::Function {
                name: "UPPER".to_string(),
                args: vec![member("c", "name")],
            },
            binding: RowBinding::Alias("c".to_string()),
            source: Box::new(Node::Scan { collection: cref() }),
        });
        assert_eq!(
            plan.explain(),
            "\
Project UPPER(c.name)
  Scan default.users"
        );
    }

    #[test]
    fn explain_analyze_annotates_rows_and_examined() {
        // Project(c.name) <- Filter(c.age > 21) <- Scan
        let plan = Plan::Query(Node::Project {
            expr: member("c", "name"),
            binding: RowBinding::Alias("c".to_string()),
            source: Box::new(Node::Filter {
                predicate: Expression::Binary {
                    op: BinOp::Gt,
                    lhs: Box::new(member("c", "age")),
                    rhs: Box::new(Expression::Literal(Literal::Int(21))),
                },
                binding: RowBinding::Alias("c".to_string()),
                source: Box::new(Node::Scan { collection: cref() }),
            }),
        });
        // Pre-order indices: Project=0, Filter=1, Scan=2.
        let stats = PlanStats::for_plan(&plan);
        for _ in 0..120 {
            stats.record_emit(2); // Scan emitted 120
        }
        for _ in 0..8 {
            stats.record_emit(1); // Filter emitted 8
            stats.record_emit(0); // Project emitted 8
        }
        assert_eq!(
            plan.explain_analyze(&stats),
            "\
Project c.name rows=8 examined=8
  Filter c.age > 21 rows=8 examined=120
    Scan default.users rows=120"
        );
    }

    #[test]
    fn explain_analyze_unrun_plan_reads_zeros() {
        // A plan whose stats were never populated annotates with zeros rather
        // than panicking — a sane default if a caller renders before running.
        let plan = Plan::Query(Node::Scan { collection: cref() });
        let stats = PlanStats::for_plan(&plan);
        assert_eq!(plan.explain_analyze(&stats), "Scan default.users rows=0");
    }

    #[test]
    fn delete_names_collection_above_its_source() {
        let plan = Plan::Delete {
            collection: cref(),
            source: Node::Scan { collection: cref() },
        };
        assert_eq!(
            plan.explain(),
            "\
Delete default.users
  Scan default.users"
        );
    }
}
