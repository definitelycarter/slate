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

impl Plan {
    /// Render this plan as an indented operator tree (see the module docs).
    ///
    /// The string has no trailing newline, so the caller chooses how to
    /// terminate it (the REPL prints it with `println!`).
    pub fn explain(&self) -> String {
        let mut lines = Vec::new();
        render_plan(self, 0, &mut lines);
        lines.join("\n")
    }
}

/// Push one operator line at `depth` (two spaces of indent per level).
fn line(lines: &mut Vec<String>, depth: usize, text: String) {
    lines.push(format!("{}{}", "  ".repeat(depth), text));
}

/// Render a top-level [`Plan`] — a read query is just its node tree; a write
/// names the target collection and then renders the source that feeds it.
fn render_plan(plan: &Plan, depth: usize, lines: &mut Vec<String>) {
    match plan {
        Plan::Query(node) => render_node(node, depth, lines),
        Plan::Insert { collection, source } => {
            line(lines, depth, format!("Insert {}", coll(collection)));
            render_node(source, depth + 1, lines);
        }
        Plan::Delete { collection, source } => {
            line(lines, depth, format!("Delete {}", coll(collection)));
            render_node(source, depth + 1, lines);
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
            render_node(source, depth + 1, lines);
        }
        Plan::Replace {
            collection, source, ..
        } => {
            line(lines, depth, format!("Replace {}", coll(collection)));
            render_node(source, depth + 1, lines);
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
            render_node(source, depth + 1, lines);
        }
        Plan::Trigger { action, plan, .. } => {
            line(lines, depth, format!("Trigger {action}"));
            render_plan(plan, depth + 1, lines);
        }
    }
}

/// Render a read [`Node`] and its children.
fn render_node(node: &Node, depth: usize, lines: &mut Vec<String>) {
    match node {
        Node::Values(values) => {
            line(lines, depth, format!("Values ({} rows)", values.len()));
        }
        Node::Scan { collection } => {
            line(lines, depth, format!("Scan {}", coll(collection)));
        }
        Node::IndexScan {
            collection,
            field,
            range,
            direction,
            limit,
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
            line(lines, depth, format!("KeyLookup {}", coll(collection)));
            render_node(source, depth + 1, lines);
        }
        Node::IndexMerge {
            logical, lhs, rhs, ..
        } => {
            let op = match logical {
                LogicalOp::And => "AND",
                LogicalOp::Or => "OR",
            };
            line(lines, depth, format!("IndexMerge {op}"));
            render_node(lhs, depth + 1, lines);
            render_node(rhs, depth + 1, lines);
        }
        Node::Bind { alias, source } => {
            line(lines, depth, format!("Bind {alias}"));
            render_node(source, depth + 1, lines);
        }
        Node::Unwind {
            alias,
            array,
            source,
        } => {
            line(lines, depth, format!("Unwind {alias} IN {}", expr(array)));
            render_node(source, depth + 1, lines);
        }
        Node::Project {
            expr: e, source, ..
        } => {
            line(lines, depth, format!("Project {}", expr(e)));
            render_node(source, depth + 1, lines);
        }
        Node::Filter {
            predicate, source, ..
        } => {
            line(lines, depth, format!("Filter {}", expr(predicate)));
            render_node(source, depth + 1, lines);
        }
        Node::Sort { keys, source, .. } => {
            line(lines, depth, format!("Sort {}", sort_keys(keys)));
            render_node(source, depth + 1, lines);
        }
        Node::Limit { skip, take, source } => {
            let take = match take {
                Some(n) => n.to_string(),
                None => "all".to_string(),
            };
            line(lines, depth, format!("Limit skip={skip} take={take}"));
            render_node(source, depth + 1, lines);
        }
        Node::Distinct { source, flatten } => {
            let tag = if *flatten { " (flatten)" } else { "" };
            line(lines, depth, format!("Distinct{tag}"));
            render_node(source, depth + 1, lines);
        }
        Node::Aggregate {
            group_keys,
            aggregates,
            source,
            ..
        } => {
            line(
                lines,
                depth,
                format!(
                    "Aggregate group=[{}] aggregates=[{}]",
                    group_list(group_keys),
                    agg_list(aggregates),
                ),
            );
            render_node(source, depth + 1, lines);
        }
        Node::Trigger { action, source, .. } => {
            line(lines, depth, format!("Trigger {action}"));
            render_node(source, depth + 1, lines);
        }
        Node::Validate { source, .. } => {
            line(lines, depth, "Validate".to_string());
            render_node(source, depth + 1, lines);
        }
        Node::Subquery {
            slot,
            kind,
            subplan,
            source,
        } => {
            line(
                lines,
                depth,
                format!("Subquery {slot} ({})", subquery_kind(*kind)),
            );
            render_node(source, depth + 1, lines);
            render_node(subplan, depth + 1, lines);
        }
        Node::CurrentRow => {
            line(lines, depth, "CurrentRow".to_string());
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
