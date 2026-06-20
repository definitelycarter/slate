//! Scalar-expression evaluation over a binding environment.
//!
//! Evaluation produces owned [`Value`]s. To keep v1 simple and correct, the
//! evaluator works over owned `bson::Bson` rather than raw bytes — the
//! raw-byte fast paths in `slate-db` are a future optimization for the
//! storage-backed lowering, not a v1 concern.

use std::cmp::Ordering;

use bson::{Bson, Document};

use crate::error::Result;
use crate::value::Value;
use slate_ast::{BinOp, Literal, ScalarExpr, UnaryOp};

/// The bindings visible to an expression: alias → bound document/value, plus
/// query parameters (`@name`).
pub struct Env<'a> {
    bindings: &'a [(&'a str, &'a Bson)],
    params: &'a Document,
}

impl<'a> Env<'a> {
    pub fn new(bindings: &'a [(&'a str, &'a Bson)], params: &'a Document) -> Self {
        Self { bindings, params }
    }

    fn lookup(&self, name: &str) -> Value {
        for (n, v) in self.bindings {
            if *n == name {
                // Clone the bound value: the evaluator yields owned values and a
                // single binding may be read many times across a row's exprs.
                // A borrowing/Cow evaluator is a noted future optimization.
                return Value::Defined((*v).clone());
            }
        }
        Value::Undefined
    }

    fn param(&self, name: &str) -> Value {
        match self.params.get(name) {
            Some(b) => Value::Defined(b.clone()),
            None => Value::Undefined,
        }
    }
}

/// Evaluate a scalar expression in `env`.
pub fn eval(expr: &ScalarExpr, env: &Env) -> Result<Value> {
    match expr {
        ScalarExpr::Literal(lit) => Ok(Value::Defined(literal_to_bson(lit))),
        ScalarExpr::Value(b) => Ok(Value::Defined(b.clone())),
        ScalarExpr::Identifier(name) => Ok(env.lookup(name)),
        ScalarExpr::Parameter(name) => Ok(env.param(name)),

        ScalarExpr::Member { base, field } => Ok(member_access(eval(base, env)?, field)),
        ScalarExpr::Index { base, index } => Ok(index_access(eval(base, env)?, eval(index, env)?)),

        ScalarExpr::Unary { op, expr } => Ok(eval_unary(*op, eval(expr, env)?)),

        ScalarExpr::Binary { op, lhs, rhs } => match op {
            BinOp::And => {
                let l = truthy(&eval(lhs, env)?);
                if l == Some(false) {
                    return Ok(Value::Defined(Bson::Boolean(false)));
                }
                let r = truthy(&eval(rhs, env)?);
                Ok(and3(l, r))
            }
            BinOp::Or => {
                let l = truthy(&eval(lhs, env)?);
                if l == Some(true) {
                    return Ok(Value::Defined(Bson::Boolean(true)));
                }
                let r = truthy(&eval(rhs, env)?);
                Ok(or3(l, r))
            }
            _ => {
                let l = eval(lhs, env)?;
                let r = eval(rhs, env)?;
                Ok(eval_binop(*op, l, r))
            }
        },

        ScalarExpr::Function { name, args } => {
            let mut vals = Vec::with_capacity(args.len());
            for a in args {
                vals.push(eval(a, env)?);
            }
            crate::functions::call(name, vals)
        }

        ScalarExpr::Object(fields) => {
            let mut doc = Document::new();
            for (k, v) in fields {
                // Undefined fields are omitted (Cosmos behavior).
                if let Value::Defined(b) = eval(v, env)? {
                    doc.insert(k.clone(), b);
                }
            }
            Ok(Value::Defined(Bson::Document(doc)))
        }

        ScalarExpr::Array(items) => {
            let mut arr = Vec::with_capacity(items.len());
            for it in items {
                // Undefined elements are omitted (Cosmos behavior).
                if let Value::Defined(b) = eval(it, env)? {
                    arr.push(b);
                }
            }
            Ok(Value::Defined(Bson::Array(arr)))
        }

        // Mongo-only constructs (the storage path uses `raweval`; these owned
        // implementations keep the variants' meaning consistent across both
        // evaluators). See [`crate::raweval`].
        ScalarExpr::PathGet { base, path } => {
            let segments: Vec<&str> = path.iter().map(String::as_str).collect();
            Ok(path_get(eval(base, env)?, &segments))
        }
        ScalarExpr::MultikeyEq {
            base,
            index_path,
            value,
        } => {
            let segments: Vec<&str> = index_path.split('.').filter(|s| *s != "[]").collect();
            let resolved = path_get(eval(base, env)?, &segments);
            Ok(array_membership(resolved, eval(value, env)?))
        }
    }
}

fn literal_to_bson(lit: &Literal) -> Bson {
    match lit {
        Literal::Null => Bson::Null,
        Literal::Bool(b) => Bson::Boolean(*b),
        Literal::Int(i) => Bson::Int64(*i),
        Literal::Float(f) => Bson::Double(*f),
        Literal::Str(s) => Bson::String(s.clone()),
    }
}

// ── Path access ─────────────────────────────────────────────────

fn member_access(base: Value, field: &str) -> Value {
    match base {
        Value::Defined(Bson::Document(mut doc)) => match doc.remove(field) {
            Some(b) => Value::Defined(b),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    }
}

/// Mongo array-distributing path resolution (owned twin of
/// [`crate::raweval`]'s `get_path`): a document consumes the next segment; an
/// array applies the *same* remaining path to each element, flattening one
/// level.
fn path_get(base: Value, segments: &[&str]) -> Value {
    let Some((head, rest)) = segments.split_first() else {
        return base;
    };
    match base {
        Value::Defined(Bson::Array(items)) => {
            let mut out = Vec::new();
            for elem in items {
                match path_get(Value::Defined(elem), segments) {
                    Value::Defined(Bson::Array(inner)) => out.extend(inner),
                    Value::Defined(v) => out.push(v),
                    Value::Undefined => {}
                }
            }
            Value::Defined(Bson::Array(out))
        }
        Value::Defined(Bson::Document(mut doc)) => match doc.remove(*head) {
            Some(v) => path_get(Value::Defined(v), rest),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    }
}

/// Array membership (owned twin of `raweval`'s `array_contains`): true when the
/// defined array contains `needle` via the shared comparator. Non-array → undefined.
fn array_membership(arr: Value, needle: Value) -> Value {
    let Value::Defined(needle) = needle else {
        return Value::Undefined;
    };
    match arr {
        Value::Defined(Bson::Array(items)) => Value::Defined(Bson::Boolean(
            items
                .iter()
                .any(|e| compare_values(e, &needle) == Some(Ordering::Equal)),
        )),
        _ => Value::Undefined,
    }
}

fn index_access(base: Value, index: Value) -> Value {
    match (base, index) {
        (Value::Defined(Bson::Array(arr)), Value::Defined(idx)) => match bson_as_usize(&idx) {
            Some(i) => arr
                .into_iter()
                .nth(i)
                .map_or(Value::Undefined, Value::Defined),
            None => Value::Undefined,
        },
        (Value::Defined(Bson::Document(mut doc)), Value::Defined(Bson::String(key))) => {
            doc.remove(&key).map_or(Value::Undefined, Value::Defined)
        }
        _ => Value::Undefined,
    }
}

fn bson_as_usize(b: &Bson) -> Option<usize> {
    match b {
        Bson::Int32(i) if *i >= 0 => Some(*i as usize),
        Bson::Int64(i) if *i >= 0 => Some(*i as usize),
        Bson::Double(f) if *f >= 0.0 && f.fract() == 0.0 => Some(*f as usize),
        _ => None,
    }
}

// ── Unary ───────────────────────────────────────────────────────

fn eval_unary(op: UnaryOp, v: Value) -> Value {
    match (op, v) {
        (UnaryOp::Not, Value::Defined(Bson::Boolean(b))) => Value::Defined(Bson::Boolean(!b)),
        (UnaryOp::Neg, Value::Defined(b)) => match as_number(&b) {
            Some(Num::Int(i)) => Value::Defined(Bson::Int64(-i)),
            Some(Num::Float(f)) => Value::Defined(Bson::Double(-f)),
            None => Value::Undefined,
        },
        _ => Value::Undefined,
    }
}

// ── Binary (comparison + arithmetic) ────────────────────────────

fn eval_binop(op: BinOp, l: Value, r: Value) -> Value {
    let (lb, rb) = match (l, r) {
        (Value::Defined(a), Value::Defined(b)) => (a, b),
        _ => return Value::Undefined,
    };

    match op {
        BinOp::Eq => Value::Defined(Bson::Boolean(
            compare_values(&lb, &rb) == Some(Ordering::Equal),
        )),
        BinOp::Neq => Value::Defined(Bson::Boolean(
            compare_values(&lb, &rb) != Some(Ordering::Equal),
        )),
        BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => match compare_values(&lb, &rb) {
            Some(o) => Value::Defined(Bson::Boolean(cmp_pred(op, o))),
            None => Value::Undefined,
        },
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            match (as_number(&lb), as_number(&rb)) {
                (Some(a), Some(b)) => arith(op, a, b),
                _ => Value::Undefined,
            }
        }
        // And/Or are short-circuited in `eval`; reached only defensively.
        BinOp::And | BinOp::Or => Value::Undefined,
    }
}

pub(crate) fn cmp_pred(op: BinOp, o: Ordering) -> bool {
    match op {
        BinOp::Lt => o == Ordering::Less,
        BinOp::Lte => o != Ordering::Greater,
        BinOp::Gt => o == Ordering::Greater,
        BinOp::Gte => o != Ordering::Less,
        _ => false,
    }
}

/// Three-valued comparison. Returns `None` when the values are not
/// order-comparable (different domains), which callers map to undefined.
pub fn compare_values(a: &Bson, b: &Bson) -> Option<Ordering> {
    compare_scalar(&scalar_of_bson(a)?, &scalar_of_bson(b)?)
}

/// A comparable, borrowing view of a scalar value. This is the neutral domain
/// the order/equality rules are defined over, so the owned evaluator (here) and
/// the raw evaluator ([`crate::raweval`]) share one comparison definition and
/// cannot drift. Non-scalar values (arrays, documents) have no `Scalar` and so
/// are never order-comparable.
pub(crate) enum Scalar<'a> {
    Num(Num),
    Str(&'a str),
    Bool(bool),
    Null,
    /// Milliseconds since the Unix epoch.
    DateTime(i64),
}

/// Extract the comparable scalar from an owned `Bson`, or `None` for non-scalars.
pub(crate) fn scalar_of_bson(b: &Bson) -> Option<Scalar<'_>> {
    Some(match b {
        Bson::Int32(i) => Scalar::Num(Num::Int(*i as i64)),
        Bson::Int64(i) => Scalar::Num(Num::Int(*i)),
        Bson::Double(f) => Scalar::Num(Num::Float(*f)),
        Bson::String(s) => Scalar::Str(s),
        Bson::Boolean(x) => Scalar::Bool(*x),
        Bson::Null => Scalar::Null,
        Bson::DateTime(dt) => Scalar::DateTime(dt.timestamp_millis()),
        _ => return None,
    })
}

/// The single comparison rule, shared by the owned and raw evaluators.
pub(crate) fn compare_scalar(a: &Scalar, b: &Scalar) -> Option<Ordering> {
    match (a, b) {
        (Scalar::Num(x), Scalar::Num(y)) => num_f64(x).partial_cmp(&num_f64(y)),
        (Scalar::Str(x), Scalar::Str(y)) => Some(x.cmp(y)),
        (Scalar::Bool(x), Scalar::Bool(y)) => Some(x.cmp(y)),
        (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
        (Scalar::DateTime(x), Scalar::DateTime(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Total order over values, for `ORDER BY`. Undefined sorts first; values from
/// different domains fall back to a stable type ranking. This is the single
/// definition of sort ordering, shared by the in-memory engine here and the v2
/// `slate-executor` `Sort` node.
pub fn order_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Undefined, Value::Undefined) => Ordering::Equal,
        (Value::Undefined, _) => Ordering::Less,
        (_, Value::Undefined) => Ordering::Greater,
        (Value::Defined(x), Value::Defined(y)) => {
            compare_values(x, y).unwrap_or_else(|| type_rank(x).cmp(&type_rank(y)))
        }
    }
}

fn type_rank(b: &Bson) -> u8 {
    match b {
        Bson::Null => 0,
        Bson::Boolean(_) => 1,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => 2,
        Bson::String(_) => 3,
        Bson::DateTime(_) => 4,
        Bson::Array(_) => 5,
        Bson::Document(_) => 6,
        _ => 7,
    }
}

// ── Numeric helpers ─────────────────────────────────────────────

pub(crate) enum Num {
    Int(i64),
    Float(f64),
}

pub(crate) fn as_number(b: &Bson) -> Option<Num> {
    match b {
        Bson::Int32(i) => Some(Num::Int(*i as i64)),
        Bson::Int64(i) => Some(Num::Int(*i)),
        Bson::Double(f) => Some(Num::Float(*f)),
        _ => None,
    }
}

pub(crate) fn num_f64(n: &Num) -> f64 {
    match n {
        Num::Int(i) => *i as f64,
        Num::Float(f) => *f,
    }
}

pub(crate) fn arith(op: BinOp, a: Num, b: Num) -> Value {
    // Division always yields a double (Cosmos numbers are doubles); the other
    // ops keep integer results when both operands are integers.
    match (op, &a, &b) {
        (BinOp::Div, _, _) => {
            let y = num_f64(&b);
            if y == 0.0 {
                Value::Undefined
            } else {
                Value::Defined(Bson::Double(num_f64(&a) / y))
            }
        }
        (BinOp::Add, Num::Int(x), Num::Int(y)) => int_or_undef(x.checked_add(*y)),
        (BinOp::Sub, Num::Int(x), Num::Int(y)) => int_or_undef(x.checked_sub(*y)),
        (BinOp::Mul, Num::Int(x), Num::Int(y)) => int_or_undef(x.checked_mul(*y)),
        (BinOp::Mod, Num::Int(x), Num::Int(y)) => {
            if *y == 0 {
                Value::Undefined
            } else {
                Value::Defined(Bson::Int64(x % y))
            }
        }
        (BinOp::Add, _, _) => Value::Defined(Bson::Double(num_f64(&a) + num_f64(&b))),
        (BinOp::Sub, _, _) => Value::Defined(Bson::Double(num_f64(&a) - num_f64(&b))),
        (BinOp::Mul, _, _) => Value::Defined(Bson::Double(num_f64(&a) * num_f64(&b))),
        (BinOp::Mod, _, _) => {
            let y = num_f64(&b);
            if y == 0.0 {
                Value::Undefined
            } else {
                Value::Defined(Bson::Double(num_f64(&a) % y))
            }
        }
        _ => Value::Undefined,
    }
}

pub(crate) fn int_or_undef(v: Option<i64>) -> Value {
    match v {
        Some(i) => Value::Defined(Bson::Int64(i)),
        None => Value::Undefined, // overflow
    }
}

// ── Boolean three-valued logic ──────────────────────────────────

fn truthy(v: &Value) -> Option<bool> {
    match v {
        Value::Defined(Bson::Boolean(b)) => Some(*b),
        _ => None,
    }
}

pub(crate) fn and3(l: Option<bool>, r: Option<bool>) -> Value {
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Value::Defined(Bson::Boolean(false)),
        (Some(true), Some(true)) => Value::Defined(Bson::Boolean(true)),
        _ => Value::Undefined,
    }
}

pub(crate) fn or3(l: Option<bool>, r: Option<bool>) -> Value {
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Value::Defined(Bson::Boolean(true)),
        (Some(false), Some(false)) => Value::Defined(Bson::Boolean(false)),
        _ => Value::Undefined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::bson;

    /// Evaluate `expr` against a single binding `c -> doc`.
    fn eval_with(expr: &ScalarExpr, alias: &str, doc: &Bson) -> Value {
        let params = Document::new();
        let binds = [(alias, doc)];
        let env = Env::new(&binds, &params);
        eval(expr, &env).unwrap()
    }

    fn parse_expr(src: &str) -> ScalarExpr {
        // Reuse the full query parser to get a scalar expression.
        let q = slate_sql::parse(&format!("SELECT VALUE {src} FROM c")).unwrap();
        let slate_ast::SelectClause::Value(e) = q.select else {
            panic!("expected SELECT VALUE")
        };
        e
    }

    #[test]
    fn literal_arithmetic() {
        let v = eval_with(&parse_expr("1 + 2 * 3"), "c", &bson!({}));
        assert_eq!(v, Value::defined(7_i64));
    }

    #[test]
    fn division_is_double() {
        let v = eval_with(&parse_expr("7 / 2"), "c", &bson!({}));
        assert_eq!(v, Value::defined(3.5));
    }

    #[test]
    fn division_by_zero_is_undefined() {
        let v = eval_with(&parse_expr("1 / 0"), "c", &bson!({}));
        assert!(v.is_undefined());
    }

    #[test]
    fn path_access() {
        let doc = bson!({ "address": { "city": "Austin" } });
        let v = eval_with(&parse_expr("c.address.city"), "c", &doc);
        assert_eq!(v, Value::defined("Austin"));
    }

    #[test]
    fn missing_path_is_undefined() {
        let doc = bson!({ "a": 1 });
        let v = eval_with(&parse_expr("c.a.b.c"), "c", &doc);
        assert!(v.is_undefined());
    }

    #[test]
    fn array_index_and_key() {
        let doc = bson!({ "tags": ["x", "y", "z"], "m": { "k": 9 } });
        assert_eq!(
            eval_with(&parse_expr("c.tags[1]"), "c", &doc),
            Value::defined("y")
        );
        // `9` is stored as Int32 in the document and returned verbatim.
        assert_eq!(
            eval_with(&parse_expr(r#"c.m["k"]"#), "c", &doc),
            Value::defined(9_i32)
        );
        assert!(eval_with(&parse_expr("c.tags[10]"), "c", &doc).is_undefined());
    }

    #[test]
    fn comparison_with_coercion() {
        let doc = bson!({ "age": 30_i32 });
        assert_eq!(
            eval_with(&parse_expr("c.age > 21"), "c", &doc),
            Value::defined(true)
        );
        assert_eq!(
            eval_with(&parse_expr("c.age >= 30"), "c", &doc),
            Value::defined(true)
        );
        assert_eq!(
            eval_with(&parse_expr("c.age < 10"), "c", &doc),
            Value::defined(false)
        );
    }

    #[test]
    fn comparison_incomparable_is_undefined() {
        let doc = bson!({ "name": "ada" });
        // string vs number → not order-comparable
        assert!(eval_with(&parse_expr("c.name > 5"), "c", &doc).is_undefined());
    }

    #[test]
    fn and_short_circuits_on_false() {
        let doc = bson!({ "a": false });
        // c.missing > 5 would be undefined, but false AND _ = false
        let v = eval_with(&parse_expr("c.a AND c.missing > 5"), "c", &doc);
        assert_eq!(v, Value::defined(false));
    }

    #[test]
    fn object_literal_omits_undefined_fields() {
        let doc = bson!({ "name": "ada" });
        let v = eval_with(&parse_expr(r#"{ "n": c.name, "x": c.missing }"#), "c", &doc);
        assert_eq!(v, Value::defined(bson!({ "n": "ada" })));
    }

    #[test]
    fn negation() {
        let doc = bson!({ "n": 5 });
        assert_eq!(
            eval_with(&parse_expr("-c.n"), "c", &doc),
            Value::defined(-5_i64)
        );
    }
}
