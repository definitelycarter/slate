//! Borrowing scalar-expression evaluation over raw BSON bytes.
//!
//! This is the storage-path twin of [`crate::eval`]. Where `eval` decodes a
//! whole document into owned `bson::Bson` and walks that, `raweval` walks the
//! raw bytes directly: identifier lookup, member/index access, and scalar
//! comparison are **zero-copy**, borrowing straight out of the input document.
//! Only genuinely *computed* values (arithmetic results, function returns,
//! constructed objects/arrays) materialize an owned `Bson`.
//!
//! The two evaluators must agree on semantics or `WHERE`/`ORDER BY` would drift
//! between the in-memory `slate-sql` engine and the storage-backed
//! `slate-executor`. They cannot: every leaf rule — comparison, numeric
//! coercion, arithmetic, three-valued logic, function dispatch — is the single
//! shared definition in [`crate::eval`], reused here verbatim. A differential
//! test (`raw_matches_owned`) pins this down.

use std::cmp::Ordering;

use bson::raw::{CString, RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf};
use bson::{Bson, RawBson};

use crate::error::{EvalError, Result};
use crate::eval::{
    Num, Scalar, and3, arith, as_number, cmp_pred, compare_scalar, or3, scalar_of_bson,
};
use crate::value::Value;
use slate_ast::{BinOp, Literal, ScalarExpr, UnaryOp};

/// The result of evaluating a [`ScalarExpr`] over raw bytes.
///
/// `Ref` borrows directly from the input document (the common, fast case);
/// `Owned`/`OwnedRaw` hold a value computed during evaluation.
#[derive(Debug, Clone)]
pub enum RawValue<'a> {
    Undefined,
    Ref(RawBsonRef<'a>),
    /// A computed scalar/value held as owned `Bson` (arithmetic, function
    /// results) — reuses the owned evaluator's logic.
    Owned(Bson),
    /// A computed value already in raw form (a constructed object/array). Kept
    /// raw so it reaches the output stream without a re-serialize round-trip.
    OwnedRaw(RawBson),
}

impl<'a> RawValue<'a> {
    pub fn is_undefined(&self) -> bool {
        matches!(self, RawValue::Undefined)
    }

    /// The boolean value, or `None` if this is not a defined boolean. Used by
    /// `Filter` to apply the three-valued `WHERE` rule (only `Some(true)` keeps
    /// a row).
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            RawValue::Ref(RawBsonRef::Boolean(b)) => Some(*b),
            RawValue::Owned(Bson::Boolean(b)) => Some(*b),
            _ => None,
        }
    }

    /// Bridge a computed owned [`Value`] (e.g. a function result) into a
    /// `RawValue`.
    pub fn from_value(v: Value) -> Self {
        match v {
            Value::Undefined => RawValue::Undefined,
            Value::Defined(b) => RawValue::Owned(b),
        }
    }

    /// Materialize into an owned [`Value`]. Used where downstream needs owned
    /// values (function arguments, `ORDER BY` keys); `Ref` pays one copy here.
    pub fn into_value(self) -> Result<Value> {
        Ok(match self {
            RawValue::Undefined => Value::Undefined,
            RawValue::Owned(b) => Value::Defined(b),
            RawValue::Ref(r) => Value::Defined(Bson::try_from(r).map_err(decode_err)?),
            RawValue::OwnedRaw(rb) => {
                Value::Defined(Bson::try_from(rb.as_raw_bson_ref()).map_err(decode_err)?)
            }
        })
    }

    /// Materialize into an owned [`RawBson`] for the output stream, or `None`
    /// when undefined (which drops the row / omits the field). A `Ref` is a
    /// single byte copy; an `Owned` re-serializes.
    pub fn into_raw(self) -> Result<Option<RawBson>> {
        Ok(match self {
            RawValue::Undefined => None,
            RawValue::Ref(r) => Some(RawBson::from(r)),
            RawValue::Owned(b) => Some(RawBson::try_from(b).map_err(|e| EvalError {
                message: format!("could not encode projected value: {e}"),
            })?),
            // Already raw — no re-serialize.
            RawValue::OwnedRaw(rb) => Some(rb),
        })
    }
}

/// The bindings visible to a raw expression: alias → bound raw value, plus
/// optional query parameters (`@name`) as a raw document.
pub struct RawEnv<'a> {
    bindings: &'a [(&'a str, RawBsonRef<'a>)],
    params: Option<&'a RawDocument>,
}

impl<'a> RawEnv<'a> {
    pub fn new(bindings: &'a [(&'a str, RawBsonRef<'a>)], params: Option<&'a RawDocument>) -> Self {
        Self { bindings, params }
    }

    fn lookup(&self, name: &str) -> RawValue<'a> {
        for (n, v) in self.bindings {
            if *n == name {
                return RawValue::Ref(*v);
            }
        }
        RawValue::Undefined
    }

    fn param(&self, name: &str) -> Result<RawValue<'a>> {
        match self.params {
            Some(p) => match get_field(p, name)? {
                Some(r) => Ok(RawValue::Ref(r)),
                None => Ok(RawValue::Undefined),
            },
            None => Ok(RawValue::Undefined),
        }
    }
}

/// Evaluate a scalar expression in `env`, borrowing from the input where
/// possible.
pub fn eval<'a>(expr: &'a ScalarExpr, env: &RawEnv<'a>) -> Result<RawValue<'a>> {
    match expr {
        ScalarExpr::Literal(lit) => Ok(literal_value(lit)),
        // A materialized value: cheap to clone (literals are scalars). The
        // borrowed-bytes fast paths apply to bound rows, not query constants.
        ScalarExpr::Value(b) => Ok(RawValue::Owned(b.clone())),
        ScalarExpr::Identifier(name) => Ok(env.lookup(name)),
        ScalarExpr::Parameter(name) => env.param(name),

        ScalarExpr::Member { base, field } => member_access(eval(base, env)?, field),
        ScalarExpr::Index { base, index } => {
            let b = eval(base, env)?;
            let i = eval(index, env)?;
            index_access(b, i)
        }

        ScalarExpr::Unary { op, expr } => Ok(eval_unary(*op, eval(expr, env)?)),

        ScalarExpr::Binary { op, lhs, rhs } => eval_binary(*op, lhs, rhs, env),

        ScalarExpr::Function { name, args } => eval_function(name, args, env),

        ScalarExpr::Object(fields) => build_object(fields, env),
        ScalarExpr::Array(items) => build_array(items, env),
    }
}

/// Dispatch a function call. The hottest predicates have zero-materialization
/// fast paths — they walk raw bytes instead of converting arguments to owned
/// `Bson` — and must agree with [`crate::functions`], which the differential
/// `raw_matches_owned` test pins down. Everything else materializes its
/// arguments and dispatches through the shared [`crate::functions::call`].
fn eval_function<'a>(name: &str, args: &'a [ScalarExpr], env: &RawEnv<'a>) -> Result<RawValue<'a>> {
    if args.len() == 1 {
        if name.eq_ignore_ascii_case("IS_DEFINED") {
            return Ok(bool_value(!eval(&args[0], env)?.is_undefined()));
        }
        if name.eq_ignore_ascii_case("IS_NULL") {
            return Ok(bool_value(is_null(&eval(&args[0], env)?)));
        }
    }
    if args.len() == 2 && name.eq_ignore_ascii_case("ARRAY_CONTAINS") {
        let arr = eval(&args[0], env)?;
        let needle = eval(&args[1], env)?;
        return Ok(array_contains(&arr, &needle));
    }

    let mut vals = Vec::with_capacity(args.len());
    for a in args {
        vals.push(eval(a, env)?.into_value()?);
    }
    crate::functions::call(name, vals).map(RawValue::from_value)
}

fn is_null(v: &RawValue) -> bool {
    matches!(
        v,
        RawValue::Ref(RawBsonRef::Null) | RawValue::Owned(Bson::Null)
    )
}

/// `ARRAY_CONTAINS(array, needle)` over raw bytes: iterate the array in place
/// and compare each element to `needle` with the shared scalar comparator — no
/// materialization of the array. Mirrors the owned [`crate::functions`] version:
/// a non-array `array` (or undefined `needle`) yields undefined.
fn array_contains<'a>(arr: &RawValue, needle: &RawValue) -> RawValue<'a> {
    if needle.is_undefined() {
        return RawValue::Undefined;
    }
    let nscalar = value_scalar(needle);
    let eq = |elem: Option<Scalar>| match (&elem, &nscalar) {
        (Some(e), Some(n)) => compare_scalar(e, n) == Some(Ordering::Equal),
        _ => false,
    };
    let found = match arr {
        RawValue::Ref(RawBsonRef::Array(a)) => a.into_iter().any(|e| match e {
            Ok(elem) => eq(scalar_of_raw(elem)),
            Err(_) => false,
        }),
        RawValue::Owned(Bson::Array(items)) => items.iter().any(|b| eq(scalar_of_bson(b))),
        _ => return RawValue::Undefined,
    };
    bool_value(found)
}

fn literal_value(lit: &Literal) -> RawValue<'_> {
    match lit {
        Literal::Null => RawValue::Ref(RawBsonRef::Null),
        Literal::Bool(b) => RawValue::Ref(RawBsonRef::Boolean(*b)),
        Literal::Int(i) => RawValue::Ref(RawBsonRef::Int64(*i)),
        Literal::Float(f) => RawValue::Ref(RawBsonRef::Double(*f)),
        Literal::Str(s) => RawValue::Ref(RawBsonRef::String(s)),
    }
}

// ── Path access ─────────────────────────────────────────────────

fn member_access<'a>(base: RawValue<'a>, field: &str) -> Result<RawValue<'a>> {
    Ok(match base {
        RawValue::Ref(RawBsonRef::Document(d)) => match get_field(d, field)? {
            Some(v) => RawValue::Ref(v),
            None => RawValue::Undefined,
        },
        // Member access on a *computed* document is rare (e.g. `{...}.x`); the
        // child must be cloned out since it cannot outlive the temporary.
        RawValue::Owned(Bson::Document(mut doc)) => doc
            .remove(field)
            .map_or(RawValue::Undefined, RawValue::Owned),
        RawValue::OwnedRaw(RawBson::Document(buf)) => match get_field(&buf, field)? {
            Some(r) => RawValue::OwnedRaw(RawBson::from(r)),
            None => RawValue::Undefined,
        },
        _ => RawValue::Undefined,
    })
}

fn index_access<'a>(base: RawValue<'a>, index: RawValue<'a>) -> Result<RawValue<'a>> {
    Ok(match base {
        RawValue::Ref(RawBsonRef::Array(a)) => match value_as_usize(&index) {
            Some(i) => match a.get(i).map_err(decode_err)? {
                Some(v) => RawValue::Ref(v),
                None => RawValue::Undefined,
            },
            None => RawValue::Undefined,
        },
        RawValue::Ref(RawBsonRef::Document(d)) => match value_as_str(&index) {
            Some(k) => match get_field(d, k)? {
                Some(v) => RawValue::Ref(v),
                None => RawValue::Undefined,
            },
            None => RawValue::Undefined,
        },
        // Computed array/object indexing (rare): values are owned and cloned out.
        RawValue::Owned(Bson::Array(arr)) => match value_as_usize(&index) {
            Some(i) => arr
                .into_iter()
                .nth(i)
                .map_or(RawValue::Undefined, RawValue::Owned),
            None => RawValue::Undefined,
        },
        RawValue::Owned(Bson::Document(mut doc)) => match value_as_str(&index) {
            Some(k) => doc.remove(k).map_or(RawValue::Undefined, RawValue::Owned),
            None => RawValue::Undefined,
        },
        // Computed *raw* array/object indexing: clone the element out.
        RawValue::OwnedRaw(RawBson::Array(buf)) => match value_as_usize(&index) {
            Some(i) => match buf.get(i).map_err(decode_err)? {
                Some(v) => RawValue::OwnedRaw(RawBson::from(v)),
                None => RawValue::Undefined,
            },
            None => RawValue::Undefined,
        },
        RawValue::OwnedRaw(RawBson::Document(buf)) => match value_as_str(&index) {
            Some(k) => match get_field(&buf, k)? {
                Some(v) => RawValue::OwnedRaw(RawBson::from(v)),
                None => RawValue::Undefined,
            },
            None => RawValue::Undefined,
        },
        _ => RawValue::Undefined,
    })
}

fn get_field<'a>(d: &'a RawDocument, field: &str) -> Result<Option<RawBsonRef<'a>>> {
    d.get(field).map_err(|e| EvalError {
        message: format!("could not read field '{field}': {e}"),
    })
}

// ── Unary ───────────────────────────────────────────────────────

fn eval_unary<'a>(op: UnaryOp, v: RawValue<'a>) -> RawValue<'a> {
    match op {
        UnaryOp::Not => match v.as_bool() {
            Some(b) => bool_value(!b),
            None => RawValue::Undefined,
        },
        UnaryOp::Neg => match value_num(&v) {
            Some(Num::Int(i)) => RawValue::Owned(Bson::Int64(-i)),
            Some(Num::Float(f)) => RawValue::Owned(Bson::Double(-f)),
            None => RawValue::Undefined,
        },
    }
}

// ── Binary (comparison + arithmetic + logic) ────────────────────

fn eval_binary<'a>(
    op: BinOp,
    lhs: &'a ScalarExpr,
    rhs: &'a ScalarExpr,
    env: &RawEnv<'a>,
) -> Result<RawValue<'a>> {
    match op {
        BinOp::And => {
            let l = eval(lhs, env)?.as_bool();
            if l == Some(false) {
                return Ok(bool_value(false));
            }
            let r = eval(rhs, env)?.as_bool();
            Ok(RawValue::from_value(and3(l, r)))
        }
        BinOp::Or => {
            let l = eval(lhs, env)?.as_bool();
            if l == Some(true) {
                return Ok(bool_value(true));
            }
            let r = eval(rhs, env)?.as_bool();
            Ok(RawValue::from_value(or3(l, r)))
        }
        _ => Ok(eval_binop(op, eval(lhs, env)?, eval(rhs, env)?)),
    }
}

fn eval_binop<'a>(op: BinOp, l: RawValue<'a>, r: RawValue<'a>) -> RawValue<'a> {
    if l.is_undefined() || r.is_undefined() {
        return RawValue::Undefined;
    }
    match op {
        BinOp::Eq => bool_value(compare(&l, &r) == Some(Ordering::Equal)),
        BinOp::Neq => bool_value(compare(&l, &r) != Some(Ordering::Equal)),
        BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => match compare(&l, &r) {
            Some(o) => bool_value(cmp_pred(op, o)),
            None => RawValue::Undefined,
        },
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            match (value_num(&l), value_num(&r)) {
                (Some(a), Some(b)) => RawValue::from_value(arith(op, a, b)),
                _ => RawValue::Undefined,
            }
        }
        // And/Or are short-circuited in `eval_binary`; reached only defensively.
        BinOp::And | BinOp::Or => RawValue::Undefined,
    }
}

// ── Object / array construction ─────────────────────────────────

fn build_object<'a>(fields: &'a [(String, ScalarExpr)], env: &RawEnv<'a>) -> Result<RawValue<'a>> {
    // Build the result directly in raw form: each field value is appended as
    // raw bytes, so the projected document needs no Bson round-trip on output.
    let mut doc = RawDocumentBuf::new();
    for (k, v) in fields {
        // Undefined fields are omitted (Cosmos behavior).
        if let Some(raw) = eval(v, env)?.into_raw()? {
            let key = CString::try_from(k.as_str()).map_err(|e| EvalError {
                message: format!("invalid object key '{k}': {e}"),
            })?;
            doc.append(key, raw);
        }
    }
    Ok(RawValue::OwnedRaw(RawBson::Document(doc)))
}

fn build_array<'a>(items: &'a [ScalarExpr], env: &RawEnv<'a>) -> Result<RawValue<'a>> {
    let mut arr = RawArrayBuf::new();
    for it in items {
        // Undefined elements are omitted (Cosmos behavior).
        if let Some(raw) = eval(it, env)?.into_raw()? {
            arr.push(raw);
        }
    }
    Ok(RawValue::OwnedRaw(RawBson::Array(arr)))
}

// ── Shared scalar views over a RawValue ─────────────────────────

/// The comparable scalar of a `RawValue`, routed through the shared
/// definitions in [`crate::eval`] so it cannot drift from the owned evaluator.
fn value_scalar<'a>(v: &'a RawValue) -> Option<Scalar<'a>> {
    match v {
        RawValue::Ref(r) => scalar_of_raw(*r),
        RawValue::Owned(b) => scalar_of_bson(b),
        RawValue::OwnedRaw(rb) => scalar_of_raw(rb.as_raw_bson_ref()),
        RawValue::Undefined => None,
    }
}

fn scalar_of_raw(r: RawBsonRef<'_>) -> Option<Scalar<'_>> {
    Some(match r {
        RawBsonRef::Int32(i) => Scalar::Num(Num::Int(i as i64)),
        RawBsonRef::Int64(i) => Scalar::Num(Num::Int(i)),
        RawBsonRef::Double(f) => Scalar::Num(Num::Float(f)),
        RawBsonRef::String(s) => Scalar::Str(s),
        RawBsonRef::Boolean(b) => Scalar::Bool(b),
        RawBsonRef::Null => Scalar::Null,
        RawBsonRef::DateTime(dt) => Scalar::DateTime(dt.timestamp_millis()),
        _ => return None,
    })
}

fn compare(a: &RawValue, b: &RawValue) -> Option<Ordering> {
    compare_scalar(&value_scalar(a)?, &value_scalar(b)?)
}

fn value_num(v: &RawValue) -> Option<Num> {
    match v {
        RawValue::Ref(r) => raw_as_number(*r),
        RawValue::Owned(b) => as_number(b),
        RawValue::OwnedRaw(rb) => raw_as_number(rb.as_raw_bson_ref()),
        RawValue::Undefined => None,
    }
}

fn raw_as_number(r: RawBsonRef<'_>) -> Option<Num> {
    match r {
        RawBsonRef::Int32(i) => Some(Num::Int(i as i64)),
        RawBsonRef::Int64(i) => Some(Num::Int(i)),
        RawBsonRef::Double(f) => Some(Num::Float(f)),
        _ => None,
    }
}

fn value_as_str<'a>(v: &'a RawValue) -> Option<&'a str> {
    match v {
        RawValue::Ref(RawBsonRef::String(s)) => Some(s),
        RawValue::Owned(Bson::String(s)) => Some(s),
        _ => None,
    }
}

fn value_as_usize(v: &RawValue) -> Option<usize> {
    match value_num(v)? {
        Num::Int(i) if i >= 0 => Some(i as usize),
        Num::Float(f) if f >= 0.0 && f.fract() == 0.0 => Some(f as usize),
        _ => None,
    }
}

fn bool_value<'a>(b: bool) -> RawValue<'a> {
    RawValue::Ref(RawBsonRef::Boolean(b))
}

fn decode_err(e: bson::error::Error) -> EvalError {
    EvalError {
        message: format!("could not decode raw value: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::{Env, eval as owned_eval};
    use bson::{Document, RawDocumentBuf, bson};
    use slate_ast::SelectClause;

    fn parse_expr(src: &str) -> ScalarExpr {
        let q = slate_sql::parse(&format!("SELECT VALUE {src} FROM c")).unwrap();
        let SelectClause::Value(e) = q.select;
        e
    }

    /// Evaluate `src` against `doc` (bound to `c`) with BOTH the owned and raw
    /// evaluators and assert they produce the same owned value. This is the
    /// anti-drift guard between [`crate::eval`] and [`crate::raweval`].
    fn assert_agree(src: &str, doc: &bson::Bson) {
        let expr = parse_expr(src);

        let params = Document::new();
        let binds = [("c", doc)];
        let owned = owned_eval(&expr, &Env::new(&binds, &params)).unwrap();

        let raw_doc = RawDocumentBuf::try_from(doc.as_document().unwrap()).unwrap();
        let cref = RawBsonRef::Document(&raw_doc);
        let rbinds = [("c", cref)];
        let raw = eval(&expr, &RawEnv::new(&rbinds, None))
            .unwrap()
            .into_value()
            .unwrap();

        assert_eq!(owned, raw, "raw/owned disagree on `{src}`");
    }

    #[test]
    fn raw_matches_owned() {
        let doc = bson!({
            "name": "ada",
            "age": 30_i32,
            "score": 4.5,
            "flag": true,
            "address": { "city": "Austin", "zip": "78701" },
            "tags": ["x", "y", "z"],
            "m": { "k": 9_i32 },
        });

        for src in [
            // identity + path
            "c",
            "c.name",
            "c.age",
            "c.address",
            "c.address.city",
            "c.nope",
            "c.address.nope",
            // index
            "c.tags[0]",
            "c.tags[2]",
            "c.tags[9]",
            r#"c.m["k"]"#,
            // arithmetic
            "c.age + 1",
            "c.age * 2",
            "c.age - 100",
            "c.score / 2",
            "7 / 2",
            "1 / 0",
            "10 % 3",
            "-c.age",
            // comparison (with coercion + incomparable)
            "c.age > 21",
            "c.age >= 30",
            "c.age < 10",
            "c.age = 30",
            "c.age != 30",
            "c.name = \"ada\"",
            "c.name > 5",
            "c.score > c.age",
            // logic + short-circuit
            "c.flag AND c.age > 1",
            "c.flag OR c.missing > 5",
            "c.missing AND c.age > 1",
            "NOT c.flag",
            // construction
            r#"{ "n": c.name, "doubled": c.age * 2, "x": c.nope }"#,
            "[c.age, c.name, c.nope]",
            // functions
            "UPPER(c.name)",
            "LENGTH(c.name)",
            "CONCAT(c.name, c.address.city)",
            "ARRAY_CONTAINS(c.tags, \"y\")",
            "ARRAY_CONTAINS(c.tags, \"q\")",
            "IS_DEFINED(c.nope)",
        ] {
            assert_agree(src, &doc);
        }
    }
}
