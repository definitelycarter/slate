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

use bson::raw::{BindRawBsonRef, CString, RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf};
use bson::{Bson, RawBson};

use crate::error::{EvalError, Result};
use crate::eval::{
    Num, Scalar, and3, arith, as_number, cmp_pred, compare_scalar, or3, scalar_of_bson,
};
use crate::value::Value;
use slate_ast::{BinOp, Literal, ScalarExpr, UnaryOp};
use slate_rawbson::RawField;

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

    /// The first (in single-binding mode, only) bound value, without a name
    /// lookup. Used by the compiled fast path for the sole `FROM` alias.
    fn sole_row(&self) -> RawValue<'a> {
        match self.bindings.first() {
            Some((_, v)) => RawValue::Ref(*v),
            None => RawValue::Undefined,
        }
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
        // Extracted into a correlated-apply node by the planner; never seen here.
        ScalarExpr::Subquery { .. } => Err(EvalError {
            message: "subquery must be lowered by the planner, not evaluated directly".into(),
        }),

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

        ScalarExpr::PathGet { base, path } => get_path(eval(base, env)?, path),
        ScalarExpr::MultikeyEq {
            base,
            index_path,
            value,
        } => {
            let resolved = {
                // `.[]` markers only say "an array is here" — drop them; GET_PATH
                // distributes over any array. The verbatim path is for the planner.
                let segments: Vec<&str> = index_path.split('.').filter(|s| *s != "[]").collect();
                get_path(eval(base, env)?, &segments)?
            };
            let needle = eval(value, env)?;
            Ok(array_contains(&resolved, &needle))
        }
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
        // A computed raw array, e.g. from GET_PATH distributing over subdocs.
        RawValue::OwnedRaw(RawBson::Array(a)) => a.into_iter().any(|e| match e {
            Ok(elem) => eq(scalar_of_raw(elem)),
            Err(_) => false,
        }),
        RawValue::Owned(Bson::Array(items)) => items.iter().any(|b| eq(scalar_of_bson(b))),
        _ => return RawValue::Undefined,
    };
    bool_value(found)
}

/// Resolve a dotted path against `base`, distributing over arrays. A document
/// consumes the next segment; an array applies the *same* remaining segments to
/// each element and flattens the results one level. Returns a scalar for a
/// plain path, or an array when an array was traversed.
fn get_path<'a, S: AsRef<str>>(base: RawValue<'a>, segments: &[S]) -> Result<RawValue<'a>> {
    let Some((head, rest)) = segments.split_first() else {
        return Ok(base);
    };
    match base {
        RawValue::Ref(RawBsonRef::Array(a)) => {
            let mut out = RawArrayBuf::new();
            for elem in a {
                let elem = elem.map_err(decode_err)?;
                // The array itself doesn't consume a segment — distribute.
                append_flatten(&mut out, get_path(RawValue::Ref(elem), segments)?)?;
            }
            Ok(RawValue::OwnedRaw(RawBson::Array(out)))
        }
        RawValue::Ref(RawBsonRef::Document(d)) => match get_field(d, head.as_ref())? {
            Some(v) => get_path(RawValue::Ref(v), rest),
            None => Ok(RawValue::Undefined),
        },
        _ => Ok(RawValue::Undefined),
    }
}

/// Append a path-resolution result to `out`, flattening one array level and
/// dropping undefined (Mongo array-path semantics).
fn append_flatten(out: &mut RawArrayBuf, value: RawValue) -> Result<()> {
    match value {
        RawValue::Undefined => {}
        RawValue::OwnedRaw(RawBson::Array(inner)) => {
            for e in &inner {
                out.push(RawBson::from(e.map_err(decode_err)?));
            }
        }
        v => {
            if let Some(raw) = v.into_raw()? {
                out.push(raw);
            }
        }
    }
    Ok(())
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
    // Scan the raw bytes directly (slate-rawbson) rather than the bson crate's
    // validating `RawDocument::get`, which UTF-8-checks every key it skips. This
    // is the hot per-row field lookup behind `Member`/path access. `RawField::get`
    // resolves one flat segment (callers descend dot-paths a hop at a time) and
    // `.value()` preserves an explicit `Null` (unlike `get_value`), so
    // `IS_NULL`/`c.field == null` still see it. Malformed bytes read as missing,
    // matching v1's filter path; stored documents are validated on write.
    Ok(RawField::get(d.as_bytes(), field).and_then(|f| f.value()))
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

// ── Compiled expressions ────────────────────────────────────────
//
// [`eval`] tree-walks the AST for **every row**, and each pass redoes work that
// depends only on the expression, not the data: resolving a function name by a
// chain of case-insensitive string compares, splitting a dotted path into a
// fresh `Vec`, and looking up the `FROM` alias by name. [`compile`] does that
// resolution once; [`eval_compiled`] then runs the pre-resolved form per row.
//
// It mirrors [`eval`] exactly — the `compiled_matches_eval` test pins the two
// together over the same expression corpus as `raw_matches_owned`. Only the
// redundant per-row work is hoisted out; every leaf rule is still the shared
// definition reused by `eval`.

/// A scalar expression with all data-independent work resolved ahead of time.
/// Built once per query by [`compile`]; evaluated per row by [`eval_compiled`].
pub enum Compiled {
    /// An expression `compile` can't lower (currently only a raw `Subquery`,
    /// which the planner is expected to have extracted). Errors if evaluated.
    Unsupported(String),
    Literal(Literal),
    /// A query constant pre-converted to raw bytes, so each row borrows it as a
    /// `Ref` instead of cloning a fresh `Bson` (the per-row cost in `eval`).
    Value(RawBson),
    /// Fallback for the rare constant that doesn't convert to `RawBson`; cloned
    /// per row as in `eval`.
    ValueBson(Bson),
    /// The sole `FROM` binding, resolved without a name lookup (single-binding
    /// mode only — see [`compile`]'s `sole` argument).
    Row,
    /// `<sole-alias>.<field>` collapsed to a direct field read on [`Row`].
    RowField(String),
    Identifier(String),
    Parameter(String),
    Member {
        base: Box<Compiled>,
        field: String,
    },
    Index {
        base: Box<Compiled>,
        index: Box<Compiled>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Compiled>,
    },
    Binary {
        op: BinOp,
        lhs: Box<Compiled>,
        rhs: Box<Compiled>,
    },
    Object(Vec<(ObjKey, Compiled)>),
    Array(Vec<Compiled>),
    /// `path` is pre-split into owned segments (no per-row `Vec`).
    PathGet {
        base: Box<Compiled>,
        path: Vec<String>,
    },
    /// `path` is pre-split with the `[]` markers already dropped.
    MultikeyEq {
        base: Box<Compiled>,
        path: Vec<String>,
        value: Box<Compiled>,
    },
    // Function dispatch resolved once: the hot predicates become dedicated
    // variants, everything else a generic call retaining the resolved name.
    IsDefined(Box<Compiled>),
    IsNull(Box<Compiled>),
    ArrayContains {
        arr: Box<Compiled>,
        needle: Box<Compiled>,
    },
    /// The Mongo implicit-equality idiom `base = value OR ARRAY_CONTAINS(base,
    /// value)`, fused so `base` is fetched **once** per row instead of twice
    /// (the interpreter walks the `Eq` and `ARRAY_CONTAINS` subtrees separately,
    /// re-fetching the field). Semantically identical to the source `Or`.
    MongoEq {
        base: Box<Compiled>,
        value: Box<Compiled>,
    },
    Call {
        name: String,
        args: Vec<Compiled>,
    },
}

/// A precompiled object-projection key. `build_object` (in `eval`) revalidates
/// the key as a `CString` on every row; here it is validated once at compile
/// time and appended by reference, so the hot projection path allocates nothing
/// for keys.
pub enum ObjKey {
    /// A valid key, ready to append by reference.
    Ready(CString),
    /// A key the `bson` `CString` rejects (an interior NUL — vanishingly rare);
    /// revalidated per row so the same error surfaces as in `eval`.
    Lazy(String),
}

/// Compile `expr` for repeated evaluation. `sole` is the single `FROM` alias
/// when the node reads bare rows ([`RowBinding::Alias`] mode); pass `None` for
/// the multi-binding environment shape so identifiers fall back to name lookup.
pub fn compile(expr: &ScalarExpr, sole: Option<&str>) -> Compiled {
    match expr {
        ScalarExpr::Literal(l) => Compiled::Literal(l.clone()),
        // Pre-convert the constant to raw bytes once. The common scalar/array
        // constants convert; anything that doesn't falls back to a per-row clone.
        ScalarExpr::Value(b) => match RawBson::try_from(b.clone()) {
            Ok(raw) => Compiled::Value(raw),
            Err(_) => Compiled::ValueBson(b.clone()),
        },
        ScalarExpr::Identifier(n) => {
            if sole == Some(n.as_str()) {
                Compiled::Row
            } else {
                Compiled::Identifier(n.clone())
            }
        }
        ScalarExpr::Parameter(n) => Compiled::Parameter(n.clone()),
        ScalarExpr::Member { base, field } => {
            // Collapse `<sole-alias>.<field>` to a direct field read on the row,
            // dropping the identifier lookup and its intermediate value.
            match compile(base, sole) {
                Compiled::Row => Compiled::RowField(field.clone()),
                base => Compiled::Member {
                    base: Box::new(base),
                    field: field.clone(),
                },
            }
        }
        ScalarExpr::Index { base, index } => Compiled::Index {
            base: Box::new(compile(base, sole)),
            index: Box::new(compile(index, sole)),
        },
        ScalarExpr::Unary { op, expr } => Compiled::Unary {
            op: *op,
            expr: Box::new(compile(expr, sole)),
        },
        ScalarExpr::Binary { op, lhs, rhs } => {
            // Fuse the Mongo implicit-equality idiom so the field is read once.
            if *op == BinOp::Or
                && let Some((base, value)) = as_eq_or_contains(lhs, rhs)
            {
                return Compiled::MongoEq {
                    base: Box::new(compile(base, sole)),
                    value: Box::new(compile(value, sole)),
                };
            }
            Compiled::Binary {
                op: *op,
                lhs: Box::new(compile(lhs, sole)),
                rhs: Box::new(compile(rhs, sole)),
            }
        }
        ScalarExpr::Function { name, args } => compile_function(name, args, sole),
        ScalarExpr::Object(fields) => Compiled::Object(
            fields
                .iter()
                .map(|(k, v)| {
                    // Validate the key once; per-row append then borrows it.
                    let key = match CString::try_from(k.as_str()) {
                        Ok(c) => ObjKey::Ready(c),
                        Err(_) => ObjKey::Lazy(k.clone()),
                    };
                    (key, compile(v, sole))
                })
                .collect(),
        ),
        ScalarExpr::Array(items) => {
            Compiled::Array(items.iter().map(|e| compile(e, sole)).collect())
        }
        ScalarExpr::PathGet { base, path } => Compiled::PathGet {
            base: Box::new(compile(base, sole)),
            path: path.clone(),
        },
        ScalarExpr::MultikeyEq {
            base,
            index_path,
            value,
        } => Compiled::MultikeyEq {
            base: Box::new(compile(base, sole)),
            // `.[]` markers only mean "an array is here"; drop them once. The
            // verbatim path was for the planner.
            path: index_path
                .split('.')
                .filter(|s| *s != "[]")
                .map(str::to_string)
                .collect(),
            value: Box::new(compile(value, sole)),
        },
        ScalarExpr::Subquery { .. } => {
            Compiled::Unsupported("subquery must be lowered by the planner".into())
        }
    }
}

/// Recognize the Mongo implicit-equality idiom `base = value OR
/// ARRAY_CONTAINS(base, value)` (what `slate-query` emits for `{field: value}`),
/// returning its shared `(base, value)` when both sides reference the same
/// operands. Used by [`compile`] to fuse the redundant double field read.
fn as_eq_or_contains<'a>(
    lhs: &'a ScalarExpr,
    rhs: &'a ScalarExpr,
) -> Option<(&'a ScalarExpr, &'a ScalarExpr)> {
    let ScalarExpr::Binary {
        op: BinOp::Eq,
        lhs: eq_base,
        rhs: eq_value,
    } = lhs
    else {
        return None;
    };
    let ScalarExpr::Function { name, args } = rhs else {
        return None;
    };
    if !name.eq_ignore_ascii_case("ARRAY_CONTAINS") || args.len() != 2 {
        return None;
    }
    // The Eq and ARRAY_CONTAINS must be over the *same* field and value, or the
    // fusion would change meaning.
    if eq_base.as_ref() == &args[0] && eq_value.as_ref() == &args[1] {
        Some((eq_base, eq_value))
    } else {
        None
    }
}

/// Resolve a function call to its compiled form, mirroring the dispatch in
/// [`eval_function`] but doing the name match once.
fn compile_function(name: &str, args: &[ScalarExpr], sole: Option<&str>) -> Compiled {
    let c = |e| Box::new(compile(e, sole));
    if args.len() == 1 {
        if name.eq_ignore_ascii_case("IS_DEFINED") {
            return Compiled::IsDefined(c(&args[0]));
        }
        if name.eq_ignore_ascii_case("IS_NULL") {
            return Compiled::IsNull(c(&args[0]));
        }
    }
    if args.len() == 2 && name.eq_ignore_ascii_case("ARRAY_CONTAINS") {
        return Compiled::ArrayContains {
            arr: c(&args[0]),
            needle: c(&args[1]),
        };
    }
    Compiled::Call {
        name: name.to_string(),
        args: args.iter().map(|a| compile(a, sole)).collect(),
    }
}

/// Evaluate a [`Compiled`] expression in `env`. Semantically identical to
/// [`eval`] over the source [`ScalarExpr`].
pub fn eval_compiled<'a>(c: &'a Compiled, env: &RawEnv<'a>) -> Result<RawValue<'a>> {
    match c {
        Compiled::Unsupported(msg) => Err(EvalError {
            message: msg.clone(),
        }),
        Compiled::Literal(l) => Ok(literal_value(l)),
        // Borrow the pre-converted constant — no per-row allocation.
        Compiled::Value(raw) => Ok(RawValue::Ref(raw.as_raw_bson_ref())),
        Compiled::ValueBson(b) => Ok(RawValue::Owned(b.clone())),
        Compiled::Row => Ok(env.sole_row()),
        Compiled::RowField(field) => member_access(env.sole_row(), field),
        Compiled::Identifier(n) => Ok(env.lookup(n)),
        Compiled::Parameter(n) => env.param(n),

        Compiled::Member { base, field } => member_access(eval_compiled(base, env)?, field),
        Compiled::Index { base, index } => {
            let b = eval_compiled(base, env)?;
            let i = eval_compiled(index, env)?;
            index_access(b, i)
        }

        Compiled::Unary { op, expr } => Ok(eval_unary(*op, eval_compiled(expr, env)?)),
        Compiled::Binary { op, lhs, rhs } => eval_compiled_binary(*op, lhs, rhs, env),

        Compiled::Object(fields) => {
            let mut doc = RawDocumentBuf::new();
            for (k, v) in fields {
                // Append the field value borrowing where possible: a `Ref` (a
                // selected document field — the common projection case) and an
                // `OwnedRaw` go straight in, skipping the owned `RawBson` that
                // `into_raw` would allocate per string/document/array value.
                match eval_compiled(v, env)? {
                    RawValue::Undefined => {} // omit (Cosmos behavior)
                    RawValue::Ref(r) => append_field(&mut doc, k, r)?,
                    RawValue::OwnedRaw(rb) => append_field(&mut doc, k, rb.as_raw_bson_ref())?,
                    RawValue::Owned(b) => {
                        let raw = RawBson::try_from(b).map_err(|e| EvalError {
                            message: format!("could not encode projected value: {e}"),
                        })?;
                        append_field(&mut doc, k, raw)?;
                    }
                }
            }
            Ok(RawValue::OwnedRaw(RawBson::Document(doc)))
        }
        Compiled::Array(items) => {
            let mut arr = RawArrayBuf::new();
            for it in items {
                if let Some(raw) = eval_compiled(it, env)?.into_raw()? {
                    arr.push(raw);
                }
            }
            Ok(RawValue::OwnedRaw(RawBson::Array(arr)))
        }

        Compiled::PathGet { base, path } => get_path(eval_compiled(base, env)?, path),
        Compiled::MultikeyEq { base, path, value } => {
            let resolved = get_path(eval_compiled(base, env)?, path)?;
            let needle = eval_compiled(value, env)?;
            Ok(array_contains(&resolved, &needle))
        }

        Compiled::IsDefined(e) => Ok(bool_value(!eval_compiled(e, env)?.is_undefined())),
        Compiled::IsNull(e) => Ok(bool_value(is_null(&eval_compiled(e, env)?))),
        Compiled::ArrayContains { arr, needle } => {
            let a = eval_compiled(arr, env)?;
            let n = eval_compiled(needle, env)?;
            Ok(array_contains(&a, &n))
        }
        Compiled::MongoEq { base, value } => {
            // `base = needle OR ARRAY_CONTAINS(base, needle)`, reusing the single
            // `base` read for both arms. Mirrors `eval_binary`'s `Or`
            // short-circuit exactly (deterministic eval → identical result).
            let b = eval_compiled(base, env)?;
            let needle = eval_compiled(value, env)?;
            // `base = needle` as a 3-valued bool, matching `eval_binop(Eq)`:
            // undefined if either side is undefined, else scalar equality.
            let eq = if b.is_undefined() || needle.is_undefined() {
                None
            } else {
                Some(compare(&b, &needle) == Some(Ordering::Equal))
            };
            if eq == Some(true) {
                return Ok(bool_value(true));
            }
            let contains = array_contains(&b, &needle).as_bool();
            Ok(RawValue::from_value(or3(eq, contains)))
        }
        Compiled::Call { name, args } => {
            let mut vals = Vec::with_capacity(args.len());
            for a in args {
                vals.push(eval_compiled(a, env)?.into_value()?);
            }
            crate::functions::call(name, vals).map(RawValue::from_value)
        }
    }
}

/// Append one projection field, using the key pre-validated at compile time
/// (borrowed by reference — no per-row key allocation; see [`ObjKey`]).
fn append_field(doc: &mut RawDocumentBuf, key: &ObjKey, value: impl BindRawBsonRef) -> Result<()> {
    match key {
        ObjKey::Ready(c) => doc.append(c, value),
        ObjKey::Lazy(s) => {
            let c = CString::try_from(s.as_str()).map_err(|e| EvalError {
                message: format!("invalid object key '{s}': {e}"),
            })?;
            doc.append(c, value);
        }
    }
    Ok(())
}

/// `And`/`Or` short-circuit, mirroring [`eval_binary`] over compiled operands.
fn eval_compiled_binary<'a>(
    op: BinOp,
    lhs: &'a Compiled,
    rhs: &'a Compiled,
    env: &RawEnv<'a>,
) -> Result<RawValue<'a>> {
    match op {
        BinOp::And => {
            let l = eval_compiled(lhs, env)?.as_bool();
            if l == Some(false) {
                return Ok(bool_value(false));
            }
            let r = eval_compiled(rhs, env)?.as_bool();
            Ok(RawValue::from_value(and3(l, r)))
        }
        BinOp::Or => {
            let l = eval_compiled(lhs, env)?.as_bool();
            if l == Some(true) {
                return Ok(bool_value(true));
            }
            let r = eval_compiled(rhs, env)?.as_bool();
            Ok(RawValue::from_value(or3(l, r)))
        }
        _ => Ok(eval_binop(
            op,
            eval_compiled(lhs, env)?,
            eval_compiled(rhs, env)?,
        )),
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
        let SelectClause::Value(e) = q.select else {
            panic!("expected SELECT VALUE")
        };
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

        // The compiled form must match the interpreter exactly, in both the
        // single-binding fast path (`sole = Some("c")`, exercising `Row`/
        // `RowField`) and the generic name-lookup path (`sole = None`).
        for sole in [Some("c"), None] {
            let prog = compile(&expr, sole);
            let compiled = eval_compiled(&prog, &RawEnv::new(&rbinds, None))
                .unwrap()
                .into_value()
                .unwrap();
            assert_eq!(
                owned, compiled,
                "compiled/owned disagree on `{src}` (sole={sole:?})"
            );
        }
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
            // Mongo implicit-equality idiom (fused to `MongoEq` when compiled):
            // array membership, scalar match, no-match (undefined), and a
            // non-idiom OR with mismatched operands that must NOT fuse.
            "c.tags = \"y\" OR ARRAY_CONTAINS(c.tags, \"y\")",
            "c.tags = \"q\" OR ARRAY_CONTAINS(c.tags, \"q\")",
            "c.name = \"ada\" OR ARRAY_CONTAINS(c.name, \"ada\")",
            "c.name = \"zzz\" OR ARRAY_CONTAINS(c.name, \"zzz\")",
            "c.age = 30 OR ARRAY_CONTAINS(c.tags, \"y\")",
        ] {
            assert_agree(src, &doc);
        }
    }
}
