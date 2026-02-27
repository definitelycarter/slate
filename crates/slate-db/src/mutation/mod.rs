mod ops;
pub(crate) mod raw;

use bson::Bson;
use bson::raw::{RawBsonRef, RawDocument};

use crate::error::DbError;

/// A single field-level mutation operator.
#[derive(Debug, Clone, PartialEq)]
pub enum MutationOp {
    /// Set a field to a value. Creates the field if it doesn't exist.
    Set(Bson),
    /// Remove a field from the document.
    Unset,
    /// Increment a numeric field by the given amount (negative for decrement).
    Inc(Bson),
    /// Rename a field. Value is the new field name.
    Rename(String),
    /// Append a value to the end of an array field. Creates the array if missing.
    Push(Bson),
    /// Prepend a value to the beginning of an array field. Creates the array if missing.
    LPush(Bson),
    /// Remove and discard the last element of an array field.
    Pop,
}

/// A single field + operator pair within a Mutation.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldMutation {
    pub field: String,
    pub op: MutationOp,
}

/// A complete mutation specification: a list of (field, operator) pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct Mutation {
    pub ops: Vec<FieldMutation>,
}

impl Mutation {
    /// Apply this mutation to a raw document.
    ///
    /// Fast path: the raw byte-level mutation engine handles flat-field operators
    /// (`$set`, `$unset`, `$inc`, `$push`, `$pop`) by splicing/overwriting bytes
    /// directly — no deserialization. Falls back to full `bson::Document` round-trip
    /// for dot-paths, `$rename`, `$lpush`, and Document/Array `$set` values.
    ///
    /// Returns `Ok(None)` if the document is unchanged.
    pub(crate) fn apply(&self, raw: &RawDocument) -> Result<Option<bson::RawDocumentBuf>, DbError> {
        use crate::mutation::raw::{RawMutationResult, raw_apply_mutation};
        use crate::mutation::ops;

        // Fast path: raw byte-level mutation engine
        match raw_apply_mutation(raw, self)? {
            RawMutationResult::Applied(buf) => return Ok(Some(buf)),
            RawMutationResult::Unchanged => return Ok(None),
            RawMutationResult::Fallback => { /* continue to slow path */ }
        }

        // Slow path: full deserialization
        let mut doc: bson::Document = bson::deserialize_from_slice(raw.as_bytes())?;
        let mut changed = false;

        for fm in &self.ops {
            let creates = matches!(
                fm.op,
                MutationOp::Set(_)
                    | MutationOp::Inc(_)
                    | MutationOp::Push(_)
                    | MutationOp::LPush(_)
            );

            match &fm.op {
                MutationOp::Set(val) => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                    {
                        changed |= ops::op_set(parent, leaf, val)?;
                    }
                }
                MutationOp::Unset => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                    {
                        changed |= ops::op_unset(parent, leaf)?;
                    }
                }
                MutationOp::Inc(amount) => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                    {
                        changed |= ops::op_inc(parent, leaf, amount)?;
                    }
                }
                MutationOp::Rename(new_name) => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                    {
                        changed |= ops::op_rename(parent, leaf, new_name)?;
                    }
                }
                MutationOp::Push(val) => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                    {
                        changed |= ops::op_push(parent, leaf, val)?;
                    }
                }
                MutationOp::LPush(val) => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, creates)?
                    {
                        changed |= ops::op_lpush(parent, leaf, val)?;
                    }
                }
                MutationOp::Pop => {
                    if let Some((parent, leaf)) =
                        ops::resolve_parent_mut(&mut doc, &fm.field, false)?
                    {
                        changed |= ops::op_pop(parent, leaf)?;
                    }
                }
            }
        }

        if !changed {
            return Ok(None);
        }

        let raw = bson::RawDocumentBuf::try_from(&doc)?;
        Ok(Some(raw))
    }
}

/// Parse a BSON update document into a validated `Mutation`.
///
/// Recognizes operator keys (`$set`, `$inc`, `$unset`, `$rename`, `$push`, `$lpush`,
/// `$pop`) whose values are sub-documents mapping field paths to operand values.
/// Bare top-level fields are treated as implicit `$set`.
///
/// # Errors
///
/// Returns an error if the document contains unknown operator keys, targets `_id`,
/// or has invalid operand types (e.g. non-numeric `$inc` value, non-string `$rename`).
pub fn parse_mutation(doc: &RawDocument) -> Result<Mutation, ParseError> {
    let mut ops = Vec::new();

    for result in doc.iter() {
        let (key, value) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;

        if key == "_id" {
            continue; // silently skip _id
        }

        match key.as_str() {
            "$set" => parse_operator_fields(value, MutationOp::Set, &mut ops)?,
            "$unset" => parse_unset_fields(value, &mut ops)?,
            "$inc" => parse_inc_fields(value, &mut ops)?,
            "$rename" => parse_rename_fields(value, &mut ops)?,
            "$push" => parse_operator_fields(value, MutationOp::Push, &mut ops)?,
            "$lpush" => parse_operator_fields(value, MutationOp::LPush, &mut ops)?,
            "$pop" => parse_pop_fields(value, &mut ops)?,
            k if k.starts_with('$') => {
                return Err(ParseError(format!("unknown operator: {k}")));
            }
            _ => {
                // Bare field — implicit $set
                let bson_val = raw_to_bson(value)?;
                ops.push(FieldMutation {
                    field: key.to_string(),
                    op: MutationOp::Set(bson_val),
                });
            }
        }
    }

    if ops.is_empty() {
        return Err(ParseError("empty mutation document".into()));
    }

    // Reject any ops targeting _id
    for fm in &ops {
        let target = fm.field.split('.').next().unwrap_or(&fm.field);
        if target == "_id" {
            return Err(ParseError("cannot mutate _id field".into()));
        }
    }

    Ok(Mutation { ops })
}

/// Parse error for mutation documents.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mutation parse error: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

// ── Internal helpers ────────────────────────────────────────────

/// Parse an operator sub-document where each field maps to a Bson value.
/// Used for $set, $push, $lpush.
fn parse_operator_fields(
    value: RawBsonRef,
    make_op: fn(Bson) -> MutationOp,
    ops: &mut Vec<FieldMutation>,
) -> Result<(), ParseError> {
    let sub_doc = match value {
        RawBsonRef::Document(d) => d,
        _ => return Err(ParseError("operator value must be a document".into())),
    };
    for result in sub_doc.iter() {
        let (field, val) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;
        let bson_val = raw_to_bson(val)?;
        ops.push(FieldMutation {
            field: field.to_string(),
            op: make_op(bson_val),
        });
    }
    Ok(())
}

/// Parse $unset sub-document. Values are ignored (MongoDB convention: `{ "field": "" }`).
fn parse_unset_fields(value: RawBsonRef, ops: &mut Vec<FieldMutation>) -> Result<(), ParseError> {
    let sub_doc = match value {
        RawBsonRef::Document(d) => d,
        _ => return Err(ParseError("$unset value must be a document".into())),
    };
    for result in sub_doc.iter() {
        let (field, _) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;
        ops.push(FieldMutation {
            field: field.to_string(),
            op: MutationOp::Unset,
        });
    }
    Ok(())
}

/// Parse $inc sub-document. Values must be numeric.
fn parse_inc_fields(value: RawBsonRef, ops: &mut Vec<FieldMutation>) -> Result<(), ParseError> {
    let sub_doc = match value {
        RawBsonRef::Document(d) => d,
        _ => return Err(ParseError("$inc value must be a document".into())),
    };
    for result in sub_doc.iter() {
        let (field, val) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;
        match val {
            RawBsonRef::Int32(_) | RawBsonRef::Int64(_) | RawBsonRef::Double(_) => {}
            _ => {
                return Err(ParseError(format!(
                    "$inc value for '{field}' must be numeric"
                )));
            }
        }
        let bson_val = raw_to_bson(val)?;
        ops.push(FieldMutation {
            field: field.to_string(),
            op: MutationOp::Inc(bson_val),
        });
    }
    Ok(())
}

/// Parse $rename sub-document. Values must be strings (the new field name).
fn parse_rename_fields(value: RawBsonRef, ops: &mut Vec<FieldMutation>) -> Result<(), ParseError> {
    let sub_doc = match value {
        RawBsonRef::Document(d) => d,
        _ => return Err(ParseError("$rename value must be a document".into())),
    };
    for result in sub_doc.iter() {
        let (field, val) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;
        let new_name = match val {
            RawBsonRef::String(s) => s.to_string(),
            _ => {
                return Err(ParseError(format!(
                    "$rename value for '{field}' must be a string"
                )));
            }
        };
        ops.push(FieldMutation {
            field: field.to_string(),
            op: MutationOp::Rename(new_name),
        });
    }
    Ok(())
}

/// Parse $pop sub-document. Values are numeric (1 = last, -1 = first; we only support 1 for now).
fn parse_pop_fields(value: RawBsonRef, ops: &mut Vec<FieldMutation>) -> Result<(), ParseError> {
    let sub_doc = match value {
        RawBsonRef::Document(d) => d,
        _ => return Err(ParseError("$pop value must be a document".into())),
    };
    for result in sub_doc.iter() {
        let (field, _) = result.map_err(|e| ParseError(format!("malformed BSON: {e}")))?;
        ops.push(FieldMutation {
            field: field.to_string(),
            op: MutationOp::Pop,
        });
    }
    Ok(())
}

/// Convert a RawBsonRef to an owned Bson value.
fn raw_to_bson(value: RawBsonRef) -> Result<Bson, ParseError> {
    value
        .to_owned()
        .try_into()
        .map_err(|e: bson::error::Error| ParseError(format!("failed to convert BSON value: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::rawdoc;

    #[test]
    fn bare_fields_become_set() {
        let doc = rawdoc! { "status": "active", "score": 10 };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 2);
        assert_eq!(m.ops[0].field, "status");
        assert_eq!(m.ops[0].op, MutationOp::Set(Bson::String("active".into())));
        assert_eq!(m.ops[1].field, "score");
        assert_eq!(m.ops[1].op, MutationOp::Set(Bson::Int32(10)));
    }

    #[test]
    fn explicit_set() {
        let doc = rawdoc! { "$set": { "a": 1, "b": "hello" } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 2);
        assert_eq!(m.ops[0].op, MutationOp::Set(Bson::Int32(1)));
        assert_eq!(m.ops[1].op, MutationOp::Set(Bson::String("hello".into())));
    }

    #[test]
    fn unset() {
        let doc = rawdoc! { "$unset": { "a": "", "b": "" } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 2);
        assert_eq!(m.ops[0].op, MutationOp::Unset);
        assert_eq!(m.ops[1].op, MutationOp::Unset);
    }

    #[test]
    fn inc() {
        let doc = rawdoc! { "$inc": { "score": 10, "lives": -1 } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 2);
        assert_eq!(m.ops[0].op, MutationOp::Inc(Bson::Int32(10)));
        assert_eq!(m.ops[1].op, MutationOp::Inc(Bson::Int32(-1)));
    }

    #[test]
    fn inc_rejects_non_numeric() {
        let doc = rawdoc! { "$inc": { "score": "ten" } };
        assert!(parse_mutation(&doc).is_err());
    }

    #[test]
    fn rename() {
        let doc = rawdoc! { "$rename": { "old_name": "new_name" } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops[0].field, "old_name");
        assert_eq!(m.ops[0].op, MutationOp::Rename("new_name".into()));
    }

    #[test]
    fn rename_rejects_non_string() {
        let doc = rawdoc! { "$rename": { "old": 123 } };
        assert!(parse_mutation(&doc).is_err());
    }

    #[test]
    fn push_and_lpush() {
        let doc = rawdoc! { "$push": { "tags": "new" }, "$lpush": { "queue": "first" } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops[0].op, MutationOp::Push(Bson::String("new".into())));
        assert_eq!(m.ops[1].op, MutationOp::LPush(Bson::String("first".into())));
    }

    #[test]
    fn pop() {
        let doc = rawdoc! { "$pop": { "tags": 1 } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops[0].op, MutationOp::Pop);
    }

    #[test]
    fn mixed_operators_and_bare_fields() {
        let doc = rawdoc! {
            "$inc": { "score": 5 },
            "status": "active",
            "$push": { "tags": "rust" }
        };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 3);
        assert_eq!(m.ops[0].op, MutationOp::Inc(Bson::Int32(5)));
        assert_eq!(m.ops[1].op, MutationOp::Set(Bson::String("active".into())));
        assert_eq!(m.ops[2].op, MutationOp::Push(Bson::String("rust".into())));
    }

    #[test]
    fn dot_path_fields() {
        let doc = rawdoc! { "$set": { "address.city": "Austin" }, "$inc": { "stats.score": 1 } };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops[0].field, "address.city");
        assert_eq!(m.ops[1].field, "stats.score");
    }

    #[test]
    fn rejects_unknown_operator() {
        let doc = rawdoc! { "$unknown": { "a": 1 } };
        assert!(parse_mutation(&doc).is_err());
    }

    #[test]
    fn rejects_id_mutation() {
        let doc = rawdoc! { "$set": { "_id": "new_id" } };
        assert!(parse_mutation(&doc).is_err());
    }

    #[test]
    fn rejects_empty_document() {
        let doc = rawdoc! {};
        assert!(parse_mutation(&doc).is_err());
    }

    #[test]
    fn skips_top_level_id() {
        let doc = rawdoc! { "_id": "ignored", "status": "active" };
        let m = parse_mutation(&doc).unwrap();
        assert_eq!(m.ops.len(), 1);
        assert_eq!(m.ops[0].field, "status");
    }
}
