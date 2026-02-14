#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    IContains,
    IStartsWith,
    IEndsWith,
    Gt,
    Gte,
    Lt,
    Lte,
    IsNull,
}
