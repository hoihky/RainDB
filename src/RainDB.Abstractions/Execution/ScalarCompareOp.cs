namespace RainDB.Execution;

/// <summary>Scalar comparison for interpreted filter predicates (type dispatch once per batch).</summary>
public enum ScalarCompareOp
{
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
