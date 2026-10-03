namespace RainDB.Logical;

/// <summary>Which plan layers <c>EXPLAIN</c> should render.</summary>
public enum SqlExplainLevel
{
    Logical,
    Physical,
    All,
}
