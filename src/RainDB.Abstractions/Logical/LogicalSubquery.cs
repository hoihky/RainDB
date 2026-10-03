namespace RainDB.Logical;

/// <summary>Parenthesized SELECT used in <c>IN</c>, <c>EXISTS</c>, or <c>FROM</c> (uncorrelated).</summary>
public sealed class LogicalSubquery
{
    public required ILogicalRoot Root { get; init; }
}
