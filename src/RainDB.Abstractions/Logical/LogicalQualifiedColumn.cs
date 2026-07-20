namespace RainDB.Logical;

/// <summary><c>table.column</c> reference for join predicates.</summary>
public sealed class LogicalQualifiedColumn
{
    public required string TableName { get; init; }

    public required string ColumnName { get; init; }
}
