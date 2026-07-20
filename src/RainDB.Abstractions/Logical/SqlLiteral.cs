namespace RainDB.Logical;

/// <summary>Literal as parsed from SQL text before schema binding.</summary>
public readonly record struct SqlLiteral(SqlLiteralKind Kind, string Text);

public enum SqlLiteralKind
{
    Integer,
    Float,
    Boolean,
    /// <summary>Single-quoted SQL string (payload in <see cref="SqlLiteral.Text"/>).</summary>
    String,
}
