using RainDB.Logical;

namespace RainDB.Sql;

/// <summary>Typed runtime value bound to a prepared statement parameter.</summary>
public sealed class SqlParameterValue
{
    public SqlParameterValue(SqlLiteralKind kind, string text)
    {
        Kind = kind;
        Text = text ?? throw new ArgumentNullException(nameof(text));
    }

    public SqlLiteralKind Kind { get; }

    public string Text { get; }

    public static SqlParameterValue FromLiteral(SqlLiteral literal) =>
        new(literal.Kind, literal.Text);

    public SqlLiteral ToLiteral() => new(Kind, Text);
}
