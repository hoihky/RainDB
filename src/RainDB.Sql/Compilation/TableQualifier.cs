namespace RainDB.Sql.Compilation;

internal static class TableQualifier
{
    internal static bool Matches(string qualifier, string tableName, string? alias) =>
        qualifier.Equals(tableName, StringComparison.OrdinalIgnoreCase)
        || alias is not null && qualifier.Equals(alias, StringComparison.OrdinalIgnoreCase);
}
