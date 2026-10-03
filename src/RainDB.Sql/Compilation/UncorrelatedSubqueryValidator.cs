using RainDB.Logical;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

internal static class UncorrelatedSubqueryValidator
{
    internal static void Validate(LogicalUncorrelatedSubqueryPredicate predicate, string outerTableName)
    {
        EnsureUncorrelated(predicate, outerTableName);
        ValidateInColumn(predicate, outerTableName, null);
    }

    internal static void ValidateForJoin(
        LogicalUncorrelatedSubqueryPredicate predicate,
        string leftTableName,
        string rightTableName)
    {
        EnsureUncorrelated(predicate, leftTableName, rightTableName);
        ValidateInColumn(predicate, leftTableName, rightTableName);
    }

    private static void EnsureUncorrelated(LogicalUncorrelatedSubqueryPredicate predicate, params string[] outerTables)
    {
        var innerTables = LogicalQueryTableCollector.Collect(predicate.Subquery.Root);
        foreach (var outer in outerTables)
        {
            if (innerTables.Contains(outer, StringComparer.OrdinalIgnoreCase))
                throw new SqlCompileException("Correlated subqueries are not supported yet.");
        }
    }

    private static void ValidateInColumn(
        LogicalUncorrelatedSubqueryPredicate predicate,
        string primaryTable,
        string? secondaryTable)
    {
        if (predicate.PredicateKind is not LogicalUncorrelatedSubqueryPredicate.Kind.In
            and not LogicalUncorrelatedSubqueryPredicate.Kind.NotIn)
            return;

        if (predicate.Column is null)
            throw new SqlCompileException("IN requires a column reference on the left-hand side.");

        if (predicate.Column.QualifierTableName is not { } q)
            return;

        if (q.Equals(primaryTable, StringComparison.OrdinalIgnoreCase))
            return;
        if (secondaryTable is not null && q.Equals(secondaryTable, StringComparison.OrdinalIgnoreCase))
            return;

        var expected = secondaryTable is null
            ? primaryTable
            : $"'{primaryTable}' or '{secondaryTable}'";
        throw new SqlCompileException($"Column qualifier '{q}' does not match join table {expected}.");
    }
}
