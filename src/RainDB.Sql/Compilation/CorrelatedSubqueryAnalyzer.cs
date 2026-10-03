using RainDB.Execution;
using RainDB.Logical;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Detects simple <c>outer.col = inner.col</c> correlations and rewrites inner WHERE.</summary>
internal static class CorrelatedSubqueryAnalyzer
{
    internal static bool TryExtractExistsCorrelations(
        ILogicalRoot innerRoot,
        IReadOnlyCollection<string> outerTableNames,
        out IReadOnlyList<LogicalSubqueryCorrelation> correlations,
        out ILogicalRoot rewrittenInner)
    {
        correlations = Array.Empty<LogicalSubqueryCorrelation>();
        rewrittenInner = innerRoot;
        if (innerRoot is not LogicalTableScan scan)
            return false;

        if (scan.WhereConjuncts is not { Count: > 0 } conjuncts)
            return false;

        var innerTable = scan.TableName;
        var corr = new List<LogicalSubqueryCorrelation>();
        var keep = new List<SimpleWhereClause>();
        foreach (var w in conjuncts)
        {
            if (w.Operator is not (ScalarCompareOp.Eq or ScalarCompareOp.Ne)
                || w.CompareColumn is not { } right
                || w.Literal is not null
                || w.UsesParameter)
            {
                keep.Add(w);
                continue;
            }

            var left = new LogicalColumnScalarRef
            {
                QualifierTableName = w.QualifierTableName,
                ColumnName = w.ColumnName,
            };
            if (!TryClassifyCorrelation(left, right, outerTableNames, innerTable, out var binding))
            {
                keep.Add(w);
                continue;
            }

            if (w.Operator != ScalarCompareOp.Eq)
                throw new SqlCompileException("Correlated subqueries support only '=' correlation predicates.");

            corr.Add(binding);
        }

        if (corr.Count == 0)
            return false;

        correlations = corr;
        rewrittenInner = new LogicalTableScan
        {
            TableName = scan.TableName,
            SelectDistinct = scan.SelectDistinct,
            Projection = scan.Projection,
            GroupByColumns = scan.GroupByColumns,
            SelectList = scan.SelectList,
            WhereConjuncts = keep.Count > 0 ? keep : null,
            SubqueryPredicates = scan.SubqueryPredicates,
            HavingConjuncts = scan.HavingConjuncts,
            Aggregate = scan.Aggregate,
            OrderBy = scan.OrderBy,
            Limit = scan.Limit,
        };
        return true;
    }

    private static bool TryClassifyCorrelation(
        LogicalColumnScalarRef a,
        LogicalColumnScalarRef b,
        IReadOnlyCollection<string> outerTables,
        string innerTable,
        out LogicalSubqueryCorrelation correlation)
    {
        correlation = null!;
        if (ReferencesTable(a, outerTables) && ReferencesTable(b, innerTable))
        {
            correlation = new LogicalSubqueryCorrelation { OuterColumn = a, InnerColumn = b };
            return true;
        }

        if (ReferencesTable(b, outerTables) && ReferencesTable(a, innerTable))
        {
            correlation = new LogicalSubqueryCorrelation { OuterColumn = b, InnerColumn = a };
            return true;
        }

        return false;
    }

    private static bool ReferencesTable(LogicalColumnScalarRef col, string table) =>
        col.QualifierTableName is not null
        && col.QualifierTableName.Equals(table, StringComparison.OrdinalIgnoreCase);

    private static bool ReferencesTable(LogicalColumnScalarRef col, IReadOnlyCollection<string> tables)
    {
        if (col.QualifierTableName is not { } q)
            return false;
        return tables.Contains(q, StringComparer.OrdinalIgnoreCase);
    }
}
