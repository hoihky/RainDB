using RainDB.Execution;
using RainDB.Logical;
using System.Linq;

namespace RainDB.Sql.Optimization;

/// <summary>Deep copies logical roots for rewrite rules and parameter binding.</summary>
internal sealed class LogicalPlanCloner
{
    public LogicalPlan ClonePlan(LogicalPlan plan) =>
        new(CloneRoot(plan.Root), plan.ExplainLevel);

    public ILogicalRoot CloneRoot(ILogicalRoot root) =>
        root switch
        {
            LogicalTableScan s => CloneTableScan(s),
            LogicalInnerJoin j => CloneJoin(j),
            LogicalUnionAll u => new LogicalUnionAll
            {
                Branches = u.Branches.Select(CloneRoot).ToArray(),
            },
            _ => throw new InvalidOperationException($"Unsupported logical root {root.GetType().Name}."),
        };

    public LogicalTableScan CloneTableScan(
        LogicalTableScan s,
        IReadOnlyList<LogicalColumnProjection>? projectionOverride = null,
        IReadOnlyList<SimpleWhereClause>? whereOverride = null) =>
        new()
        {
            TableName = s.TableName,
            Projection = projectionOverride ?? CloneProjectionList(s.Projection),
            GroupByColumns = CloneProjectionList(s.GroupByColumns),
            SelectList = CloneSelectList(s.SelectList),
            WhereConjuncts = whereOverride ?? CloneWhereList(s.WhereConjuncts),
            Aggregate = s.Aggregate is null
                ? null
                : new LogicalAggregate { Kind = s.Aggregate.Kind, ColumnName = s.Aggregate.ColumnName },
            OrderBy = CloneOrderBy(s.OrderBy),
            Limit = s.Limit,
        };

    public LogicalInnerJoin CloneJoin(
        LogicalInnerJoin j,
        IReadOnlyList<LogicalColumnProjection>? selectProjectionOverride = null,
        IReadOnlyList<SimpleWhereClause>? whereOverride = null,
        IReadOnlyList<SimpleWhereClause>? probeWhereOverride = null,
        IReadOnlyList<SimpleWhereClause>? buildWhereOverride = null) =>
        new()
        {
            Semantics = j.Semantics,
            LeftTableName = j.LeftTableName,
            RightTableName = j.RightTableName,
            LeftKeyColumns = CloneQualifiedColumns(j.LeftKeyColumns),
            RightKeyColumns = CloneQualifiedColumns(j.RightKeyColumns),
            SelectProjection = selectProjectionOverride ?? CloneProjectionList(j.SelectProjection),
            WhereConjuncts = ResolveWhereOverride(whereOverride, j.WhereConjuncts),
            ProbeSideWhereConjuncts = ResolveWhereOverride(probeWhereOverride, j.ProbeSideWhereConjuncts),
            BuildSideWhereConjuncts = ResolveWhereOverride(buildWhereOverride, j.BuildSideWhereConjuncts),
            GroupByColumns = CloneProjectionList(j.GroupByColumns),
            SelectList = CloneSelectList(j.SelectList),
            OrderBy = CloneOrderBy(j.OrderBy),
            Limit = j.Limit,
        };

    public SimpleWhereClause CloneWhere(SimpleWhereClause w) =>
        new()
        {
            QualifierTableName = w.QualifierTableName,
            ColumnName = w.ColumnName,
            LeftExpression = CloneScalarExpression(w.LeftExpression),
            Operator = w.Operator,
            Literal = w.Literal is { } lit ? new SqlLiteral(lit.Kind, lit.Text) : null,
            ParameterName = w.ParameterName,
        };

    private static IReadOnlyList<LogicalQualifiedColumn>? CloneQualifiedColumns(IReadOnlyList<LogicalQualifiedColumn> cols)
    {
        if (cols is null)
            return null;
        var list = new List<LogicalQualifiedColumn>(cols.Count);
        foreach (var c in cols)
            list.Add(new LogicalQualifiedColumn { TableName = c.TableName, ColumnName = c.ColumnName });
        return list;
    }

    private IReadOnlyList<SimpleWhereClause>? ResolveWhereOverride(
        IReadOnlyList<SimpleWhereClause>? overrideList,
        IReadOnlyList<SimpleWhereClause>? original)
    {
        if (overrideList is null)
            return CloneWhereList(original);
        return overrideList.Count == 0 ? null : overrideList;
    }

    private IReadOnlyList<SimpleWhereClause>? CloneWhereList(IReadOnlyList<SimpleWhereClause>? conjuncts)
    {
        if (conjuncts is null)
            return null;
        var list = new List<SimpleWhereClause>(conjuncts.Count);
        foreach (var w in conjuncts)
            list.Add(CloneWhere(w));
        return list;
    }

    private static IReadOnlyList<LogicalColumnProjection>? CloneProjectionList(IReadOnlyList<LogicalColumnProjection>? cols)
    {
        if (cols is null)
            return null;
        var list = new List<LogicalColumnProjection>(cols.Count);
        foreach (var p in cols)
            list.Add(new LogicalColumnProjection { QualifierTableName = p.QualifierTableName, ColumnName = p.ColumnName });
        return list;
    }

    private static IReadOnlyList<LogicalSelectListItem>? CloneSelectList(IReadOnlyList<LogicalSelectListItem>? items)
    {
        if (items is null)
            return null;
        var list = new List<LogicalSelectListItem>(items.Count);
        foreach (var item in items)
        {
            list.Add(item switch
            {
                LogicalColumnProjection c => new LogicalColumnProjection
                {
                    QualifierTableName = c.QualifierTableName,
                    ColumnName = c.ColumnName,
                },
                LogicalAggregationCall a => new LogicalAggregationCall
                {
                    Kind = a.Kind,
                    ArgumentColumnName = a.ArgumentColumnName,
                    ArgumentQualifierTableName = a.ArgumentQualifierTableName,
                },
                LogicalScalarProjection s => new LogicalScalarProjection
                {
                    Expression = CloneScalarExpression(s.Expression)!,
                    OutputAlias = s.OutputAlias,
                },
                _ => item,
            });
        }

        return list;
    }

    private static LogicalScalarExpression? CloneScalarExpression(LogicalScalarExpression? expr)
    {
        if (expr is null)
            return null;
        return expr switch
        {
            LogicalColumnScalarRef c => new LogicalColumnScalarRef
            {
                QualifierTableName = c.QualifierTableName,
                ColumnName = c.ColumnName,
            },
            LogicalLiteralScalar l => new LogicalLiteralScalar
            {
                Literal = new SqlLiteral(l.Literal.Kind, l.Literal.Text),
            },
            LogicalBinaryScalar b => new LogicalBinaryScalar
            {
                Operator = b.Operator,
                Left = CloneScalarExpression(b.Left)!,
                Right = CloneScalarExpression(b.Right)!,
            },
            LogicalCastScalar cast => new LogicalCastScalar
            {
                Operand = CloneScalarExpression(cast.Operand)!,
                TargetType = cast.TargetType,
            },
            _ => expr,
        };
    }

    private static IReadOnlyList<LogicalSortKey>? CloneOrderBy(IReadOnlyList<LogicalSortKey>? keys)
    {
        if (keys is null)
            return null;
        var list = new List<LogicalSortKey>(keys.Count);
        foreach (var k in keys)
        {
            LogicalColumnProjection? col = null;
            if (k.Column is { } c)
                col = new LogicalColumnProjection { QualifierTableName = c.QualifierTableName, ColumnName = c.ColumnName };
            list.Add(new LogicalSortKey
            {
                Column = col,
                SortExpression = k.SortExpression is null ? null : CloneScalarExpression(k.SortExpression),
                Descending = k.Descending,
            });
        }
        return list;
    }
}
