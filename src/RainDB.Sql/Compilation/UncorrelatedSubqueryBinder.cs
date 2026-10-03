using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Binds <see cref="LogicalUncorrelatedSubqueryPredicate"/> to physical subquery specs.</summary>
public sealed class UncorrelatedSubqueryBinder
{
    private readonly LogicalPlanCompiler _compiler;

    public UncorrelatedSubqueryBinder(LogicalPlanCompiler compiler) =>
        _compiler = compiler ?? throw new ArgumentNullException(nameof(compiler));

    public (SubqueryInPhysicalSpec[]? InSpecs, SubqueryExistsPhysicalSpec[]? ExistsSpecs) Bind(
        IReadOnlyList<LogicalUncorrelatedSubqueryPredicate>? predicates,
        ICatalog catalog,
        string outerTableName,
        TableSchema outerSchema,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm)
    {
        if (predicates is null or { Count: 0 })
            return (null, null);

        var inList = new List<SubqueryInPhysicalSpec>();
        var existsList = new List<SubqueryExistsPhysicalSpec>();
        foreach (var p in predicates)
        {
            UncorrelatedSubqueryValidator.Validate(p, outerTableName);
            var physical = _compiler.CompilePhysical(p.Subquery.Root, catalog, scanOptions, joinAlgorithm);
            switch (p.PredicateKind)
            {
                case LogicalUncorrelatedSubqueryPredicate.Kind.In:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotIn:
                {
                    if (p.Column is null)
                        throw new SqlCompileException("IN subquery predicate requires a column reference.");
                    var colIx = LogicalTableScanBinder.ResolveColumn(outerSchema, p.Column.ColumnName, outerTableName);
                    var colType = outerSchema.Columns[colIx].Type;
                    var subCol = ResolveSingleSubqueryColumn(physical, catalog);
                    inList.Add(new SubqueryInPhysicalSpec(
                        colIx,
                        colType,
                        p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotIn,
                        physical,
                        subCol));
                    break;
                }
                case LogicalUncorrelatedSubqueryPredicate.Kind.Exists:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotExists:
                    existsList.Add(new SubqueryExistsPhysicalSpec(
                        p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotExists,
                        physical));
                    break;
            }
        }

        return (
            inList.Count > 0 ? inList.ToArray() : null,
            existsList.Count > 0 ? existsList.ToArray() : null);
    }

    public JoinSubqueryPhysicalSpecs BindForJoin(
        IReadOnlyList<LogicalUncorrelatedSubqueryPredicate>? predicates,
        ICatalog catalog,
        ITableSource left,
        ITableSource right,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm)
    {
        if (predicates is null or { Count: 0 })
            return new JoinSubqueryPhysicalSpecs();

        var probeIn = new List<SubqueryInPhysicalSpec>();
        var buildIn = new List<SubqueryInPhysicalSpec>();
        var existsList = new List<SubqueryExistsPhysicalSpec>();
        foreach (var p in predicates)
        {
            UncorrelatedSubqueryValidator.ValidateForJoin(p, left.Name, right.Name);
            var physical = _compiler.CompilePhysical(p.Subquery.Root, catalog, scanOptions, joinAlgorithm);
            switch (p.PredicateKind)
            {
                case LogicalUncorrelatedSubqueryPredicate.Kind.In:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotIn:
                {
                    var (onProbe, colIx, colType) = ResolveJoinInColumn(p, left, right);
                    var subCol = ResolveSingleSubqueryColumn(physical, catalog);
                    var spec = new SubqueryInPhysicalSpec(
                        colIx,
                        colType,
                        p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotIn,
                        physical,
                        subCol);
                    if (onProbe)
                        probeIn.Add(spec);
                    else
                        buildIn.Add(spec);
                    break;
                }
                case LogicalUncorrelatedSubqueryPredicate.Kind.Exists:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotExists:
                    existsList.Add(new SubqueryExistsPhysicalSpec(
                        p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotExists,
                        physical));
                    break;
            }
        }

        return new JoinSubqueryPhysicalSpecs
        {
            ProbeInSubqueries = probeIn.Count > 0 ? probeIn.ToArray() : null,
            BuildInSubqueries = buildIn.Count > 0 ? buildIn.ToArray() : null,
            ExistsSubqueries = existsList.Count > 0 ? existsList.ToArray() : null,
        };
    }

    private static (bool OnProbe, int ColumnIndex, RainDbType ColumnType) ResolveJoinInColumn(
        LogicalUncorrelatedSubqueryPredicate predicate,
        ITableSource left,
        ITableSource right)
    {
        if (predicate.Column is null)
            throw new SqlCompileException("IN subquery predicate requires a column reference.");

        var col = predicate.Column;
        if (col.QualifierTableName is { } qt)
        {
            if (TableEq(qt, left.Name))
            {
                var ix = LogicalTableScanBinder.ResolveColumn(left.Schema, col.ColumnName, left.Name);
                return (true, ix, left.Schema.Columns[ix].Type);
            }

            if (TableEq(qt, right.Name))
            {
                var ix = LogicalTableScanBinder.ResolveColumn(right.Schema, col.ColumnName, right.Name);
                return (false, ix, right.Schema.Columns[ix].Type);
            }

            throw new SqlCompileException(
                $"IN column references unknown table '{qt}' (expected '{left.Name}' or '{right.Name}').");
        }

        var li = TryResolveColumn(left.Schema, col.ColumnName);
        var ri = TryResolveColumn(right.Schema, col.ColumnName);
        if (li >= 0 && ri >= 0)
        {
            throw new SqlCompileException(
                $"IN column '{col.ColumnName}' is ambiguous between '{left.Name}' and '{right.Name}'; use qualified table.column.");
        }

        if (li >= 0)
            return (true, li, left.Schema.Columns[li].Type);
        if (ri >= 0)
            return (false, ri, right.Schema.Columns[ri].Type);

        throw new SqlCompileException(
            $"Unknown IN column '{col.ColumnName}' (not found on '{left.Name}' or '{right.Name}').");
    }

    private static int TryResolveColumn(TableSchema schema, string name)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static bool TableEq(string a, string b) => a.Equals(b, StringComparison.OrdinalIgnoreCase);

    private static int ResolveSingleSubqueryColumn(IPhysicalPlan subquery, ICatalog catalog)
    {
        var schema = PhysicalPlanOutputSchema.Resolve(subquery, catalog);
        if (schema.Columns.Count != 1)
            throw new SqlCompileException("IN subquery must return exactly one column.");
        return 0;
    }
}
