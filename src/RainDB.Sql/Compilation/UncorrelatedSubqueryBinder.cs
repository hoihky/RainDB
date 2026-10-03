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
            switch (p.PredicateKind)
            {
                case LogicalUncorrelatedSubqueryPredicate.Kind.In:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotIn:
                    inList.Add(BindIn(
                        p,
                        [outerTableName],
                        outerSchema,
                        catalog,
                        scanOptions,
                        joinAlgorithm,
                        (col, schema, table) => LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, table)));
                    break;
                case LogicalUncorrelatedSubqueryPredicate.Kind.Exists:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotExists:
                    existsList.Add(BindExists(
                        p,
                        [outerTableName],
                        outerSchema,
                        catalog,
                        scanOptions,
                        joinAlgorithm,
                        (col, schema, table) => LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, table)));
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
            switch (p.PredicateKind)
            {
                case LogicalUncorrelatedSubqueryPredicate.Kind.In:
                case LogicalUncorrelatedSubqueryPredicate.Kind.NotIn:
                {
                    UncorrelatedSubqueryValidator.ValidateForJoin(p, left.Name, right.Name);
                    var physical = _compiler.CompilePhysical(p.Subquery.Root, catalog, scanOptions, joinAlgorithm);
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
                    existsList.Add(BindExists(
                        p,
                        [left.Name, right.Name],
                        left.Schema,
                        catalog,
                        scanOptions,
                        joinAlgorithm,
                        (col, _, _) => ResolveJoinOuterColumn(col, left, right)));
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

    private static int ResolveJoinOuterColumn(
        LogicalColumnScalarRef col,
        ITableSource left,
        ITableSource right)
    {
        if (col.QualifierTableName is { } q)
        {
            if (TableEq(q, left.Name))
                return LogicalTableScanBinder.ResolveColumn(left.Schema, col.ColumnName, left.Name);
            if (TableEq(q, right.Name))
                return LogicalTableScanBinder.ResolveColumn(right.Schema, col.ColumnName, right.Name);
            throw new SqlCompileException($"Unknown outer table '{q}' in correlated subquery.");
        }

        throw new SqlCompileException("Correlated subquery columns must be qualified with a table name.");
    }

    private SubqueryInPhysicalSpec BindIn(
        LogicalUncorrelatedSubqueryPredicate predicate,
        string[] outerTableNames,
        TableSchema outerSchema,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm,
        Func<LogicalColumnScalarRef, TableSchema, string, int> resolveOuterColumn)
    {
        if (predicate.Column is null)
            throw new SqlCompileException("IN subquery predicate requires a column reference.");

        var outerTable = predicate.Column.QualifierTableName ?? outerTableNames[0];
        var colIx = resolveOuterColumn(predicate.Column, outerSchema, outerTable);
        var colType = outerSchema.Columns[colIx].Type;

        var innerRoot = predicate.Subquery.Root;
        CorrelatedEqualityBinding[]? bindings = null;
        if (CorrelatedSubqueryAnalyzer.TryExtractExistsCorrelations(
                innerRoot,
                outerTableNames,
                out var correlations,
                out var rewritten))
        {
            if (outerTableNames.Length > 1)
                throw new SqlCompileException("Correlated IN on joins is not supported yet.");
            innerRoot = rewritten;
            bindings = BindCorrelations(correlations, outerSchema, innerRoot, catalog, resolveOuterColumn);
        }
        else if (outerTableNames.Length == 1)
        {
            UncorrelatedSubqueryValidator.Validate(predicate, outerTableNames[0]);
        }
        else
        {
            UncorrelatedSubqueryValidator.ValidateForJoin(predicate, outerTableNames[0], outerTableNames[1]);
        }

        var physical = _compiler.CompilePhysical(innerRoot, catalog, scanOptions, joinAlgorithm);
        var subCol = ResolveSingleSubqueryColumn(physical, catalog);
        return new SubqueryInPhysicalSpec(
            colIx,
            colType,
            predicate.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotIn,
            physical,
            subCol,
            bindings);
    }

    private SubqueryExistsPhysicalSpec BindExists(
        LogicalUncorrelatedSubqueryPredicate predicate,
        string[] outerTableNames,
        TableSchema outerSchema,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm,
        Func<LogicalColumnScalarRef, TableSchema, string, int> resolveOuterColumn)
    {
        var innerRoot = predicate.Subquery.Root;
        CorrelatedEqualityBinding[]? bindings = null;
        if (CorrelatedSubqueryAnalyzer.TryExtractExistsCorrelations(
                innerRoot,
                outerTableNames,
                out var correlations,
                out var rewritten))
        {
            if (outerTableNames.Length > 1)
                throw new SqlCompileException("Correlated EXISTS on joins is not supported yet.");
            innerRoot = rewritten;
            bindings = BindCorrelations(correlations, outerSchema, innerRoot, catalog, resolveOuterColumn);
        }
        else if (outerTableNames.Length == 1)
        {
            UncorrelatedSubqueryValidator.Validate(predicate, outerTableNames[0]);
        }
        else
        {
            UncorrelatedSubqueryValidator.ValidateForJoin(predicate, outerTableNames[0], outerTableNames[1]);
        }

        var physical = _compiler.CompilePhysical(innerRoot, catalog, scanOptions, joinAlgorithm);
        return new SubqueryExistsPhysicalSpec
        {
            Negated = predicate.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotExists,
            Subquery = physical,
            Correlations = bindings,
        };
    }

    private static CorrelatedEqualityBinding[] BindCorrelations(
        IReadOnlyList<LogicalSubqueryCorrelation> correlations,
        TableSchema outerSchema,
        ILogicalRoot innerRoot,
        ICatalog catalog,
        Func<LogicalColumnScalarRef, TableSchema, string, int> resolveOuterColumn)
    {
        if (innerRoot is not LogicalTableScan innerScan)
            throw new SqlCompileException("Correlated subqueries must use a single-table inner SELECT.");
        if (!catalog.TryGetTable(innerScan.TableName, out var innerTs))
            throw new SqlCompileException($"Inner table '{innerScan.TableName}' was not found.");
        var innerSchema = innerTs!.Schema;
        var arr = new CorrelatedEqualityBinding[correlations.Count];
        for (var i = 0; i < correlations.Count; i++)
        {
            var c = correlations[i];
            var outerTable = c.OuterColumn.QualifierTableName
                ?? throw new SqlCompileException("Correlated outer column must be qualified.");
            var outerIx = resolveOuterColumn(c.OuterColumn, outerSchema, outerTable);
            var innerTable = c.InnerColumn.QualifierTableName ?? innerScan.TableName;
            if (!innerTable.Equals(innerScan.TableName, StringComparison.OrdinalIgnoreCase))
                throw new SqlCompileException("Correlated inner column must reference the inner FROM table.");
            var innerIx = LogicalTableScanBinder.ResolveColumn(innerSchema, c.InnerColumn.ColumnName, innerScan.TableName);
            var t = innerSchema.Columns[innerIx].Type;
            arr[i] = new CorrelatedEqualityBinding(outerIx, innerIx, t);
        }

        return arr;
    }

    private static int ResolveSingleSubqueryColumn(IPhysicalPlan subquery, ICatalog catalog)
    {
        var schema = PhysicalPlanOutputSchema.Resolve(subquery, catalog);
        if (schema.Columns.Count != 1)
            throw new SqlCompileException("IN subquery must return exactly one column.");
        return 0;
    }
}
