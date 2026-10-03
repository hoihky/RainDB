using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;

namespace RainDB.Query.Execution;

internal static class SubqueryFilterResolver
{
    internal static async ValueTask<ResolvedSubqueryFilters> ResolveAsync(
        SubqueryInPhysicalSpec[]? inFilters,
        SubqueryExistsPhysicalSpec[]? existsFilters,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        if (inFilters is null && existsFilters is null)
            return ResolvedSubqueryFilters.Empty;

        ColumnInSetFilter[]? inResolved = null;
        SubqueryInPhysicalSpec[]? correlatedIn = null;
        if (inFilters is { Length: > 0 })
        {
            var resolved = new List<ColumnInSetFilter>();
            var corrIn = new List<SubqueryInPhysicalSpec>();
            for (var i = 0; i < inFilters.Length; i++)
            {
                var spec = inFilters[i];
                if (spec.Correlations is { Length: > 0 })
                {
                    corrIn.Add(spec);
                    continue;
                }

                var result = await executor.ExecuteAsync(spec.Subquery, context).ConfigureAwait(false);
                if (result is not IColumnarQueryResult col)
                    throw new InvalidOperationException("IN subquery must return a columnar row set.");
                var set = ScalarValueSet.FromSingleColumn(col, spec.SubqueryResultColumnIndex, spec.ColumnType);
                resolved.Add(new ColumnInSetFilter(spec.ColumnIndex, spec.Negated, set));
            }

            inResolved = resolved.Count > 0 ? resolved.ToArray() : null;
            correlatedIn = corrIn.Count > 0 ? corrIn.ToArray() : null;
        }

        SubqueryExistsPhysicalSpec[]? correlatedExists = null;
        if (existsFilters is { Length: > 0 })
        {
            var corr = new List<SubqueryExistsPhysicalSpec>();
            foreach (var ex in existsFilters)
            {
                if (ex.Correlations is { Length: > 0 })
                {
                    corr.Add(ex);
                    continue;
                }

                var result = await executor.ExecuteAsync(ex.Subquery, context).ConfigureAwait(false);
                var any = result is IColumnarQueryResult c && c.RowCount > 0;
                if (ex.Negated)
                    any = !any;
                if (!any)
                    return ResolvedSubqueryFilters.AllDenied;
            }

            correlatedExists = corr.Count > 0 ? corr.ToArray() : null;
        }

        return new ResolvedSubqueryFilters(inResolved, correlatedExists, correlatedIn);
    }

    internal static async ValueTask<ResolvedJoinSubqueryFilters> ResolveJoinAsync(
        SubqueryInPhysicalSpec[]? probeIn,
        SubqueryInPhysicalSpec[]? buildIn,
        SubqueryExistsPhysicalSpec[]? existsFilters,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        if (probeIn is null && buildIn is null && existsFilters is null)
            return ResolvedJoinSubqueryFilters.Empty;

        ColumnInSetFilter[]? probeResolved = null;
        if (probeIn is { Length: > 0 })
            probeResolved = await ResolveInListAsync(probeIn, executor, context).ConfigureAwait(false);

        ColumnInSetFilter[]? buildResolved = null;
        if (buildIn is { Length: > 0 })
            buildResolved = await ResolveInListAsync(buildIn, executor, context).ConfigureAwait(false);

        if (existsFilters is { Length: > 0 })
        {
            foreach (var ex in existsFilters)
            {
                if (ex.Correlations is { Length: > 0 })
                    throw new NotSupportedException("Correlated EXISTS is not supported on join queries yet.");
                var result = await executor.ExecuteAsync(ex.Subquery, context).ConfigureAwait(false);
                var any = result is IColumnarQueryResult c && c.RowCount > 0;
                if (ex.Negated)
                    any = !any;
                if (!any)
                    return ResolvedJoinSubqueryFilters.AllDenied;
            }
        }

        return new ResolvedJoinSubqueryFilters(probeResolved, buildResolved);
    }

    private static async Task<ColumnInSetFilter[]> ResolveInListAsync(
        SubqueryInPhysicalSpec[] inFilters,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        var inResolved = new ColumnInSetFilter[inFilters.Length];
        for (var i = 0; i < inFilters.Length; i++)
        {
            var spec = inFilters[i];
            var result = await executor.ExecuteAsync(spec.Subquery, context).ConfigureAwait(false);
            if (result is not IColumnarQueryResult col)
                throw new InvalidOperationException("IN subquery must return a columnar row set.");
            var set = ScalarValueSet.FromSingleColumn(col, spec.SubqueryResultColumnIndex, spec.ColumnType);
            inResolved[i] = new ColumnInSetFilter(spec.ColumnIndex, spec.Negated, set);
        }

        return inResolved;
    }
}

internal sealed class ResolvedJoinSubqueryFilters
{
    public static readonly ResolvedJoinSubqueryFilters Empty = new(null, null);
    public static readonly ResolvedJoinSubqueryFilters AllDenied = new(null, null, denyAll: true);

    public ResolvedJoinSubqueryFilters(ColumnInSetFilter[]? probeInFilters, ColumnInSetFilter[]? buildInFilters, bool denyAll = false)
    {
        ProbeInFilters = probeInFilters;
        BuildInFilters = buildInFilters;
        IsDenyAll = denyAll;
    }

    public ColumnInSetFilter[]? ProbeInFilters { get; }

    public ColumnInSetFilter[]? BuildInFilters { get; }

    public bool IsDenyAll { get; }
}

internal sealed class ResolvedSubqueryFilters
{
    public static readonly ResolvedSubqueryFilters Empty = new(null);
    public static readonly ResolvedSubqueryFilters AllDenied = new(null, denyAll: true);

    public ResolvedSubqueryFilters(
        ColumnInSetFilter[]? inFilters,
        SubqueryExistsPhysicalSpec[]? correlatedExists = null,
        SubqueryInPhysicalSpec[]? correlatedIn = null,
        bool denyAll = false)
    {
        InFilters = inFilters;
        CorrelatedExists = correlatedExists;
        CorrelatedIn = correlatedIn;
        IsDenyAll = denyAll;
    }

    public ColumnInSetFilter[]? InFilters { get; }

    public SubqueryExistsPhysicalSpec[]? CorrelatedExists { get; }

    public SubqueryInPhysicalSpec[]? CorrelatedIn { get; }

    public bool IsDenyAll { get; }
}
