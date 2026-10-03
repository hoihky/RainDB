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

        var (probeResolved, corrProbeIn) = await SplitInListAsync(probeIn, executor, context).ConfigureAwait(false);
        var (buildResolved, corrBuildIn) = await SplitInListAsync(buildIn, executor, context).ConfigureAwait(false);

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
                    return ResolvedJoinSubqueryFilters.AllDenied;
            }

            correlatedExists = corr.Count > 0 ? corr.ToArray() : null;
        }

        return new ResolvedJoinSubqueryFilters(probeResolved, buildResolved, correlatedExists, corrProbeIn, corrBuildIn);
    }

    private static async ValueTask<(ColumnInSetFilter[]? Resolved, SubqueryInPhysicalSpec[]? Correlated)> SplitInListAsync(
        SubqueryInPhysicalSpec[]? inFilters,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        if (inFilters is null or { Length: 0 })
            return (null, null);

        var resolved = new List<ColumnInSetFilter>();
        var corr = new List<SubqueryInPhysicalSpec>();
        for (var i = 0; i < inFilters.Length; i++)
        {
            var spec = inFilters[i];
            if (spec.Correlations is { Length: > 0 })
            {
                corr.Add(spec);
                continue;
            }

            var result = await executor.ExecuteAsync(spec.Subquery, context).ConfigureAwait(false);
            if (result is not IColumnarQueryResult col)
                throw new InvalidOperationException("IN subquery must return a columnar row set.");
            var set = ScalarValueSet.FromSingleColumn(col, spec.SubqueryResultColumnIndex, spec.ColumnType);
            resolved.Add(new ColumnInSetFilter(spec.ColumnIndex, spec.Negated, set));
        }

        return (
            resolved.Count > 0 ? resolved.ToArray() : null,
            corr.Count > 0 ? corr.ToArray() : null);
    }
}

internal sealed class ResolvedJoinSubqueryFilters
{
    public static readonly ResolvedJoinSubqueryFilters Empty = new(null, null);
    public static readonly ResolvedJoinSubqueryFilters AllDenied = new(null, null, denyAll: true);

    public ResolvedJoinSubqueryFilters(
        ColumnInSetFilter[]? probeInFilters,
        ColumnInSetFilter[]? buildInFilters,
        SubqueryExistsPhysicalSpec[]? correlatedExists = null,
        SubqueryInPhysicalSpec[]? correlatedProbeIn = null,
        SubqueryInPhysicalSpec[]? correlatedBuildIn = null,
        bool denyAll = false)
    {
        ProbeInFilters = probeInFilters;
        BuildInFilters = buildInFilters;
        CorrelatedExists = correlatedExists;
        CorrelatedProbeIn = correlatedProbeIn;
        CorrelatedBuildIn = correlatedBuildIn;
        IsDenyAll = denyAll;
    }

    public ColumnInSetFilter[]? ProbeInFilters { get; }

    public ColumnInSetFilter[]? BuildInFilters { get; }

    public SubqueryExistsPhysicalSpec[]? CorrelatedExists { get; }

    public SubqueryInPhysicalSpec[]? CorrelatedProbeIn { get; }

    public SubqueryInPhysicalSpec[]? CorrelatedBuildIn { get; }

    public bool IsDenyAll { get; }

    public bool HasCorrelated =>
        CorrelatedExists is { Length: > 0 }
        || CorrelatedProbeIn is { Length: > 0 }
        || CorrelatedBuildIn is { Length: > 0 };
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
