using RainDB.Schema;

namespace RainDB.Execution;

/// <summary>Single-value aggregate output (partial or final combine).</summary>
public interface IAggregateQueryResult : IQueryResult
{
    RainDbType ResultType { get; }

    double Float64Value { get; }

    long Int64Value { get; }

    /// <summary>Rows that contributed after filter (non-null measure rows for SUM; COUNT uses same total).</summary>
    long ContributingRowCount { get; }

    /// <summary>When true, numeric result is SQL NULL (e.g. SUM with no non-null inputs).</summary>
    bool ValueIsNull { get; }
}
