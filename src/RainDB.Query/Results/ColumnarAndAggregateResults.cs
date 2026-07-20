using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Results;

public sealed class ColumnarMaterializedQueryResult : IColumnarQueryResult
{
    private readonly IReadOnlyList<IColumnarBatch> _batches;

    public ColumnarMaterializedQueryResult(IReadOnlyList<IColumnarBatch> batches)
    {
        ArgumentNullException.ThrowIfNull(batches);
        _batches = batches;
        long rows = 0;
        foreach (var b in batches)
            rows += b.RowCount;
        RowCount = rows;
    }

    public long RowCount { get; }

    public IReadOnlyList<IColumnarBatch> Batches => _batches;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class AggregateQueryResult : IAggregateQueryResult
{
    public AggregateQueryResult(
        RainDbType resultType,
        double float64Value,
        long int64Value,
        long contributingRowCount,
        bool valueIsNull = false)
    {
        ResultType = resultType;
        Float64Value = float64Value;
        Int64Value = int64Value;
        ContributingRowCount = contributingRowCount;
        ValueIsNull = valueIsNull;
        RowCount = 1;
    }

    public long RowCount { get; }

    public RainDbType ResultType { get; }

    public double Float64Value { get; }

    public long Int64Value { get; }

    public long ContributingRowCount { get; }

    public bool ValueIsNull { get; }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
