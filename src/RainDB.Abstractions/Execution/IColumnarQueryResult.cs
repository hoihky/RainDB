using RainDB.Columnar;

namespace RainDB.Execution;

/// <summary>Query result materialized as columnar batches (OLAP pull model).</summary>
public interface IColumnarQueryResult : IQueryResult
{
    IReadOnlyList<IColumnarBatch> Batches { get; }
}
