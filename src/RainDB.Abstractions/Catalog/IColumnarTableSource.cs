using RainDB.Columnar;

namespace RainDB.Catalog;

/// <summary>Table whose storage is exposed as append-only columnar batches (scan target for P1+).</summary>
public interface IColumnarTableSource : ITableSource
{
    IReadOnlyList<IColumnarBatch> Batches { get; }
}
