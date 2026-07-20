using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Non-catalog columnar source used as the input to hash aggregation over a materialized join result.</summary>
internal sealed class EphemeralColumnarTableSource : IColumnarTableSource
{
    public EphemeralColumnarTableSource(TableId id, string name, TableSchema schema, IReadOnlyList<IColumnarBatch> batches)
    {
        Id = id;
        Name = name;
        Schema = schema;
        Batches = batches;
    }

    public TableId Id { get; }

    public string Name { get; }

    public TableSchema Schema { get; }

    public int SchemaVersion => 0;

    public IReadOnlyList<IColumnarBatch> Batches { get; }
}
