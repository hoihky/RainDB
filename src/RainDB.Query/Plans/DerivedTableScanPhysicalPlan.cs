using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Materialize a subquery then scan/project/sort it like a base table.</summary>
public sealed class DerivedTableScanPhysicalPlan : IPhysicalPlan
{
    public DerivedTableScanPhysicalPlan(
        IPhysicalPlan subquery,
        TableId ephemeralTableId,
        string alias,
        TableSchema derivedSchema,
        IPhysicalPlan outerPlan)
    {
        ArgumentNullException.ThrowIfNull(subquery);
        ArgumentNullException.ThrowIfNull(outerPlan);
        Subquery = subquery;
        EphemeralTableId = ephemeralTableId;
        Alias = alias;
        DerivedSchema = derivedSchema;
        OuterPlan = outerPlan;
    }

    public IPhysicalPlan Subquery { get; }

    public TableId EphemeralTableId { get; }

    public string Alias { get; }

    public TableSchema DerivedSchema { get; }

    public IPhysicalPlan OuterPlan { get; }

    public string Explain(string indent = "") =>
        $"{indent}DerivedTableScan(alias={Alias})";
}
