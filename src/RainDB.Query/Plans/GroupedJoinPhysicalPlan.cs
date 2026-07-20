using System;
using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>Inner join followed by hash aggregation over the join output rows (no catalog entry for the intermediate rowset).</summary>
public sealed class GroupedJoinPhysicalPlan : IPhysicalPlan
{
    public GroupedJoinPhysicalPlan(JoinPhysicalPlan join, HashAggregatePhysicalPlan aggregate)
    {
        ArgumentNullException.ThrowIfNull(join);
        ArgumentNullException.ThrowIfNull(aggregate);
        Join = join;
        Aggregate = aggregate;
    }

    public JoinPhysicalPlan Join { get; }

    /// <summary>Aggregation over <see cref="JoinPhysicalPlan.OutputSchema"/> column indices (same as join output batches).</summary>
    public HashAggregatePhysicalPlan Aggregate { get; }

    public string Explain(string indent = "") =>
        $"{indent}GroupedJoin\n{indent}  {Join.Explain()}\n{indent}  {Aggregate.Explain(indent + "  ")}";
}
