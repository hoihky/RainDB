using RainDB.Query.Plans;

namespace RainDB.Sql.Planning;

/// <summary>Heuristic physical planning knobs (join algorithm, etc.).</summary>
public sealed class PhysicalPlanningOptions
{
    public JoinAlgorithmPreference JoinPreference { get; init; } = JoinAlgorithmPreference.Auto;

    public PhysicalJoinAlgorithm? ForcedJoinAlgorithm { get; init; }
}

public enum JoinAlgorithmPreference
{
    Auto,
    PreferHash,
    PreferSortMerge,
}
