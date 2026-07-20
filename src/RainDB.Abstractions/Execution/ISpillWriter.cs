namespace RainDB.Execution;

/// <summary>
/// Spill / telemetry hook for operators that may exceed memory in future (grace hash join, large grouping).
/// Hash aggregation: when <see cref="IsEnabled"/> and <see cref="RainDB.Query.Plans.HashAggregatePhysicalPlan.SpillPartialEntryThreshold"/> is positive,
/// partial maps that exceed the threshold invoke <see cref="SpillChunkAsync"/> with a UTF-8 JSON **metrics** line (the operator still completes in-memory).
/// Implementations may later persist serialized partial state and drive an external merge; that protocol is not fixed yet.
/// </summary>
public interface ISpillWriter
{
    /// <summary>When false, operators skip spill branches.</summary>
    bool IsEnabled { get; }

    /// <summary>
    /// Serialized partial state overflow (future). No-op writers complete without persisting.
    /// </summary>
    ValueTask SpillChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken = default);
}

/// <summary>Default spill writer that performs no I/O.</summary>
public sealed class NoOpSpillWriter : ISpillWriter
{
    public static NoOpSpillWriter Instance { get; } = new();

    private NoOpSpillWriter()
    {
    }

    public bool IsEnabled => false;

    public ValueTask SpillChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}
