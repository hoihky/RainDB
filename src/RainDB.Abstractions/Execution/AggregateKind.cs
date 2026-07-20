namespace RainDB.Execution;

public enum AggregateKind
{
    None,
    Sum,
    Min,
    Max,

    /// <summary><c>COUNT(*)</c> uses <see cref="AggregateSpec.SourceColumnIndex"/> -1; <c>COUNT(col)</c> uses the column index.</summary>
    Count,
}
