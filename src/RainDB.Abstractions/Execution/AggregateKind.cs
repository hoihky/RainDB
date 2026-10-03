namespace RainDB.Execution;

public enum AggregateKind
{
    None,
    Sum,
    Min,
    Max,

    /// <summary><c>COUNT(*)</c> uses <see cref="AggregateSpec.SourceColumnIndex"/> -1; <c>COUNT(col)</c> uses the column index.</summary>
    Count,

    /// <summary><c>COUNT(DISTINCT col)</c> — counts unique non-null values per group.</summary>
    CountDistinct,
}
