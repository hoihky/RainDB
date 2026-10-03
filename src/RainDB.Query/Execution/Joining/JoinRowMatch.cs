namespace RainDB.Query.Execution.Joining;

/// <summary>One inner-join output row as probe/build batch coordinates.</summary>
internal readonly record struct JoinRowMatch(int LeftBatchIdx, int LeftRow, int RightBatchIdx, int RightRow)
{
    public bool HasRight => RightBatchIdx >= 0;

    public bool HasLeft => LeftBatchIdx >= 0;

    public static JoinRowMatch ProbeOnly(int leftBatchIdx, int leftRow) => new(leftBatchIdx, leftRow, -1, -1);

    public static JoinRowMatch BuildOnly(int rightBatchIdx, int rightRow) => new(-1, -1, rightBatchIdx, rightRow);
}
