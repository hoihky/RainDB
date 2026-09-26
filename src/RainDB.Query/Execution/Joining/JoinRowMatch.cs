namespace RainDB.Query.Execution.Joining;

/// <summary>One inner-join output row as probe/build batch coordinates.</summary>
internal readonly record struct JoinRowMatch(int LeftBatchIdx, int LeftRow, int RightBatchIdx, int RightRow);
