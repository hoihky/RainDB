namespace RainDB.Core.Persistence;

public sealed class RainDbBatchCodecOptions
{
    public bool EnableInt32DictionaryEncoding { get; init; } = true;

    public static RainDbBatchCodecOptions Default { get; } = new();
}
