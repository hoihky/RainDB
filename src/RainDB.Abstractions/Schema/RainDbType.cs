namespace RainDB.Schema;

/// <summary>Physical storage class for a column (OLAP-friendly primitive set; extend over time).</summary>
public enum RainDbType
{
    Int32,
    Int64,
    Float64,
    Boolean,
    /// <summary>Length-prefixed UTF-8 or dictionary-encoded string bucket (implementation-specific).</summary>
    Utf8,
}
