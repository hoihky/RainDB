namespace RainDB.Logical;

/// <summary>Root relational operator produced by the SQL parser (scan, join, …).</summary>
public interface ILogicalRoot
{
    string Explain(string indent = "");
}
