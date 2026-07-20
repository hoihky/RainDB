namespace RainDB.Logical;

/// <summary>Root of a logical relational plan (parse → logical → physical).</summary>
public interface ILogicalPlan
{
    string Explain(string indent = "");
}
