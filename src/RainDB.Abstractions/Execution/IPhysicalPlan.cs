namespace RainDB.Execution;

/// <summary>Root of an executable physical operator tree (volcano / Morsel / vector model behind this boundary).</summary>
public interface IPhysicalPlan
{
    string Explain(string indent = "");
}
