namespace RainDB.Logical;

/// <summary>Logical plan with a single root operator (strict subset entry point).</summary>
public sealed class LogicalPlan : ILogicalPlan
{
    public LogicalPlan(ILogicalRoot root, SqlExplainLevel? explainLevel = null)
    {
        Root = root;
        ExplainLevel = explainLevel;
    }

    public ILogicalRoot Root { get; }

    /// <summary>When set, compilation produces an explain bundle instead of executing the inner plan directly.</summary>
    public SqlExplainLevel? ExplainLevel { get; }

    public string Explain(string indent = "") => Root.Explain(indent);
}
