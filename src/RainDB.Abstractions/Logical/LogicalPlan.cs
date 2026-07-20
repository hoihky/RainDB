namespace RainDB.Logical;

/// <summary>Logical plan with a single root operator (strict subset entry point).</summary>
public sealed class LogicalPlan : ILogicalPlan
{
    public LogicalPlan(ILogicalRoot root) => Root = root;

    public ILogicalRoot Root { get; }

    public string Explain(string indent = "") => Root.Explain(indent);
}
