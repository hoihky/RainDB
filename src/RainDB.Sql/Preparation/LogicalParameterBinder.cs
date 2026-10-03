using RainDB.Logical;
using RainDB.Sql.Optimization;

namespace RainDB.Sql.Preparation;

/// <summary>Substitutes parameter placeholders in logical WHERE conjuncts before physical binding.</summary>
public sealed class LogicalParameterBinder
{
    private readonly LogicalPlanCloner _cloner = new();

    public LogicalPlan BindParameters(LogicalPlan template, IReadOnlyDictionary<string, SqlParameterValue> parameters)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(parameters);
        var names = CollectParameterNames(template.Root);
        foreach (var name in names)
        {
            if (!parameters.ContainsKey(name))
                throw new SqlCompileException($"Missing value for parameter '@{name}'.");
        }

        var root = BindRoot(template.Root, parameters);
        return new LogicalPlan(root, template.ExplainLevel);
    }

    public IReadOnlyList<string> CollectParameterNames(ILogicalRoot root)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        switch (root)
        {
            case LogicalTableScan s:
                CollectFromWhere(s.WhereConjuncts, set);
                break;
            case LogicalInnerJoin j:
                CollectFromWhere(j.WhereConjuncts, set);
                CollectFromWhere(j.ProbeSideWhereConjuncts, set);
                CollectFromWhere(j.BuildSideWhereConjuncts, set);
                break;
        }

        return set.OrderBy(static n => n, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private ILogicalRoot BindRoot(ILogicalRoot root, IReadOnlyDictionary<string, SqlParameterValue> parameters) =>
        root switch
        {
            LogicalTableScan s => BindTableScan(s, parameters),
            LogicalInnerJoin j => BindJoin(j, parameters),
            _ => throw new InvalidOperationException($"Unsupported logical root {root.GetType().Name}."),
        };

    private LogicalTableScan BindTableScan(LogicalTableScan s, IReadOnlyDictionary<string, SqlParameterValue> parameters) =>
        _cloner.CloneTableScan(s, whereOverride: BindWhereList(s.WhereConjuncts, parameters));

    private LogicalInnerJoin BindJoin(LogicalInnerJoin j, IReadOnlyDictionary<string, SqlParameterValue> parameters) =>
        _cloner.CloneJoin(
            j,
            whereOverride: BindWhereList(j.WhereConjuncts, parameters),
            probeWhereOverride: BindWhereList(j.ProbeSideWhereConjuncts, parameters),
            buildWhereOverride: BindWhereList(j.BuildSideWhereConjuncts, parameters));

    private static IReadOnlyList<SimpleWhereClause>? BindWhereList(
        IReadOnlyList<SimpleWhereClause>? conjuncts,
        IReadOnlyDictionary<string, SqlParameterValue> parameters)
    {
        if (conjuncts is null)
            return null;
        var list = new List<SimpleWhereClause>(conjuncts.Count);
        foreach (var w in conjuncts)
        {
            if (!w.UsesParameter)
            {
                if (w.Literal is null)
                    throw new SqlCompileException($"Predicate on '{w.ColumnName}' is missing a literal or parameter.");
                list.Add(w);
                continue;
            }

            if (!parameters.TryGetValue(w.ParameterName!, out var value))
                throw new SqlCompileException($"Missing value for parameter '@{w.ParameterName}'.");
            list.Add(new SimpleWhereClause
            {
                QualifierTableName = w.QualifierTableName,
                ColumnName = w.ColumnName,
                Operator = w.Operator,
                Literal = value.ToLiteral(),
            });
        }

        return list;
    }

    private static void CollectFromWhere(IReadOnlyList<SimpleWhereClause>? conjuncts, HashSet<string> names)
    {
        if (conjuncts is null)
            return;
        foreach (var w in conjuncts)
        {
            if (w.UsesParameter)
                names.Add(w.ParameterName!);
        }
    }
}
