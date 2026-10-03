using RainDB.Sql.Optimization.Rules;

namespace RainDB.Sql.Optimization;

internal static class DefaultRules
{
    public static IReadOnlyList<ILogicalRewriteRule> Create() =>
    [
        new JoinPredicatePartitionRule(),
        new TableScanProjectionPruningRule(),
        new JoinProjectionPruningRule(),
        new LimitPushdownValidationRule(),
    ];
}
