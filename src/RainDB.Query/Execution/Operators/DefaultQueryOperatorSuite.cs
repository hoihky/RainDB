namespace RainDB.Query.Execution.Operators;

/// <summary>Default wiring of physical operators with constructor injection for tests and custom engines.</summary>
public sealed class DefaultQueryOperatorSuite : IQueryOperatorSuite
{
    public DefaultQueryOperatorSuite(
        IVectorizedScanOperator? scan = null,
        IHashAggregateOperator? hashAggregate = null,
        IJoinOperator? join = null,
        ISortTopNOperator? sortTopN = null,
        IGroupedJoinOperator? groupedJoin = null)
        : this(new QueryOperatorDependencies(), scan, hashAggregate, join, sortTopN, groupedJoin)
    {
    }

    internal DefaultQueryOperatorSuite(
        QueryOperatorDependencies dependencies,
        IVectorizedScanOperator? scan = null,
        IHashAggregateOperator? hashAggregate = null,
        IJoinOperator? join = null,
        ISortTopNOperator? sortTopN = null,
        IGroupedJoinOperator? groupedJoin = null)
    {
        var deps = dependencies ?? throw new ArgumentNullException(nameof(dependencies));
        Dependencies = deps;
        var joinOperator = join ?? new JoinOperator(deps);

        IHashAggregateGroupingSupport hashGrouping;
        if (hashAggregate is IHashAggregateGroupingSupport groupingSupport)
        {
            hashGrouping = groupingSupport;
            HashAggregate = hashAggregate;
        }
        else if (hashAggregate is null)
        {
            var defaultHash = new HashAggregateOperator(deps);
            hashGrouping = defaultHash;
            HashAggregate = defaultHash;
        }
        else
        {
            throw new ArgumentException(
                "A custom hash aggregate must implement IHashAggregateGroupingSupport for grouped join.",
                nameof(hashAggregate));
        }

        Join = joinOperator;
        Scan = scan ?? new VectorizedScanOperator(deps);
        SortTopN = sortTopN ?? new SortTopNOperator(joinOperator, deps);
        GroupedJoin = groupedJoin ?? new GroupedJoinOperator(joinOperator, hashGrouping);
    }

    internal QueryOperatorDependencies Dependencies { get; }

    public IVectorizedScanOperator Scan { get; }

    public IHashAggregateOperator HashAggregate { get; }

    public IJoinOperator Join { get; }

    public ISortTopNOperator SortTopN { get; }

    public IGroupedJoinOperator GroupedJoin { get; }
}
