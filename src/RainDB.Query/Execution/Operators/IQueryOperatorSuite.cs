namespace RainDB.Query.Execution.Operators;

/// <summary>Composition root for physical OLAP operators (facade over scan, agg, join, sort, grouped join).</summary>
public interface IQueryOperatorSuite
{
    IVectorizedScanOperator Scan { get; }

    IHashAggregateOperator HashAggregate { get; }

    IJoinOperator Join { get; }

    ISortTopNOperator SortTopN { get; }

    IGroupedJoinOperator GroupedJoin { get; }
}
