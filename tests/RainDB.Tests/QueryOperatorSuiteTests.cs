using RainDB.Query.Execution;
using RainDB.Query.Execution.Operators;

namespace RainDB.Tests;

public class QueryOperatorSuiteTests
{
    [Fact]
    public void DefaultQueryExecutor_exposes_injected_operator_suite()
    {
        var suite = new DefaultQueryOperatorSuite();
        var executor = new DefaultQueryExecutor(suite);
        Assert.Same(suite, executor.Operators);
        Assert.IsType<VectorizedScanOperator>(executor.Operators.Scan);
        Assert.IsType<JoinOperator>(executor.Operators.Join);
    }

    [Fact]
    public void DefaultQueryOperatorSuite_wires_sort_top_n_to_join_operator()
    {
        var join = new JoinOperator();
        var suite = new DefaultQueryOperatorSuite(join: join);
        Assert.Same(join, suite.Join);
        Assert.IsType<SortTopNOperator>(suite.SortTopN);
    }
}
