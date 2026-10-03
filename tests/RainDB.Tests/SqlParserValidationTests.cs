using RainDB.Sql;

namespace RainDB.Tests;

public class SqlParserValidationTests
{
    [Fact]
    public void Limit_zero_is_rejected()
    {
        var ex = Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan("SELECT x FROM t LIMIT 0"));
        Assert.Contains("LIMIT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Limit_negative_is_rejected()
    {
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan("SELECT x FROM t LIMIT -1"));
    }

    [Fact]
    public void Limit_decimal_is_rejected()
    {
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan("SELECT x FROM t LIMIT 1.5"));
    }

    [Fact]
    public void Having_parses_into_grouped_scan()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT x, SUM(v) FROM t GROUP BY x HAVING SUM(v) > 1");
        var scan = Assert.IsType<RainDB.Logical.LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.HavingConjuncts);
        Assert.Single(scan.HavingConjuncts);
    }

    [Fact]
    public void Right_join_is_rejected()
    {
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan(
                "SELECT * FROM a RIGHT JOIN b ON a.id = b.id"));
    }

    [Fact]
    public void Left_join_parses()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
        var join = Assert.IsType<RainDB.Logical.LogicalInnerJoin>(plan.Root);
        Assert.Equal(RainDB.Logical.LogicalJoinSemantics.LeftOuter, join.Semantics);
    }

    [Fact]
    public void Line_comment_is_ignored()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT x FROM t -- trailing comment\nWHERE x = 1");
        Assert.NotNull(plan.Root);
    }
}
