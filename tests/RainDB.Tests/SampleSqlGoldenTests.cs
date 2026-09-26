using RainDB;
using RainDB.Execution;

namespace RainDB.Tests;

/// <summary>Runs <c>samples/sql/*.sql</c> against the analytics demo dataset (compile + execute smoke tests).</summary>
public class SampleSqlGoldenTests
{
    private static string SamplesSqlDirectory =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "samples", "sql"));

    public static IEnumerable<object[]> SqlFiles()
    {
        var dir = SamplesSqlDirectory;
        if (!Directory.Exists(dir))
            yield break;
        foreach (var file in Directory.EnumerateFiles(dir, "*.sql").OrderBy(Path.GetFileName, StringComparer.Ordinal))
            yield return [file];
    }

    [Theory]
    [MemberData(nameof(SqlFiles))]
    public async Task Sample_sql_executes_without_error(string sqlFilePath)
    {
        var sql = await File.ReadAllTextAsync(sqlFilePath);
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);

        await using var result = await engine.ExecuteSqlAsync(sql);
        Assert.True(result.RowCount >= 0);
    }

    [Fact]
    public async Task Demo_sum_line_total_matches_expected_revenue()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(line_total) FROM order_lines");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.False(agg.ValueIsNull);
        Assert.Equal(18259.48, agg.Float64Value, precision: 2);
    }

    [Fact]
    public async Task Demo_select_star_row_count()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM order_lines");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(7, col.RowCount);
        Assert.Equal(2, col.Batches.Count);
    }

    [Fact]
    public async Task Demo_join_with_filter_returns_rows()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);
        var sql = """
            SELECT order_lines.region, rebate_tiers.rebate_pct
            FROM order_lines
            INNER JOIN rebate_tiers ON order_lines.quantity = rebate_tiers.min_qty
            WHERE order_lines.quantity >= 6
            """;
        await using var r = await engine.ExecuteSqlAsync(sql);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.True(col.RowCount >= 1);
    }
}
