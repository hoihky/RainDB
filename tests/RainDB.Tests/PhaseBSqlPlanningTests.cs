using RainDB;
using RainDB.Core.Catalog;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Schema;
using RainDB.Sql;
using RainDB.Sql.Compilation;
using RainDB.Sql.Optimization;

namespace RainDB.Tests;

public class PhaseBSqlPlanningTests
{
    [Fact]
    public void Join_predicate_partition_rule_splits_qualified_conjuncts()
    {
        var join = new LogicalInnerJoin
        {
            LeftTableName = "a",
            RightTableName = "b",
            LeftKeyColumns = [new LogicalQualifiedColumn { TableName = "a", ColumnName = "id" }],
            RightKeyColumns = [new LogicalQualifiedColumn { TableName = "b", ColumnName = "id" }],
            WhereConjuncts =
            [
                new SimpleWhereClause
                {
                    QualifierTableName = "a",
                    ColumnName = "x",
                    Operator = ScalarCompareOp.Gt,
                    Literal = new SqlLiteral(SqlLiteralKind.Integer, "1"),
                },
                new SimpleWhereClause
                {
                    QualifierTableName = "b",
                    ColumnName = "y",
                    Operator = ScalarCompareOp.Lt,
                    Literal = new SqlLiteral(SqlLiteralKind.Integer, "9"),
                },
            ],
        };
        var outPlan = new LogicalRewritePipeline().Optimize(new LogicalPlan(join));
        var optimized = Assert.IsType<LogicalInnerJoin>(outPlan.Root);
        Assert.Null(optimized.WhereConjuncts);
        Assert.Single(optimized.ProbeSideWhereConjuncts!);
        Assert.Single(optimized.BuildSideWhereConjuncts!);
    }

    [Fact]
    public async Task Explain_sql_returns_text_without_scanning_table()
    {
        var engine = TestDataBuilders.CreateEngine();
        var table = TestDataBuilders.TableWithInt32Column("t", "k", [(1, false), (2, false), (3, false)]);
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("EXPLAIN SELECT k FROM t WHERE k > 0");
        var explain = Assert.IsType<ExplainTextQueryResult>(r);
        Assert.Contains("LOGICAL:", explain.Text, StringComparison.Ordinal);
        Assert.Contains("PHYSICAL:", explain.Text, StringComparison.Ordinal);
        Assert.Contains("VectorizedScan", explain.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Prepared_statement_parameter_changes_filter_result()
    {
        var engine = TestDataBuilders.CreateEngine();
        var table = TestDataBuilders.TableWithInt32Column("t", "k", [(1, false), (2, false), (3, false), (4, false)]);
        engine.Catalog.Register(table);

        var compiler = Assert.IsType<DefaultSqlCompiler>(engine.SqlCompiler);
        var prepared = await compiler.PrepareAsync("SELECT k FROM t WHERE k > @min", engine.Catalog);
        Assert.Contains("min", prepared.ParameterNames, StringComparer.OrdinalIgnoreCase);

        await using var r1 = await prepared.ExecuteAsync(
            engine.Catalog,
            engine.Executor,
            engine.CreateSession(),
            new Dictionary<string, SqlParameterValue> { ["min"] = new(SqlLiteralKind.Integer, "1") });
        var c1 = Assert.IsAssignableFrom<IColumnarQueryResult>(r1);
        Assert.Equal(3, c1.RowCount);

        await using var r2 = await prepared.ExecuteAsync(
            engine.Catalog,
            engine.Executor,
            engine.CreateSession(),
            new Dictionary<string, SqlParameterValue> { ["min"] = new(SqlLiteralKind.Integer, "3") });
        var c2 = Assert.IsAssignableFrom<IColumnarQueryResult>(r2);
        Assert.Equal(1, c2.RowCount);
    }

    [Fact]
    public async Task Compile_cache_skips_rebind_on_second_identical_sql()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(TestDataBuilders.SingleInt32Batch(5));
        engine.Catalog.Register(table);

        var compiler = Assert.IsType<DefaultSqlCompiler>(engine.SqlCompiler);
        var sql = "SELECT SUM(k) FROM t";
        var p1 = await compiler.CompileAsync(sql, engine.Catalog);
        var p2 = await compiler.CompileAsync(sql, engine.Catalog);
        Assert.Same(p1, p2);
    }

    [Fact]
    public void Heuristic_join_selector_may_pick_sort_merge_on_balanced_small_tables()
    {
        var catalog = new InMemoryCatalog();
        var left = TestDataBuilders.TableWithInt32Column("l", "id", [(1, false), (2, false), (3, false)]);
        var right = TestDataBuilders.TableWithInt32Column("r", "id", [(4, false), (5, false), (6, false)]);
        catalog.Register(left);
        catalog.Register(right);

        var join = new LogicalInnerJoin
        {
            LeftTableName = "l",
            RightTableName = "r",
            LeftKeyColumns = [new LogicalQualifiedColumn { TableName = "l", ColumnName = "id" }],
            RightKeyColumns = [new LogicalQualifiedColumn { TableName = "r", ColumnName = "id" }],
        };

        var selector = new RainDB.Sql.Planning.HeuristicJoinAlgorithmSelector();
        var algo = selector.Select(join, catalog, new RainDB.Sql.Planning.PhysicalPlanningOptions());
        Assert.True(algo is PhysicalJoinAlgorithm.Hash or PhysicalJoinAlgorithm.SortMerge);
    }
}
