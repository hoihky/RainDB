using RainDB;
using RainDB.Core.Catalog;
using RainDB.Core.Tables;
using RainDB.Catalog;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

public class CatalogAndEngineTests
{
    [Fact]
    public void Register_rejects_duplicate_table_name()
    {
        var cat = new InMemoryCatalog();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        cat.Register(new MemoryTable("t", schema));
        Assert.Throws<InvalidOperationException>(() => cat.Register(new MemoryTable("t", schema)));
    }

    [Fact]
    public void Register_rejects_duplicate_table_id()
    {
        var cat = new InMemoryCatalog();
        var id = TableId.New();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        cat.Register(new MemoryTable("a", schema, id));
        Assert.Throws<InvalidOperationException>(() => cat.Register(new MemoryTable("b", schema, id)));
    }

    [Fact]
    public void TableNames_is_case_insensitive_for_lookup()
    {
        var cat = new InMemoryCatalog();
        var t = new MemoryTable("Sales", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        cat.Register(t);
        Assert.True(cat.TryGetTable("sales", out var found));
        Assert.Same(t, found);
    }

    [Fact]
    public async Task ExecuteSql_throws_on_unknown_table()
    {
        var engine = TestDataBuilders.CreateEngine();
        await Assert.ThrowsAsync<SqlCompileException>(() =>
            engine.ExecuteSqlAsync("SELECT x FROM missing").AsTask());
    }

    [Fact]
    public void OpenPersistent_creates_empty_catalog_when_directory_new()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_new_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.OpenPersistent(root);
            Assert.NotNull(engine.FileDatabase);
            Assert.Empty(engine.Catalog.TableNames);
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }
}
