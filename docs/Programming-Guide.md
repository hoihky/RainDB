# RainDB programming guide

This guide teaches how to embed RainDB in a .NET application: create tables, load data, run SQL, read results, use physical plans, and persist data. It assumes .NET 10 and a local clone or project reference to the RainDB solution.

**See also:** [RainDB internals](RainDB-Internals.md) (algorithms and architecture), [README](../README.md) (full SQL subset reference).

---

## 1. Getting started

### 1.1 Build and test

```bash
cd RainDB
dotnet build RainDB.slnx
dotnet test RainDB.slnx
dotnet run --project samples/RainDB.AnalyticsDemo
```

### 1.2 Referencing RainDB from your app

Reference the driver project (composition root) and, when building tables in code, **RainDB.Core**:

| Project | When you need it |
|---------|------------------|
| `RainDB.Driver` (`RainDB` assembly) | `RainDbEngine`, `ExecuteSqlAsync`, `ExecutePhysicalAsync` |
| `RainDB.Core` | `MemoryTable`, column chunks, `RainDbFileDatabase` |
| `RainDB.Abstractions` | `ICatalog`, `IQueryResult`, `IPhysicalPlan`, schema types |
| `RainDB.Sql` | `StrictSqlSubset` for compile-only tooling |

Typical usings:

```csharp
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;
```

---

## 2. Engine lifecycle

### 2.1 In-memory engine (default)

```csharp
var engine = RainDbEngine.CreateDefault();
```

This creates:

- `InMemoryCatalog`
- `HybridBufferPool` (general + aligned allocation)
- `DefaultQueryExecutor`, `DefaultSqlCompiler`, stub `DefaultLinqCompiler`
- No-op spill writer

### 2.2 Persistent directory database

```csharp
var engine = RainDbEngine.OpenPersistent("/path/to/dataDir");
var fileDb = engine.FileDatabase!; // keeps persistence alive
```

- Loads `catalog.json` and existing `.batch` files into memory if present.
- New tables should be created with `fileDb.CreateMemoryTable(...)` so **appends are mirrored to disk**.

### 2.3 Custom catalog

```csharp
using RainDB.Core.Catalog;

var catalog = new InMemoryCatalog();
// ... register tables ...
var engine = RainDbEngine.CreateDefault(catalog);
```

### 2.4 Disposal pattern

Query results may own pooled memory (`await using`):

```csharp
await using var result = await engine.ExecuteSqlAsync("SELECT ...");
```

Call `DisposeAsync` on results when you are done reading columns, especially after wide scans with filters.

---

## 3. Defining tables and schema

### 3.1 Schema

```csharp
var schema = new TableSchema([
    new ColumnDef("region", RainDbType.Utf8),
    new ColumnDef("amount", RainDbType.Float64),
    new ColumnDef("qty", RainDbType.Int32),
    new ColumnDef("active", RainDbType.Boolean),
]);
```

Supported types today: `Int32`, `Int64`, `Float64`, `Boolean`, `Utf8`.

### 3.2 Registering a memory table

```csharp
var table = new MemoryTable("sales", schema);
engine.Catalog.Register(table);
```

With persistence:

```csharp
var table = fileDb.CreateMemoryTable("sales", schema);
// already registered on fileDb.Catalog (same as engine.Catalog)
```

### 3.3 Strict vector sizing (optional)

For DuckDB-style batch sizes (64K–1M rows per append):

```csharp
var table = new MemoryTable(
    "sales",
    schema,
    options: new MemoryTableOptions(StrictVectorChunkRows: true));
```

Without strict mode, small batches are allowed (useful in unit tests).

---

## 4. Building columnar data

### 4.1 Fixed-width columns

Values are **little-endian** packed bytes.

```csharp
// Two Int32 values: 1 and 2
var qtyBytes = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 };
var qty = new FixedWidthColumnChunk(
    RainDbType.Int32,
    rowCount: 2,
    values: qtyBytes,
    nullBitmap: ReadOnlyMemory<byte>.Empty,
    hasNulls: false);

// Two Float64 values
var amtBytes = new byte[16];
BitConverter.GetBytes(10.0).CopyTo(amtBytes, 0);
BitConverter.GetBytes(20.0).CopyTo(amtBytes, 8);
var amt = new FixedWidthColumnChunk(
    RainDbType.Float64, 2, amtBytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
```

### 4.2 Null bitmap

One bit per row: `1` = NULL. Example: row 1 null in a 2-row Int32 column:

```csharp
var values = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };
var nullBitmap = new byte[] { 0b0000_0010 }; // bit 1 set
var col = new FixedWidthColumnChunk(RainDbType.Int32, 2, values, nullBitmap, hasNulls: true);
```

### 4.3 UTF-8 columns (Arrow style)

```csharp
var offsets = new[] { 0, 2, 4 }; // two strings in one blob
var blob = "usuk"u8.ToArray();
var region = new Utf8ColumnChunk(2, offsets, blob, ReadOnlyMemory<byte>.Empty, hasNulls: false);
```

### 4.4 Appending batches

```csharp
table.AppendBatch(new ColumnarBatch(rowCount: 2, new IColumnChunk[] { region, amt }));
```

`AppendBatch` validates column count, types, and row counts. Failed validation throws `ArgumentException`.

---

## 5. Running SQL

### 5.1 Execute a query

```csharp
await using var result = await engine.ExecuteSqlAsync(
    "SELECT region, amount FROM sales WHERE amount > 0.5");
```

RainDB accepts a **single statement** per call (strict subset). See README for full grammar.

### 5.2 Examples by result shape

**Row set** — cast to `IColumnarQueryResult`:

```csharp
await using var rows = await engine.ExecuteSqlAsync("SELECT qty FROM sales WHERE qty > 1");
if (rows is IColumnarQueryResult col)
{
    foreach (var batch in col.Batches)
    {
        var chunk = batch.Columns[0];
        // read bytes from chunk.Values.Span per row
    }
}
```

**Scalar aggregate** — cast to `IAggregateQueryResult`:

```csharp
await using var result = await engine.ExecuteSqlAsync("SELECT SUM(amount) FROM sales");
if (result is IAggregateQueryResult agg)
{
    if (agg.ValueIsNull)
        Console.WriteLine("NULL");
    else if (agg.ResultType == RainDbType.Float64)
        Console.WriteLine(agg.Float64Value);
    else
        Console.WriteLine(agg.Int64Value);
}
```

**GROUP BY** — again `IColumnarQueryResult` with one or more output batches:

```csharp
await using var result = await engine.ExecuteSqlAsync(
    "SELECT region, SUM(amount) FROM sales GROUP BY region");
```

### 5.3 NULL semantics (important)

| Construct | Behavior |
|-----------|----------|
| `WHERE col = literal` | Rows where `col` IS NULL do **not** match. |
| `SUM` / `MIN` / `MAX` (no rows or all null) | `ValueIsNull == true` on global aggregate. |
| `COUNT(*)` | Counts rows after `WHERE`; empty table → `0`, not NULL. |
| `COUNT(col)` | Counts non-null `col` among surviving rows. |

### 5.4 Compile without executing

Useful for tests, EXPLAIN-style tools, or custom execution:

```csharp
using RainDB.Sql;

var plan = StrictSqlSubset.CompilePhysicalPlan(
    "SELECT qty FROM sales WHERE qty > 1",
    engine.Catalog);

var logicalOnly = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM sales");
```

Compile failures throw `SqlCompileException` with a message (unsupported syntax, unknown table, type mismatch).

---

## 6. Physical plans (advanced)

Bypass SQL when benchmarking or prototyping operators:

```csharp
using RainDB.Query.Plans;

var plan = new VectorizedScanPhysicalPlan(
    table.Id,
    outputColumnIndices: [1],
    filters: [new ColumnCompareFilter(0, ScalarCompareOp.Gt, 1)],
    aggregate: null,
    options: new VectorizedScanExecutionOptions
    {
        MaxDegreeOfParallelism = Environment.ProcessorCount,
        UseChannelScheduler = false,
        UseAvx2DoubleSum = true,
    });

await using var result = await engine.ExecutePhysicalAsync(plan);
```

### 6.1 Parallelism options

| Option | Effect |
|--------|--------|
| `MaxDegreeOfParallelism = -1` | Use all logical processors. |
| `MaxDegreeOfParallelism = 1` | Single-threaded (deterministic debugging). |
| `UseChannelScheduler = true` | Channel-based morsel queue instead of `Parallel.For`. |

Output batch order matches **source table batch order**.

### 6.2 Join plan (direct)

Prefer SQL unless you need a specific `PhysicalJoinAlgorithm`:

```csharp
var joinPlan = new JoinPhysicalPlan(
    PhysicalJoinAlgorithm.Hash,
    probeTableId: left.Id,
    buildTableId: right.Id,
    probeKeyColumnIndices: [0],
    buildKeyColumnIndices: [0],
    outputSchema: /* TableSchema for wide output */,
    outputColumnOrder: null,
    probeSideFilters: null,
    buildSideFilters: null);

await using var joined = await engine.ExecutePhysicalAsync(joinPlan);
```

---

## 7. Reading query results

### 7.1 `IQueryResult`

- `RowCount` — total logical rows (sum of batch rows for columnar; `1` for scalar aggregate).
- `DisposeAsync()` — release pooled column memory when applicable.

### 7.2 `IColumnarQueryResult`

- `Batches` — `IReadOnlyList<IColumnarBatch>` in stable order.
- Each batch: `RowCount`, `Columns[i]` aligned with `SELECT` list order (not always table ordinal order).

### 7.3 Reading a fixed-width cell

```csharp
using System.Buffers.Binary;

static int ReadInt32(IColumnChunk col, int row)
{
    var offset = row * sizeof(int);
    return BinaryPrimitives.ReadInt32LittleEndian(col.Values.Span.Slice(offset, 4));
}

static double ReadFloat64(IColumnChunk col, int row)
{
    var offset = row * sizeof(double);
    return BitConverter.Int64BitsToDouble(
        BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(offset, 8)));
}
```

Check `chunk.HasNulls` and null bitmap before reading (see internals doc for bit layout).

### 7.4 Reading UTF-8

For `Utf8ColumnChunk`, use offset table; for `Utf8LengthPrefixedColumnChunk`, use `GetPayloadSpan(row)`.

---

## 8. Persistence workflows

### 8.1 Create and append on disk

```csharp
var engine = RainDbEngine.OpenPersistent("./mydb");
var table = engine.FileDatabase!.CreateMemoryTable("events", schema);
table.AppendBatch(batch); // writes tables/{id}/000000.batch
```

### 8.2 Reopen

```csharp
var engine2 = RainDbEngine.OpenPersistent("./mydb");
await using var r = await engine2.ExecuteSqlAsync("SELECT COUNT(*) FROM events");
```

### 8.3 Export / import snapshot

```csharp
using RainDB.Core.Persistence;

RainDbFileDatabase.ExportCatalog(engine.Catalog, "/backup/snapshot");
var imported = RainDbFileDatabase.ImportCatalog("/backup/snapshot");
var engineFromImport = RainDbEngine.CreateDefault(imported);
```

Import does **not** attach live append persistence unless you open a new `RainDbFileDatabase` and register tables manually.

### 8.4 Flush catalog metadata

```csharp
engine.FileDatabase?.FlushCatalog();
```

---

## 9. Customizing the engine

Inject collaborators when you outgrow defaults:

```csharp
var engine = new RainDbEngine(
    catalog: myCatalog,
    bufferPool: myPool,
    alignedBufferPool: myAlignedPool,
    executor: myExecutor,
    sqlCompiler: myCompiler,
    linqCompiler: myLinq,
    spillWriter: mySpill);
```

Implement:

- `IQueryExecutor` — run or wrap `IPhysicalPlan`.
- `ISqlCompiler` — parse/bind pipeline.
- `ISpillWriter` — future external sort/spill (metrics hook today).

Use `CreateSession(CancellationToken)` for manual execution:

```csharp
var ctx = engine.CreateSession(cancellationToken);
var plan = await engine.SqlCompiler.CompileAsync(sql, engine.Catalog, cancellationToken);
await using var result = await engine.Executor.ExecuteAsync(plan, ctx);
```

---

## 10. SQL quick reference

Supported (non-exhaustive; see README):

- `SELECT` / `FROM` / `WHERE` (AND conjuncts) / `GROUP BY` / `ORDER BY` / `LIMIT`
- `INNER JOIN` … `ON` equi-keys
- Aggregates: `SUM`, `MIN`, `MAX`, `COUNT`, `COUNT(*)`
- Comparisons: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Literals: integers, `1.0` floats, `TRUE`/`FALSE`, `'utf8 strings'`

**Not supported:** `HAVING`, subqueries, `DISTINCT`, outer joins, expressions in `SELECT`/`WHERE`, `ORDER BY` with `GROUP BY`, etc.

Example scripts: `samples/sql/*.sql` and `samples/RainDB.AnalyticsDemo`.

---

## 11. Common pitfalls

| Pitfall | Guidance |
|---------|----------|
| Forgetting `await using` on results | Pooled scan output may leak array pool slots. |
| Expecting full SQL | Use README strict subset; unsupported syntax throws at compile time. |
| LINQ provider | `DefaultLinqCompiler` is a stub; use SQL or physical plans. |
| Huge single batch + strict mode | Stay within 64K–1M rows per append when strict is on. |
| Crash-safe durability | Batch + catalog writes are not WAL-journaled yet; plan for export snapshots. |
| Join memory | Large joins materialize matches in memory; size workloads accordingly. |

---

## 12. LINQ (roadmap)

`ILinqCompiler` is wired on `RainDbEngine` but returns `ExplainOnlyPhysicalPlan` until the LINQ provider is implemented. Do not use for production queries.

---

## 13. Sample: end-to-end analytics

```csharp
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;

var engine = RainDbEngine.CreateDefault();

var schema = new TableSchema([
    new ColumnDef("region", RainDbType.Utf8),
    new ColumnDef("amount", RainDbType.Float64),
]);

var table = new MemoryTable("sales", schema);
var utf8 = new Utf8ColumnChunk(2, new[] { 0, 2, 4 }, "usuk"u8.ToArray(), ReadOnlyMemory<byte>.Empty, false);
var amt = new FixedWidthColumnChunk(
    RainDbType.Float64,
    2,
    BitConverter.GetBytes(1.0).Concat(BitConverter.GetBytes(2.0)).ToArray(),
    ReadOnlyMemory<byte>.Empty,
    false);
table.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { utf8, amt }));
engine.Catalog.Register(table);

await using (var rows = await engine.ExecuteSqlAsync(
    "SELECT region, amount FROM sales WHERE amount > 0.5"))
{
    if (rows is IColumnarQueryResult col)
        Console.WriteLine($"Rows: {col.RowCount}");
}

await using (var agg = await engine.ExecuteSqlAsync("SELECT SUM(amount) FROM sales"))
{
    if (agg is IAggregateQueryResult sum && !sum.ValueIsNull)
        Console.WriteLine($"Total: {sum.Float64Value}");
}
```

---

## 14. Where to go next

| Goal | Resource |
|------|----------|
| Algorithms & data structures | [RainDB-Internals.md](RainDB-Internals.md) |
| Roadmap | [Development-Roadmap.md](Development-Roadmap.md) |
| Behavioral contract | `tests/RainDB.Tests`, README SQL section |
| Demo app | `samples/RainDB.AnalyticsDemo/Program.cs` |
