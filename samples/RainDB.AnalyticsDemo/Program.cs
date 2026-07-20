using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.IO;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

// Demo: order-line analytics — register columnar batches, run vectorized plans (aggregate + filter/project), then run SQL files.
await RunAnalyticsDemoAsync();

static async Task RunAnalyticsDemoAsync()
{
    Console.WriteLine("RainDB analytics demo — embedded columnar engine");
    Console.WriteLine(new string('=', 60));

    var engine = RainDbEngine.CreateDefault();
    var schema = new TableSchema([
        new ColumnDef("region", RainDbType.Utf8),
        new ColumnDef("quantity", RainDbType.Int32),
        new ColumnDef("line_total", RainDbType.Float64),
    ]);

    var table = new MemoryTable("order_lines", schema);

    // Batch 1: four rows (OLAP-style chunk; small for clarity)
    table.AppendBatch(
        new ColumnarBatch(
            4,
            [
                Utf8Column(["US-East", "US-West", "EU", "US-East"]),
                Int32Column([12, 4, 20, 2]),
                Float64Column([1200d, 199.5d, 4500d, 49.99d]),
            ]));

    // Batch 2: three more rows (second morsel for parallel merge demo)
    table.AppendBatch(
        new ColumnarBatch(
            3,
            [
                Utf8Column(["US-West", "EU", "US-East"]),
                Int32Column([1, 50, 6]),
                Float64Column([9.99d, 12000d, 300d]),
            ]));

    engine.Catalog.Register(table);

    // Lookup used by sample join SQL (fixed-width join key on quantity).
    var tierSchema = new TableSchema([
        new ColumnDef("min_qty", RainDbType.Int32),
        new ColumnDef("rebate_pct", RainDbType.Float64),
    ]);
    var tiers = new MemoryTable("rebate_tiers", tierSchema);
    tiers.AppendBatch(
        new ColumnarBatch(
            5,
            [
                Int32Column([1, 4, 6, 12, 20]),
                Float64Column([0.01, 0.02, 0.03, 0.04, 0.05]),
            ]));
    engine.Catalog.Register(tiers);

    Console.WriteLine($"Registered table '{table.Name}' ({table.RowCount} rows in {table.Batches.Count} batches).");
    Console.WriteLine($"Registered table '{tiers.Name}' ({tiers.RowCount} rows) for JOIN samples.");
    Console.WriteLine();

    // --- 1) Total revenue: SUM(line_total) ---
    var sumPlan = new VectorizedScanPhysicalPlan(
        table.Id,
        outputColumnIndices: [2],
        aggregate: new AggregateSpec(2, AggregateKind.Sum),
        options: new VectorizedScanExecutionOptions
        {
            MaxDegreeOfParallelism = -1,
            UseAvx2DoubleSum = true,
        });

    await using (var sumResult = await engine.ExecutePhysicalAsync(sumPlan))
    {
        var agg = (IAggregateQueryResult)sumResult;
        Console.WriteLine("1) Total revenue (SUM line_total)");
        Console.WriteLine($"   {agg.Float64Value.ToString("N2", CultureInfo.InvariantCulture)}  ({agg.ContributingRowCount} contributing rows)");
    }

    Console.WriteLine();

    // --- 2) Largest line: MAX(line_total) ---
    var maxPlan = new VectorizedScanPhysicalPlan(
        table.Id,
        outputColumnIndices: [2],
        aggregate: new AggregateSpec(2, AggregateKind.Max),
        options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = -1 });

    await using (var maxResult = await engine.ExecutePhysicalAsync(maxPlan))
    {
        var agg = (IAggregateQueryResult)maxResult;
        Console.WriteLine("2) Largest single line (MAX line_total)");
        Console.WriteLine($"   {agg.Float64Value.ToString("N2", CultureInfo.InvariantCulture)}");
    }

    Console.WriteLine();

    // --- 3) Filter + project: quantity >= 6 → show region + line_total ---
    var filterPlan = new VectorizedScanPhysicalPlan(
        table.Id,
        outputColumnIndices: [0, 2],
        filters: [new ColumnCompareFilter(1, ScalarCompareOp.Ge, 6)],
        aggregate: null,
        options: new VectorizedScanExecutionOptions
        {
            MaxDegreeOfParallelism = -1,
            UseChannelScheduler = true,
        });

    await using (var rows = await engine.ExecutePhysicalAsync(filterPlan))
    {
        var col = (IColumnarQueryResult)rows;
        Console.WriteLine("3) High-volume lines (quantity >= 6) — region, line_total");
        PrintProjectedBatches(col);
    }

    Console.WriteLine();

    // --- 4) Optional: persist one column and memory-map it (zero-copy read) ---
    string? tmpPath = null;
    try
    {
        tmpPath = Path.Combine(Path.GetTempPath(), $"raindb_demo_{Guid.NewGuid():N}.col");
        var amountChunk = (FixedWidthColumnChunk)table.Batches[0].Columns[2];
        ColumnarFixedWidthFileFormat.WriteFile(tmpPath, amountChunk);
        using var mmap = ColumnarFixedWidthMmapReader.Open(tmpPath);
        Console.WriteLine("4) Zero-copy mmap of first batch's line_total column (first 4 doubles)");
        var mapped = mmap.Chunk;
        var span = mapped.Values.Span;
        for (var i = 0; i < mapped.RowCount; i++)
        {
            var d = BinaryPrimitives.ReadDoubleLittleEndian(span.Slice(i * sizeof(double), sizeof(double)));
            Console.WriteLine($"   row {i}: {d.ToString("N2", CultureInfo.InvariantCulture)}");
        }
    }
    finally
    {
        try
        {
            if (tmpPath is not null)
                File.Delete(tmpPath);
        }
        catch
        {
            // ignore
        }
    }

    Console.WriteLine();
    Console.WriteLine("--- SQL samples (files under sql/, from ../../samples/sql/) ---");
    await RunSqlFileSamplesAsync(engine);

    Console.WriteLine();
    Console.WriteLine("Done. Physical API: ExecutePhysicalAsync; SQL: ExecuteSqlAsync / StrictSqlSubset.");
}

static async Task RunSqlFileSamplesAsync(RainDbEngine engine)
{
    var sqlDir = Path.Combine(AppContext.BaseDirectory, "sql");
    if (!Directory.Exists(sqlDir))
    {
        Console.WriteLine("(No sql/ directory next to the executable — build copies samples from ../../samples/sql/.)");
        return;
    }

    foreach (var path in Directory.GetFiles(sqlDir, "*.sql").OrderBy(Path.GetFileName, StringComparer.Ordinal))
    {
        var sql = await File.ReadAllTextAsync(path).ConfigureAwait(false);
        var name = Path.GetFileName(path);
        Console.WriteLine();
        Console.WriteLine($">> {name}");
        try
        {
            var logical = StrictSqlSubset.ParseLogicalPlan(sql);
            Console.WriteLine($"   logical: {logical.Explain()}");
        }
        catch (SqlCompileException ex)
        {
            Console.WriteLine($"   parse error: {ex.Message}");
            continue;
        }

        await using var result = await engine.ExecuteSqlAsync(sql).ConfigureAwait(false);
        PrintQuerySummary(result, name);
    }
}

static void PrintQuerySummary(IQueryResult result, string label)
{
    switch (result)
    {
        case IAggregateQueryResult agg:
        {
            var value = agg.ResultType == RainDbType.Float64
                ? agg.Float64Value.ToString("N2", CultureInfo.InvariantCulture)
                : agg.Int64Value.ToString(CultureInfo.InvariantCulture);
            Console.WriteLine($"   aggregate ({label}): {value}  (contributing rows: {agg.ContributingRowCount})");
            break;
        }
        case IColumnarQueryResult col:
        {
            long rows = 0;
            foreach (var b in col.Batches)
                rows += b.RowCount;
            Console.WriteLine($"   columnar ({label}): {col.Batches.Count} batch(es), {rows} total row(s).");
            break;
        }
        default:
            Console.WriteLine($"   ({label}): {result.GetType().Name}, RowCount={result.RowCount}");
            break;
    }
}

static Utf8ColumnChunk Utf8Column(string[] rows)
{
    var offsets = new int[rows.Length + 1];
    var blob = new List<byte>(rows.Length * 8);
    for (var i = 0; i < rows.Length; i++)
    {
        offsets[i] = blob.Count;
        blob.AddRange(Encoding.UTF8.GetBytes(rows[i]));
    }

    offsets[^1] = blob.Count;
    return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, hasNulls: false);
}

static FixedWidthColumnChunk Int32Column(int[] values)
{
    var bytes = new byte[values.Length * sizeof(int)];
    for (var i = 0; i < values.Length; i++)
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
    return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
}

static FixedWidthColumnChunk Float64Column(double[] values)
{
    var bytes = new byte[values.Length * sizeof(double)];
    for (var i = 0; i < values.Length; i++)
        BinaryPrimitives.WriteDoubleLittleEndian(bytes.AsSpan(i * sizeof(double)), values[i]);
    return new FixedWidthColumnChunk(RainDbType.Float64, values.Length, bytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
}

static void PrintProjectedBatches(IColumnarQueryResult result)
{
    var batchIndex = 0;
    foreach (var batch in result.Batches)
    {
        if (batch.RowCount == 0)
        {
            Console.WriteLine($"   (batch {batchIndex}: 0 rows)");
            batchIndex++;
            continue;
        }

        var region = (Utf8ColumnChunk)batch.Columns[0];
        var total = (FixedWidthColumnChunk)batch.Columns[1];
        Console.WriteLine($"   batch {batchIndex}:");
        for (var r = 0; r < batch.RowCount; r++)
        {
            var start = region.Offsets.Span[r];
            var end = region.Offsets.Span[r + 1];
            var name = Encoding.UTF8.GetString(region.Values.Span.Slice(start, end - start));
            var amt = BinaryPrimitives.ReadDoubleLittleEndian(total.Values.Span.Slice(r * sizeof(double), sizeof(double)));
            Console.WriteLine($"      {name,-10} {amt,12:N2}");
        }

        batchIndex++;
    }
}
