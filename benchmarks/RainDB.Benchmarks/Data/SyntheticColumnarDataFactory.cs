using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Schema;

namespace RainDB.Benchmarks.Data;

/// <summary>Builds in-memory columnar tables with stable batch sizing for benchmarks.</summary>
public static class SyntheticColumnarDataFactory
{
    public const int DefaultBatchRows = 65_536;

    public static MemoryTable CreateSalesFact(string name, int rowCount, int batchRows = DefaultBatchRows)
    {
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        if (batchRows <= 0)
            throw new ArgumentOutOfRangeException(nameof(batchRows));

        var schema = new TableSchema([
            new ColumnDef("category_id", RainDbType.Int32),
            new ColumnDef("region_id", RainDbType.Int32),
            new ColumnDef("amount", RainDbType.Float64),
        ]);
        var table = new MemoryTable(name, schema);
        var rng = new DeterministicRng((ulong)rowCount);

        var remaining = rowCount;
        while (remaining > 0)
        {
            var n = Math.Min(batchRows, remaining);
            table.AppendBatch(BuildSalesBatch(n, rng));
            remaining -= n;
        }

        return table;
    }

    public static MemoryTable CreateRegionDimension(string name, int distinctRegions)
    {
        if (distinctRegions <= 0)
            throw new ArgumentOutOfRangeException(nameof(distinctRegions));

        var schema = new TableSchema([new ColumnDef("region_id", RainDbType.Int32)]);
        var ids = new int[distinctRegions];
        for (var i = 0; i < distinctRegions; i++)
            ids[i] = i + 1;
        var table = new MemoryTable(name, schema);
        table.AppendBatch(new ColumnarBatch(distinctRegions, [Int32Column(ids)]));
        return table;
    }

    private static ColumnarBatch BuildSalesBatch(int rowCount, DeterministicRng rng)
    {
        var cat = new byte[rowCount * sizeof(int)];
        var region = new byte[rowCount * sizeof(int)];
        var amount = new byte[rowCount * sizeof(double)];
        for (var i = 0; i < rowCount; i++)
        {
            var off = i * sizeof(int);
            BinaryPrimitives.WriteInt32LittleEndian(cat.AsSpan(off, sizeof(int)), rng.NextInt32(0, 256));
            BinaryPrimitives.WriteInt32LittleEndian(region.AsSpan(off, sizeof(int)), rng.NextInt32(1, 4096));
            BinaryPrimitives.WriteDoubleLittleEndian(amount.AsSpan(i * sizeof(double), sizeof(double)), rng.NextUnitDouble() * 1000.0);
        }

        return new ColumnarBatch(rowCount, [
            new FixedWidthColumnChunk(RainDbType.Int32, rowCount, cat, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, rowCount, region, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Float64, rowCount, amount, ReadOnlyMemory<byte>.Empty, false),
        ]);
    }

    private static FixedWidthColumnChunk Int32Column(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int), sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
    }
}
