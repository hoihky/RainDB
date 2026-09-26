using System.Buffers.Binary;
using System.Text;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Schema;

namespace RainDB.Tests;

/// <summary>Shared column/table builders for tests (mirrors samples/RainDB.AnalyticsDemo data shapes).</summary>
internal static class TestDataBuilders
{
    public static RainDbEngine CreateEngine() => RainDbEngine.CreateDefault();

    public static ColumnarBatch SingleInt32Batch(int value)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(b, value);
        return new ColumnarBatch(1, [new FixedWidthColumnChunk(RainDbType.Int32, 1, b, ReadOnlyMemory<byte>.Empty, false)]);
    }

    public static MemoryTable EmptyTable(string name, RainDbType type, string colName) =>
        new(name, new TableSchema([new ColumnDef(colName, type)]));

    public static MemoryTable TableWithInt32Column(string name, string colName, (int value, bool isNull)[] rows)
    {
        var t = new MemoryTable(name, new TableSchema([new ColumnDef(colName, RainDbType.Int32)]));
        var vals = new byte[rows.Length * 4];
        var anyNull = rows.Any(r => r.isNull);
        byte[]? nb = anyNull ? new byte[(rows.Length + 7) >> 3] : null;
        for (var i = 0; i < rows.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(vals.AsSpan(i * 4, 4), rows[i].value);
            if (rows[i].isNull && nb is not null)
                nb[i >> 3] |= (byte)(1 << (i & 7));
        }

        t.AppendBatch(new ColumnarBatch(rows.Length, [
            new FixedWidthColumnChunk(RainDbType.Int32, rows.Length, vals, nb ?? ReadOnlyMemory<byte>.Empty, anyNull),
        ]));
        return t;
    }

    public static void WriteF64(byte[] buf, int offset, double v) =>
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(offset, 8), BitConverter.DoubleToInt64Bits(v));

    public static double ReadF64(IColumnChunk col, int row) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(row * 8, 8)));

    public static int ReadInt32(IColumnChunk col, int row) =>
        BinaryPrimitives.ReadInt32LittleEndian(col.Values.Span.Slice(row * 4, 4));

    public static Utf8ColumnChunk Utf8Column(string[] rows)
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

    public static FixedWidthColumnChunk Int32Column(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
    }

    public static FixedWidthColumnChunk Float64Column(double[] values)
    {
        var bytes = new byte[values.Length * sizeof(double)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteDoubleLittleEndian(bytes.AsSpan(i * sizeof(double)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Float64, values.Length, bytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
    }

    /// <summary>Registers <c>order_lines</c> and <c>rebate_tiers</c> like RainDB.AnalyticsDemo.</summary>
    public static void RegisterAnalyticsDemoTables(RainDbEngine engine)
    {
        var schema = new TableSchema([
            new ColumnDef("region", RainDbType.Utf8),
            new ColumnDef("quantity", RainDbType.Int32),
            new ColumnDef("line_total", RainDbType.Float64),
        ]);
        var table = new MemoryTable("order_lines", schema);
        table.AppendBatch(new ColumnarBatch(4, [
            Utf8Column(["US-East", "US-West", "EU", "US-East"]),
            Int32Column([12, 4, 20, 2]),
            Float64Column([1200d, 199.5d, 4500d, 49.99d]),
        ]));
        table.AppendBatch(new ColumnarBatch(3, [
            Utf8Column(["US-West", "EU", "US-East"]),
            Int32Column([1, 50, 6]),
            Float64Column([9.99d, 12000d, 300d]),
        ]));
        engine.Catalog.Register(table);

        var tiers = new MemoryTable("rebate_tiers", new TableSchema([
            new ColumnDef("min_qty", RainDbType.Int32),
            new ColumnDef("rebate_pct", RainDbType.Float64),
        ]));
        tiers.AppendBatch(new ColumnarBatch(5, [
            Int32Column([1, 4, 6, 12, 20]),
            Float64Column([0.01, 0.02, 0.03, 0.04, 0.05]),
        ]));
        engine.Catalog.Register(tiers);
    }

    public static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // best-effort temp cleanup
        }
    }
}
