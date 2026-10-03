using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Logical;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

public abstract class BoundFloat64RowExpression
{
    public abstract bool TryGetFloat64(IColumnarBatch batch, int row, out double value);
}

public sealed class BoundFloat64ColumnExpression : BoundFloat64RowExpression
{
    private readonly int _columnIndex;

    public BoundFloat64ColumnExpression(int columnIndex) => _columnIndex = columnIndex;

    public override bool TryGetFloat64(IColumnarBatch batch, int row, out double value)
    {
        var col = batch.Columns[_columnIndex];
        if (col.PhysicalType != RainDbType.Float64)
        {
            value = 0;
            return false;
        }

        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (RowNullBitmap.IsNull(nb, row, col.HasNulls))
        {
            value = 0;
            return false;
        }

        var bits = BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(row * sizeof(double), sizeof(double)));
        value = BitConverter.Int64BitsToDouble(bits);
        return true;
    }
}

public sealed class BoundFloat64LiteralExpression : BoundFloat64RowExpression
{
    private readonly double _value;

    public BoundFloat64LiteralExpression(double value) => _value = value;

    public override bool TryGetFloat64(IColumnarBatch batch, int row, out double value)
    {
        value = _value;
        return true;
    }
}

public sealed class BoundFloat64BinaryExpression : BoundFloat64RowExpression
{
    private readonly BoundFloat64RowExpression _left;
    private readonly BoundFloat64RowExpression _right;
    private readonly BinaryScalarOp _op;

    public BoundFloat64BinaryExpression(BinaryScalarOp op, BoundFloat64RowExpression left, BoundFloat64RowExpression right)
    {
        _op = op;
        _left = left;
        _right = right;
    }

    public override bool TryGetFloat64(IColumnarBatch batch, int row, out double value)
    {
        if (!_left.TryGetFloat64(batch, row, out var l) || !_right.TryGetFloat64(batch, row, out var r))
        {
            value = 0;
            return false;
        }

        value = _op switch
        {
            BinaryScalarOp.Add => l + r,
            BinaryScalarOp.Sub => l - r,
            BinaryScalarOp.Mul => l * r,
            BinaryScalarOp.Div => r == 0 ? double.NaN : l / r,
            _ => throw new InvalidOperationException(),
        };
        return !double.IsNaN(value);
    }
}

public sealed class BoundFloat64FromInt32Expression : BoundFloat64RowExpression
{
    private readonly BoundInt32RowExpression _inner;

    public BoundFloat64FromInt32Expression(BoundInt32RowExpression inner) => _inner = inner;

    public override bool TryGetFloat64(IColumnarBatch batch, int row, out double value)
    {
        if (!_inner.TryGetInt32(batch, row, out var v))
        {
            value = 0;
            return false;
        }

        value = v;
        return true;
    }
}

public sealed class BoundFloat64CaseExpression : BoundFloat64RowExpression
{
    private readonly (BoundScalarCompare Condition, BoundFloat64RowExpression Result)[] _whenClauses;
    private readonly BoundFloat64RowExpression _else;

    public BoundFloat64CaseExpression(
        IReadOnlyList<(BoundScalarCompare Condition, BoundFloat64RowExpression Result)> whenClauses,
        BoundFloat64RowExpression elseBranch)
    {
        _whenClauses = whenClauses.ToArray();
        _else = elseBranch;
    }

    public override bool TryGetFloat64(IColumnarBatch batch, int row, out double value)
    {
        foreach (var (cond, result) in _whenClauses)
        {
            if (!cond.RowMatches(batch, row))
                continue;
            return result.TryGetFloat64(batch, row, out value);
        }

        return _else.TryGetFloat64(batch, row, out value);
    }
}
