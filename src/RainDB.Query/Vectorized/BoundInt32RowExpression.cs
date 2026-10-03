using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Logical;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

/// <summary>Evaluates to a nullable Int32 per row (used for WHERE and SELECT expressions).</summary>
public abstract class BoundInt32RowExpression
{
    public abstract bool TryGetInt32(IColumnarBatch batch, int row, out int value);
}

public sealed class BoundInt32ColumnExpression : BoundInt32RowExpression
{
    private readonly int _columnIndex;

    public BoundInt32ColumnExpression(int columnIndex) => _columnIndex = columnIndex;

    public override bool TryGetInt32(IColumnarBatch batch, int row, out int value)
    {
        var col = batch.Columns[_columnIndex];
        if (col.PhysicalType != RainDbType.Int32)
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

        value = BinaryPrimitives.ReadInt32LittleEndian(col.Values.Span.Slice(row * sizeof(int), sizeof(int)));
        return true;
    }
}

public sealed class BoundInt32LiteralExpression : BoundInt32RowExpression
{
    private readonly int _value;

    public BoundInt32LiteralExpression(int value) => _value = value;

    public override bool TryGetInt32(IColumnarBatch batch, int row, out int value)
    {
        value = _value;
        return true;
    }
}

public sealed class BoundInt32BinaryExpression : BoundInt32RowExpression
{
    private readonly BoundInt32RowExpression _left;
    private readonly BoundInt32RowExpression _right;
    private readonly BinaryScalarOp _op;

    public BoundInt32BinaryExpression(BinaryScalarOp op, BoundInt32RowExpression left, BoundInt32RowExpression right)
    {
        _op = op;
        _left = left;
        _right = right;
    }

    public override bool TryGetInt32(IColumnarBatch batch, int row, out int value)
    {
        if (!_left.TryGetInt32(batch, row, out var l) || !_right.TryGetInt32(batch, row, out var r))
        {
            value = 0;
            return false;
        }

        try
        {
            value = _op switch
            {
                BinaryScalarOp.Add => l + r,
                BinaryScalarOp.Sub => l - r,
                BinaryScalarOp.Mul => l * r,
                BinaryScalarOp.Div => r == 0 ? throw new DivideByZeroException() : l / r,
                _ => throw new InvalidOperationException(),
            };
            return true;
        }
        catch (DivideByZeroException)
        {
            value = 0;
            return false;
        }
    }
}

public sealed class BoundInt32CaseExpression : BoundInt32RowExpression
{
    private readonly (BoundScalarCompare Condition, BoundInt32RowExpression Result)[] _whenClauses;
    private readonly BoundInt32RowExpression _else;

    public BoundInt32CaseExpression(
        IReadOnlyList<(BoundScalarCompare Condition, BoundInt32RowExpression Result)> whenClauses,
        BoundInt32RowExpression elseBranch)
    {
        _whenClauses = whenClauses.ToArray();
        _else = elseBranch;
    }

    public override bool TryGetInt32(IColumnarBatch batch, int row, out int value)
    {
        foreach (var (cond, result) in _whenClauses)
        {
            if (!cond.RowMatches(batch, row))
                continue;
            return result.TryGetInt32(batch, row, out value);
        }

        return _else.TryGetInt32(batch, row, out value);
    }
}
