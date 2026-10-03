using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

public sealed class BoundScalarCompare
{
    private readonly BoundInt32RowExpression? _leftInt32;
    private readonly BoundFloat64RowExpression? _leftFloat64;
    private readonly BoundInt32RowExpression? _rightInt32;
    private readonly BoundFloat64RowExpression? _rightFloat64;
    private readonly long _rightImmediateBits;
    private readonly ScalarCompareOp _op;
    private readonly RainDbType _numericType;

    private BoundScalarCompare(
        ScalarCompareOp op,
        RainDbType numericType,
        BoundInt32RowExpression? leftInt32,
        BoundFloat64RowExpression? leftFloat64,
        BoundInt32RowExpression? rightInt32,
        BoundFloat64RowExpression? rightFloat64,
        long rightImmediateBits)
    {
        _op = op;
        _numericType = numericType;
        _leftInt32 = leftInt32;
        _leftFloat64 = leftFloat64;
        _rightInt32 = rightInt32;
        _rightFloat64 = rightFloat64;
        _rightImmediateBits = rightImmediateBits;
    }

    public static BoundScalarCompare Bind(
        LogicalCompareScalar cmp,
        RainDB.Schema.TableSchema schema,
        string tableName,
        Func<LogicalScalarExpression, RainDB.Schema.RainDbType> inferType,
        Func<LogicalScalarExpression, BoundInt32RowExpression> bindInt32,
        Func<LogicalScalarExpression, BoundFloat64RowExpression> bindFloat64)
    {
        var leftType = inferType(cmp.Left);
        if (leftType == RainDbType.Int32)
        {
            var left = bindInt32(cmp.Left);
            if (cmp.RightExpression is { } re)
            {
                var right = bindInt32(re);
                return new BoundScalarCompare(cmp.Operator, RainDbType.Int32, left, null, right, null, 0);
            }

            if (cmp.RightLiteral is not { } lit)
                throw new InvalidOperationException("Compare expression requires a right-hand literal or expression.");
            var imm = ExpressionLiteralBits.Coerce(RainDbType.Int32, lit);
            return new BoundScalarCompare(cmp.Operator, RainDbType.Int32, left, null, null, null, imm);
        }

        if (leftType == RainDbType.Float64)
        {
            var left = bindFloat64(cmp.Left);
            if (cmp.RightExpression is { } re)
            {
                var right = bindFloat64(re);
                return new BoundScalarCompare(cmp.Operator, RainDbType.Float64, null, left, null, right, 0);
            }

            if (cmp.RightLiteral is not { } lit)
                throw new InvalidOperationException("Compare expression requires a right-hand literal or expression.");
            var imm = ExpressionLiteralBits.Coerce(RainDbType.Float64, lit);
            return new BoundScalarCompare(cmp.Operator, RainDbType.Float64, null, left, null, null, imm);
        }

        throw new InvalidOperationException($"Comparison is not supported for expression type {leftType}.");
    }

    public bool RowMatches(IColumnarBatch batch, int row)
    {
        if (_numericType == RainDbType.Int32)
        {
            if (_leftInt32 is null || !_leftInt32.TryGetInt32(batch, row, out var l))
                return false;
            if (_rightInt32 is not null)
            {
                if (!_rightInt32.TryGetInt32(batch, row, out var r))
                    return false;
                return CompareInt32(l, r, _op);
            }

            return CompareInt32(l, (int)_rightImmediateBits, _op);
        }

        if (_leftFloat64 is null || !_leftFloat64.TryGetFloat64(batch, row, out var lf))
            return false;
        if (_rightFloat64 is not null)
        {
            if (!_rightFloat64.TryGetFloat64(batch, row, out var rf))
                return false;
            return CompareDouble(lf, rf, _op);
        }

        var imm = BitConverter.Int64BitsToDouble(_rightImmediateBits);
        return CompareDouble(lf, imm, _op);
    }

    private static bool CompareInt32(int v, int imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => v < imm,
            ScalarCompareOp.Le => v <= imm,
            ScalarCompareOp.Gt => v > imm,
            ScalarCompareOp.Ge => v >= imm,
            _ => false,
        };

    private static bool CompareDouble(double v, double imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => v < imm,
            ScalarCompareOp.Le => v <= imm,
            ScalarCompareOp.Gt => v > imm,
            ScalarCompareOp.Ge => v >= imm,
            _ => false,
        };
}
