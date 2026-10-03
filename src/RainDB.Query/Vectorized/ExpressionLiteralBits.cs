using System.Globalization;
using RainDB.Logical;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

internal static class ExpressionLiteralBits
{
    internal static long Coerce(RainDbType type, SqlLiteral literal) =>
        type switch
        {
            RainDbType.Int32 => CoerceInt32(literal),
            RainDbType.Float64 => CoerceFloat64(literal),
            _ => throw new InvalidOperationException($"Literal coercion for {type} is not supported."),
        };

    private static long CoerceInt32(SqlLiteral literal)
    {
        if (literal.Kind != SqlLiteralKind.Integer
            || !int.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            throw new InvalidOperationException("Expected integer literal.");
        return v;
    }

    private static long CoerceFloat64(SqlLiteral literal)
    {
        if (literal.Kind == SqlLiteralKind.Integer
            && int.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv))
            return BitConverter.DoubleToInt64Bits(iv);
        if (literal.Kind == SqlLiteralKind.Float
            && double.TryParse(literal.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
            return BitConverter.DoubleToInt64Bits(dv);
        throw new InvalidOperationException("Expected numeric literal.");
    }
}
