using RainDB.Logical;
using RainDB.Query.Vectorized;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Lowers <see cref="LogicalScalarExpression"/> trees to row evaluators (Int32 subset).</summary>
public sealed class LogicalScalarExpressionBinder
{
    public BoundInt32RowExpression BindInt32(LogicalScalarExpression expr, TableSchema schema, string tableName)
    {
        ArgumentNullException.ThrowIfNull(expr);
        return expr switch
        {
            LogicalColumnScalarRef col => BindColumn(col, schema, tableName),
            LogicalLiteralScalar lit => BindLiteral(lit),
            LogicalBinaryScalar bin => new BoundInt32BinaryExpression(
                bin.Operator,
                BindInt32(bin.Left, schema, tableName),
                BindInt32(bin.Right, schema, tableName)),
            LogicalCastScalar cast => BindCast(cast, schema, tableName),
            _ => throw new SqlCompileException("Unsupported scalar expression."),
        };
    }

    private static BoundInt32ColumnExpression BindColumn(LogicalColumnScalarRef col, TableSchema schema, string tableName)
    {
        if (col.QualifierTableName is { } q && !q.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            throw new SqlCompileException(
                $"Expression references table '{q}' but the FROM clause scans '{tableName}' only.");
        var ix = LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, tableName);
        if (schema.Columns[ix].Type != RainDbType.Int32)
            throw new SqlCompileException(
                $"Int32 expression requires Int32 columns; '{col.ColumnName}' is {schema.Columns[ix].Type}.");
        return new BoundInt32ColumnExpression(ix);
    }

    private static BoundInt32LiteralExpression BindLiteral(LogicalLiteralScalar lit)
    {
        if (lit.Literal.Kind != SqlLiteralKind.Integer)
            throw new SqlCompileException("Int32 expressions require integer literals.");
        if (!int.TryParse(lit.Literal.Text, out var v))
            throw new SqlCompileException($"Integer literal '{lit.Literal.Text}' is out of range for Int32.");
        return new BoundInt32LiteralExpression(v);
    }

    private BoundInt32RowExpression BindCast(LogicalCastScalar cast, TableSchema schema, string tableName)
    {
        if (cast.TargetType != RainDbType.Int32)
            throw new SqlCompileException($"CAST to {cast.TargetType} is not supported yet.");
        return BindInt32(cast.Operand, schema, tableName);
    }

    public static void ValidateExpressionTableRefs(LogicalScalarExpression expr, string tableName)
    {
        switch (expr)
        {
            case LogicalColumnScalarRef col:
                if (col.QualifierTableName is { } q && !q.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    throw new SqlCompileException(
                        $"Expression references table '{q}' but the FROM clause scans '{tableName}' only.");
                break;
            case LogicalBinaryScalar bin:
                ValidateExpressionTableRefs(bin.Left, tableName);
                ValidateExpressionTableRefs(bin.Right, tableName);
                break;
            case LogicalCastScalar c:
                ValidateExpressionTableRefs(c.Operand, tableName);
                break;
        }
    }
}
