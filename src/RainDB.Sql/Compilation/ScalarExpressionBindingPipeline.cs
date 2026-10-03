using System.Globalization;
using RainDB.Logical;
using RainDB.Query.Vectorized;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Strategy pipeline: binds logical scalar trees to typed row evaluators.</summary>
public sealed class ScalarExpressionBindingPipeline
{
    private readonly Int32ScalarBinder _int32;
    private readonly Float64ScalarBinder _float64;

    public ScalarExpressionBindingPipeline()
    {
        _int32 = new Int32ScalarBinder(this);
        _float64 = new Float64ScalarBinder(this);
    }

    public BoundInt32RowExpression BindInt32(LogicalScalarExpression expr, TableSchema schema, string tableName) =>
        _int32.Bind(expr, schema, tableName);

    public BoundFloat64RowExpression BindFloat64(LogicalScalarExpression expr, TableSchema schema, string tableName) =>
        _float64.Bind(expr, schema, tableName);

    public RainDbType InferType(LogicalScalarExpression expr, TableSchema schema, string tableName) =>
        expr switch
        {
            LogicalLiteralScalar l => l.Literal.Kind switch
            {
                SqlLiteralKind.Integer => RainDbType.Int32,
                SqlLiteralKind.Float => RainDbType.Float64,
                SqlLiteralKind.Boolean => RainDbType.Boolean,
                _ => throw new SqlCompileException("Unsupported literal in expression."),
            },
            LogicalColumnScalarRef c => schema.Columns[LogicalTableScanBinder.ResolveColumn(schema, c.ColumnName, tableName)].Type,
            LogicalBinaryScalar b => InferType(b.Left, schema, tableName) switch
            {
                RainDbType.Float64 => RainDbType.Float64,
                RainDbType.Int32 => RainDbType.Int32,
                var t => throw new SqlCompileException($"Arithmetic on {t} is not supported."),
            },
            LogicalCastScalar cast => cast.TargetType,
            LogicalCaseScalar caseExpr => InferType(caseExpr.Else, schema, tableName),
            LogicalCompareScalar cmp => InferType(cmp.Left, schema, tableName),
            _ => throw new SqlCompileException("Unsupported scalar expression."),
        };

    public static void ValidateTableRefs(LogicalScalarExpression expr, string tableName)
    {
        switch (expr)
        {
            case LogicalColumnScalarRef col:
                if (col.QualifierTableName is { } q && !q.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    throw new SqlCompileException(
                        $"Expression references table '{q}' but the FROM clause scans '{tableName}' only.");
                break;
            case LogicalBinaryScalar bin:
                ValidateTableRefs(bin.Left, tableName);
                ValidateTableRefs(bin.Right, tableName);
                break;
            case LogicalCastScalar c:
                ValidateTableRefs(c.Operand, tableName);
                break;
            case LogicalCompareScalar cmp:
                ValidateTableRefs(cmp.Left, tableName);
                if (cmp.RightExpression is { } re)
                    ValidateTableRefs(re, tableName);
                break;
            case LogicalCaseScalar caseExpr:
                foreach (var w in caseExpr.WhenClauses)
                {
                    ValidateTableRefs(w.Condition.Left, tableName);
                    if (w.Condition.RightExpression is { } r)
                        ValidateTableRefs(r, tableName);
                    ValidateTableRefs(w.Result, tableName);
                }

                ValidateTableRefs(caseExpr.Else, tableName);
                break;
        }
    }

    private abstract class TypedScalarBinder<TBound>
    {
        protected abstract TBound BindColumn(LogicalColumnScalarRef col, TableSchema schema, string tableName);
        protected abstract TBound BindLiteral(LogicalLiteralScalar lit);
        protected abstract TBound BindBinary(BinaryScalarOp op, TBound left, TBound right);
        protected abstract TBound BindCast(LogicalCastScalar cast, TableSchema schema, string tableName);
        protected abstract TBound BindCase(LogicalCaseScalar caseExpr, TableSchema schema, string tableName);

        public TBound Bind(LogicalScalarExpression expr, TableSchema schema, string tableName) =>
            expr switch
            {
                LogicalColumnScalarRef col => BindColumn(col, schema, tableName),
                LogicalLiteralScalar lit => BindLiteral(lit),
                LogicalBinaryScalar bin => BindBinary(bin.Operator, Bind(bin.Left, schema, tableName), Bind(bin.Right, schema, tableName)),
                LogicalCastScalar cast => BindCast(cast, schema, tableName),
                LogicalCaseScalar caseExpr => BindCase(caseExpr, schema, tableName),
                _ => throw new SqlCompileException("Unsupported scalar expression for this type."),
            };
    }

    private sealed class Int32ScalarBinder(ScalarExpressionBindingPipeline owner) : TypedScalarBinder<BoundInt32RowExpression>
    {
        protected override BoundInt32RowExpression BindColumn(LogicalColumnScalarRef col, TableSchema schema, string tableName)
        {
            ValidateTableRefs(col, tableName);
            var ix = LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, tableName);
            if (schema.Columns[ix].Type != RainDbType.Int32)
                throw new SqlCompileException($"Int32 expression requires Int32 columns; '{col.ColumnName}' is {schema.Columns[ix].Type}.");
            return new BoundInt32ColumnExpression(ix);
        }

        protected override BoundInt32RowExpression BindLiteral(LogicalLiteralScalar lit)
        {
            if (lit.Literal.Kind != SqlLiteralKind.Integer)
                throw new SqlCompileException("Int32 expressions require integer literals.");
            if (!int.TryParse(lit.Literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
                throw new SqlCompileException($"Integer literal '{lit.Literal.Text}' is out of range for Int32.");
            return new BoundInt32LiteralExpression(v);
        }

        protected override BoundInt32RowExpression BindBinary(BinaryScalarOp op, BoundInt32RowExpression left, BoundInt32RowExpression right) =>
            new BoundInt32BinaryExpression(op, left, right);

        protected override BoundInt32RowExpression BindCast(LogicalCastScalar cast, TableSchema schema, string tableName)
        {
            if (cast.TargetType != RainDbType.Int32)
                throw new SqlCompileException($"CAST to {cast.TargetType} is not supported in Int32 context.");
            return Bind(cast.Operand, schema, tableName);
        }

        protected override BoundInt32RowExpression BindCase(LogicalCaseScalar caseExpr, TableSchema schema, string tableName)
        {
            var whens = new List<(BoundScalarCompare, BoundInt32RowExpression)>(caseExpr.WhenClauses.Count);
            foreach (var w in caseExpr.WhenClauses)
            {
                var cond = BoundScalarCompare.Bind(
                    w.Condition,
                    schema,
                    tableName,
                    e => owner.InferType(e, schema, tableName),
                    e => owner.BindInt32(e, schema, tableName),
                    _ => throw new SqlCompileException("Float64 compare branches are not supported in Int32 CASE."));
                whens.Add((cond, Bind(w.Result, schema, tableName)));
            }

            return new BoundInt32CaseExpression(whens, Bind(caseExpr.Else, schema, tableName));
        }
    }

    private sealed class Float64ScalarBinder(ScalarExpressionBindingPipeline owner) : TypedScalarBinder<BoundFloat64RowExpression>
    {
        protected override BoundFloat64RowExpression BindColumn(LogicalColumnScalarRef col, TableSchema schema, string tableName)
        {
            ValidateTableRefs(col, tableName);
            var ix = LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, tableName);
            if (schema.Columns[ix].Type != RainDbType.Float64)
                throw new SqlCompileException($"Float64 expression requires Float64 columns; '{col.ColumnName}' is {schema.Columns[ix].Type}.");
            return new BoundFloat64ColumnExpression(ix);
        }

        protected override BoundFloat64RowExpression BindLiteral(LogicalLiteralScalar lit)
        {
            if (lit.Literal.Kind == SqlLiteralKind.Integer
                && int.TryParse(lit.Literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv))
                return new BoundFloat64LiteralExpression(iv);
            if (lit.Literal.Kind == SqlLiteralKind.Float
                && double.TryParse(lit.Literal.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
                return new BoundFloat64LiteralExpression(dv);
            throw new SqlCompileException("Float64 expressions require numeric literals.");
        }

        protected override BoundFloat64RowExpression BindBinary(BinaryScalarOp op, BoundFloat64RowExpression left, BoundFloat64RowExpression right) =>
            new BoundFloat64BinaryExpression(op, left, right);

        protected override BoundFloat64RowExpression BindCast(LogicalCastScalar cast, TableSchema schema, string tableName)
        {
            if (cast.TargetType != RainDbType.Float64)
                throw new SqlCompileException($"CAST to {cast.TargetType} is not supported in Float64 context.");
            var operandType = new ScalarExpressionBindingPipeline().InferType(cast.Operand, schema, tableName);
            if (operandType == RainDbType.Int32)
            {
                var inner = owner.BindInt32(cast.Operand, schema, tableName);
                return new BoundFloat64FromInt32Expression(inner);
            }

            return Bind(cast.Operand, schema, tableName);
        }

        protected override BoundFloat64RowExpression BindCase(LogicalCaseScalar caseExpr, TableSchema schema, string tableName)
        {
            var whens = new List<(BoundScalarCompare, BoundFloat64RowExpression)>(caseExpr.WhenClauses.Count);
            foreach (var w in caseExpr.WhenClauses)
            {
                var cond = BoundScalarCompare.Bind(
                    w.Condition,
                    schema,
                    tableName,
                    e => owner.InferType(e, schema, tableName),
                    e => throw new SqlCompileException("Int32 compare branches are not supported in Float64 CASE."),
                    e => owner.BindFloat64(e, schema, tableName));
                whens.Add((cond, Bind(w.Result, schema, tableName)));
            }

            return new BoundFloat64CaseExpression(whens, Bind(caseExpr.Else, schema, tableName));
        }
    }
}
