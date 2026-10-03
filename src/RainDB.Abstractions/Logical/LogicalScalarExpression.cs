using RainDB.Schema;

namespace RainDB.Logical;

public enum BinaryScalarOp
{
    Add,
    Sub,
    Mul,
    Div,
}

public abstract class LogicalScalarExpression
{
}

public sealed class LogicalColumnScalarRef : LogicalScalarExpression
{
    public string? QualifierTableName { get; init; }

    public required string ColumnName { get; init; }
}

public sealed class LogicalLiteralScalar : LogicalScalarExpression
{
    public required SqlLiteral Literal { get; init; }
}

public sealed class LogicalBinaryScalar : LogicalScalarExpression
{
    public required BinaryScalarOp Operator { get; init; }

    public required LogicalScalarExpression Left { get; init; }

    public required LogicalScalarExpression Right { get; init; }
}

public sealed class LogicalCastScalar : LogicalScalarExpression
{
    public required LogicalScalarExpression Operand { get; init; }

    public required RainDbType TargetType { get; init; }
}
