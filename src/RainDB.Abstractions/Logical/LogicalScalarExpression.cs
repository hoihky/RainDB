using RainDB.Execution;
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

public sealed class LogicalCompareScalar : LogicalScalarExpression
{
    public required LogicalScalarExpression Left { get; init; }

    public required ScalarCompareOp Operator { get; init; }

    public LogicalScalarExpression? RightExpression { get; init; }

    public SqlLiteral? RightLiteral { get; init; }
}

public sealed class LogicalCaseWhenClause
{
    public required LogicalCompareScalar Condition { get; init; }

    public required LogicalScalarExpression Result { get; init; }
}

public sealed class LogicalCaseScalar : LogicalScalarExpression
{
    public required IReadOnlyList<LogicalCaseWhenClause> WhenClauses { get; init; }

    public required LogicalScalarExpression Else { get; init; }
}
