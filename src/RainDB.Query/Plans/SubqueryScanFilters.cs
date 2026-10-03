using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Uncorrelated <c>IN</c> / <c>NOT IN</c>: membership tested against one subquery result column.</summary>
public readonly record struct SubqueryInPhysicalSpec(
    int ColumnIndex,
    RainDbType ColumnType,
    bool Negated,
    IPhysicalPlan Subquery,
    int SubqueryResultColumnIndex);

/// <summary>Uncorrelated <c>EXISTS</c> / <c>NOT EXISTS</c>.</summary>
public readonly record struct SubqueryExistsPhysicalSpec(bool Negated, IPhysicalPlan Subquery);
