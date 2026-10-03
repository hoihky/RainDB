using RainDB.Catalog;
using RainDB.Logical;
using RainDB.Query.Plans;

namespace RainDB.Sql.Planning;

/// <summary>Chooses a physical join algorithm from logical join shape and catalog statistics.</summary>
public interface IJoinAlgorithmSelector
{
    PhysicalJoinAlgorithm Select(LogicalInnerJoin join, ICatalog catalog, PhysicalPlanningOptions options);
}
