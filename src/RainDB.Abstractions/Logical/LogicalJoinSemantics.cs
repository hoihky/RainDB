namespace RainDB.Logical;

/// <summary>Equi-join null-padding behavior (extensible for additional outer variants).</summary>
public enum LogicalJoinSemantics
{
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}
