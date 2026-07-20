namespace RainDB.Catalog;

/// <summary>Stable table identity for catalog and plans (separate from display <see cref="ITableSource.Name"/>).</summary>
public readonly record struct TableId(Guid Value)
{
    public static TableId New() => new(Guid.NewGuid());

    public static TableId From(Guid g) => new(g);

    public override string ToString() => Value.ToString("N");
}
