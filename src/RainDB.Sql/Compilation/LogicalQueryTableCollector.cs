using RainDB.Logical;

namespace RainDB.Sql.Compilation;

internal static class LogicalQueryTableCollector
{
    internal static HashSet<string> Collect(ILogicalRoot root)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        CollectRoot(root, set);
        return set;
    }

    private static void CollectRoot(ILogicalRoot root, HashSet<string> tables)
    {
        switch (root)
        {
            case LogicalTableScan s:
                tables.Add(s.TableName);
                break;
            case LogicalDerivedTableScan d:
                CollectRoot(d.Subquery.Root, tables);
                break;
            case LogicalInnerJoin j:
                tables.Add(j.LeftTableName);
                tables.Add(j.RightTableName);
                break;
            case LogicalUnionAll u:
                foreach (var b in u.Branches)
                    CollectRoot(b, tables);
                break;
        }
    }
}
