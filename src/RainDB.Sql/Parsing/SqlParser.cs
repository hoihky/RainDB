using System.Globalization;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Parsing;

public sealed class SqlParser
{
    public LogicalPlan Parse(string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        var trimmed = sql.Trim();
        var lexer = new SqlLexer(trimmed);
        var parser = new SelectParser(lexer, trimmed);
        return parser.Parse();
    }

    private sealed class SelectParser
    {
        private readonly SqlLexer _lexer;
        private readonly string _src;
        private SqlToken _cur;
        private SqlExplainLevel? _explainLevel;

        public SelectParser(SqlLexer lexer, string src)
        {
            _lexer = lexer;
            _src = src;
            _cur = default;
        }

        public LogicalPlan Parse()
        {
            _cur = _lexer.NextToken();
            _explainLevel = TryParseExplainPrefix();
            var root = ParseUnionAllChain(ParseOneSelectRoot());
            ExpectEnd();
            return new LogicalPlan(root, _explainLevel);
        }

        private ILogicalRoot ParseUnionAllChain(ILogicalRoot first)
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "UNION"))
                return first;

            var branches = new List<ILogicalRoot> { first };
            var distinctBetween = new List<bool>();
            while (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "UNION"))
            {
                Advance();
                var thisAll = _cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "ALL");
                distinctBetween.Add(!thisAll);
                if (thisAll)
                    Advance();
                branches.Add(ParseOneSelectRoot());
            }

            if (distinctBetween.Count == 0)
                return new LogicalUnionAll { Branches = branches, UnionAll = true };

            var allDistinct = distinctBetween.TrueForAll(static d => d);
            var allAll = distinctBetween.TrueForAll(static d => !d);
            return new LogicalUnionAll
            {
                Branches = branches,
                DistinctBetweenBranches = distinctBetween,
                UnionAll = allAll && !allDistinct ? true : !allAll && allDistinct ? false : true,
            };
        }

        private ILogicalRoot ParseOneSelectRoot()
        {
            Expect(SqlTokenKind.KwSelect, "SELECT");
            var distinct = false;
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "DISTINCT"))
            {
                distinct = true;
                Advance();
            }

            var selectItems = ParseSelectItems(out var starOnly);
            Expect(SqlTokenKind.KwFrom, "FROM");
            var from = ParseFromClause();

            if (from is JoinFrom jf)
            {
                var (joinWhereConjuncts, joinSubqueryPreds) = TryParseWhereExtended();
                var groupByCols = TryParseGroupByColumns();
                if (groupByCols is { Count: > 0 })
                {
                    if (starOnly)
                        throw new SqlCompileException("SELECT * cannot be used with GROUP BY.");
                    if (selectItems.Count == 0)
                        throw new SqlCompileException("GROUP BY requires an explicit SELECT list.");
                    ValidateGroupedSelectJoin(selectItems, groupByCols, jf.Left, jf.Right);
                    var joinGbOrder = TryParseOrderBy();
                    var joinGbLimit = TryParseLimit();
                    ExpectEnd();
                    return new LogicalInnerJoin
                    {
                        Semantics = jf.Semantics,
                        LeftTableName = jf.Left,
                        LeftTableAlias = jf.LeftAlias,
                        RightTableName = jf.Right,
                        RightTableAlias = jf.RightAlias,
                        LeftKeyColumns = jf.LeftKeys,
                        RightKeyColumns = jf.RightKeys,
                        WhereConjuncts = joinWhereConjuncts,
                        SubqueryPredicates = joinSubqueryPreds,
                        SelectProjection = null,
                        GroupByColumns = groupByCols,
                        SelectList = selectItems,
                        OrderBy = joinGbOrder,
                        Limit = joinGbLimit,
                    };
                }

                List<LogicalColumnProjection>? joinProj = null;
                if (!starOnly)
                {
                    if (!selectItems.TrueForAll(static x => x is LogicalColumnProjection))
                        throw new SqlCompileException("JOIN SELECT list may only contain column references (no aggregates).");
                    joinProj = new List<LogicalColumnProjection>(selectItems.Count);
                    foreach (var x in selectItems)
                        joinProj.Add((LogicalColumnProjection)x);
                }

                var joinOrderBy = TryParseOrderBy();
                var joinLimit = TryParseLimit();
                return new LogicalInnerJoin
                {
                    Semantics = jf.Semantics,
                    LeftTableName = jf.Left,
                    LeftTableAlias = jf.LeftAlias,
                    RightTableName = jf.Right,
                    RightTableAlias = jf.RightAlias,
                    LeftKeyColumns = jf.LeftKeys,
                    RightKeyColumns = jf.RightKeys,
                    WhereConjuncts = joinWhereConjuncts,
                    SubqueryPredicates = joinSubqueryPreds,
                    SelectProjection = joinProj,
                    OrderBy = joinOrderBy,
                    Limit = joinLimit,
                };
            }

            if (from is SubqueryFrom derived)
            {
                if (TryParseGroupByColumns() is { Count: > 0 })
                    throw new SqlCompileException("GROUP BY on a derived table is not supported yet.");
                var (whereConjunctsD, subPredsD) = TryParseWhereExtended();
                if (starOnly)
                {
                    var ob = TryParseOrderBy();
                    var lim = TryParseLimit();
                    return new LogicalDerivedTableScan
                    {
                        Alias = derived.Alias,
                        Subquery = new LogicalSubquery { Root = derived.Root },
                        Projection = null,
                        WhereConjuncts = whereConjunctsD,
                        SubqueryPredicates = subPredsD,
                        OrderBy = ob,
                        Limit = lim,
                    };
                }

                if (selectItems.TrueForAll(static x => x is LogicalColumnProjection or LogicalScalarProjection))
                {
                    var ob2 = TryParseOrderBy();
                    var lim2 = TryParseLimit();
                    var columnOnly = selectItems.TrueForAll(static x => x is LogicalColumnProjection);
                    return new LogicalDerivedTableScan
                    {
                        Alias = derived.Alias,
                        Subquery = new LogicalSubquery { Root = derived.Root },
                        WhereConjuncts = whereConjunctsD,
                        SubqueryPredicates = subPredsD,
                        Projection = columnOnly
                            ? selectItems.Cast<LogicalColumnProjection>().ToList()
                            : null,
                        SelectList = columnOnly ? null : selectItems,
                        OrderBy = ob2,
                        Limit = lim2,
                    };
                }

                throw new SqlCompileException("Derived table queries support column projections only (no aggregates).");
            }

            var singleFrom = (SingleTableFrom)from;
            var table = singleFrom.TableName;
            var tableAlias = singleFrom.Alias;
            var (whereConjuncts, subqueryPreds) = TryParseWhereExtended();
            var groupByColsSingle = TryParseGroupByColumns();

            if (groupByColsSingle is { Count: > 0 })
            {
                if (starOnly)
                    throw new SqlCompileException("SELECT * cannot be used with GROUP BY.");
                if (selectItems.Count == 0)
                    throw new SqlCompileException("GROUP BY requires an explicit SELECT list.");
                ValidateGroupedSelect(selectItems, groupByColsSingle, table);
                var havingConjuncts = TryParseHavingClause();
                var gbOrder = TryParseOrderBy();
                var gbLimit = TryParseLimit();
                ExpectEnd();
                return new LogicalTableScan
                {
                    TableName = table,
                    TableAlias = tableAlias,
                    SelectDistinct = distinct,
                    WhereConjuncts = whereConjuncts,
                    SubqueryPredicates = subqueryPreds,
                    GroupByColumns = groupByColsSingle,
                    SelectList = selectItems,
                    HavingConjuncts = havingConjuncts,
                    OrderBy = gbOrder,
                    Limit = gbLimit,
                };
            }

            if (starOnly)
            {
                var ob = TryParseOrderBy();
                var lim = TryParseLimit();
                return new LogicalTableScan
                {
                    TableName = table,
                    TableAlias = tableAlias,
                    SelectDistinct = distinct,
                    WhereConjuncts = whereConjuncts,
                    SubqueryPredicates = subqueryPreds,
                    Projection = null,
                    OrderBy = ob,
                    Limit = lim,
                };
            }

            if (selectItems.Count == 1 && selectItems[0] is LogicalAggregationCall lone)
            {
                ValidateAggregationCall(lone);
                RejectOrderByLimitAfterGrouped();
                return new LogicalTableScan
                {
                    TableName = table,
                    TableAlias = tableAlias,
                    SelectDistinct = distinct,
                    WhereConjuncts = whereConjuncts,
                    SubqueryPredicates = subqueryPreds,
                    Aggregate = new LogicalAggregate { Kind = lone.Kind, ColumnName = lone.ArgumentColumnName },
                };
            }

            if (selectItems.TrueForAll(static x => x is LogicalColumnProjection or LogicalScalarProjection))
            {
                var ob2 = TryParseOrderBy();
                var lim2 = TryParseLimit();
                return new LogicalTableScan
                {
                    TableName = table,
                    TableAlias = tableAlias,
                    SelectDistinct = distinct,
                    WhereConjuncts = whereConjuncts,
                    SubqueryPredicates = subqueryPreds,
                    SelectList = selectItems,
                    OrderBy = ob2,
                    Limit = lim2,
                };
            }

            throw new SqlCompileException("Mixing aggregate functions with bare columns requires GROUP BY.");
        }

        private SqlExplainLevel? TryParseExplainPrefix()
        {
            if (_cur.Kind != SqlTokenKind.KwExplain)
                return null;
            Advance();
            if (_cur.Kind == SqlTokenKind.KwLogical)
            {
                Advance();
                return SqlExplainLevel.Logical;
            }

            if (_cur.Kind == SqlTokenKind.KwPhysical)
            {
                Advance();
                return SqlExplainLevel.Physical;
            }

            return SqlExplainLevel.All;
        }

        private abstract class FromClause;

        private sealed class SingleTableFrom(string tableName, string? alias) : FromClause
        {
            public string TableName { get; } = tableName;

            public string? Alias { get; } = alias;
        }

        private sealed class JoinFrom(
            string left,
            string? leftAlias,
            string right,
            string? rightAlias,
            List<LogicalQualifiedColumn> leftKeys,
            List<LogicalQualifiedColumn> rightKeys,
            LogicalJoinSemantics semantics)
            : FromClause
        {
            public string Left { get; } = left;

            public string? LeftAlias { get; } = leftAlias;

            public string Right { get; } = right;

            public string? RightAlias { get; } = rightAlias;

            public List<LogicalQualifiedColumn> LeftKeys { get; } = leftKeys;

            public List<LogicalQualifiedColumn> RightKeys { get; } = rightKeys;

            public LogicalJoinSemantics Semantics { get; } = semantics;
        }

        private sealed class SubqueryFrom(ILogicalRoot root, string alias) : FromClause
        {
            public ILogicalRoot Root { get; } = root;

            public string Alias { get; } = alias;
        }

        private FromClause ParseFromClause()
        {
            if (_cur.Kind == SqlTokenKind.LParen)
            {
                Advance();
                var root = ParseUnionAllChain(ParseOneSelectRoot());
                Expect(SqlTokenKind.RParen, ")");
                var alias = ParseDerivedTableAlias();
                return new SubqueryFrom(root, alias);
            }

            var (left, leftAlias) = ParseTableReference();
            if (!TryParseJoinIntro(out var semantics))
                return new SingleTableFrom(left, leftAlias);

            var (right, rightAlias) = ParseTableReference();
            Expect(SqlTokenKind.KwOn, "ON");
            var leftKeys = new List<LogicalQualifiedColumn>();
            var rightKeys = new List<LogicalQualifiedColumn>();
            ParseJoinEquiConditions(left, leftAlias, right, rightAlias, leftKeys, rightKeys);
            return new JoinFrom(left, leftAlias, right, rightAlias, leftKeys, rightKeys, semantics);
        }

        private (string TableName, string? Alias) ParseTableReference()
        {
            var name = ExpectIdentifier("table name");
            if (_cur.Kind != SqlTokenKind.Identifier || IsReservedSqlKeyword(_cur))
                return (name, null);
            if (LexemeEqualsIgnoreCase(_cur, "WHERE")
                || LexemeEqualsIgnoreCase(_cur, "GROUP")
                || LexemeEqualsIgnoreCase(_cur, "ORDER")
                || LexemeEqualsIgnoreCase(_cur, "LIMIT")
                || LexemeEqualsIgnoreCase(_cur, "HAVING")
                || LexemeEqualsIgnoreCase(_cur, "UNION")
                || LexemeEqualsIgnoreCase(_cur, "INNER")
                || LexemeEqualsIgnoreCase(_cur, "LEFT")
                || LexemeEqualsIgnoreCase(_cur, "RIGHT")
                || LexemeEqualsIgnoreCase(_cur, "FULL")
                || LexemeEqualsIgnoreCase(_cur, "JOIN")
                || LexemeEqualsIgnoreCase(_cur, "ON")
                || LexemeEqualsIgnoreCase(_cur, "AND"))
                return (name, null);
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "AS"))
            {
                Advance();
                return (name, ExpectIdentifier("table alias"));
            }

            var alias = ExpectIdentifier("table alias");
            return (name, alias);
        }

        private bool IsReservedSqlKeyword(SqlToken token)
        {
            if (token.Kind == SqlTokenKind.KwSelect || token.Kind == SqlTokenKind.KwFrom || token.Kind == SqlTokenKind.KwWhere
                || token.Kind == SqlTokenKind.KwExplain || token.Kind == SqlTokenKind.KwJoin || token.Kind == SqlTokenKind.KwOn
                || token.Kind == SqlTokenKind.KwInner)
                return true;
            if (token.Kind != SqlTokenKind.Identifier)
                return false;
            return LexemeEqualsIgnoreCase(token, "GROUP")
                || LexemeEqualsIgnoreCase(token, "BY")
                || LexemeEqualsIgnoreCase(token, "ORDER")
                || LexemeEqualsIgnoreCase(token, "LIMIT")
                || LexemeEqualsIgnoreCase(token, "HAVING")
                || LexemeEqualsIgnoreCase(token, "UNION")
                || LexemeEqualsIgnoreCase(token, "DISTINCT")
                || LexemeEqualsIgnoreCase(token, "WHERE")
                || LexemeEqualsIgnoreCase(token, "LEFT")
                || LexemeEqualsIgnoreCase(token, "RIGHT")
                || LexemeEqualsIgnoreCase(token, "FULL")
                || LexemeEqualsIgnoreCase(token, "OUTER")
                || LexemeEqualsIgnoreCase(token, "INNER")
                || LexemeEqualsIgnoreCase(token, "JOIN")
                || LexemeEqualsIgnoreCase(token, "ON")
                || LexemeEqualsIgnoreCase(token, "AND")
                || LexemeEqualsIgnoreCase(token, "AS");
        }

        private bool TryParseJoinIntro(out LogicalJoinSemantics semantics)
        {
            semantics = LogicalJoinSemantics.Inner;
            if (_cur.Kind == SqlTokenKind.KwInner)
            {
                Advance();
                Expect(SqlTokenKind.KwJoin, "JOIN");
                return true;
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "LEFT"))
            {
                Advance();
                if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "OUTER"))
                    Advance();
                Expect(SqlTokenKind.KwJoin, "JOIN");
                semantics = LogicalJoinSemantics.LeftOuter;
                return true;
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "RIGHT"))
            {
                Advance();
                if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "OUTER"))
                    Advance();
                Expect(SqlTokenKind.KwJoin, "JOIN");
                semantics = LogicalJoinSemantics.RightOuter;
                return true;
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "FULL"))
            {
                Advance();
                if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "OUTER"))
                    Advance();
                Expect(SqlTokenKind.KwJoin, "JOIN");
                semantics = LogicalJoinSemantics.FullOuter;
                return true;
            }

            if (_cur.Kind == SqlTokenKind.KwJoin)
            {
                Advance();
                return true;
            }

            return false;
        }

        private void ParseJoinEquiConditions(
            string leftTable,
            string? leftAlias,
            string rightTable,
            string? rightAlias,
            List<LogicalQualifiedColumn> leftKeys,
            List<LogicalQualifiedColumn> rightKeys)
        {
            while (true)
            {
                var a = ExpectQualifiedColumn("JOIN");
                Expect(SqlTokenKind.Eq, "=");
                var b = ExpectQualifiedColumn("JOIN");
                MapJoinPair(leftTable, leftAlias, rightTable, rightAlias, a, b, leftKeys, rightKeys);
                if (_cur.Kind == SqlTokenKind.KwAnd)
                {
                    Advance();
                    continue;
                }

                break;
            }
        }

        private static void MapJoinPair(
            string leftTable,
            string? leftAlias,
            string rightTable,
            string? rightAlias,
            LogicalQualifiedColumn x,
            LogicalQualifiedColumn y,
            List<LogicalQualifiedColumn> leftKeys,
            List<LogicalQualifiedColumn> rightKeys)
        {
            if (RefEq(x.TableName, leftTable, leftAlias) && RefEq(y.TableName, rightTable, rightAlias))
            {
                leftKeys.Add(x);
                rightKeys.Add(y);
                return;
            }

            if (RefEq(x.TableName, rightTable, rightAlias) && RefEq(y.TableName, leftTable, leftAlias))
            {
                leftKeys.Add(y);
                rightKeys.Add(x);
                return;
            }

            throw new SqlCompileException(
                "Each JOIN ON predicate must equate one column from the left table with one column from the right table " +
                "(both written as qualified table.column).");
        }

        private static bool TableEq(string a, string b) => a.Equals(b, StringComparison.OrdinalIgnoreCase);

        private static bool RefEq(string qualifier, string table, string? alias) =>
            Compilation.TableQualifier.Matches(qualifier, table, alias);

        private LogicalQualifiedColumn ExpectQualifiedColumn(string context)
        {
            var tbl = ExpectIdentifier($"{context} table");
            Expect(SqlTokenKind.Dot, ".");
            var col = ExpectIdentifier($"{context} column");
            return new LogicalQualifiedColumn { TableName = tbl, ColumnName = col };
        }

        private static void ValidateGroupedSelect(List<LogicalSelectListItem> items, IReadOnlyList<LogicalColumnProjection> groupBy, string scanTable)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var g in groupBy)
                set.Add(NormalizeGroupKey(g, scanTable));

            foreach (var item in items)
            {
                if (item is LogicalColumnProjection p)
                {
                    if (!set.Contains(NormalizeGroupKey(p, scanTable)))
                    {
                        throw new SqlCompileException(
                            $"Column '{ExplainProj(p)}' must appear in the GROUP BY clause or be used inside an aggregate function.");
                    }
                }

                if (item is LogicalAggregationCall a)
                    ValidateAggregationCall(a);
            }
        }

        private static void ValidateGroupedSelectJoin(
            List<LogicalSelectListItem> items,
            IReadOnlyList<LogicalColumnProjection> groupBy,
            string leftTable,
            string rightTable)
        {
            foreach (var g in groupBy)
            {
                if (g.QualifierTableName is null)
                {
                    throw new SqlCompileException(
                        "GROUP BY with INNER JOIN requires qualified columns (table.column) when grouping.");
                }
            }

            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var g in groupBy)
                set.Add(NormalizeJoinGroupKey(g));

            foreach (var item in items)
            {
                if (item is LogicalColumnProjection p)
                {
                    if (p.QualifierTableName is null)
                    {
                        throw new SqlCompileException(
                            "SELECT with INNER JOIN and GROUP BY requires qualified table.column for non-aggregated columns.");
                    }

                    if (!set.Contains(NormalizeJoinGroupKey(p)))
                    {
                        throw new SqlCompileException(
                            $"Column '{ExplainProj(p)}' must appear in the GROUP BY clause or be used inside an aggregate function.");
                    }
                }

                if (item is LogicalAggregationCall a)
                    ValidateAggregationCall(a);
            }

            _ = leftTable;
            _ = rightTable;
        }

        private static string NormalizeGroupKey(LogicalColumnProjection p, string scanTable) =>
            $"{(p.QualifierTableName ?? scanTable)}\u001f{p.ColumnName}";

        private static string NormalizeJoinGroupKey(LogicalColumnProjection p) =>
            $"{p.QualifierTableName}\u001f{p.ColumnName}";

        private static string ExplainProj(LogicalColumnProjection p) =>
            p.QualifierTableName is { } q ? $"{q}.{p.ColumnName}" : p.ColumnName;

        private static void ValidateAggregationCall(LogicalAggregationCall a)
        {
            switch (a.Kind)
            {
                case AggregateKind.Count:
                    if (a.ArgumentColumnName is null)
                        return;
                    return;
                case AggregateKind.Sum or AggregateKind.Min or AggregateKind.Max:
                    if (string.IsNullOrEmpty(a.ArgumentColumnName))
                        throw new SqlCompileException($"{a.Kind} requires a column argument.");
                    return;
                default:
                    throw new SqlCompileException($"Aggregate {a.Kind} is not supported.");
            }
        }

        private List<LogicalSelectListItem> ParseSelectItems(out bool starOnly)
        {
            starOnly = false;
            if (_cur.Kind == SqlTokenKind.Star)
            {
                Advance();
                starOnly = true;
                return new List<LogicalSelectListItem>();
            }

            var list = new List<LogicalSelectListItem>();
            while (true)
            {
                if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "DISTINCT"))
                    throw new SqlCompileException("DISTINCT is not supported in the strict SQL subset.");

                if (TryParseAggregationCall(out var agg))
                    list.Add(agg);
                else
                    list.Add(ParseSelectListItem());

                if (_cur.Kind == SqlTokenKind.Comma)
                {
                    Advance();
                    continue;
                }

                break;
            }

            return list;
        }

        private bool TryParseAggregationCall(out LogicalAggregationCall call)
        {
            call = null!;
            if (_cur.Kind != SqlTokenKind.Identifier)
                return false;
            if (!IsAggFunctionName(_lexer.Lexeme(_cur)))
                return false;

            var savePos = _lexer.Save();
            var saveTok = _cur;
            Advance();
            if (_cur.Kind != SqlTokenKind.LParen)
            {
                _lexer.Restore(savePos);
                _cur = saveTok;
                return false;
            }

            Advance();
            var kind = ToAggregateKind(_lexer.Lexeme(saveTok));
            var isDistinct = false;
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "DISTINCT"))
            {
                if (kind != AggregateKind.Count)
                    throw new SqlCompileException("DISTINCT is only supported with COUNT.");
                isDistinct = true;
                Advance();
            }

            string? arg = null;
            string? argQual = null;
            if (_cur.Kind == SqlTokenKind.Star)
            {
                Advance();
                if (isDistinct)
                    throw new SqlCompileException("COUNT(DISTINCT *) is not supported.");
                if (kind != AggregateKind.Count)
                {
                    throw new SqlCompileException($"*{kind} is not supported; use COUNT(*) only.");
                }
            }
            else if (_cur.Kind == SqlTokenKind.Identifier)
            {
                var id1 = ExpectIdentifier("aggregate argument");
                if (_cur.Kind == SqlTokenKind.Dot)
                {
                    Advance();
                    argQual = id1;
                    arg = ExpectIdentifier("aggregate argument");
                }
                else
                    arg = id1;
            }
            else
            {
                throw new SqlCompileException($"Expected * or column name in aggregate at position {_cur.Start}.");
            }

            Expect(SqlTokenKind.RParen, ")");
            call = new LogicalAggregationCall
            {
                Kind = kind,
                ArgumentColumnName = arg,
                ArgumentQualifierTableName = argQual,
                IsDistinct = isDistinct,
            };
            return true;
        }

        private IReadOnlyList<LogicalColumnProjection>? TryParseGroupByColumns()
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "GROUP"))
                return null;
            Advance();
            ExpectKeyword("BY");
            var cols = new List<LogicalColumnProjection> { ParseColumnProjection() };
            while (_cur.Kind == SqlTokenKind.Comma)
            {
                Advance();
                cols.Add(ParseColumnProjection());
            }

            return cols;
        }

        private void RejectOrderByLimitAfterGrouped()
        {
            var ob = TryParseOrderBy();
            var lim = TryParseLimit();
            if (ob is { Count: > 0 } || lim is not null)
            {
                throw new SqlCompileException(
                    "ORDER BY and LIMIT are not supported with GROUP BY or with global aggregate queries in the strict SQL subset yet.");
            }

            ExpectEnd();
        }

        private List<LogicalSortKey>? TryParseOrderBy()
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "ORDER"))
                return null;
            Advance();
            ExpectKeyword("BY");
            var keys = new List<LogicalSortKey> { ParseSortKey() };
            while (_cur.Kind == SqlTokenKind.Comma)
            {
                Advance();
                keys.Add(ParseSortKey());
            }

            return keys;
        }

        private List<LogicalHavingConjunct>? TryParseHavingClause()
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "HAVING"))
                return null;
            Advance();
            var list = new List<LogicalHavingConjunct> { ParseHavingPredicate() };
            while (_cur.Kind == SqlTokenKind.KwAnd)
            {
                Advance();
                list.Add(ParseHavingPredicate());
            }

            return list;
        }

        private LogicalHavingConjunct ParseHavingPredicate()
        {
            if (TryParseAggregationCall(out var agg))
            {
                var op = ParseCompareOp();
                var lit = ParseLiteral();
                return new LogicalHavingConjunct { Aggregate = agg, Operator = op, Literal = lit };
            }

            var col = ParseColumnProjection();
            var op2 = ParseCompareOp();
            var lit2 = ParseLiteral();
            return new LogicalHavingConjunct { GroupKeyColumn = col, Operator = op2, Literal = lit2 };
        }

        private LogicalSortKey ParseSortKey()
        {
            var expr = ParseAdditiveExpression();
            LogicalColumnProjection? col = null;
            LogicalScalarExpression? sortExpr = null;
            if (expr is LogicalColumnScalarRef crefOnly)
            {
                col = new LogicalColumnProjection
                {
                    QualifierTableName = crefOnly.QualifierTableName,
                    ColumnName = crefOnly.ColumnName,
                };
            }
            else
                sortExpr = expr;

            var desc = false;
            if (_cur.Kind == SqlTokenKind.Identifier)
            {
                if (LexemeEqualsIgnoreCase(_cur, "ASC"))
                    Advance();
                else if (LexemeEqualsIgnoreCase(_cur, "DESC"))
                {
                    Advance();
                    desc = true;
                }
            }

            return new LogicalSortKey { Column = col, SortExpression = sortExpr, Descending = desc };
        }

        private int? TryParseLimit()
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "LIMIT"))
                return null;
            Advance();
            if (_cur.Kind != SqlTokenKind.Number)
                throw new SqlCompileException($"Expected positive integer after LIMIT at position {_cur.Start}.");
            var text = _lexer.Lexeme(_cur).ToString();
            if (text.Contains('.', StringComparison.Ordinal))
                throw new SqlCompileException("LIMIT requires a positive integer (no decimal).");
            if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) || v < 1)
                throw new SqlCompileException($"Invalid LIMIT value '{text}'.");
            Advance();
            return v;
        }

        private void ExpectKeyword(string ascii)
        {
            if (_cur.Kind != SqlTokenKind.Identifier || !_lexer.Lexeme(_cur).Equals(ascii, StringComparison.OrdinalIgnoreCase))
                throw new SqlCompileException($"Expected '{ascii}' at position {_cur.Start}; found {_cur.Kind}.");
            Advance();
        }

        private string ParseDerivedTableAlias()
        {
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "AS"))
                Advance();
            return ExpectIdentifier("derived table alias");
        }

        private (List<SimpleWhereClause>? Compares, List<LogicalUncorrelatedSubqueryPredicate>? Subqueries) TryParseWhereExtended()
        {
            if (_cur.Kind != SqlTokenKind.KwWhere)
                return (null, null);
            Advance();
            var compares = new List<SimpleWhereClause>();
            var subs = new List<LogicalUncorrelatedSubqueryPredicate>();
            ParseOneWhereItem(compares, subs);
            while (_cur.Kind == SqlTokenKind.KwAnd)
            {
                Advance();
                ParseOneWhereItem(compares, subs);
            }

            return (
                compares.Count > 0 ? compares : null,
                subs.Count > 0 ? subs : null);
        }

        private void ParseOneWhereItem(
            List<SimpleWhereClause> compares,
            List<LogicalUncorrelatedSubqueryPredicate> subs)
        {
            if (TryParseExistsPredicate(out var exists))
            {
                subs.Add(exists);
                return;
            }

            var leftExpr = ParseAdditiveExpression();
            var negated = false;
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "NOT"))
            {
                negated = true;
                Advance();
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "IN"))
            {
                if (leftExpr is not LogicalColumnScalarRef col)
                    throw new SqlCompileException("IN subquery requires a column reference on the left-hand side.");
                Advance();
                var sub = ParseParenthesizedSubqueryRoot();
                subs.Add(new LogicalUncorrelatedSubqueryPredicate
                {
                    PredicateKind = negated
                        ? LogicalUncorrelatedSubqueryPredicate.Kind.NotIn
                        : LogicalUncorrelatedSubqueryPredicate.Kind.In,
                    Column = col,
                    Subquery = new LogicalSubquery { Root = sub },
                });
                return;
            }

            if (negated)
                throw new SqlCompileException("Expected IN or EXISTS after NOT in WHERE.");

            compares.Add(ParseComparePredicate(leftExpr));
        }

        private bool TryParseExistsPredicate(out LogicalUncorrelatedSubqueryPredicate predicate)
        {
            predicate = null!;
            var negated = false;
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "NOT"))
            {
                negated = true;
                Advance();
            }

            if (_cur.Kind != SqlTokenKind.Identifier || !LexemeEqualsIgnoreCase(_cur, "EXISTS"))
                return false;
            Advance();
            var sub = ParseParenthesizedSubqueryRoot();
            predicate = new LogicalUncorrelatedSubqueryPredicate
            {
                PredicateKind = negated
                    ? LogicalUncorrelatedSubqueryPredicate.Kind.NotExists
                    : LogicalUncorrelatedSubqueryPredicate.Kind.Exists,
                Subquery = new LogicalSubquery { Root = sub },
            };
            return true;
        }

        private ILogicalRoot ParseParenthesizedSubqueryRoot()
        {
            Expect(SqlTokenKind.LParen, "(");
            if (_cur.Kind != SqlTokenKind.KwSelect)
                throw new SqlCompileException("Subquery must start with SELECT.");
            var root = ParseUnionAllChain(ParseOneSelectRoot());
            Expect(SqlTokenKind.RParen, ")");
            return root;
        }

        private SimpleWhereClause ParseComparePredicate(LogicalScalarExpression leftExpr)
        {
            var op = ParseCompareOp();
            if (_cur.Kind == SqlTokenKind.Parameter)
            {
                var name = ParseParameterNameFromToken();
                if (leftExpr is LogicalColumnScalarRef col)
                {
                    return new SimpleWhereClause
                    {
                        QualifierTableName = col.QualifierTableName,
                        ColumnName = col.ColumnName,
                        Operator = op,
                        ParameterName = name,
                    };
                }

                throw new SqlCompileException("Parameterized predicates on expressions are not supported yet.");
            }

            if (leftExpr is LogicalColumnScalarRef colOnly
                && op is ScalarCompareOp.Eq or ScalarCompareOp.Ne
                && _cur.Kind == SqlTokenKind.Identifier
                && !IsBooleanLiteralToken(_cur)
                && TryParseColumnScalarRef(out var rightCol))
            {
                return new SimpleWhereClause
                {
                    QualifierTableName = colOnly.QualifierTableName,
                    ColumnName = colOnly.ColumnName,
                    Operator = op,
                    CompareColumn = rightCol,
                };
            }

            var lit = ParseLiteral();
            if (leftExpr is LogicalColumnScalarRef colLit)
            {
                return new SimpleWhereClause
                {
                    QualifierTableName = colLit.QualifierTableName,
                    ColumnName = colLit.ColumnName,
                    Operator = op,
                    Literal = lit,
                };
            }

            return new SimpleWhereClause { LeftExpression = leftExpr, Operator = op, Literal = lit };
        }

        private bool IsBooleanLiteralToken(SqlToken token) =>
            token.Kind == SqlTokenKind.Identifier
            && (LexemeEqualsIgnoreCase(token, "true") || LexemeEqualsIgnoreCase(token, "false"));

        private bool TryParseColumnScalarRef(out LogicalColumnScalarRef column)
        {
            column = null!;
            if (_cur.Kind != SqlTokenKind.Identifier)
                return false;
            var id1 = _lexer.Lexeme(_cur);
            Advance();
            if (_cur.Kind == SqlTokenKind.Dot)
            {
                Advance();
                if (_cur.Kind != SqlTokenKind.Identifier)
                    return false;
                var id2 = _lexer.Lexeme(_cur);
                Advance();
                column = new LogicalColumnScalarRef { QualifierTableName = id1.ToString(), ColumnName = id2.ToString() };
                return true;
            }

            column = new LogicalColumnScalarRef { ColumnName = id1.ToString() };
            return true;
        }

        private LogicalSelectListItem ParseSelectListItem()
        {
            var expr = ParseAdditiveExpression();
            string? alias = null;
            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "AS"))
            {
                Advance();
                alias = ExpectIdentifier("SELECT alias");
            }

            if (expr is LogicalColumnScalarRef col && alias is null)
                return new LogicalColumnProjection { QualifierTableName = col.QualifierTableName, ColumnName = col.ColumnName };
            return new LogicalScalarProjection { Expression = expr, OutputAlias = alias };
        }

        private LogicalScalarExpression ParseAdditiveExpression()
        {
            var left = ParseMultiplicativeExpression();
            while (_cur.Kind == SqlTokenKind.Plus || _cur.Kind == SqlTokenKind.Minus)
            {
                var op = _cur.Kind == SqlTokenKind.Plus ? BinaryScalarOp.Add : BinaryScalarOp.Sub;
                Advance();
                left = new LogicalBinaryScalar { Operator = op, Left = left, Right = ParseMultiplicativeExpression() };
            }

            return left;
        }

        private LogicalScalarExpression ParseMultiplicativeExpression()
        {
            var left = ParseUnaryScalarExpression();
            while (_cur.Kind == SqlTokenKind.Star || _cur.Kind == SqlTokenKind.Slash)
            {
                var op = _cur.Kind == SqlTokenKind.Star ? BinaryScalarOp.Mul : BinaryScalarOp.Div;
                Advance();
                left = new LogicalBinaryScalar { Operator = op, Left = left, Right = ParseUnaryScalarExpression() };
            }

            return left;
        }

        private LogicalScalarExpression ParseUnaryScalarExpression()
        {
            if (_cur.Kind == SqlTokenKind.Minus)
            {
                Advance();
                var lit = ParseScalarPrimary();
                if (lit is LogicalLiteralScalar { Literal.Kind: SqlLiteralKind.Integer } iLit
                    && int.TryParse(iLit.Literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
                {
                    return new LogicalLiteralScalar
                    {
                        Literal = new SqlLiteral(SqlLiteralKind.Integer, (-v).ToString(CultureInfo.InvariantCulture)),
                    };
                }

                throw new SqlCompileException("Unary minus is only supported on integer literals.");
            }

            return ParseScalarPrimary();
        }

        private LogicalScalarExpression ParseScalarPrimary()
        {
            if (_cur.Kind == SqlTokenKind.LParen)
            {
                Advance();
                var inner = ParseAdditiveExpression();
                Expect(SqlTokenKind.RParen, ")");
                return inner;
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "CASE"))
            {
                Advance();
                var whens = new List<LogicalCaseWhenClause>();
                while (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "WHEN"))
                {
                    Advance();
                    var cond = ParseCompareScalar();
                    ExpectKeyword("THEN");
                    var result = ParseAdditiveExpression();
                    whens.Add(new LogicalCaseWhenClause { Condition = cond, Result = result });
                }

                ExpectKeyword("ELSE");
                var elseExpr = ParseAdditiveExpression();
                ExpectKeyword("END");
                return new LogicalCaseScalar { WhenClauses = whens, Else = elseExpr };
            }

            if (_cur.Kind == SqlTokenKind.Identifier && LexemeEqualsIgnoreCase(_cur, "CAST"))
            {
                Advance();
                Expect(SqlTokenKind.LParen, "(");
                var operand = ParseAdditiveExpression();
                ExpectKeyword("AS");
                var typeName = ExpectIdentifier("CAST target type");
                Expect(SqlTokenKind.RParen, ")");
                var target = ParseRainDbTypeName(typeName);
                return new LogicalCastScalar { Operand = operand, TargetType = target };
            }

            if (_cur.Kind == SqlTokenKind.Number)
            {
                var lit = ParseLiteral();
                return new LogicalLiteralScalar { Literal = lit };
            }

            if (_cur.Kind == SqlTokenKind.Identifier)
            {
                var col = ParseColumnProjection();
                return new LogicalColumnScalarRef { QualifierTableName = col.QualifierTableName, ColumnName = col.ColumnName };
            }

            throw new SqlCompileException($"Expected scalar expression at position {_cur.Start}.");
        }

        private LogicalCompareScalar ParseCompareScalar()
        {
            var left = ParseAdditiveExpression();
            var op = ParseCompareOp();
            if (_cur.Kind is SqlTokenKind.Number or SqlTokenKind.Identifier or SqlTokenKind.LParen)
            {
                return new LogicalCompareScalar
                {
                    Left = left,
                    Operator = op,
                    RightExpression = ParseAdditiveExpression(),
                };
            }

            var lit = ParseLiteral();
            return new LogicalCompareScalar { Left = left, Operator = op, RightLiteral = lit };
        }

        private static RainDbType ParseRainDbTypeName(string name) =>
            name.ToUpperInvariant() switch
            {
                "INT" or "INT32" or "INTEGER" => RainDbType.Int32,
                "BIGINT" or "INT64" => RainDbType.Int64,
                "DOUBLE" or "FLOAT64" or "FLOAT" => RainDbType.Float64,
                _ => throw new SqlCompileException($"Unsupported CAST target type '{name}'."),
            };

        private string ParseParameterNameFromToken()
        {
            if (_cur.Kind != SqlTokenKind.Parameter)
                throw new SqlCompileException($"Expected parameter at position {_cur.Start}.");
            var lex = _lexer.Lexeme(_cur);
            if (lex.Length < 2 || lex[0] != '@')
                throw new SqlCompileException("Internal error: malformed parameter token.");
            var name = lex.Slice(1).ToString();
            Advance();
            return name;
        }

        private LogicalColumnProjection ParseColumnProjection()
        {
            if (_cur.Kind != SqlTokenKind.Identifier)
                throw new SqlCompileException($"Expected column name at position {_cur.Start}.");
            string? qualifier = null;
            string column;
            var savePos = _lexer.Save();
            var saveTok = _cur;
            Advance();
            if (_cur.Kind == SqlTokenKind.Dot)
            {
                qualifier = _lexer.Lexeme(saveTok).ToString();
                Advance();
                column = ExpectIdentifier("column name");
            }
            else
            {
                _lexer.Restore(savePos);
                _cur = saveTok;
                column = ExpectIdentifier("column name");
            }

            return new LogicalColumnProjection { QualifierTableName = qualifier, ColumnName = column };
        }

        private static string DecodeSqlStringLexeme(ReadOnlySpan<char> lexemeIncludingQuotes)
        {
            if (lexemeIncludingQuotes.Length < 2 || lexemeIncludingQuotes[0] != '\'' || lexemeIncludingQuotes[^1] != '\'')
                throw new SqlCompileException("Internal error: malformed string literal token.");
            var inner = lexemeIncludingQuotes.Slice(1, lexemeIncludingQuotes.Length - 2);
            Span<char> buf = inner.Length <= 512 ? stackalloc char[inner.Length] : new char[inner.Length];
            var w = 0;
            for (var i = 0; i < inner.Length; i++)
            {
                if (inner[i] == '\'' && i + 1 < inner.Length && inner[i + 1] == '\'')
                {
                    buf[w++] = '\'';
                    i++;
                    continue;
                }

                buf[w++] = inner[i];
            }

            return new string(buf[..w]);
        }
        private ScalarCompareOp ParseCompareOp()
        {
            var op = _cur.Kind switch
            {
                SqlTokenKind.Eq => ScalarCompareOp.Eq,
                SqlTokenKind.Ne => ScalarCompareOp.Ne,
                SqlTokenKind.Lt => ScalarCompareOp.Lt,
                SqlTokenKind.Le => ScalarCompareOp.Le,
                SqlTokenKind.Gt => ScalarCompareOp.Gt,
                SqlTokenKind.Ge => ScalarCompareOp.Ge,
                _ => throw new SqlCompileException($"Expected comparison operator at position {_cur.Start}; found {_cur.Kind}."),
            };
            Advance();
            return op;
        }

        private SqlLiteral ParseLiteral()
        {
            if (_cur.Kind == SqlTokenKind.StringLiteral)
            {
                var lex = _lexer.Lexeme(_cur);
                Advance();
                var decoded = DecodeSqlStringLexeme(lex);
                return new SqlLiteral(SqlLiteralKind.String, decoded);
            }

            if (_cur.Kind == SqlTokenKind.Identifier)
            {
                if (LexemeEqualsIgnoreCase(_cur, "TRUE"))
                {
                    Advance();
                    return new SqlLiteral(SqlLiteralKind.Boolean, "TRUE");
                }

                if (LexemeEqualsIgnoreCase(_cur, "FALSE"))
                {
                    Advance();
                    return new SqlLiteral(SqlLiteralKind.Boolean, "FALSE");
                }

                throw new SqlCompileException($"Invalid literal '{_lexer.Lexeme(_cur)}' at position {_cur.Start}.");
            }

            if (_cur.Kind == SqlTokenKind.Number)
            {
                var text = _lexer.Lexeme(_cur).ToString();
                var kind = text.Contains('.', StringComparison.Ordinal) ? SqlLiteralKind.Float : SqlLiteralKind.Integer;
                Advance();
                return new SqlLiteral(kind, text);
            }

            throw new SqlCompileException($"Expected literal at position {_cur.Start}.");
        }

        private void ExpectEnd()
        {
            if (_cur.Kind == SqlTokenKind.Semicolon)
                Advance();
            if (_cur.Kind != SqlTokenKind.EndOfFile)
                throw new SqlCompileException($"Unexpected token after statement at position {_cur.Start}.");
        }

        private void Expect(SqlTokenKind kind, string label)
        {
            if (_cur.Kind != kind)
                throw new SqlCompileException($"Expected {label} at position {_cur.Start}; found {_cur.Kind}.");
            Advance();
        }

        private string ExpectIdentifier(string context)
        {
            if (_cur.Kind != SqlTokenKind.Identifier)
                throw new SqlCompileException($"Expected identifier ({context}) at position {_cur.Start}; found {_cur.Kind}.");
            var s = _lexer.Lexeme(_cur).ToString();
            if (IsReservedWord(s))
                throw new SqlCompileException($"The name '{s}' is reserved and cannot be used as an identifier here.");
            Advance();
            return s;
        }

        private static bool IsReservedWord(string s) =>
            s.Equals("SELECT", StringComparison.OrdinalIgnoreCase)
            || s.Equals("FROM", StringComparison.OrdinalIgnoreCase)
            || s.Equals("WHERE", StringComparison.OrdinalIgnoreCase)
            || s.Equals("GROUP", StringComparison.OrdinalIgnoreCase)
            || s.Equals("BY", StringComparison.OrdinalIgnoreCase)
            || s.Equals("INNER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("JOIN", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ON", StringComparison.OrdinalIgnoreCase)
            || s.Equals("AND", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ORDER", StringComparison.OrdinalIgnoreCase)
            || s.Equals("BY", StringComparison.OrdinalIgnoreCase)
            || s.Equals("ASC", StringComparison.OrdinalIgnoreCase)
            || s.Equals("DESC", StringComparison.OrdinalIgnoreCase)
            || s.Equals("LIMIT", StringComparison.OrdinalIgnoreCase);

        private void Advance() => _cur = _lexer.NextToken();

        private bool LexemeEqualsIgnoreCase(in SqlToken t, string ascii) =>
            _lexer.Lexeme(t).Equals(ascii, StringComparison.OrdinalIgnoreCase);

        private static bool IsAggFunctionName(ReadOnlySpan<char> word) =>
            word.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            || word.Equals("MIN", StringComparison.OrdinalIgnoreCase)
            || word.Equals("MAX", StringComparison.OrdinalIgnoreCase)
            || word.Equals("COUNT", StringComparison.OrdinalIgnoreCase);

        private static AggregateKind ToAggregateKind(ReadOnlySpan<char> word)
        {
            if (word.Equals("SUM", StringComparison.OrdinalIgnoreCase))
                return AggregateKind.Sum;
            if (word.Equals("MIN", StringComparison.OrdinalIgnoreCase))
                return AggregateKind.Min;
            if (word.Equals("MAX", StringComparison.OrdinalIgnoreCase))
                return AggregateKind.Max;
            if (word.Equals("COUNT", StringComparison.OrdinalIgnoreCase))
                return AggregateKind.Count;
            throw new SqlCompileException("Internal error: unknown aggregate.");
        }
    }
}
