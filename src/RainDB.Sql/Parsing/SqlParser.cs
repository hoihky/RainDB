using System.Globalization;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Sql;

namespace RainDB.Sql.Parsing;

internal static class SqlParser
{
    public static LogicalPlan Parse(string sql)
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

        public SelectParser(SqlLexer lexer, string src)
        {
            _lexer = lexer;
            _src = src;
            _cur = default;
        }

        public LogicalPlan Parse()
        {
            _cur = _lexer.NextToken();
            Expect(SqlTokenKind.KwSelect, "SELECT");
            var selectItems = ParseSelectItems(out var starOnly);
            Expect(SqlTokenKind.KwFrom, "FROM");
            var from = ParseFromClause();

            if (from is JoinFrom jf)
            {
                var joinWhereConjuncts = TryParseWhereClause();
                var groupByCols = TryParseGroupByColumns();
                if (groupByCols is { Count: > 0 })
                {
                    if (starOnly)
                        throw new SqlCompileException("SELECT * cannot be used with GROUP BY.");
                    if (selectItems.Count == 0)
                        throw new SqlCompileException("GROUP BY requires an explicit SELECT list.");
                    ValidateGroupedSelectJoin(selectItems, groupByCols, jf.Left, jf.Right);
                    RejectOrderByLimitAfterGrouped();
                    return new LogicalPlan(
                        new LogicalInnerJoin
                        {
                            LeftTableName = jf.Left,
                            RightTableName = jf.Right,
                            LeftKeyColumns = jf.LeftKeys,
                            RightKeyColumns = jf.RightKeys,
                            WhereConjuncts = joinWhereConjuncts,
                            SelectProjection = null,
                            GroupByColumns = groupByCols,
                            SelectList = selectItems,
                        });
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
                ExpectEnd();
                return new LogicalPlan(
                    new LogicalInnerJoin
                    {
                        LeftTableName = jf.Left,
                        RightTableName = jf.Right,
                        LeftKeyColumns = jf.LeftKeys,
                        RightKeyColumns = jf.RightKeys,
                        WhereConjuncts = joinWhereConjuncts,
                        SelectProjection = joinProj,
                        OrderBy = joinOrderBy,
                        Limit = joinLimit,
                    });
            }

            var table = ((SingleTableFrom)from).TableName;
            var whereConjuncts = TryParseWhereClause();
            var groupByColsSingle = TryParseGroupByColumns();

            if (groupByColsSingle is { Count: > 0 })
            {
                if (starOnly)
                    throw new SqlCompileException("SELECT * cannot be used with GROUP BY.");
                if (selectItems.Count == 0)
                    throw new SqlCompileException("GROUP BY requires an explicit SELECT list.");
                ValidateGroupedSelect(selectItems, groupByColsSingle, table);
                RejectOrderByLimitAfterGrouped();
                return new LogicalPlan(
                    new LogicalTableScan
                    {
                        TableName = table,
                        WhereConjuncts = whereConjuncts,
                        GroupByColumns = groupByColsSingle,
                        SelectList = selectItems,
                    });
            }

            if (starOnly)
            {
                var ob = TryParseOrderBy();
                var lim = TryParseLimit();
                ExpectEnd();
                return new LogicalPlan(
                    new LogicalTableScan
                    {
                        TableName = table,
                        WhereConjuncts = whereConjuncts,
                        Projection = null,
                        OrderBy = ob,
                        Limit = lim,
                    });
            }

            if (selectItems.Count == 1 && selectItems[0] is LogicalAggregationCall lone)
            {
                ValidateAggregationCall(lone);
                RejectOrderByLimitAfterGrouped();
                return new LogicalPlan(
                    new LogicalTableScan
                    {
                        TableName = table,
                        WhereConjuncts = whereConjuncts,
                        Aggregate = new LogicalAggregate { Kind = lone.Kind, ColumnName = lone.ArgumentColumnName },
                    });
            }

            if (selectItems.TrueForAll(static x => x is LogicalColumnProjection))
            {
                var proj = new List<LogicalColumnProjection>(selectItems.Count);
                foreach (var x in selectItems)
                    proj.Add((LogicalColumnProjection)x);
                var ob2 = TryParseOrderBy();
                var lim2 = TryParseLimit();
                ExpectEnd();
                return new LogicalPlan(
                    new LogicalTableScan
                    {
                        TableName = table,
                        WhereConjuncts = whereConjuncts,
                        Projection = proj,
                        OrderBy = ob2,
                        Limit = lim2,
                    });
            }

            throw new SqlCompileException("Mixing aggregate functions with bare columns requires GROUP BY.");
        }

        private abstract class FromClause;

        private sealed class SingleTableFrom(string tableName) : FromClause
        {
            public string TableName { get; } = tableName;
        }

        private sealed class JoinFrom(string left, string right, List<LogicalQualifiedColumn> leftKeys, List<LogicalQualifiedColumn> rightKeys)
            : FromClause
        {
            public string Left { get; } = left;

            public string Right { get; } = right;

            public List<LogicalQualifiedColumn> LeftKeys { get; } = leftKeys;

            public List<LogicalQualifiedColumn> RightKeys { get; } = rightKeys;
        }

        private FromClause ParseFromClause()
        {
            var left = ExpectIdentifier("table name");
            if (_cur.Kind != SqlTokenKind.KwInner)
                return new SingleTableFrom(left);

            Advance();
            Expect(SqlTokenKind.KwJoin, "JOIN");
            var right = ExpectIdentifier("table name");
            Expect(SqlTokenKind.KwOn, "ON");
            var leftKeys = new List<LogicalQualifiedColumn>();
            var rightKeys = new List<LogicalQualifiedColumn>();
            ParseJoinEquiConditions(left, right, leftKeys, rightKeys);
            return new JoinFrom(left, right, leftKeys, rightKeys);
        }

        private void ParseJoinEquiConditions(
            string leftTable,
            string rightTable,
            List<LogicalQualifiedColumn> leftKeys,
            List<LogicalQualifiedColumn> rightKeys)
        {
            while (true)
            {
                var a = ExpectQualifiedColumn("JOIN");
                Expect(SqlTokenKind.Eq, "=");
                var b = ExpectQualifiedColumn("JOIN");
                MapJoinPair(leftTable, rightTable, a, b, leftKeys, rightKeys);
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
            string rightTable,
            LogicalQualifiedColumn x,
            LogicalQualifiedColumn y,
            List<LogicalQualifiedColumn> leftKeys,
            List<LogicalQualifiedColumn> rightKeys)
        {
            if (TableEq(x.TableName, leftTable) && TableEq(y.TableName, rightTable))
            {
                leftKeys.Add(x);
                rightKeys.Add(y);
                return;
            }

            if (TableEq(x.TableName, rightTable) && TableEq(y.TableName, leftTable))
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
                    list.Add(ParseColumnProjection());

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
            string? arg = null;
            string? argQual = null;
            if (_cur.Kind == SqlTokenKind.Star)
            {
                Advance();
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

        private LogicalSortKey ParseSortKey()
        {
            var col = ParseColumnProjection();
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

            return new LogicalSortKey { Column = col, Descending = desc };
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

        private List<SimpleWhereClause>? TryParseWhereClause()
        {
            if (_cur.Kind != SqlTokenKind.KwWhere)
                return null;
            Advance();
            var list = new List<SimpleWhereClause> { ParseWherePredicate() };
            while (_cur.Kind == SqlTokenKind.KwAnd)
            {
                Advance();
                list.Add(ParseWherePredicate());
            }

            return list;
        }

        private SimpleWhereClause ParseWherePredicate()
        {
            if (_cur.Kind != SqlTokenKind.Identifier)
                throw new SqlCompileException($"Expected column reference in WHERE at position {_cur.Start}.");
            string? qualifier = null;
            string column;
            var savePos = _lexer.Save();
            var saveTok = _cur;
            Advance();
            if (_cur.Kind == SqlTokenKind.Dot)
            {
                qualifier = _lexer.Lexeme(saveTok).ToString();
                Advance();
                column = ExpectIdentifier("column name in WHERE");
            }
            else
            {
                _lexer.Restore(savePos);
                _cur = saveTok;
                column = ExpectIdentifier("column name in WHERE");
            }

            var op = ParseCompareOp();
            var lit = ParseLiteral();
            return new SimpleWhereClause { QualifierTableName = qualifier, ColumnName = column, Operator = op, Literal = lit };
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
