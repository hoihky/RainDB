using RainDB.Sql;

namespace RainDB.Sql.Parsing;

internal enum SqlTokenKind
{
    EndOfFile,
    Identifier,
    Star,
    Comma,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Semicolon,
    Dot,
    StringLiteral,
    Number,
    KwSelect,
    KwFrom,
    KwWhere,
    KwInner,
    KwJoin,
    KwOn,
    KwAnd,
}

internal readonly record struct SqlToken(SqlTokenKind Kind, int Start, int Length);

/// <summary>Strict-subset lexer (ASCII identifiers; <c>--</c> line comments). Only SELECT/FROM/WHERE are keyword tokens.</summary>
internal sealed class SqlLexer
{
    private readonly string _src;
    private int _pos;

    public SqlLexer(string source, int initialPosition = 0)
    {
        _src = source;
        _pos = initialPosition;
    }

    public int Position => _pos;

    public int Save() => _pos;

    public void Restore(int p) => _pos = p;

    public SqlToken NextToken()
    {
        SkipWhitespaceAndComments();
        if (_pos >= _src.Length)
            return new SqlToken(SqlTokenKind.EndOfFile, _pos, 0);

        var start = _pos;
        var c = _src[_pos];

        if (c == '*')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.Star, start, 1);
        }

        if (c == ',')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.Comma, start, 1);
        }

        if (c == '(')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.LParen, start, 1);
        }

        if (c == ')')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.RParen, start, 1);
        }

        if (c == ';')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.Semicolon, start, 1);
        }

        if (c == '.')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.Dot, start, 1);
        }

        if (c == '\'')
            return LexStringLiteral(start);

        if (c == '=')
        {
            _pos++;
            return new SqlToken(SqlTokenKind.Eq, start, 1);
        }

        if (c == '!' && _pos + 1 < _src.Length && _src[_pos + 1] == '=')
        {
            _pos += 2;
            return new SqlToken(SqlTokenKind.Ne, start, 2);
        }

        if (c == '<')
        {
            if (_pos + 1 < _src.Length && _src[_pos + 1] == '>')
            {
                _pos += 2;
                return new SqlToken(SqlTokenKind.Ne, start, 2);
            }

            if (_pos + 1 < _src.Length && _src[_pos + 1] == '=')
            {
                _pos += 2;
                return new SqlToken(SqlTokenKind.Le, start, 2);
            }

            _pos++;
            return new SqlToken(SqlTokenKind.Lt, start, 1);
        }

        if (c == '>')
        {
            if (_pos + 1 < _src.Length && _src[_pos + 1] == '=')
            {
                _pos += 2;
                return new SqlToken(SqlTokenKind.Ge, start, 2);
            }

            _pos++;
            return new SqlToken(SqlTokenKind.Gt, start, 1);
        }

        if (char.IsAsciiDigit(c) || (c is '+' or '-' && _pos + 1 < _src.Length && char.IsAsciiDigit(_src[_pos + 1])))
            return LexNumber(start);

        if (IsIdentStart(c))
            return LexIdentifierOrKeyword(start);

        throw new SqlCompileException($"Unexpected character '{c}' at position {start}.");
    }

    public ReadOnlySpan<char> Lexeme(in SqlToken t) => _src.AsSpan(t.Start, t.Length);

    private void SkipWhitespaceAndComments()
    {
        while (_pos < _src.Length)
        {
            var c = _src[_pos];
            if (char.IsWhiteSpace(c))
            {
                _pos++;
                continue;
            }

            if (c == '-' && _pos + 1 < _src.Length && _src[_pos + 1] == '-')
            {
                _pos += 2;
                while (_pos < _src.Length && _src[_pos] is not '\r' and not '\n')
                    _pos++;
                continue;
            }

            break;
        }
    }

    private static bool IsIdentStart(char c) => char.IsAsciiLetter(c) || c == '_';

    private static bool IsIdentCont(char c) => char.IsAsciiLetterOrDigit(c) || c == '_';

    private SqlToken LexStringLiteral(int start)
    {
        _pos++; // opening '
        while (_pos < _src.Length)
        {
            var c = _src[_pos];
            if (c == '\'')
            {
                if (_pos + 1 < _src.Length && _src[_pos + 1] == '\'')
                {
                    _pos += 2;
                    continue;
                }

                _pos++;
                var len = _pos - start;
                return new SqlToken(SqlTokenKind.StringLiteral, start, len);
            }

            _pos++;
        }

        throw new SqlCompileException($"Unterminated string literal starting at position {start}.");
    }

    private SqlToken LexNumber(int start)
    {
        var i = _pos;
        if (_src[i] is '+' or '-')
            i++;
        var digitStart = i;
        while (i < _src.Length && char.IsAsciiDigit(_src[i]))
            i++;
        if (i == digitStart)
            throw new SqlCompileException($"Invalid number at position {start}.");
        if (i < _src.Length && _src[i] == '.')
        {
            i++;
            var fracStart = i;
            while (i < _src.Length && char.IsAsciiDigit(_src[i]))
                i++;
            if (i == fracStart)
                throw new SqlCompileException($"Invalid fractional part in number at position {start}.");
        }

        var len = i - _pos;
        _pos = i;
        return new SqlToken(SqlTokenKind.Number, start, len);
    }

    private SqlToken LexIdentifierOrKeyword(int start)
    {
        var i = _pos + 1;
        while (i < _src.Length && IsIdentCont(_src[i]))
            i++;
        var len = i - start;
        _pos = i;
        var kind = ClassifyKeyword(_src.AsSpan(start, len));
        return new SqlToken(kind, start, len);
    }

    private static SqlTokenKind ClassifyKeyword(ReadOnlySpan<char> word)
    {
        Span<char> upper = stackalloc char[word.Length];
        for (var j = 0; j < word.Length; j++)
            upper[j] = char.ToUpperInvariant(word[j]);
        var w = upper;
        if (w.SequenceEqual("SELECT"))
            return SqlTokenKind.KwSelect;
        if (w.SequenceEqual("FROM"))
            return SqlTokenKind.KwFrom;
        if (w.SequenceEqual("WHERE"))
            return SqlTokenKind.KwWhere;
        if (w.SequenceEqual("INNER"))
            return SqlTokenKind.KwInner;
        if (w.SequenceEqual("JOIN"))
            return SqlTokenKind.KwJoin;
        if (w.SequenceEqual("ON"))
            return SqlTokenKind.KwOn;
        if (w.SequenceEqual("AND"))
            return SqlTokenKind.KwAnd;
        return SqlTokenKind.Identifier;
    }
}
