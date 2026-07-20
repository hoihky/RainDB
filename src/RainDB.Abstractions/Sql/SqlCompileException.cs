namespace RainDB.Sql;

public sealed class SqlCompileException : Exception
{
    public SqlCompileException(string message) : base(message)
    {
    }

    public SqlCompileException(string message, Exception inner) : base(message, inner)
    {
    }
}
