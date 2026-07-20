namespace RainDB.Linq;

public sealed class LinqCompileException : Exception
{
    public LinqCompileException(string message) : base(message)
    {
    }

    public LinqCompileException(string message, Exception inner) : base(message, inner)
    {
    }
}
