namespace RainDB.Query.Vectorized;

internal readonly record struct ColumnInSetFilter(int ColumnIndex, bool Negated, ScalarValueSet Values);
