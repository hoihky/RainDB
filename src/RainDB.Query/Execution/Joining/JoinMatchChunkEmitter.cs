using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Query.Execution.Joining;

/// <summary>Buffers join row matches and emits fixed-size columnar batches to reduce peak match-list memory.</summary>
internal sealed class JoinMatchChunkEmitter
{
    internal const int DefaultChunkRowCount = 8192;

    private readonly JoinPhysicalPlan _plan;
    private readonly IReadOnlyList<IColumnarBatch> _probeBatches;
    private readonly IReadOnlyList<IColumnarBatch> _buildBatches;
    private readonly TableSchema _probeSchema;
    private readonly TableSchema _buildSchema;
    private readonly Action<ColumnarBatch> _emitBatch;
    private readonly JoinBatchMaterializer _materializer;
    private readonly int _chunkRowCount;
    private readonly List<JoinRowMatch> _pending;
    private readonly Func<JoinRowMatch, bool>? _shouldEmit;

    public JoinMatchChunkEmitter(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        Action<ColumnarBatch> emitBatch,
        JoinBatchMaterializer materializer,
        int chunkRowCount = DefaultChunkRowCount,
        Func<JoinRowMatch, bool>? shouldEmit = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeBatches);
        ArgumentNullException.ThrowIfNull(buildBatches);
        ArgumentNullException.ThrowIfNull(probeSchema);
        ArgumentNullException.ThrowIfNull(buildSchema);
        ArgumentNullException.ThrowIfNull(emitBatch);
        ArgumentNullException.ThrowIfNull(materializer);
        _materializer = materializer;
        if (chunkRowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkRowCount));

        _plan = plan;
        _probeBatches = probeBatches;
        _buildBatches = buildBatches;
        _probeSchema = probeSchema;
        _buildSchema = buildSchema;
        _emitBatch = emitBatch;
        _chunkRowCount = chunkRowCount;
        _pending = new List<JoinRowMatch>(Math.Min(chunkRowCount, 256));
        _shouldEmit = shouldEmit;
    }

    public void Add(in JoinRowMatch match)
    {
        if (_shouldEmit is not null && !_shouldEmit(match))
            return;
        _pending.Add(match);
        if (_pending.Count >= _chunkRowCount)
            Flush();
    }

    public void Flush()
    {
        if (_pending.Count == 0)
            return;

        var batch = _materializer.Materialize(_plan, _probeBatches, _buildBatches, _probeSchema, _buildSchema, _pending);
        _emitBatch(batch);
        _pending.Clear();
    }
}
