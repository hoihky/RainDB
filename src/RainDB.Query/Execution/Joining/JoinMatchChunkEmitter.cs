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
    private readonly int _chunkRowCount;
    private readonly List<JoinRowMatch> _pending;

    public JoinMatchChunkEmitter(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        Action<ColumnarBatch> emitBatch,
        int chunkRowCount = DefaultChunkRowCount)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeBatches);
        ArgumentNullException.ThrowIfNull(buildBatches);
        ArgumentNullException.ThrowIfNull(probeSchema);
        ArgumentNullException.ThrowIfNull(buildSchema);
        ArgumentNullException.ThrowIfNull(emitBatch);
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
    }

    public void Add(in JoinRowMatch match)
    {
        _pending.Add(match);
        if (_pending.Count >= _chunkRowCount)
            Flush();
    }

    public void Flush()
    {
        if (_pending.Count == 0)
            return;

        var batch = JoinBatchMaterializer.Materialize(_plan, _probeBatches, _buildBatches, _probeSchema, _buildSchema, _pending);
        _emitBatch(batch);
        _pending.Clear();
    }
}
