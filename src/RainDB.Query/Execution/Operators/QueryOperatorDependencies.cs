using RainDB.Core.Columnar;
using RainDB.Query.Execution.Joining;
using RainDB.Query.Execution.Sorting;
using RainDB.Query.Vectorized;

namespace RainDB.Query.Execution.Operators;

/// <summary>Shared columnar helpers composed into physical operators (reduces static orchestration types).</summary>
internal sealed class QueryOperatorDependencies
{
    public QueryOperatorDependencies(
        SelectionEvaluator? selection = null,
        FixedWidthSelectionKernels? selectionKernels = null,
        ProjectGather? projectGather = null,
        JoinBatchMaterializer? joinMaterializer = null,
        SortTopNRowSelector? sortTopNSelection = null,
        GroupKeyFactory? groupKeys = null,
        CompositeJoinKeyFactory? compositeJoinKeys = null,
        IColumnarAggregateIntrinsics? aggregates = null)
    {
        Selection = selection ?? new SelectionEvaluator();
        SelectionKernels = selectionKernels ?? new FixedWidthSelectionKernels(Selection);
        Selection.BindSelectionKernels(SelectionKernels);
        ProjectGather = projectGather ?? new ProjectGather(Selection);
        JoinMaterializer = joinMaterializer ?? new JoinBatchMaterializer(Selection);
        SortTopNSelection = sortTopNSelection ?? new SortTopNRowSelector(Selection);
        GroupKeys = groupKeys ?? new GroupKeyFactory(Selection);
        CompositeJoinKeys = compositeJoinKeys ?? new CompositeJoinKeyFactory(Selection, GroupKeys);
        Aggregates = aggregates ?? new ColumnarAggregateIntrinsics();
    }

    public SelectionEvaluator Selection { get; }

    public FixedWidthSelectionKernels SelectionKernels { get; }

    public ProjectGather ProjectGather { get; }

    public JoinBatchMaterializer JoinMaterializer { get; }

    public SortTopNRowSelector SortTopNSelection { get; }

    public GroupKeyFactory GroupKeys { get; }

    public CompositeJoinKeyFactory CompositeJoinKeys { get; }

    public IColumnarAggregateIntrinsics Aggregates { get; }
}
