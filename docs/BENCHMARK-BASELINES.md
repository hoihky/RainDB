# RainDB benchmark baselines (manual)

Recorded from **BenchmarkDotNet** on a developer machine. Re-run after performance work and append a dated section — not enforced in CI.

## How to reproduce

```bash
dotnet run -c Release --project benchmarks/RainDB.Benchmarks -- --filter '*'
```

See [benchmarks/README.md](../benchmarks/README.md) for workload definitions.

## Dataset

| Setting | Value |
|---------|--------|
| Fact rows | 250,000 (`BenchmarkWorkloadSize.Standard`) |
| Batch size | 65,536 rows |
| Dimension rows | 4,096 regions |
| Fact columns | `category_id`, `region_id`, `amount` (Int32, Int32, Float64) |

## Snapshot — 2026-09-26

**Environment:** Apple M4, macOS 15.3, .NET 10.0.0, Release, BenchmarkDotNet 0.14.0  
**Job:** WarmupCount=1, IterationCount=3 (quick capture run; use default config for official re-runs)

| Workload | Method | Mean latency | Notes |
|----------|--------|--------------|--------|
| Scan | `scan_all_columns` | **61.5 μs** / invocation | Full 3-column scan, 250k rows |
| Filter + project | `filter_project` | **307.9 μs** / invocation | `amount > 250`, 2 output columns |
| Hash aggregate | `hash_group_by_sum` | **2.71 ms** / invocation | `GROUP BY category_id`, `SUM(amount)` |
| Hash join | `hash_inner_join` | **14.8 ms** / invocation | Equi-join on `region_id` |
| Sort top-N | `order_by_limit_top100` | **2.41 ms** / invocation | `ORDER BY amount DESC LIMIT 100` (heap top-k) |

Latencies are **per benchmark invocation** (one end-to-end `ExecutePhysicalAsync` for the workload). Compare before/after on the **same machine** and job settings.

## After A2 (top-N)

The sort/top-N row reflects **bounded heap selection** (`BoundedTopKHeap`) rather than full sort over all 250k rows. Re-benchmark after A3/A4 changes to join/scan paths.
