# RainDB benchmarks

Manual performance baselines for core OLAP operators (BenchmarkDotNet). Not wired into CI.

## Run (Release recommended)

```bash
cd RainDB
dotnet run -c Release --project benchmarks/RainDB.Benchmarks -- --filter '*'
```

Run a single workload:

```bash
dotnet run -c Release --project benchmarks/RainDB.Benchmarks -- --filter '*ScanBenchmarks*'
```

## Workloads

| Class | Operator |
|-------|----------|
| `ScanBenchmarks` | Full columnar scan (3 columns) |
| `FilterProjectBenchmarks` | `WHERE amount > 250` + project 2 columns |
| `HashAggregateBenchmarks` | `GROUP BY category_id` + `SUM(amount)` |
| `JoinBenchmarks` | Hash inner join `sales_fact.region_id = region_dim.region_id` |
| `SortTopNBenchmarks` | `ORDER BY amount DESC LIMIT 100` |

Default dataset: **250k** fact rows in **65,536**-row batches, **4,096** dimension keys. Use `BenchmarkWorkloadSize.Small` (50k) in code for quicker iteration.

Recorded numbers: see [docs/BENCHMARK-BASELINES.md](../docs/BENCHMARK-BASELINES.md).
