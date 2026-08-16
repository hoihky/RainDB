# Implementation status and phased plan

This document tracks what RainDB implements today and the original phased delivery plan. For forward-looking sequencing and exit criteria, see **[Development Roadmap](Development-Roadmap.md)**.

---

## Phase 0 — Foundations (implemented)

- **Columnar storage model**
  - Fixed-width: `FixedWidthColumnChunk` for `Int32`, `Int64`, `Float64`, `Boolean` (one byte per bool in P0) with optional packed null bitmap (`IColumnChunk`).
  - UTF-8: **`Utf8ColumnChunk`** (Arrow-style `offsets[rowCount+1]` + blob) **or** **`Utf8LengthPrefixedColumnChunk`** (`int32` little-endian length + UTF-8 bytes per row, prefix index built at construction).
  - **Vector sizing**: `VectorChunkLimits.MinRows` = **64K**, `MaxRows` = **1M**. `MemoryTableOptions.StrictVectorChunkRows` enforces that every non-empty batch lies in that range (default **off** for small tests / dev).
- **Buffers**
  - `IBufferPool` + `ArrayPool<byte>` for general rent/return (documented: keep under LOH when possible).
  - `IAlignedBufferPool.RentAligned` default alignment **`SimdAlignment.Vector256` (32 bytes)**; minimum accepted alignment is **32** (reject 16). `SimdAlignment.CacheLine128` (128) available for wider alignment. `HybridBufferPool` sub-slices a pooled array with `Unsafe` so the exposed `Memory<byte>` is aligned.
- **Catalog + table metadata**
  - Stable **`TableId`**, `ICatalog` / `InMemoryCatalog` resolve by **name** and **`TableId`**.
  - **`MemoryTable.SchemaVersion`**, **`BumpSchemaVersion()`** (returns new version), and **`SchemaVersionChanged`** event (`SchemaVersionChangedEventArgs`) for planner cache invalidation.
- **File-backed persistence (MVP)**
  - On-disk layout: **`catalog.json`** (table ids, names, column types) plus **`tables/{tableId}/######.batch`** binary segments (`RainDbBatchBinaryCodec` v1: fixed-width, Arrow-style UTF-8, length-prefixed UTF-8).
  - **`RainDbFileDatabase.Open`**: hydrates **`InMemoryCatalog`**; **`CreateMemoryTable`** registers a **`MemoryTable`** whose **`AppendBatch`** calls mirror each new batch to disk (replace-on-write per file).
  - **`RainDbFileDatabase.ExportCatalog` / `ImportCatalog`**: snapshot any **`IColumnarTableSource`** to a directory, or load into a fresh in-memory catalog (no automatic append mirroring after import).
  - **`RainDbEngine.OpenPersistent(path)`** keeps a live **`RainDbFileDatabase`** handle on **`RainDbEngine.FileDatabase`** so the sink is not collected. **`RainDbEngine.CreateDefault(ICatalog)`** composes the default executor/compiler over an existing catalog (e.g. after import).

**Tests**: `tests/RainDB.Tests` — Phase 0–2 coverage, **hash aggregate / GROUP BY** (`SqlGroupByTests`, `HashAggregatePhysicalTests`), **joins** (`JoinExecutionTests`), **ORDER BY / LIMIT** (`SqlOrderByLimitTests`), plus **strict SQL subset** (`SqlStrictSubsetCompilerTests`).

---

## Phase 1 — Read path & latency (implemented)

- **Vectorized scan + filter + project**: `VectorizedScanPhysicalPlan` + `VectorizedScanEngine` (`SelectionEvaluator`, `ProjectGather`), executed by `DefaultQueryExecutor` when the catalog table implements `IColumnarTableSource` (e.g. `MemoryTable`). UTF-8 columns can be projected; **`WHERE`** supports UTF-8 columns with **`=` / `!=` / `<>`** and a **single-quoted string literal** (UTF-8 bytes compared to cell payload).
- **Morsel parallelism**: per-batch `Parallel.For`, or `Channel<int>`-scheduled workers (`UseChannelScheduler`); results merged by **batch index** (deterministic OLAP order).
- **Aggregates**: `AggregateSpec` with `AggregateKind` (`Sum` on `Int32`/`Int64`/`Float64`; `Min`/`Max` on **`Float64`** only; **`Count`** for `COUNT(*)` / `COUNT(col)`). **`SUM`/`MIN`/`MAX` SQL NULL**: global aggregate exposed via `IAggregateQueryResult.ValueIsNull` when no non-null inputs; grouped aggregates use null bits on output chunks. `AggregateIntrinsics.SumFloat64` supports optional **AVX2** (`UseAvx2DoubleSum`, no nulls, full column).
- **Zero-copy mmap**: `ColumnarFixedWidthFileFormat.WriteFile` / `ColumnarFixedWidthMmapReader` — `IColumnChunk` backed by mapped memory (`ReadOnlySpan`/`ReadOnlyMemory` over the file view). Dictionary / RLE / compression called out as later work.
- **Strict SQL subset**: hand-written lexer/parser (`RainDB.Sql/Parsing`), logical IR (`RainDB.Logical`), binders → **`IPhysicalPlan`**: `VectorizedScanPhysicalPlan`, **`SortTopNPhysicalPlan`** (scan + optional **`ORDER BY`** / **`LIMIT`**), **`HashAggregatePhysicalPlan`**, **`JoinPhysicalPlan`**, **`JoinSortTopNPhysicalPlan`**, **`GroupedJoinPhysicalPlan`**. `ISqlCompiler.CompileAsync(sql, ICatalog, ...)`; `RainDbEngine.ExecuteSqlAsync`. Example scripts: `samples/sql/*.sql` (UTF-8 `WHERE`, `AND` conjuncts, joins in `05_*`–`07_*`; also run from `RainDB.AnalyticsDemo` via copied `sql/` folder).
- **Driver**: `RainDbEngine.ExecutePhysicalAsync(IPhysicalPlan, ...)` and **`ExecuteSqlAsync`**; **`OpenPersistent(directory)`** for file-backed **`MemoryTable`** appends; **`CreateDefault(ICatalog)`** after **`RainDbFileDatabase.ImportCatalog`**.

---

## Phase 2 — Analytics operators (implemented, in-memory OLAP)

7. **Hash aggregation** — Per-batch partial hash maps (`Parallel.For` / optional channel scheduler), deterministic global merge, sorted key materialization. **`Utf8`** and mixed fixed-width group keys use composite keys. **`ISpillWriter`**: when enabled and `SpillPartialEntryThreshold` is set, large partial maps emit UTF-8 **metrics** chunks via `SpillChunkAsync` (operator still completes in-memory). **Full spill/repartition of partial aggregates** remains a future extension of the same hook.
8. **Hash / sort-merge join** — `PhysicalJoinAlgorithm.Hash` (build/probe) and **`SortMerge`** over equi-keys (fixed-width and **`Utf8`**). Predicate pushdown to probe/build filters. **Grace/partitioned spill join** is not implemented; the spill interface above is shared for future work.
9. **Sort + Top-N** — `SortTopNEngine`: **`Array.Sort`** over row locations with null-aware comparators for **`Int32`**, **`Int64`**, **`Float64`**, **`Boolean`**, **`Utf8`** (Arrow + length-prefixed chunks). **`SortTopNPhysicalPlan`** (single-table) and **`JoinSortTopNPhysicalPlan`** (post-join). SQL: **`ORDER BY`** / **`LIMIT`** as in the strict SQL subset section of the [Programming Guide](Programming-Guide.md).

---

## Phase 2b — Durable catalog snapshot (MVP implemented)

10. **Directory-backed database** — `RainDbFileDatabase.Open(directory)` loads **`catalog.json`** + batch files into **`MemoryTable`** instances wired with **`IRainDbBatchPersistence`**; each **`AppendBatch`** persists **`######.batch`** (not a WAL: crash between catalog write and batch write can leave inconsistency until we add journaling).
11. **Save / load in-memory** — `RainDbFileDatabase.ExportCatalog(catalog, dir)` writes a full snapshot from any **`IColumnarTableSource`** tables; `ImportCatalog(dir)` rebuilds **`InMemoryCatalog`** without auto-persist. **`FlushCatalog()`** rewrites **`catalog.json`** for **`MemoryTable`** entries only (other **`ITableSource`** implementations are skipped until typed export is extended).

---

## Phase 3 — Planning & SQL (not started)

12. **Logical plan IR** — relational algebra nodes (scan, filter, project, join, agg, sort).
13. **Rule-based optimizer** — predicate pushdown, projection pruning, join reordering heuristics.
14. **Cost model v0** — row counts + distinct counts from statistics; **histograms** optional.
15. **SQL parser** — start with **subset** (SELECT, FROM, WHERE, GROUP BY, ORDER BY, JOIN); ANTLR or hand-written recursive descent; errors with source locations.
16. **`ISqlCompiler`** — parse → logical → physical; single pipeline shared with LINQ.

> **Note:** Much of Phase 3 is partially delivered (logical IR, strict SQL subset, `ISqlCompiler`). Remaining work is optimizer, broader SQL, and compile-once semantics — see [Development Roadmap](Development-Roadmap.md) Phases B and D.

---

## Phase 4 — LINQ (stub only)

17. **`IQueryable` provider** — visit `MethodCallExpression` (Where, Select, GroupBy, Join); map to same logical IR as SQL (**DRY**).
18. **Translation limits** — document unsupported patterns; throw `LinqCompileException` with clear messages.

---

## Phase 5 — Durability & production hardening (not started)

19. **WAL + snapshot** — append-only log, checkpoint; optional MVCC read snapshots for long scans (Phase 2b batch files are **not** a WAL; they are replace-on-write segment snapshots plus a catalog sidecar).
20. **Statistics + adaptive execution** — runtime feedback to replan or pick join order.
21. **Observability** — `ActivitySource`, structured query plans, slow-query threshold.

---

## Cross-cutting (throughout)

- **Testing**: golden SQL files, property-based tests for optimizer rules, micro-benchmarks (`BenchmarkDotNet`) on scan/agg.
- **Unsafe vs safe**: isolate `unsafe` SIMD in `RainDB.Native` optional project behind interface.
- **API stability**: version `IPhysicalPlan` and wire formats once logical IR stabilizes.

---

## Mapping to the development roadmap

| README phase | Roadmap mapping |
|--------------|-----------------|
| Phase 0–1 (foundations, read path) | Largely **complete**; Roadmap Phase A extends Phase 1 |
| Phase 2 (agg, join, sort) | **Complete** in-memory; Roadmap Phase A + E2 mature it |
| Phase 2b (directory persistence) | **MVP complete**; Roadmap Phase C + F harden it |
| Phase 3 (planning & SQL) | Roadmap Phases B + D |
| Phase 4 (LINQ) | Roadmap Phase G1 |
| Phase 5 (durability & stats) | Roadmap Phases C4, E3, F |
