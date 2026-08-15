> **Disclaimer:** This project is an experimental, work-in-progress prototype built with the help of "vibe coding". Things will break. Features are currently missing, and the build scripts might not work at all. Please be aware that it may not be stable enough for production use now.
# RainDB

Embedded **OLAP-oriented** database engine for .NET (DuckDB-inspired goals: columnar analytics, single-process, low latency). This repository contains the **solution skeleton** and a **prioritized roadmap**; SQL and LINQ surfaces compile to a shared physical-plan IR (not yet feature-complete).

## Build

```bash
cd RainDB
dotnet build RainDB.slnx
dotnet test RainDB.slnx
dotnet run --project samples/RainDB.AnalyticsDemo
```

## Phase 0 status (implemented)

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

## Phase 1 status (implemented)

- **Vectorized scan + filter + project**: `VectorizedScanPhysicalPlan` + `VectorizedScanEngine` (`SelectionEvaluator`, `ProjectGather`), executed by `DefaultQueryExecutor` when the catalog table implements `IColumnarTableSource` (e.g. `MemoryTable`). UTF-8 columns can be projected; **`WHERE`** supports UTF-8 columns with **`=` / `!=` / `<>`** and a **single-quoted string literal** (UTF-8 bytes compared to cell payload).
- **Morsel parallelism**: per-batch `Parallel.For`, or `Channel<int>`-scheduled workers (`UseChannelScheduler`); results merged by **batch index** (deterministic OLAP order).
- **Aggregates**: `AggregateSpec` with `AggregateKind` (`Sum` on `Int32`/`Int64`/`Float64`; `Min`/`Max` on **`Float64`** only; **`Count`** for `COUNT(*)` / `COUNT(col)`). **`SUM`/`MIN`/`MAX` SQL NULL**: global aggregate exposed via `IAggregateQueryResult.ValueIsNull` when no non-null inputs; grouped aggregates use null bits on output chunks. `AggregateIntrinsics.SumFloat64` supports optional **AVX2** (`UseAvx2DoubleSum`, no nulls, full column).
- **Zero-copy mmap**: `ColumnarFixedWidthFileFormat.WriteFile` / `ColumnarFixedWidthMmapReader` — `IColumnChunk` backed by mapped memory (`ReadOnlySpan`/`ReadOnlyMemory` over the file view). Dictionary / RLE / compression called out as later work.
- **Strict SQL subset**: hand-written lexer/parser (`RainDB.Sql/Parsing`), logical IR (`RainDB.Logical`), binders → **`IPhysicalPlan`**: `VectorizedScanPhysicalPlan`, **`SortTopNPhysicalPlan`** (scan + optional **`ORDER BY`** / **`LIMIT`**), **`HashAggregatePhysicalPlan`**, **`JoinPhysicalPlan`**, **`JoinSortTopNPhysicalPlan`**, **`GroupedJoinPhysicalPlan`**. `ISqlCompiler.CompileAsync(sql, ICatalog, ...)`; `RainDbEngine.ExecuteSqlAsync`. Example scripts: `samples/sql/*.sql` (UTF-8 `WHERE`, `AND` conjuncts, joins in `05_*`–`07_*`; also run from `RainDB.AnalyticsDemo` via copied `sql/` folder).
- **Driver**: `RainDbEngine.ExecutePhysicalAsync(IPhysicalPlan, ...)` and **`ExecuteSqlAsync`**; **`OpenPersistent(directory)`** for file-backed **`MemoryTable`** appends; **`CreateDefault(ICatalog)`** after **`RainDbFileDatabase.ImportCatalog`**.

## Strict SQL subset (supported today)

Single statement; ASCII identifiers; `SELECT` / `FROM` / optional `WHERE` / optional **`GROUP BY`** / optional **`ORDER BY`** / optional **`LIMIT`**; line comments `--`.

- **Row query**: `SELECT * FROM table` **or** `SELECT col [, col …] FROM table` **[WHERE col op literal]** (conjuncts with **`AND`**). Optional **`ORDER BY col [ASC | DESC] [, …]`** and **`LIMIT n`** (positive integer). **`ORDER BY`** keys: **fixed-width** types or **`Utf8`**. Without **`ORDER BY`**, **`LIMIT`** applies in stable batch/row order. **`ORDER BY` / `LIMIT`** are **not** supported with **`GROUP BY`** or global aggregates in this subset. Table-qualified columns allowed where documented below.
- **Compare ops**: `=`, `!=` or `<>`, `<`, `<=`, `>`, `>=`.
- **Literals**: integers, decimals (must include `.` for float literal), `TRUE` / `FALSE` for boolean columns, **single-quoted strings** (escape `''` inside the string) for **UTF-8 column** predicates.
- **Global aggregate** (single grouping bucket): `SELECT SUM(col) FROM table [WHERE ...]`; `MIN` / `MAX` on **`Float64`** only; **`SUM`** on `Int32` / `Int64` / `Float64`; **`COUNT(*)`** and **`COUNT(col)`** (non-null rows only for `COUNT(col)`). **`SUM` / `MIN` / `MAX`**: when there are **no non-null values** (including **no rows after `WHERE`**), the scalar result is **SQL NULL**, surfaced as **`IAggregateQueryResult.ValueIsNull == true`**. **`COUNT(*)`** on an empty table returns **0**, not NULL.
- **`GROUP BY`** (single-table): `SELECT … FROM table [WHERE …] GROUP BY col [, col …]` where the **`SELECT`** list may mix **grouping columns** (each must appear in **`GROUP BY`**, matching optional table qualifier against the scanned table) and aggregates (`SUM`, `MIN`, `MAX`, `COUNT`). **`SUM(tbl.col)`**-style qualified arguments are accepted when the qualifier matches the **`FROM`** table. Output column order matches **`SELECT`**. Grouping keys may be **fixed-width** (`Int32`, `Int64`, `Float64`, `Boolean`) or **`Utf8`**. **`COUNT(col)`** allows **`Utf8`** (null-bitmap only). **`SUM`/`MIN`/`MAX`** null semantics per group match SQL (null bits on result columns where applicable).
- **`INNER JOIN`**: `SELECT … FROM left INNER JOIN right ON left.col = right.col [AND …]` (equi-join keys: same type, **fixed-width or `Utf8`**). Row query: **`SELECT *`** (all left columns then all right, output names `LeftName_col`) **or** explicit **`SELECT L.c, R.d`** with optional qualifiers; ambiguous bare names are rejected.
- **`INNER JOIN` + `GROUP BY`**: `SELECT … FROM … INNER JOIN … ON … GROUP BY …` is supported. **`GROUP BY`** and non-aggregated **`SELECT`** columns must use **qualified** `table.column` (parser rule). Aggregates may use **`SUM(R.amt)`** or an **unambiguous** bare column name if it appears on only one side. The executor runs the join, then hash-aggregates over the **materialized join rowset** (`GroupedJoinPhysicalPlan`).
- **`INNER JOIN` + sort / limit**: non-grouped joins may end with **`ORDER BY`** (qualified `table.column`; with an explicit **`SELECT`** list, each sort key must appear in **`SELECT`**) and **`LIMIT`** (`JoinSortTopNPhysicalPlan`).
- **Not supported**: `HAVING`, subqueries, `DISTINCT`, outer joins, quoted identifiers, expressions in `SELECT`/`WHERE`/`GROUP BY`, **`ORDER BY` / `LIMIT` with `GROUP BY`**, grouping by types other than fixed-width + **`Utf8`**.

**API**: `StrictSqlSubset.ParseLogicalPlan(sql)` and **`StrictSqlSubset.CompilePhysicalPlan(sql, catalog) → IPhysicalPlan`** (scan, sort/top-N, hash aggregate, join, join+sort, or grouped join) for debugging / tools without going through `ISqlCompiler`.

## Solution layout (SOLID)

| Project | Responsibility (SRP) | Principle |
|---------|----------------------|-----------|
| **RainDB.Abstractions** | Contracts: catalog, columnar batch/chunk, buffers, execution, SQL/Linq | DIP |
| **RainDB.Core** | `HybridBufferPool`, `InMemoryCatalog`, `MemoryTable`, columnar chunks, **mmap column I/O**, **`RainDbFileDatabase` / batch codec** | LSP |
| **RainDB.Query** | Physical plans, executor, session context | OCP — add operators/planners behind interfaces |
| **RainDB.Sql** | SQL text → `IPhysicalPlan` | ISP — separate from LINQ |
| **RainDB.Linq** | Expression trees → `IPhysicalPlan` | ISP — separate from SQL |
| **RainDB** (Driver) | `RainDbEngine` composition root (`CreateDefault`, `CreateDefault(ICatalog)`, **`OpenPersistent`**) | DIP — host injects collaborators |

## Prioritized implementation plan

### Phase 0 — Foundations (highest priority)

1. **Columnar storage model** — fixed-width columns (`int`, `long`, `double`), nullable bitmaps, length-prefixed or Arrow-style variable strings; **64KB–1M row chunks** for cache locality (DuckDB-like vectors).
2. **`IBufferPool` + aligned allocations** — SIMD-friendly (`Vector256`-aligned) rent/return; avoid LOH churn on hot paths.
3. **Catalog + table handles** — `ICatalog`, stable `TableId`, schema evolution hooks (version per table).

### Phase 1 — Read path & latency

4. **Vectorized scan + filter + project** — codegen or expression interpreter with branch-free hot loops; optional **hardware intrinsics** for aggregates (sum/min/max).
5. **Morsel / parallel** partition of chunks across threads (`Parallel.For`, `Channel<T>`), deterministic merge for OLAP.
6. **Zero-copy I/O** — memory-mapped column files (`MemoryMappedFile`), `ReadOnlySpan<byte>` readers; later: dictionary encoding / RLE / lightweight compression per column.

### Phase 2 — Analytics operators (OLAP core) — **implemented (in-memory OLAP)**

7. **Hash aggregation** — Per-batch partial hash maps (`Parallel.For` / optional channel scheduler), deterministic global merge, sorted key materialization. **`Utf8`** and mixed fixed-width group keys use composite keys. **`ISpillWriter`**: when enabled and `SpillPartialEntryThreshold` is set, large partial maps emit UTF-8 **metrics** chunks via `SpillChunkAsync` (operator still completes in-memory). **Full spill/repartition of partial aggregates** remains a future extension of the same hook.
8. **Hash / sort-merge join** — `PhysicalJoinAlgorithm.Hash` (build/probe) and **`SortMerge`** over equi-keys (fixed-width and **`Utf8`**). Predicate pushdown to probe/build filters. **Grace/partitioned spill join** is not implemented; the spill interface above is shared for future work.
9. **Sort + Top-N** — `SortTopNEngine`: **`Array.Sort`** over row locations with null-aware comparators for **`Int32`**, **`Int64`**, **`Float64`**, **`Boolean`**, **`Utf8`** (Arrow + length-prefixed chunks). **`SortTopNPhysicalPlan`** (single-table) and **`JoinSortTopNPhysicalPlan`** (post-join). SQL: **`ORDER BY`** / **`LIMIT`** as in the strict subset section.

### Phase 2b — Durable catalog snapshot (MVP implemented)

10. **Directory-backed database** — `RainDbFileDatabase.Open(directory)` loads **`catalog.json`** + batch files into **`MemoryTable`** instances wired with **`IRainDbBatchPersistence`**; each **`AppendBatch`** persists **`######.batch`** (not a WAL: crash between catalog write and batch write can leave inconsistency until we add journaling).
11. **Save / load in-memory** — `RainDbFileDatabase.ExportCatalog(catalog, dir)` writes a full snapshot from any **`IColumnarTableSource`** tables; `ImportCatalog(dir)` rebuilds **`InMemoryCatalog`** without auto-persist. **`FlushCatalog()`** rewrites **`catalog.json`** for **`MemoryTable`** entries only (other **`ITableSource`** implementations are skipped until typed export is extended).

### Phase 3 — Planning & SQL

12. **Logical plan IR** — relational algebra nodes (scan, filter, project, join, agg, sort).
13. **Rule-based optimizer** — predicate pushdown, projection pruning, join reordering heuristics.
14. **Cost model v0** — row counts + distinct counts from statistics; **histograms** optional.
15. **SQL parser** — start with **subset** (SELECT, FROM, WHERE, GROUP BY, ORDER BY, JOIN); ANTLR or hand-written recursive descent; errors with source locations.
16. **`ISqlCompiler`** — parse → logical → physical; single pipeline shared with LINQ.

### Phase 4 — LINQ

17. **`IQueryable` provider** — visit `MethodCallExpression` (Where, Select, GroupBy, Join); map to same logical IR as SQL (**DRY**).
18. **Translation limits** — document unsupported patterns; throw `LinqCompileException` with clear messages.

### Phase 5 — Durability & production hardening

19. **WAL + snapshot** — append-only log, checkpoint; optional MVCC read snapshots for long scans (Phase 2b batch files are **not** a WAL; they are replace-on-write segment snapshots plus a catalog sidecar).
20. **Statistics + adaptive execution** — runtime feedback to replan or pick join order.
21. **Observability** — `ActivitySource`, structured query plans, slow-query threshold.

### Cross-cutting (throughout)

- **Testing**: golden SQL files, property-based tests for optimizer rules, micro-benchmarks (`BenchmarkDotNet`) on scan/agg.
- **Unsafe vs safe**: isolate `unsafe` SIMD in `RainDB.Native` optional project behind interface.
- **API stability**: version `IPhysicalPlan` and wire formats once logical IR stabilizes.

## Current API sketch

```csharp
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

// In-memory only (default):
var engine = RainDbEngine.CreateDefault();

// File-backed: loads or creates `catalog.json` + `tables/{id}/*.batch` under `dataDir`.
// Use engine.FileDatabase!.CreateMemoryTable(...) so appends auto-persist.
// var engine = RainDbEngine.OpenPersistent("/path/to/dataDir");

var schema = new TableSchema([
    new ColumnDef("region", RainDbType.Utf8),
    new ColumnDef("amount", RainDbType.Float64),
]);
var table = new MemoryTable("sales", schema);
var offsets = new[] { 0, 2, 4 };
var utf8 = new Utf8ColumnChunk(2, offsets, "usuk"u8.ToArray(), ReadOnlyMemory<byte>.Empty, hasNulls: false);
var amt = new FixedWidthColumnChunk(RainDbType.Float64, 2, BitConverter.GetBytes(1.0).Concat(BitConverter.GetBytes(2.0)).ToArray(), ReadOnlyMemory<byte>.Empty, hasNulls: false);
table.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { utf8, amt }));
engine.Catalog.Register(table);

var scan = new VectorizedScanPhysicalPlan(
    table.Id,
    outputColumnIndices: [0, 1],
    filters: [new ColumnCompareFilter(1, ScalarCompareOp.Gt, BitConverter.DoubleToInt64Bits(0.5))],
    aggregate: null,
    options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = -1 });

await using var rows = await engine.ExecutePhysicalAsync(scan);
// rows implements IColumnarQueryResult (materialized batches per source batch)

// Same shape via SQL (requires table registered on engine.Catalog):
await using var viaSql = await engine.ExecuteSqlAsync(
    "SELECT region, amount FROM sales WHERE amount > 0.5");

// Parse/bind without executing (returns scan, sort/top-N, hash aggregate, join, join+sort, or grouped join):
// Snapshot in-memory catalog to disk and reload:
// using RainDB.Core.Persistence;
// RainDbFileDatabase.ExportCatalog(engine.Catalog, "/path/to/export");
// var cat = RainDbFileDatabase.ImportCatalog("/path/to/export");
// var rehydrated = RainDbEngine.CreateDefault(cat);

## Performance principles (summary)

- **Columnar batches** over row-at-a-time APIs on hot paths.
- **`ArrayPool` / slab allocators**; avoid allocations inside operator `MoveNext`.
- **Parallelism at chunk granularity** to reduce synchronization.
- **Compile SQL/LINQ once**, execute many times (prepared plans + parameter binding — future).

## License

Specify your license in this folder when you productize RainDB.

## Documentation

| Guide | Description |
|-------|-------------|
| [Programming Guide](docs/Programming-Guide.md) | How to use `RainDbEngine`, tables, SQL, results, and persistence |
| [RainDB Internals](docs/RainDB-Internals.md) | Architecture, algorithms, and data structures |
| [Development Roadmap](docs/Development-Roadmap.md) | Phased implementation plan |
