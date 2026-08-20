> **Disclaimer:** This project is an experimental, work-in-progress prototype built with the help of "vibe coding". Things will break. Features are currently missing, and the build scripts might not work at all. Please be aware that it may not be stable enough for production use now.

# RainDB

Embedded **OLAP-oriented** database engine for .NET — columnar analytics, single-process, low latency, DuckDB-inspired goals without leaving the .NET ecosystem.

RainDB is built for applications that need **fast analytical queries over columnar data** inside a single process: services that aggregate telemetry, desktop tools that crunch local datasets, edge nodes that summarize events, and prototypes that should not depend on an external database server. SQL and (planned) LINQ both compile to the same physical-plan intermediate representation so query surfaces stay consistent while the execution engine stays focused.

---

## Why RainDB?

.NET has excellent row-oriented databases and cloud analytics platforms, but there is no mature **embedded columnar OLAP engine** that feels native to the runtime: vectorized execution, `Span`/`Memory`–friendly buffers, optional SIMD, and a composition-root driver you can drop into any app. RainDB fills that gap by borrowing proven ideas from embedded analytics systems (notably DuckDB) and implementing them with .NET-first APIs and module boundaries.

| Gap today | RainDB direction |
|-----------|----------------|
| Ship analytics without Postgres/SQL Server overhead | Single-process embed; no server to operate |
| Columnar scans and aggregates on in-app data | Batches and chunks as the unit of parallel work |
| One query model for SQL and code | Shared logical + physical IR across compilers |
| Predictable latency on medium datasets | Morsel parallelism, aligned buffers, mmap I/O path |
| Persisted tables for reopen-and-query | Directory-backed catalog + batch segments (MVP today) |

**Rationale:** Analytical workloads dominate memory bandwidth and CPU vector units; row-at-a-time APIs and opaque ORM translation leave that hardware idle. RainDB keeps hot paths columnar, explicit, and testable so contributors can optimize operators without rewriting the whole stack.

---

## Production vision

RainDB is not aiming to replace general-purpose OLTP databases or warehouse appliances. The **target end state** is a **production-grade embedded OLAP engine** for .NET:

- **Query surfaces:** A practical SQL dialect (expressions, outer joins, subqueries, window functions over time) plus an `IQueryable` provider that lowers to the same plans as SQL.
- **Execution:** Vectorized operators with spill-to-disk when RAM is exceeded; prepared plans with parameter binding; rule-based optimization and statistics-informed join choices.
- **Storage:** mmap-first column segments, optional encodings (dictionary, lightweight compression), WAL + checkpoint for crash-safe writes, and snapshot reads for long scans alongside writers.
- **Interop:** Load from CSV/Parquet/Arrow; export snapshots; versioned on-disk formats with migration notes.
- **Operations:** `EXPLAIN` / analyze hooks, `ActivitySource` tracing, slow-query logging — enough observability to run inside a real service.

Success looks like: **open a data directory, run prepared analytical SQL over hundreds of millions of rows within a configured memory budget, recover cleanly after crash, and ship it inside a .NET app without a separate database process.**

Current code is an early prototype on that path: core columnar storage, a strict SQL subset, hash aggregation, inner joins, sort/limit, and directory-backed persistence are in place; durability, full SQL, LINQ, spill, and production hardening are roadmap work. See **[Implementation status](docs/Implementation-Status.md)** and **[Development roadmap](docs/Development-Roadmap.md)** for detail.

---

## What works today (summary)

- **Columnar storage** — fixed-width and UTF-8 chunks, nullable bitmaps, 64K–1M row batch sizing, in-memory `MemoryTable` and catalog with schema versioning.
- **Vectorized read path** — scan, filter, project, global and grouped aggregates, inner equi-joins (hash and sort-merge), `ORDER BY` / `LIMIT` on supported shapes.
- **Strict SQL subset** — hand-written parser/compiler for `SELECT` / `FROM` / `WHERE` / `GROUP BY` / `INNER JOIN` / `ORDER BY` / `LIMIT` with documented limits (full list in the [Programming Guide](docs/Programming-Guide.md)).
- **Persistence MVP** — `catalog.json` + per-table `.batch` files; `RainDbEngine.OpenPersistent` and export/import snapshots.
- **Parallelism** — morsel-style execution over batches with deterministic merge order for OLAP semantics.

---

## Quick start

```bash
cd RainDB
dotnet build RainDB.slnx
dotnet test RainDB.slnx
dotnet run --project samples/RainDB.AnalyticsDemo
```

```csharp
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

var engine = RainDbEngine.CreateDefault();

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

await using var rows = await engine.ExecuteSqlAsync(
    "SELECT region, amount FROM sales WHERE amount > 0.5");
```

For persistent storage, file-backed tables, result shapes, and the full SQL reference, see the **[Programming Guide](docs/Programming-Guide.md)**.

---

## Solution layout

| Project | Responsibility |
|---------|----------------|
| **RainDB.Abstractions** | Contracts: catalog, columnar batches, buffers, execution, SQL/LINQ, logical IR |
| **RainDB.Core** | `MemoryTable`, column chunks, buffer pools, mmap I/O, `RainDbFileDatabase` |
| **RainDB.Query** | Physical plans, vectorized engines, query executor |
| **RainDB.Sql** | SQL text → `IPhysicalPlan` |
| **RainDB.Linq** | Expression trees → `IPhysicalPlan` (stub today) |
| **RainDB** (Driver) | `RainDbEngine` composition root |

Architecture and algorithms are documented in **[RainDB Internals](docs/RainDB-Internals.md)**.

---

## Performance principles

- **Columnar batches** over row-at-a-time APIs on hot paths.
- **`ArrayPool` / aligned slabs**; avoid allocations inside operator hot loops.
- **Parallelism at chunk granularity** to reduce synchronization.
- **Compile once, execute many** — prepared plans and parameters (roadmap).

---

## Documentation

| Guide | Description |
|-------|-------------|
| [RainDB docs site](docs/index.html) | Web overview, architecture intro, and HTML documentation |
| [Programming Guide](docs/Programming-Guide.md) | Using `RainDbEngine`, tables, SQL, results, and persistence |
| [RainDB Internals](docs/RainDB-Internals.md) | Architecture, algorithms, and data structures |
| [Implementation Status](docs/Implementation-Status.md) | What is implemented and the phased delivery plan |
| [Development Roadmap](docs/Development-Roadmap.md) | Forward-looking sequencing toward production |

---

## License

MIT — see [LICENSE](LICENSE).
