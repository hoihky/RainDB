# RainDB internals

This document describes how RainDB is structured, how the OLAP-style execution path works, how SQL is parsed and lowered to physical plans, and how applications typically use the public API. It reflects the codebase as of the current repository layout.

## Goals and design stance

RainDB is an **embedded**, **single-process**, **column-oriented** analytics engine for .NET. The design favors:

- **Clear layering (SRP)**: abstractions (contracts), core storage, query execution, SQL and LINQ front ends, and a small driver that wires defaults.
- **A single physical-plan IR** shared by SQL and (eventually) LINQ, so optimization and execution stay **DRY**.
- **Vectorized execution** over `IColumnarBatch` / `IColumnChunk` segments rather than row iterators on hot paths.
- **Deterministic OLAP ordering**: parallel work is partitioned by source batch index and merged in stable order where it matters.

The repository README’s solution table remains the high-level map of projects and responsibilities.

## Architecture (projects and dependencies)

| Layer | Project | Role |
|--------|---------|------|
| Contracts | `RainDB.Abstractions` | Catalog (`ICatalog`, `ITableSource`, `IColumnarTableSource`, `TableId`), columnar model (`IColumnarBatch`, `IColumnChunk`), buffers, execution context, `IPhysicalPlan`, logical IR (`RainDB.Logical`), SQL compiler interface (`ISqlCompiler`), optional persistence hook (`IRainDbBatchPersistence`). |
| Storage & I/O | `RainDB.Core` | `InMemoryCatalog`, `MemoryTable`, column chunks (`FixedWidthColumnChunk`, `Utf8ColumnChunk`, `Utf8LengthPrefixedColumnChunk`), pools (`HybridBufferPool`), mmap helpers, **`RainDbFileDatabase`** and **`RainDbBatchBinaryCodec`** for on-disk batch snapshots. |
| Execution | `RainDB.Query` | Physical plan types (`RainDB.Query.Plans`), **`DefaultQueryExecutor`**, vectorized operators (`VectorizedScanEngine`, `HashAggregateEngine`, `JoinExecutionEngine`, `SortTopNEngine`, …), result materialization. |
| SQL | `RainDB.Sql` | **`SqlLexer`**, **`SqlParser`** (internal `SelectParser`), **`DefaultSqlCompiler`**, binders (**`LogicalTableScanBinder`**, **`LogicalJoinBinder`**), **`StrictSqlSubset`** (parse → bind without going through `ISqlCompiler` if desired). |
| LINQ | `RainDB.Linq` | **`DefaultLinqCompiler`** (currently returns a stub **`ExplainOnlyPhysicalPlan`**; expression translation is roadmap work). |
| Host | `RainDB` (driver) | **`RainDbEngine`**: composition root, `CreateDefault`, `CreateDefault(ICatalog)`, **`OpenPersistent`**, `ExecuteSqlAsync`, `ExecutePhysicalAsync`, session factory. |

Dependency flow is **inward**: Driver → Sql / Linq / Query → Core → Abstractions. Query references Core for concrete column types used when building or interpreting plans at execution time.

## Data model

### Columnar batches

A table’s storage (for scans) is exposed as an ordered list of **`IColumnarBatch`** instances. Each batch has a row count and one **`IColumnChunk`** per column. Chunks carry:

- **`RainDbType`** (fixed-width primitives or `Utf8`),
- optional **null bitmap** (packed bits; semantics documented on `IColumnChunk`),
- **`Values`** (packed fixed-width bytes, or UTF-8 payload depending on chunk kind).

Concrete chunk types in **`RainDB.Core.Columnar`** include fixed-width vectors, Arrow-style UTF-8 (`offsets[rowCount+1]` + blob), and length-prefixed UTF-8 per row. **`MemoryTable`** owns an append-only list of batches and validates appends against its **`TableSchema`**.

### Catalog

**`ICatalog`** maps table **names** and **`TableId`** values to **`ITableSource`**. Scans and plans refer to **`TableId`** after binding. **`IColumnarTableSource`** extends **`ITableSource`** with **`Batches`** for columnar execution.

### Execution context

**`IExecutionContext`** (created by **`RainDbEngine.CreateSession`**) carries the catalog, buffer pools, spill writer, and cancellation token. Operators rent memory from pools and respect cancellation where async paths exist.

## OLAP engine (physical plans and execution)

### Physical plan as the execution contract

**`IPhysicalPlan`** (`RainDB.Execution`) is the root of what **`IQueryExecutor`** runs. Implementations live under **`RainDB.Query.Plans`** and include, among others:

- **`VectorizedScanPhysicalPlan`** — single-table scan with optional column filters, projection indices, optional **global** aggregate, and **`VectorizedScanExecutionOptions`** (parallelism, channel-based scheduling, etc.).
- **`HashAggregatePhysicalPlan`** — grouped aggregation over one **`IColumnarTableSource`** (single-table `GROUP BY` or an **ephemeral** columnar source wrapping join output).
- **`JoinPhysicalPlan`** — inner equi-join (build/probe; hash or sort-merge per plan configuration from the binder).
- **`SortTopNPhysicalPlan`** / **`JoinSortTopNPhysicalPlan`** — `ORDER BY` / `LIMIT` over one table or over join output.
- **`GroupedJoinPhysicalPlan`** — join then hash-aggregate over the materialized join rowset.

Each plan type’s **`Explain`** method is used for lightweight introspection (the default executor calls **`plan.Explain()`** before dispatch).

### Default executor dispatch

**`DefaultQueryExecutor`** (`RainDB.Query.Execution`) pattern-matches on **`IPhysicalPlan`** and delegates to the matching **engine** static class. It resolves **`TableId`** from **`context.Catalog`** and requires **`IColumnarTableSource`** for storage-backed operators. For **`GroupedJoinPhysicalPlan`**, it runs the join engine, wraps the columnar result in **`EphemeralColumnarTableSource`**, then runs **`HashAggregateEngine`** on that ephemeral table.

This keeps **join** and **aggregate** concerns separated while still supporting SQL shapes that combine them.

### Vectorized scan engine (representative OLAP path)

**`VectorizedScanEngine`** processes each source batch (filter → project → optional scalar aggregate). For **non-aggregating** queries it can parallelize across batches using either **`Parallel.For`** or a **channel scheduler** (`UseChannelScheduler` in options), then **reassembles outputs by batch index** so order stays stable for analytics.

Global aggregates (`SELECT SUM(x) FROM t`) use a dedicated path inside the same engine that combines partials deterministically.

### Other engines (brief)

- **`HashAggregateEngine`** — partial maps per batch (optionally parallel), merge into global grouped results; supports spill hooks via **`ISpillWriter`** for large partials (full external aggregation is future work).
- **`JoinExecutionEngine`** — validates equi-keys and executes hash or sort-merge join plans.
- **`SortTopNEngine`** — comparison-based sort keys with null-aware ordering for supported types; top-N pruning when `LIMIT` is present.

### Query results

Engines return **`IQueryResult`** implementations such as **`IColumnarQueryResult`** (materialized batches) or **`IAggregateQueryResult`** (single scalar bucket for global aggregates, including SQL NULL semantics via **`ValueIsNull`**).

## SQL parser and compiler

### Lexer

**`SqlLexer`** (`RainDB.Sql.Parsing`) tokenizes the **strict subset** dialect: ASCII identifiers, punctuation, string/number literals, and a **small set** of hard-coded keywords (`SELECT`, `FROM`, `WHERE`, `INNER`, `JOIN`, `ON`, `AND`) returned as dedicated **`SqlTokenKind`** values. Other spellings (including `GROUP`, `BY`, `ORDER`, `LIMIT`, aggregate names, and table/column names) are emitted as **`Identifier`** tokens; the parser recognizes multi-token clauses such as **`GROUP BY`**, **`ORDER BY`**, and **`LIMIT`** by context. The lexer skips whitespace and **`--`** line comments.

### Parser

**`SqlParser.Parse`** constructs a **`SqlLexer`**, then an internal **`SelectParser`** that consumes tokens and builds a **`LogicalPlan`** whose **`Root`** is either:

- **`LogicalTableScan`** — single table, optional `WHERE` conjuncts, optional global aggregate, optional `GROUP BY` + select list, optional `ORDER BY` / `LIMIT` (where allowed by subset rules), or
- **`LogicalInnerJoin`** — `INNER JOIN` … `ON` equi-predicates (and optional `WHERE`, optional `GROUP BY`, optional `ORDER BY` / `LIMIT` for non-grouped joins).

The parser enforces subset constraints (for example, rejecting `SELECT *` with `GROUP BY`, or disallowing `ORDER BY` in certain grouped shapes) by throwing **`SqlCompileException`** with parser-oriented messages.

### Logical IR

Logical nodes live in **`RainDB.Logical`** (Abstractions). They are **relational-shaped** but still close to SQL: table names, qualified columns, simple `WHERE` comparisons, aggregate calls, sort keys, and limits. They intentionally **do not** carry buffer pointers or SIMD details.

### Compilation pipeline

1. **`DefaultSqlCompiler.CompileAsync`** (`RainDB.Sql.Compilation`) calls **`SqlParser.Parse`**, then switches on **`logical.Root`**:
   - **`LogicalTableScan`** → **`LogicalTableScanBinder.BindAndLower`**
   - **`LogicalInnerJoin`** → **`LogicalJoinBinder.BindAndLower`** (join algorithm argument, e.g. hash join)

2. **Binders** resolve names against **`ICatalog`**, map column names to ordinals, validate types for aggregates and predicates, and emit a concrete **`IPhysicalPlan`** (e.g. **`VectorizedScanPhysicalPlan`**, **`HashAggregatePhysicalPlan`**, **`SortTopNPhysicalPlan`**, **`JoinPhysicalPlan`**, **`JoinSortTopNPhysicalPlan`**, **`GroupedJoinPhysicalPlan`**).

3. **`StrictSqlSubset`** exposes the same binding path for tools and tests: **`ParseLogicalPlan`** (parse only) and **`CompilePhysicalPlan`** (parse + bind), without allocating a compiler instance.

There is **no separate optimizer pass** yet; lowering is largely **rule-shaped** inside the binders (single-pass from logical to physical).

### What SQL is supported today

The strict subset is documented in the root **`README.md`** (predicates, joins, `GROUP BY`, global aggregates, `ORDER BY` / `LIMIT` combinations, and explicit non-goals). If behavior differs between docs and code, treat the README and tests under **`tests/RainDB.Tests`** as the contract.

## LINQ

**`DefaultLinqCompiler`** currently does **not** translate expression trees into the same logical IR as SQL; it returns **`ExplainOnlyPhysicalPlan`**. The architectural intent is documented in the roadmap: LINQ and SQL should converge on **`IPhysicalPlan`**.

## Public API usage (typical flows)

### In-memory engine

```csharp
var engine = RainDbEngine.CreateDefault();
// Register RainDB.Core.Tables.MemoryTable (or another IColumnarTableSource) on engine.Catalog.
await using var result = await engine.ExecuteSqlAsync("SELECT ...");
```

### Physical plan directly

For benchmarks or custom planners, compile or build an **`IPhysicalPlan`**, then:

```csharp
await using var rows = await engine.ExecutePhysicalAsync(plan);
```

### SQL without the compiler service

```csharp
using RainDB.Sql;
var plan = StrictSqlSubset.CompilePhysicalPlan("SELECT ...", catalog);
```

### Custom catalog with default collaborators

```csharp
var engine = RainDbEngine.CreateDefault(myCatalog);
```

### File-backed persistence (MVP)

```csharp
var engine = RainDbEngine.OpenPersistent("/path/to/dbdir");
// engine.FileDatabase is non-null; use FileDatabase.CreateMemoryTable(...) so AppendBatch mirrors new batches to disk.
// Export / import snapshots: RainDbFileDatabase.ExportCatalog(...), ImportCatalog(...).
```

See **`RainDB.Core.Persistence.RainDbFileDatabase`** and the README **Phase 2b** section for format and limitations (not a WAL; tables are still executed from in-memory batches after load).

### Sessions

**`RainDbEngine.CreateSession`** returns **`IExecutionContext`** used by **`ExecuteSqlAsync`** / **`ExecutePhysicalAsync`** internally. Custom hosts can construct **`RainDbEngine`** with injected **`IQueryExecutor`**, **`ISqlCompiler`**, buffer pools, and spill writer if they outgrow defaults.

## Where to read next

| Topic | Primary locations |
|--------|-------------------|
| Physical plan shapes | `src/RainDB.Query/Plans/` |
| Operator implementations | `src/RainDB.Query/Execution/` |
| SQL parse + bind | `src/RainDB.Sql/Parsing/`, `src/RainDB.Sql/Compilation/` |
| Logical IR types | `src/RainDB.Abstractions/Logical/` |
| Columnar storage | `src/RainDB.Core/Columnar/`, `src/RainDB.Core/Tables/` |
| Driver entry | `src/RainDB.Driver/RainDbEngine.cs` |
| Behavioral tests | `tests/RainDB.Tests/` |

## Glossary

| Term | Meaning |
|------|---------|
| **Morsel / batch** | A columnar chunk spanning many rows; unit of parallel work and cache-friendly scanning. |
| **Binding** | Resolving SQL names to catalog objects and column ordinals, and emitting a physical plan. |
| **Lowering** | Translating logical operators into executable physical operators (here, mostly in binders). |
| **Strict subset** | The hand-written SQL surface the lexer/parser/compiler implement today; not general SQL. |
