# RainDB development roadmap

This roadmap turns the DuckDB-class gap analysis into a sequenced plan for RainDB. It assumes the current baseline: columnar in-memory storage, strict SQL subset, vectorized scan, hash aggregation, inner joins, sort/limit, and directory-backed batch persistence (hydrate-to-RAM).

**Guiding principles** (unchanged from the product direction):

- One physical-plan IR for SQL, LINQ, and programmatic planners.
- Columnar batches as the unit of parallel work (morsels).
- Deterministic OLAP ordering where semantics require it.
- Minimize allocations and copies on hot paths; isolate unsafe SIMD behind clear boundaries.
- Expand SQL and storage only when execution and durability foundations can support them.

**How to read this document**

- **Phases** are ordered by dependency, not calendar time.
- **Exit criteria** describe when a phase is “done enough” to start the next without rework.
- **Out of scope** for a phase is explicit so scope does not creep.

---

## Phase A — Execution efficiency (in-memory core)

**Goal:** Make the existing operators fast and memory-safe at scale *before* adding much new SQL. This is the largest gap versus DuckDB throughput on workloads RainDB already supports.

### A1. Vectorized selection and projection — **implemented**

- **Selection**: conjunctive `WHERE` applies the first predicate with fixed-width compare kernels (optional `Vector128` fast path for null-free `Int32` equality), then **intersects** dense row-index selection vectors for remaining predicates; UTF-8 literals stay on a dedicated equality path (documented on `SelectionEvaluator`).
- **Projection**: `ProjectGather` uses `IAlignedBufferPool` / `IBufferPool` for fixed-width gathers via `PooledFixedWidthColumnChunk` (returned on `IColumnarQueryResult` dispose); full-column copies use a single block copy when no row selection is applied.
- **Tests**: `VectorizedSelectionPerformanceTests` (1M-row correctness + time budget).


### A2. True top-N and sort discipline

- When `ORDER BY` + `LIMIT` are present, use **partial sort / heap top-N** instead of full `Array.Sort` over all rows.
- Retain full sort only when required (no limit, or sort keys need global order without a small k).
- Document memory bounds: O(n) vs O(k) for limited queries.

**Exit criteria:** `ORDER BY … LIMIT k` on large tables stays bounded by k and key width, not full row count.

### A3. Join and grouped-join memory model

- Stream join output where possible (probe-driven batches) instead of materializing `List<RowRefMatch>` for all matches up front.
- For `GroupedJoinPhysicalPlan`, prefer **pipeline or single-pass** strategies (join batches fed into hash agg) before falling back to full ephemeral tables.
- Preserve correct inner-join NULL-key behavior.

**Exit criteria:** Join + `GROUP BY` workloads on medium cardinality complete without proportional spike in peak RSS vs output size.

### A4. SIMD and native boundaries

- Extend intrinsics beyond `SumFloat64`: min/max on `Float64`, optional integer sum paths, hash combine helpers for fixed-width keys.
- Introduce optional **`RainDB.Native`** (or similar) only when C# intrinsics are insufficient; keep `AggregateIntrinsics`-style entry points in Core.

**Exit criteria:** AVX2 paths are opt-in and tested; scalar fallbacks always available.

### A5. Micro-benchmarks

- Add **BenchmarkDotNet** projects for scan, filter+project, hash agg, hash join, sort/top-N.
- Record baseline numbers in CI or a documented manual run (no need for perf gates in CI initially).

**Exit criteria:** Repeatable benchmarks exist; at least one “before/after” comparison per A1–A3 change.

**Phase A out of scope:** New SQL constructs, WAL, Parquet, optimizer cost model.

---

## Phase B — Planning and compile-once semantics

**Goal:** Reduce per-query overhead and improve plan quality without a full cost-based optimizer.

### B1. Logical rewrite (rule-based optimizer v0)

- Single pass (or fixed-point small set) over logical IR:
  - Predicate pushdown to scan/join sides.
  - Projection pruning (drop unreferenced columns early).
  - Limit pushdown where semantics allow.
- Keep rules **testable in isolation** (golden logical plans before/after).

**Exit criteria:** Rewrites are applied before binders emit physical plans; tests cover pushdown and pruning.

### B2. Physical choices (heuristics, not DP)

- Choose hash vs sort-merge join using simple rules (sorted inputs hint, estimated row counts when stats exist).
- Expose join algorithm in `EXPLAIN` output.

**Exit criteria:** `DefaultSqlCompiler` no longer hard-codes hash join only; explain shows the choice.

### B3. Prepared execution

- **Compile once:** parse + bind + optional rewrite → cached `IPhysicalPlan` (keyed by SQL text or plan handle).
- **Parameter binding:** typed parameters for literals in `WHERE` (and later expressions).
- Invalidate cache on `SchemaVersion` / catalog changes.

**Exit criteria:** Second execution of the same prepared statement avoids parse/bind; parameters change results correctly.

### B4. Explain and introspection

- Structured `EXPLAIN` (logical + physical) via SQL or API.
- Optional `EXPLAIN ANALYZE` later hooks into operator timings (can start as no-op timers).

**Exit criteria:** Developers can see physical plan shape and join algorithm without reading binder code.

**Phase B out of scope:** Histogram-based join ordering, adaptive re-optimization mid-query.

---

## Phase C — Storage, I/O, and memory budget

**Goal:** Stop treating durability and analytics storage as “load everything into `MemoryTable` byte arrays.”

### C1. mmap-first column segments

- Wire **`ColumnarFixedWidthMmapReader`** (and file format writers) into table storage as first-class `IColumnChunk` implementations.
- Hydration path: map batch files instead of `File.ReadAllBytes` where format allows.
- Scan engine reads through mapped spans without an extra copy.

**Exit criteria:** File-backed tables query with mmap chunks; persistence round-trip tests pass.

### C2. Buffer manager v0

- Central policy for mapped regions, pin/unpin, and optional memory cap.
- Eviction policy can be naive (LRU per table) initially.

**Exit criteria:** Configurable memory budget; behavior documented when exceeded (fail or evict cold batches).

### C3. Column encodings (read path first)

- Dictionary encoding and/or lightweight integer compression for suitable columns on **write**.
- Transparent decode in scan or lazy decode per vector.

**Exit criteria:** At least one encoding round-trips through batch codec; scan correctness tests green.

### C4. Durability hardening (pre-WAL)

- Atomic catalog updates (write temp + rename).
- Defined ordering: batch durable before catalog references it (or reverse with recovery rules).
- Document crash recovery behavior.

**Exit criteria:** Documented recovery story; tests simulate crash between catalog and batch writes.

**Phase C out of scope:** Full WAL/MVCC (Phase F).

---

## Phase D — SQL and analytics surface

**Goal:** Cover common BI/OLAP queries while execution can handle them efficiently (Phases A–B).

Sequence inside Phase D is suggested; each slice should ship with parser, logical IR, binder, physical plan, and tests.

### D1. Scalar expressions

- Arithmetic, comparisons, `CASE`, casts between supported types in `SELECT`, `WHERE`, and eventually `GROUP BY` keys.

### D2. Aggregates and grouping polish

- `MIN` / `MAX` on all numeric types and UTF-8 (with defined ordering).
- `AVG`, `SUM` null semantics already partially there — align with SQL for grouped queries.
- `HAVING` (filter after aggregation).
- `SELECT DISTINCT` / `COUNT(DISTINCT)` (may require new physical operators).

### D3. Joins and set operations

- `LEFT` / `RIGHT` / `FULL` outer joins (null-padding semantics).
- `UNION ALL` / `UNION` (distinct union needs extra operator).

### D4. Subqueries

- `IN`, `EXISTS`, correlated patterns — start with uncorrelated subqueries in `WHERE` and `FROM`.

### D5. Sort/group interactions

- `ORDER BY` / `LIMIT` with `GROUP BY` (legal SQL shapes only after semantics are clear).

### D6. Types and functions

- `DATE` / `TIMESTAMP` (store as Int64 epoch or dedicated physical type).
- Common functions: `COALESCE`, string `LIKE` / `SUBSTRING` as needed for real workloads.

**Phase D exit criteria:** README “strict subset” section updated; sample SQL under `samples/sql/` grows per milestone; test count scales with features.

**Phase D out of scope:** Window functions (Phase E), nested struct/array types.

---

## Phase E — Advanced analytics operators

**Goal:** Features that define “serious OLAP” after the SQL core is usable.

### E1. Window functions

- `ROW_NUMBER`, `RANK`, `SUM() OVER (PARTITION BY … ORDER BY …)` with framing rules (start with default frame).

### E2. External spill (real)

- Extend `ISpillWriter` to partition and merge **aggregate and join** state, not only metrics JSON.
- Grace hash join and external hash aggregation under memory pressure.

### E3. Statistics

- `ANALYZE table`: row counts, NDV, min/max per column; optional histograms.
- Feed Phase B heuristics and join ordering improvements.

### E4. Adaptive execution (optional)

- Runtime feedback (cardinality mis-estimates) to pick spill or join strategy on retry — only after E2 and E3 exist.

**Phase E exit criteria:** Queries that exceed RAM complete via spill with correct results; analyze improves plans on skewed join tests.

---

## Phase F — Production durability and concurrency

**Goal:** Embedded database semantics suitable for long-lived processes and concurrent readers.

### F1. WAL + checkpoint

- Append-only log for batch/catalog changes; periodic checkpoint to segment files.
- Recovery on open.

### F2. MVCC or snapshot reads (analytics-oriented)

- Readers see consistent snapshot; writers append new versions without blocking long scans (scope TBD: single-writer multi-reader is a reasonable first target).

### F3. Observability

- `ActivitySource` for compile and execute.
- Slow-query threshold, structured plan logging.

**Phase F exit criteria:** Crash during write recovers to last committed state; documented concurrency model.

---

## Phase G — Ecosystem and host integration

**Goal:** Meet .NET developers where they work.

### G1. LINQ provider

- `DefaultLinqCompiler` lowers `IQueryable` to the **same logical IR** as SQL (Where, Select, GroupBy, Join).
- Clear `LinqCompileException` for unsupported patterns.

### G2. Ingest and interchange

- **CSV** and **Parquet** table functions or `COPY`-style load into `MemoryTable` / persistent tables.
- Arrow IPC interop (optional; aligns with UTF-8 chunk layouts).

### G3. API stability

- Version physical plan and on-disk formats once logical IR stabilizes after Phase D.
- Migration notes between format versions.

**Phase G exit criteria:** End-to-end demo: load Parquet → SQL aggregate → persistent store → reopen.

---

## Cross-cutting work (all phases)

| Track | Intent |
|--------|--------|
| **Testing** | Golden SQL files, property tests for optimizer rules, larger integration tests for spill and persistence. |
| **Documentation** | Keep `README.md`, `RainDB-Internals.md`, and this roadmap aligned when phases complete. |
| **Security** | Fuzz SQL parser; bound work for user SQL in hosted scenarios. |

---

## Dependency overview

```text
Phase A (fast in-memory ops)
    ↓
Phase B (rewrite + prepared plans) ──→ Phase D (more SQL) ──→ Phase E (windows + spill + stats)
    ↓                                      ↑
Phase C (mmap + encodings + durability) ───┘
    ↓
Phase F (WAL / MVCC)
    ↓
Phase G (LINQ + formats + API stability)
```

Phases **A** and **C** can overlap lightly (mmap helps A on file-backed data), but **D** should not run far ahead of **A** for heavy operators (join/sort/agg), or new SQL will only expose performance debt.

---

## Suggested success metrics (product-level)

| Milestone | Indicator |
|-----------|-----------|
| Embedded analytics MVP | Prepared SQL + top-N + mmap scans on 100M rows within a configurable RAM budget |
| BI-shaped SQL | Expressions, `HAVING`, outer joins, `UNION ALL` with tests |
| Large RAM exceedance | Spilling hash agg/join passes correctness suite on synthetic “bigger than RAM” cases |
| Production embed | WAL recovery + single-writer / multi-reader documented and tested |

---

## Relationship to existing README phases

| README phase | Roadmap mapping |
|--------------|-----------------|
| Phase 0–1 (foundations, read path) | Largely **complete**; Phase A extends Phase 1 |
| Phase 2 (agg, join, sort) | **Complete** in-memory; Phase A + E2 mature it |
| Phase 2b (directory persistence) | **MVP complete**; Phase C + F harden it |
| Phase 3 (planning & SQL) | Phase B + D |
| Phase 4 (LINQ) | Phase G1 |
| Phase 5 (durability & stats) | Phase C4, E3, F |

This roadmap does not replace the README; it sequences the next layers of work toward a DuckDB-class *embedded OLAP* engine on .NET, with explicit stopping points so each phase delivers a coherent increment.
