# Phases

> The project's delivery history (what exists and why) and the roadmap (what could come next, with
> acceptance criteria). Update this file when a phase lands (Rules R-GIT-3).

## 1. Delivered

| # | Phase | Commit(s) | Date |
|---|---|---|---|
| 1 | Streaming lakehouse core | `597571f`, `a11b25b`, `241a06e` | 2026-09-27 |
| 2 | Neo-brutalist dashboard | `9338ef5` | 2026-09-28 |
| 3 | Dark mode | `0e38231` | 2026-09-28 |
| 4 | Lakeshop storefront | `4025bc1` | 2026-09-28 |
| 5 | Test suite and testing rule | `fcac497` | 2026-09-28 |
| 6 | Screenshots, demo guide, real-data fixes | `5b283ea` | 2026-09-28 |
| 7 | AI reference docs (this folder) | see `git log` | 2026-09-28 |

### Phase 1: Streaming lakehouse core
Kafka → Flink SQL → Iceberg (REST catalog) → Trino → Streamlit, plus a Python simulator.
Hard-won fixes are recorded in Memory.md: MinIO replaced by RustFS, the SQLite catalog in WAL mode,
the Trino heap, the duplicate-job guard, the 8090 port, and LF endings.

### Phase 2: Neo-brutalist dashboard
Hero, coloured KPI tiles, bordered chart cards, Altair charts with a validated category palette,
`card()` / `draw()` helpers, and `.streamlit/config.toml`.

### Phase 3: Dark mode
A sidebar toggle persisted in `?theme=`, a CSS-variable palette, per-mode validated chart colours,
inverted data grids, and a fix for `[object Object]` code blocks.

### Phase 4: Lakeshop storefront
A FastAPI + bento-grid UI that emits real `page_view` / `add_to_cart` / `orders` events in the
existing contract, with fake payments and a shared catalog. The simulator became opt-in.

### Phase 5: Test suite
89 → 91 pytest tests across seven files (see Rules R-TEST and README "Testing"). The owner's rule
"test everything before done" went into CLAUDE.md. A friendly offline error replaced "Failed to fetch".

### Phase 6: Screenshots and demo
`scripts/demo.py` (traffic + Playwright screenshots), `docs/DEMO.md`, and a README gallery. Running on
real data exposed and fixed: the `summary['…']` query failure on empty commits, a stale payment error,
an emoji unsupported on Windows 10, and a mixed-timezone chart.

## 2. Roadmap (not started)
Ordered roughly by value. Each item lists acceptance criteria; per R-TEST, every item
also ships with tests.

### 2.1 CI on GitHub Actions *(high value, low effort)*
- A workflow runs the unit, contract, API, dashboard and e2e suites on every push/PR (Playwright
  Chromium on ubuntu-latest).
- A separate job, or a nightly run, boots `docker compose` and runs `test_integration.py`.
- ✅ Done when: a README badge is green and a PR with a broken contract fails CI.

### 2.2 Schema Registry + Avro, with a schema-evolution demo
- Confluent-compatible registry (e.g. Apicurio or Karapace) in compose; the shop produces Avro; Flink
  reads with `avro-confluent`.
- Demo: add an optional field (e.g. `device`) mid-stream; `ALTER TABLE … ADD COLUMN` in Iceberg; no restart of readers.
- ✅ Done when: old and new events coexist, and the dashboard shows the new column.

### 2.3 CDC upsert table
- Postgres `customers` table → Flink CDC → Iceberg v2 table with a primary key (equality deletes).
- The store "My account" page edits trigger updates.
- ✅ Done when: Trino shows the latest row per customer and time travel shows history.

### 2.4 Late events and a dead-letter queue
- Events later than the watermark go to a side output → `late_events` table, with a dashboard counter.
- ✅ Done when: the simulator can inject late events and the counter moves.

### 2.5 Scheduled table maintenance
- A scheduler (an Airflow DAG or a small cron container) runs
  `optimize`, `expire_snapshots` and `remove_orphan_files` per table.
- ✅ Done when: the file count stays bounded over a 1-hour soak.

### 2.6 Postgres-backed Iceberg catalog
- Replace the SQLite fixture (e.g. with Lakekeeper or Polaris on Postgres).
- ✅ Done when: several concurrent writers commit without `SQLITE_BUSY`, and data survives `down`/`up`.

### 2.7 Data-quality checks on gold tables
- Soda or Great Expectations checks (no negative revenue, funnel monotonicity, freshness SLA) with
  results shown on the dashboard.

### 2.8 Hosted demo / Kubernetes
- A Helm chart or the Flink Kubernetes operator; optionally a small cloud VM running the stack behind
  basic auth, so anyone can try it from a link.

## 3. How to add a phase
1. Write the goal and acceptance criteria here first.
2. Build it following Rules.md (tests in the same change).
3. Update Architecture.md (interfaces), Rules.md (new invariants), Memory.md (lessons), README, and
   screenshots if the UI changed.
4. Move the item to §1 with its commit hash.
