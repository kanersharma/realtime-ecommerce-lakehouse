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
| 7 | AI reference docs (this folder) | `b6fee8a` | 2026-09-28 |
| 8 | Catalog management, reviews, Postgres catalog | `4a03290`, `47591c5` | 2026-09-28 |
| 9 | Inventory and demand forecasting; Docker toolbox | `fa5d9b5` | 2026-09-28 |
| 10 | Schema Registry + Avro, schema evolution | `5d8d635` | 2026-09-29 |
| 11 | Scheduled table maintenance; slim Trino | see `git log` | 2026-09-29 |
| 12 | Easy first run: configurable ports, AGENTS.md | see `git log` | 2026-09-29 |

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

### Phase 7: AI reference docs
PRD, Architecture, Rules, Design, Phases and Memory in `docs/ai/`, indexed from `CLAUDE.md`.

### Phase 8: Catalog management
- The catalog grew from 24 to **48 seed products** (8 per category).
- The shop's catalog moved from a read-only JSON file to **SQLite** (`SHOP_DB`, volume `shop-data`),
  seeded idempotently from `catalog/products.json`. Admin-added products survive restarts.
- **Catalog admin** at `/admin.html`: bento stats, a searchable and filterable product table, and an
  **"+ Add product"** dialog with an emoji picker (Windows-10-safe emoji per category), a live
  preview, and validation that mirrors the server. Admin-added products can be deleted; seed products
  can't.
- API: `POST /api/products`, `DELETE /api/products/{id}`, `GET /api/catalog/options`. Store deep link
  `/?q=…`; shared front-end helpers in `common.js`. The simulator reads the live catalog from the shop.
- Tests: `test_catalog_api.py`, admin e2e flows (add → view in store → buy → events), and an
  integration test that orders an admin-added product and finds it in Iceberg.
- **Reviews** for admin-added products (manual or 🎲 random, when adding or via ⭐ later).
- **Reliability:** the Iceberg catalog moved from SQLite to **Postgres** (roadmap 2.6, done early), after
  a crash loop from `SQLITE_BUSY_SNAPSHOT`. The revenue chart shows still-open minutes live from bronze,
  so single orders appear in a quiet store.

### Phase 9: Inventory and demand forecasting
Decisions by the owner: orders beyond stock are **rejected** (no back-orders), the forecast is
**EWMA + linear trend**, and the Docker work is a **toolbox image** for tests and demo.

- **Shop = system of record for stock.** `products` gained `on_hand`, `lead_time_days`,
  `target_cover_days` (existing databases migrate in place; seed products get 15–74 units, admin-added
  ones 50). A `stock_movements` ledger records every change with a global `seq`.
- **Checkout reserves stock atomically** (`BEGIN IMMEDIATE`, all lines or none). Short stock gives 409
  ("Only N left of X" / "X is sold out") and emits nothing. A 25-thread race test proves no overselling.
- **Restock / settings API** (`POST …/restock`, `PUT …/inventory`), a startup **snapshot** of all stock
  (re-syncs the lakehouse), and a `removed` movement on delete.
- **New Kafka topic `inventory`** → Flink → bronze `lakehouse.shop.inventory_movements` (no gold: the
  forecast needs zero-filled daily series, computed in Trino from bronze orders).
- **Store:** "Only N left" (≤ 5), SOLD OUT tiles with disabled buttons, quantities capped at stock,
  stock refreshed after every checkout attempt.
- **Admin:** Stock column and stats tile, a 📦 dialog for every product (restock with +10/+50/+100,
  lead time, target cover), and a "3 · Inventory" section when adding products.
- **Dashboard 📦 Inventory tab:** KPIs, reorder suggestions with one-click **Restock** (calls the shop),
  days-of-cover chart (lead-time ticks), per-product demand + dashed forecast, and a full table. The math
  lives in `dashboard/inventory.py` (pure, unit-tested). One demo day = `DEMO_DAY_SECONDS` (60 s).
- **Simulator** orders now go through the shop checkout, so simulated sales also consume stock.
- **Docker toolbox:** a root `Dockerfile` (Python + Chromium) with compose services `tests` and `demo`,
  so anyone can run the full suite and the demo with only Docker.
- **Found on the way** (details in Memory.md): product ids reused after deletes, sold-out products
  without a suggestion, e2e tests writing to the developer database, a blank store over plain http
  (`crypto.randomUUID`), and the toolbox base on Debian 13. Reviewing the screenshots found a product
  picker that reset on every refresh, a tab that looked faded ~40 % of the time during auto-refresh,
  add-dialog sections numbered 1, 2, 4, 3, crowded axis labels, and two screenshots that never
  scrolled to the products.
- Tests: `test_inventory_api.py`, `test_inventory_plan.py`, and inventory cases in the dashboard, e2e,
  contract, generator and integration suites (217 tests in total).

### Phase 10: Schema Registry + Avro, with schema evolution
> **Delivered.** Owner's decisions: Confluent Schema Registry (`cp-schema-registry`).

Goal: events are Avro, validated against registered schemas when they are produced, instead of JSON
that Flink parses leniently (`json.ignore-parse-errors` drops bad events silently today).

- **Schemas are the contract:** `schemas/clicks.avsc`, `orders.avsc`, `inventory.avsc`. Producers
  (shop, simulator) serialize with them; Flink reads with `avro-confluent`; tests tie producers →
  schemas → Flink DDL together.
- **Registry** in compose (heap capped), compatibility **FULL**: producers deploy before the Flink job,
  so the job's old reader schema must read new events (forward), and new readers old events
  (backward). BACKWARD alone would allow removing a field, which Flink would turn into silent NULLs.
  Kafka UI shows schemas and decodes Avro messages.
- **Evolution demo:** clicks v2 adds optional `device` (mobile / tablet / desktop, derived by the shop
  from the User-Agent). Iceberg gets `ADD COLUMN device` in place (no rewrite); old rows read NULL and
  the dashboard labels them "before v2".
- **Cutover for existing stacks** without data loss: stop producers, let Flink drain the JSON, cancel
  the job, add the column, start the Avro pipeline (it resumes from committed offsets).

✅ Done when:
1. All three topics carry Avro (magic byte 0 + schema id) and the registry lists `clicks-value`,
   `orders-value`, `inventory-value` with compatibility FULL.
2. A contract test fails if a producer's event, a schema and the Flink DDL disagree.
3. Integration tests against the live registry: an optional field with a default is accepted; a
   required new field, a removed field and a changed type are rejected.
4. A click from a phone lands in Iceberg with `device = 'mobile'`; rows from before v2 are NULL; the
   dashboard shows sessions by device.
5. The owner's existing stack is migrated with no lost events (counts before/after match).
6. Full suite green locally and in the toolbox; screenshots and docs updated.

What shipped, beyond the list above: the SQL playground's *Schema evolution* example, a
`dashboard-devices` screenshot, `in_stock()` for integration tests, and fixes the demo run exposed (the
playground guard refused commented SQL; the dashboard container lacked the registry URL). The owner's
stack was migrated: 20,433 pre-Avro clicks read `device = NULL`, exactly the count before the cutover.
Tests: 242 (contract, device, registry, Avro wire format, every playground example through the UI).

### Phase 11: Scheduled table maintenance
> **Delivered.** Roadmap 2.5, chosen after Trino stalled at its memory limit two days running.

Measured before (owner's stack, 2026-09-29): S3 held **859 MB of `metadata.json`** (2,993 files) for
**17 MB of Parquet**. Every Flink commit writes a new metadata file containing the whole snapshot
history, and nothing deletes the old ones; snapshots (684 per table in a day) and small files only
shrank when someone pressed Compact.

- **`maintenance` service** (Python + Trino client, no Airflow: the stack must stay under ~5 GB). Every
  `MAINTENANCE_INTERVAL_MINUTES` (10), for every table in `lakehouse.shop`: `optimize` (compaction),
  `expire_snapshots` (keep `SNAPSHOT_RETENTION`, 1 h of time travel), `remove_orphan_files`. One log
  line per table: files and snapshots before → after. One failing table doesn't stop the others.
- **Metadata files:** `write.metadata.delete-after-commit.enabled` with `previous-versions-max = 20`
  (set in `pipeline.sql`), the setting Iceberg recommends for streaming writers.
- **Trino:** lower `iceberg.expire-snapshots.min-retention` / `remove-orphan-files.min-retention`
  (default 7 d) so a 1 h retention is allowed.
- **Dashboard (Internals):** last compaction and the history kept, read from `$snapshots`.

✅ Done when:
1. The service runs on its own and logs every table; unit tests cover order, retention and failures.
2. Integration: one maintenance run against live Trino compacts, expires old snapshots and keeps every
   row (a compaction's added-records = deleted-records); the Flink job keeps running (`restored` 0)
   and a new order still lands.
3. A 1-hour soak with the simulator: data files, snapshots and metadata files per table plateau,
   S3 stops growing with commits, and query latency and Trino memory stay flat. Evidence recorded here.
4. Full suite green locally and in the toolbox; docs and screenshots updated.

**Results.** The first run on the grown warehouse: 12,158 objects / 915 MB → ~330 / 54 MB, every row
intact. Two soaks with the simulator (5 sessions/s), maintenance every 10 min, retention 20 min (so the
window fills within the hour):

| | Soak 1 (stock Trino, 65 min) | Soak 2 (slim Trino, 35 min) |
|---|---|---|
| Snapshots per busy table | grew to ~150, then **flat** (~116 after each run) | 123–156, flat |
| Data files per table | sawtooth, 4–8 after each run, ≤ 122 before | 5–84 |
| `metadata.json` in S3 | 105 files (21 per table), 7–11 MB | 105 files, ~8 MB |
| Manifests / Parquet in S3 | peaked 1,384 / 863 files, then fell | ~800–1,000 / ~450–530 files |
| Funnel query | 0.7–2 s, **3 of 11 samples timed out** | 0.7–2 s, **no timeouts** |
| Trino memory stalled (`memory.pressure` full) | up to **59 %** of the time | ≤ 0.25 % |
| Flink restores | 0 new | 0 new |

Found on the way: Trino's stalls came from memory pressure (56 plugins' native memory left the 1 GB heap
no headroom): **`trino/Dockerfile` keeps only the Iceberg plugin**. Orphan removal was the slowest step
(15 s a table) for a rare problem: now hourly.

### Phase 12: Easy first run, for people and AI agents
Asked by the owner mid-phase: anyone (or their AI assistant: Claude Code, Codex, Antigravity, …)
should get the stack running, and a busy port should move our port, not stop the other program.

- Every host port is `${VAR:-default}` in `docker-compose.yml`; `.env.example` lists them all.
- `scripts/ports.py`: checks each port (connect + bind), moves busy ones to the next free port that no
  other service uses, merges them into `.env`, prints the URLs; `--check` only reports; leaves a
  running stack alone. `host_port()` gives tests and `scripts/demo.py` the same ports.
- Links follow the ports: the shop serves `/config.js` for the store and admin pages; the dashboard
  reads `*_LINK` variables.
- `AGENTS.md`: the first-run checklist and rules for any AI agent (and people); CLAUDE.md and the
  README point to it.
- Tests: `test_ports.py` (incl. "no hard-coded host port in compose"), `/config.js`, dashboard links,
  and an e2e test that the store's links follow the configuration.

## 2. Roadmap (not started)
Ordered roughly by value. Each item lists acceptance criteria; per R-TEST, every item
also ships with tests.

### 2.0 Inventory follow-ups
- **Supplier lead times in real time:** model an order that's *in transit* (placed but not yet
  received), so a restock arrives after its lead time instead of instantly, and the reorder point
  accounts for stock on order.
- **Transactional outbox** for inventory events (exactly-once between the shop DB and Kafka; today the
  startup snapshot re-syncs after a crash between commit and publish).
- **Stockout-aware forecasting:** exclude sold-out days from the demand series instead of the current
  "at least the plain average" correction.
- **Intermittent demand:** Croston / SBA for lumpy, low-volume products. The normal-approximation safety
  stock (1.65·σ·√L) is high for them, so a product can show "Reorder now" with weeks of cover.

### 2.1 CI on GitHub Actions *(high value, low effort)*
- A workflow runs the unit, contract, API, dashboard and e2e suites on every push/PR (Playwright
  Chromium on ubuntu-latest).
- A separate job, or a nightly run, boots `docker compose` and runs `test_integration.py`.
- ✅ Done when: a README badge is green and a PR with a broken contract fails CI.

### 2.3 CDC upsert table
- Postgres `customers` table → Flink CDC → Iceberg v2 table with a primary key (equality deletes).
- The store "My account" page edits trigger updates.
- ✅ Done when: Trino shows the latest row per customer and time travel shows history.

### 2.4 Late events and a dead-letter queue
- Events later than the watermark go to a side output → `late_events` table, with a dashboard counter.
- ✅ Done when: the simulator can inject late events and the counter moves.

### 2.6 Production-grade catalog
- The catalog already runs on Postgres (Phase 8). The remaining step is to replace the REST *fixture*
  (a test server) with Lakekeeper or Polaris, adding auth and a UI.

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
