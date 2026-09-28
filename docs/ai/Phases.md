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
| 9 | Inventory and demand forecasting; Docker toolbox | see `git log` | 2026-09-28 |

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
