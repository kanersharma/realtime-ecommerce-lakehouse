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
| 8 | Catalog management | see `git log` | 2026-09-28 |

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

## 2. Roadmap (not started)
Ordered roughly by value. Each item lists acceptance criteria; per R-TEST, every item
also ships with tests.

### 2.0 NEXT: Inventory and demand forecasting *(Phase 9, agreed with the owner)*
**Goal:** every product has stock. Orders reduce it, restocks increase it, and an inventory dashboard
uses sales trends to forecast demand and **suggest when and how much to reorder**, based on how many
days a refill takes (lead time) and how many days of stock we want to hold.

**Operational side (shop, source of truth for stock)**
- Add `on_hand`, `lead_time_days` (days a refill takes to arrive) and `target_cover_days` (days of
  demand to keep in stock) to the shop's `products` table. Seed products get starting stock.
- Checkout **reserves stock atomically** (`BEGIN IMMEDIATE`, check then decrement all lines, or none).
  Insufficient stock gives 409 and a clear message. The store shows "Only N left" and "Sold out" (the
  add button is disabled).
- The admin gets a **Restock** action per product (quantity, optional lead-time override), a stock
  column with status chips (In stock / Low / Out), and the "+ Add product" dialog gains starting stock.

**Event and lakehouse side**
- A new Kafka topic `inventory` (key = `product_id`) carrying movements:
  `movement_id, product_id, category, delta, reason (order | restock | adjustment), on_hand_after,
  lead_time_days, event_time`. The event contract is extended per R-EVT-2 (producers, `pipeline.sql`,
  tests, Architecture §4).
- Flink writes bronze `inventory_movements` and a gold `sales_per_bucket` (units sold per product per
  time bucket) with window TVFs, so it stays append-only (R-FLINK-2).
- Current stock in Trino is the latest `on_hand_after` per product (`max_by(on_hand_after, event_time)`).

**Inventory dashboard (new tab "📦 Inventory")**
- KPIs: products low or out of stock, stock value, units sold (last bucket window), and the number of
  products that need reordering now.
- Per product: on hand, **sales velocity** (exponentially weighted average of units per bucket), a
  **trend** (slope via Trino `regr_slope`), and a **forecast** of demand over the lead time.
- **Days of cover** = on_hand ÷ forecast daily demand.
- **Reorder point** = forecast demand × lead_time_days + safety stock, with
  safety stock = z × σ(demand) × √lead_time_days (z ≈ 1.65 for about a 95 % service level).
- **Suggested order quantity** = max(0, forecast demand × (lead_time_days + target_cover_days)
  + safety stock − on_hand).
- Charts: sales over time with a dashed forecast line per product, and a "days of cover" bar list
  sorted from most urgent. A table of suggestions has one-click **Restock suggested qty** (calls the shop API).
- **Demo time scaling:** real "days" are too slow for a live demo, so a setting `DEMO_DAY_SECONDS`
  (e.g. 60 s = 1 simulated day) makes buckets, lead times and forecasts move visibly within minutes.
  All formulas work in "days" and only the bucket size changes.

**Acceptance criteria**
- Buying the last unit sets `on_hand` to 0. The store shows "Sold out" and checkout returns 409 without
  emitting orders. Restocking brings the product back.
- Concurrent checkouts never oversell (an API test with parallel requests).
- Every movement appears in `inventory_movements`, and Trino's current stock equals the shop's
  `on_hand` for every product (integration test).
- Driving steady simulated demand makes days of cover fall and a reorder suggestion appear before
  stock hits 0. Accepting it restocks, and the suggestion clears.
- Dashboard queries pass `test_dashboard_queries_run_on_real_trino`; UI checked in light, dark and mobile.

**Open design choices to confirm before building**
- Forecast method: EWMA plus linear trend (simple and explainable), or Holt's double exponential
  smoothing.
- Whether an order that exceeds stock should be rejected or back-ordered. (Proposal: rejected.)

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
