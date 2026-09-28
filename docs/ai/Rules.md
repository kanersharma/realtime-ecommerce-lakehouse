# Rules

> The rulebook for anyone (human or AI) changing this repo. **MUST** and **NEVER** are hard rules;
> *prefer* is guidance. Each rule has an id so commits and reviews can cite it ("per R-EVT-2").
> The reasons behind many rules are in [Memory.md](Memory.md).

## R-TEST: Testing (the owner's standing rule)
- **R-TEST-1** MUST run the **whole** suite before any change is considered done (commit, push, or
  saying "done"): `.venv/Scripts/python -m pytest -rs`.
- **R-TEST-2** MUST add or update tests in the same change. A bug fix MUST include a test that fails
  without the fix.
- **R-TEST-3** MUST prove each new test can fail: break the code on purpose, see it go red, restore.
- **R-TEST-4** For changes touching the shop, events, pipeline, catalog, Trino config, dashboard SQL
  or docker-compose, integration tests MUST **run, not skip**: `docker compose up -d --build`, wait
  for the Flink job to be RUNNING, then run pytest again.
- **R-TEST-5** UI changes MUST be looked at in a real browser: store and dashboard, light and dark,
  desktop and ~375 px mobile.
- **R-TEST-6** NEVER delete, skip or weaken a test to go green. Fix the code, or explain it to the owner.
- **R-TEST-7** Report honestly: pass/fail/skip counts, and anything not verified.

## R-EVT: Event contract
- **R-EVT-1** Producers MUST emit exactly the columns of `clicks_src` / `orders_src` in
  `flink/sql/pipeline.sql` (enforced by `tests/test_contract.py`).
- **R-EVT-2** Adding or renaming a field MUST change, in one commit: `pipeline.sql` (source + Iceberg
  DDL + INSERT), `shop/main.py`, `generator/generator.py`, the tests, and Architecture §4.
- **R-EVT-3** Timestamps MUST be UTC, timezone-naive, formatted `yyyy-MM-dd HH:mm:ss.SSS`. NEVER
  introduce `TIMESTAMP_LTZ` or a `Z` suffix without changing producers, Flink and the dashboard together.
- **R-EVT-4** `orders` grain is one row per product line. Kafka key is `user_id`.

## R-SHOP: Storefront and money
- **R-SHOP-1** The server owns prices, totals, order ids and `event_time`. NEVER trust these from the browser.
- **R-SHOP-2** Validate every request with pydantic models (patterns, ranges, `Literal`s).
- **R-SHOP-3** Price every line before publishing anything, so there are no partial orders.
- **R-SHOP-4** Payments are fake: only `TEST_CARDS` numbers may succeed; card inputs keep
  `autocomplete="off"` and non-standard names; card data NEVER appears in logs, storage, responses
  (beyond the last 4 digits) or events.
- **R-SHOP-5** Network failures MUST show a human message, never a raw "Failed to fetch".
- **R-SHOP-6** No frontend build step: plain HTML/CSS/JS served by FastAPI. Prefer native elements
  (`<dialog>`, `<output>`, radio groups) over libraries. Helpers shared by the store and the admin
  (`$`, `esc`, `money`, `CATS`, `api`, `toast`, `ratingText`) live in `common.js`; don't duplicate them.
- **R-SHOP-7** Everything user-supplied (product names and descriptions from the admin) is rendered with
  `esc()`. Names are also restricted server-side to letters, numbers, spaces and `. , ' & ( ) + / -`.
- **R-SHOP-8** The admin API has no auth (a `ponytail:` note in `main.py`). NEVER expose the stack
  beyond localhost without adding a login first.

## R-CAT: Catalog
- **R-CAT-1** `catalog/products.json` is the **seed** catalog. At runtime the shop's SQLite database
  (seed plus admin-added products) is the source of truth, served by `GET /api/products`. Seeding stays
  idempotent (`INSERT OR IGNORE`); never overwrite existing rows from the seed file.
- **R-CAT-4** New products go through `create_product()`, which enforces the next `P###` id,
  2-decimal prices, a unique name (case-insensitive) and an emoji from that category's `EMOJI` list.
  The admin UI's checks are only for speed; the server decides.
- **R-CAT-5** Seed products can't be deleted (403). Tests that create products on the live stack MUST
  delete them afterwards, so the demo catalog stays clean.
- **R-CAT-7** Reviews (rating + count) can be set only on admin-added products. Ratings have one decimal
  and are 1.0–5.0 when there are reviews; 0 reviews means rating 0 ("No reviews yet"). The UI always
  shows one decimal (4.0, not 4).
- **R-CAT-6** Tests MUST NOT use the developer database: `conftest.shop_db` points `SHOP_DB` at a fresh
  temp file for every test.
- **R-CAT-2** Exactly 6 categories, kept in sync with `CATEGORY_COLORS` (dashboard) and `CATS`
  (`shop/static/app.js`). Tests enforce this.
- **R-CAT-3** Product emoji MUST render on Windows 10 (Emoji ≤ 12, code points < U+1FA70), and seed
  emoji MUST be in that category's admin picker list (`EMOJI` in `shop/main.py`).

## R-FLINK: Pipeline
- **R-FLINK-1** DDL stays idempotent (`CREATE … IF NOT EXISTS`).
- **R-FLINK-2** Gold tables stay append-only (window TVFs). A non-windowed `GROUP BY` needs an
  upsert v2 table with a primary key; that's a design change, so discuss it first.
- **R-FLINK-3** Connector jar versions are coupled: Kafka connector `-1.20`, `iceberg-flink-runtime-1.20`
  and `iceberg-aws-bundle` at the same Iceberg version. Keep `flink-shaded-hadoop-2-uber`.
- **R-FLINK-4** To deploy a changed `pipeline.sql`: cancel the running job, then
  `docker compose run --rm flink-job`. NEVER remove the duplicate-job guard in `flink-job`.

## R-SQL: Queries (dashboard, docs, tests)
- **R-SQL-1** Read Iceberg `summary` keys with `element_at(summary, 'key')`, NEVER `summary['key']`
  (Flink's empty commits lack keys).
- **R-SQL-2** Time filters use `localtimestamp` (the dashboard session is UTC). Charts show UTC.
- **R-SQL-3** The SQL playground stays read-only (`SELECT/WITH/SHOW/DESCRIBE/EXPLAIN`). The only write
  in the dashboard is the explicit OPTIMIZE button.
- **R-SQL-5** Charts built on windowed gold tables MUST also show the still-open windows (computed from
  bronze, marked provisional), because windows don't close while the store is quiet.
- **R-SQL-6** Test connections to Trino MUST use `timezone="UTC"`, like the dashboard. Otherwise
  `localtimestamp` is local time, and "last N minutes" filters silently return nothing.
- **R-SQL-4** Any new dashboard query MUST be exercised against real Trino
  (`test_dashboard_queries_run_on_real_trino`). The fake Trino accepts any SQL.

## R-UI: Design system (details in [Design.md](Design.md))
- **R-UI-1** Theme colours go through `MODES` → CSS variables (`var(--ink)`, `var(--card)`, …).
  NEVER hardcode black or white in dashboard CSS. Text on bright fills uses `TILE_INK`.
- **R-UI-2** Charts: render with `draw()` inside `card()`; colours from `CATS` (per mode, fixed per
  category). New categorical palettes MUST pass the dataviz validator in both modes.
- **R-UI-3** Show code with the `code()` helper, NEVER `st.code` or ```` ```sql ```` fences (they render
  `[object Object]` after fragment re-runs in Streamlit 1.41).
- **R-UI-4** The CSS relies on Streamlit `data-testid`s. After bumping Streamlit, re-check the cards,
  tabs and KPI tiles in a browser.
- **R-UI-5** Accessibility basics: labelled inputs, keyboard-operable controls, visible focus, and
  colour is never the only signal.

## R-OPS: Docker and runtime
- **R-OPS-1** In `docker-compose.yml`, keep each shell command in `command:` on one line (YAML
  folded scalars keep newlines on more-indented lines).
- **R-OPS-2** Keep the stack under ~5 GB RAM. Trino heap (`jvm.config`) must stay well below its `mem_limit`.
- **R-OPS-3** The Iceberg catalog MUST run on Postgres (`CATALOG_URI=jdbc:postgresql://…`). NEVER go
  back to SQLite: its single-writer locking fails Flink's concurrent commits (`SQLITE_BUSY_SNAPSHOT`),
  and the job crash-loops.
- **R-OPS-4** Host ports: 8000 shop, 8081 Flink, 8088 Kafka UI, 8090 Trino, 8181 REST, 8501 dashboard,
  9000/9001 RustFS, 29092 Kafka. 8080 is avoided on purpose (usually taken).
- **R-OPS-5** Files mounted into containers are LF-only (`.gitattributes`). Don't remove it.
- **R-OPS-6** Pin image and package versions. Never use `:latest` in compose.

## R-GIT: Repository hygiene
- **R-GIT-1** Commit or push only when the owner asks. Commit messages explain *why*, and end with the
  co-author line configured for the session.
- **R-GIT-2** NEVER commit secrets, `.venv/`, `__pycache__`, or real personal data.
- **R-GIT-3** When behaviour changes, update the docs in the same change: README, `CLAUDE.md`, and
  the relevant `docs/ai/*` file (Architecture for interfaces, Rules for invariants, Memory for lessons,
  Phases for milestones).
- **R-GIT-4** When the UI changes visibly, regenerate the screenshots with `scripts/demo.py`, then
  review every image before committing.
