# Memory

> Project memory for AI agents: decisions, the lessons behind them, and preferences, so nobody
> re-learns them the hard way. **Append** new entries at the top of §2 (newest first). Keep entries
> factual: what happened, why, and what we do now. Related rules in [Rules.md](Rules.md) are cited.

## 1. Owner preferences (standing instructions)
- **Test everything before calling it done** (R-TEST). The owner asked for this after a demo bug slipped
  through: run the full suite, run integration tests against the live stack, check UI in a browser.
- **Ask before pushing or publishing.** The owner approves commits and pushes explicitly.
- **Style:** neo-brutalism for both apps; bento grid for the store; dark mode on the dashboard.
- **Real over mock:** events should come from real UI interactions; the simulator is optional.
- **Docs for agents:** keep `CLAUDE.md` and `docs/ai/*` current when behaviour changes.
- **Docs describe the project only:** no personal background or motivation for building it.
- **Environment:** Windows 10, Docker Desktop with ~6 GB of RAM, local Python 3.9 (containers use
  3.12), Microsoft Edge available for Playwright, GitHub account `kanersharma`. The owner also runs an
  unrelated Airflow stack on port 8080; don't run it alongside this one on 6 GB.

## 2. Decision and incident log (newest first)

### 2026-09-28 · Inventory and demand forecasting (Phase 9); Docker toolbox
- **Owner decisions:** orders beyond stock are **rejected** (no back-orders), the forecast is **EWMA +
  linear trend**, and the Docker work is a **toolbox image** for tests and the demo.
- **Why the shop owns stock:** a checkout needs a synchronous, transactional yes/no. The lakehouse is
  ~10 s behind and can't give one. `BEGIN IMMEDIATE` serializes checkouts (R-INV-2); the lakehouse
  gets every movement as an event for analytics (R-INV-1, R-INV-4).
- **Product ids were reused after deletes.** Tests that created and deleted products on the live stack
  freed `P050`, and the next product took it, inheriting the deleted one's sales and stock history in
  Iceberg. **Now:** the next id is past the highest id ever seen, including `stock_movements` (R-CAT-4).
- **A sold-out product got no reorder suggestion** while the KPI said "Reorder now". Nobody can buy a
  sold-out product, so its recent days are zeros, the EWMA drops to 0, and so does the suggestion
  (censored demand). **Now:** at least the plain average, and at least `MIN_SOLD_OUT_DEMAND` (R-INV-6).
- **The e2e tests wrote to the developer database:** the shop's startup stock snapshot ran against
  `shop/data/shop.db`. **Now:** the e2e server gets its own temp `SHOP_DB`, and a session guard
  (`developer_db_untouched` in `conftest.py`) fails the run if the developer database changes (R-CAT-6).
- **The store was blank over plain http on any host but localhost** (the toolbox opens
  `http://shop:8000`, and so does a LAN IP): `crypto.randomUUID` exists only in secure contexts. **Now:**
  `crypto.getRandomValues`, and an e2e test opens the store under another hostname.
- **The toolbox base is pinned to bookworm:** `python:3.12-slim` moved to Debian 13, where Playwright
  1.49's `install --with-deps` fails (it asks for `ttf-unifont`) (R-OPS-7).
- **A dashboard test passed locally but failed in the toolbox:** it hardcoded `localhost:8000`, while
  the toolbox sets `SHOP_URL=http://shop:8000`. **Now:** the test sets `SHOP_URL` itself. Tests must not
  depend on the machine's environment.
- **After hours of demo traffic, Trino sat at its 1.5 GiB limit** with hundreds of small files per
  table: queries stalled (one integration test took 240 s, another run failed with "failed after 3
  attempts"). `OPTIMIZE` on every table brought it to 48 s. Before a long test or demo session on an
  old stack, compact (Internals tab) or restart Trino.
- **A `pytest.skip` inside a test's `if`** hid whether the interesting path ran at all. The test now
  builds its own data so the path always runs.
- **The screenshot review caught five more:** the add dialog numbered its sections 1, 2, 4, 3; the
  forecast chart's axis labels ran together (`labelSeparation`); `store-products` / `store-search`
  had never scrolled to the grid (since Phase 6: `scroll_into_view_if_needed` does nothing when the
  heading is already on screen); and the forecast's product picker jumped back to the first product
  within a minute. Streamlit resets a select whose options change, and the options were in urgency
  order, which shifts every demo day. **Now:** alphabetical options, with the default seeded to the
  most urgent product through `st.session_state`, plus a regression test. Finally, the Inventory tab
  looked faded ~40 % of the time: Streamlit fades elements while a fragment re-runs, and with a 10 s
  auto-refresh and 4–5 s queries that is a large part of every cycle. **Now:** a CSS rule keeps stale
  elements opaque (the header's RUNNING indicator still shows activity).

### 2026-09-28 · "I can't see revenue per minute": two root causes
- **1. The Flink job had crash-looped for 15+ minutes** (115 restarts). Every Iceberg commit failed with
  `SQLITE_BUSY_SNAPSHOT`. In WAL mode a pooled connection with a stale read snapshot can't write, and
  SQLite fails it immediately (`busy_timeout` doesn't help). Restarting the catalog only masked it.
  **Now:** the REST catalog runs on **Postgres** (a thin image adds the JDBC driver; R-OPS-3). Under
  the same 4-minute load: 0 restarts, sub-second checkpoints. The old SQLite catalog's tables weren't
  migrated. They were recreated empty; old Parquet files remain in S3 as orphans.
- **2. Windows don't close in a quiet store.** The watermark only advances with new events, so a single
  real order's minute never reached gold until someone else shopped. **Now:** the revenue chart adds
  provisional open minutes from bronze (R-SQL-5), with an integration test and a mutation check.
- **Also found:** the integration test's Trino session wasn't UTC, so `localtimestamp` filters returned
  nothing (R-SQL-6). `stars()` threw on ratings above 5 (`repeat(-1)`), caught by the e2e console guard.
  A random-reviews test was flaky because whole ratings displayed as "4" instead of "4.0"; the UI now
  always shows one decimal.
- **Self-inflicted restarts:** `docker compose up -d --build dashboard` also rebuilt and recreated `trino`
  and `iceberg-rest` (dependencies), so Flink lost the catalog for ~20 s and restarted (10 restores).
  Use `--no-deps` to rebuild a single service. Flink recovers on its own, but it looks alarming in the counters.
- **Watch:** small files grow about 2 per table per checkpoint, and after ~1 hour queries went from ~1 s
  to 2.5 s. Scheduled compaction (Phases 2.5) is the fix. The Compact button is the manual one.

### 2026-09-28 · Catalog moved into the shop's SQLite database
- **Why:** admin-added products must persist, and the next phase (inventory) needs transactional
  stock updates. A JSON file mounted read-only can't do either. SQLite is in the standard library,
  needs no extra service, and handles one writer (the shop) well.
- **How:** `catalog/products.json` became the *seed*. `INSERT OR IGNORE` on startup is idempotent and
  never overwrites edits. The simulator reads the live catalog from `GET /api/products`.
- **Found while testing:** names with leading spaces were rejected (the pattern check ran before
  trimming). Now a pydantic `mode="before"` validator trims and collapses whitespace first.
- **Owner's plan:** the next phase is inventory (stock, restock, forecasting and reorder suggestions);
  the design is in Phases.md §2.0.
- **Docker note:** after a Docker Desktop restart, BuildKit failed with "parent snapshot … does not
  exist". `docker builder prune -f` (build cache only) fixed it.

### 2026-09-28 · Screenshots on real data exposed four bugs
- **Iceberg `summary` keys are optional.** Flink commits *empty* snapshots on idle checkpoints (to
  advance its checkpoint bookkeeping); they have no `added-records`. `summary['added-records']`
  failed the whole Internals query. **Now:** `element_at(summary, …)` (R-SQL-1), plus an integration
  test that runs the real dashboard against real Trino (R-SQL-4). The fake Trino could never catch it.
- **A stale payment error** stayed visible after entering a valid card. **Now:** the error clears on any
  form input.
- **🪴 renders as a box on Windows 10** (Emoji 13). **Now:** 🌵, and a test caps emoji at < U+1FA70 (R-CAT-3).
- **The internals chart showed browser-local time** while everything else was UTC. **Now:** a UTC scale.
- **Lesson:** unit tests with fakes are not enough. Look at real output (R-TEST-5, R-GIT-4).

### 2026-09-28 · "Failed to fetch" on cash-on-delivery checkout
- The owner saw it in the browser pane. Root cause: the local dev server had been **stopped** while
  its page stayed open. COD itself was fine (proven with a test of the exact browser payload).
- **Now:** `app.js` shows "Can't reach the Lakeshop server…" (R-SHOP-5), with e2e tests that abort
  requests. This triggered the full test suite and the mandatory testing rule.
- **Lesson:** after local testing, stop the server *and* close its tab, or tell the owner the page is dead.

### 2026-09-28 · Storefront added; simulator made opt-in
- Real events via FastAPI; same schema, so no Flink change (R-EVT-1). The server owns money (R-SHOP-1).
  Only test cards work (R-SHOP-4). The shared catalog is `catalog/products.json`.
- The simulator moved behind `--profile simulator` because the owner wanted real events. Consequence:
  the dashboard is empty until someone shops, and the waiting message points to the store.

### 2026-09-28 · `st.code` renders "[object Object]" (Streamlit 1.41)
- Any syntax-highlighted block (`st.code`, and even ```` ```sql ```` markdown fences) breaks after a
  fragment re-run. Plain fences without a language work. **Now:** the `code()` helper (R-UI-3) and a
  regression test.

### 2026-09-28 · Dark mode via our own toggle, not Streamlit's theme
- Streamlit 1.50 (the newest version supporting local Python 3.9) has no `[theme.light]`/`[theme.dark]`,
  and `st.context.theme` is documented as unreliable during theme switches. **Now:** `?theme=` query
  param → `MODES` → CSS variables. Canvas data grids can't be styled by CSS, so dark mode inverts them.

### 2026-09-28 · Chart palette validated, not eyeballed
- Categorical colours were run through the dataviz validator. Stacked bars only need **adjacent**
  pairs to separate (not all pairs), which is why the palette works. Dark mode needed its own steps
  (the lightness band is 0.48–0.67).

### 2026-09-27 · Trino OOM-killed during OPTIMIZE (exit 137)
- The image default heap is 80 % of container RAM, which leaves no native headroom. **Now:**
  `trino/jvm.config` with `-Xmx1G` inside `mem_limit: 1536m` (R-OPS-2).

### 2026-09-27 · Duplicate Flink jobs risk
- `docker compose up` re-runs exited one-shot containers, so it would submit a second job and
  double-count data. **Now:** `flink-job` checks `/jobs/overview` first (R-FLINK-4).

### 2026-09-27 · Iceberg REST catalog: in-memory SQLite, then SQLITE_BUSY
- The fixture defaults to `jdbc:sqlite::memory:`, which is **per connection**, so tables "vanished"
  ("no such table: iceberg_tables"). A file-backed SQLite then failed with `SQLITE_BUSY` →
  `CommitStateUnknownException` → Flink crash loop, because 4 committers commit at once.
  **Now:** `?journal_mode=WAL&busy_timeout=30000` (R-OPS-3). The upgrade path is a Postgres catalog.

### 2026-09-27 · MinIO images no longer published
- `minio/minio` and `minio/mc` fail to pull (Docker Hub and quay) as of 2025. **Now:** RustFS 1.0
  (Apache-2.0, S3-compatible, has a console) and `amazon/aws-cli` for bucket creation.

### 2026-09-27 · Other setup lessons
- **YAML `>` folded scalars** keep newlines on more-indented lines, which broke `kafka-topics.sh` (R-OPS-1).
- **Port 8080** was taken by the owner's Airflow, so Trino moved to **8090** (R-OPS-4).
- **Docker VM froze** (API 500) when this stack and Airflow both ran on 6 GB. The fix was a Docker
  Desktop restart. Note that restarting Docker also restarts other stacks that have restart policies.
- **CRLF:** `core.autocrlf=true` on Windows would corrupt `jvm.config` inside Linux containers.
  **Now:** `.gitattributes` forces LF (R-OPS-5).
- **Flink + Iceberg needs Hadoop classes** even with a REST catalog: `flink-shaded-hadoop-2-uber`.
- **The first checkpoint may fail** with `UnknownHostException` if containers start out of order;
  the fixed-delay restart strategy recovers.
- **Headless Edge screenshots of Streamlit** capture only the loading skeleton (virtual time doesn't
  wait for websockets). Use Playwright with real waits (`scripts/demo.py`).

## 3. Useful facts
- End-to-end latency is ~7–15 s. Gold windows appear ~65 s after a minute starts.
- Stack memory is ~4.5–5 GB. Test suite: 217 tests, ~7 min locally, ~4.5 min in the toolbox.
- Test cards: `4242 4242 4242 4242` approves; `4000 0000 0000 0002` and `4000 0000 0000 9995` decline.
- The shop without Docker: `cd shop && uvicorn main:app --port 8000` prints events (dry run).
- Recreate screenshots: `docker compose run --rm demo` (or `.venv/Scripts/python scripts/demo.py`);
  ~7 min, needs the stack. Rebuild the toolbox first (`docker compose build tests`) if code changed.
- Inventory: 1 demo day = 60 s (`DEMO_DAY_SECONDS`), so 14 days of history is 14 minutes of traffic.
  Seed products start with 15–74 units; admin-added ones with 50.
