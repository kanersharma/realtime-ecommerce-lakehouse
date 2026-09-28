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
- **Environment:** Windows 10, Docker Desktop with ~6 GB of RAM, local Python 3.9 (containers use
  3.12), Microsoft Edge available for Playwright, GitHub account `kanersharma`. The owner also runs an
  unrelated Airflow stack on port 8080; don't run it alongside this one on 6 GB.

## 2. Decision and incident log (newest first)

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
- Stack memory is ~4.5–5 GB. Test suite: 91 tests, ~2.5 min with e2e and integration.
- Test cards: `4242 4242 4242 4242` approves; `4000 0000 0000 0002` and `4000 0000 0000 9995` decline.
- The shop without Docker: `cd shop && uvicorn main:app --port 8000` prints events (dry run).
- Recreate screenshots: `.venv/Scripts/python scripts/demo.py` (~6 min, needs the stack).
