# CLAUDE.md

Context for AI coding assistants working on this repository. Read this before changing anything.

## What this is
A local, Docker Compose–based **streaming lakehouse** portfolio project:
`Lakeshop storefront (FastAPI + bento UI) → Kafka → Flink SQL → Apache Iceberg (REST catalog, S3 on RustFS) → Trino → Streamlit`.
An optional simulator (`generator/`) can add background traffic.
It has two goals: be easy to run (`docker compose up -d --build`) and show senior-level data
engineering (event time, watermarks, exactly-once, table maintenance, time travel).

## ⚠️ Testing is mandatory: every create or update, before calling it done
The owner's standing rule: **whenever anything is created or changed, run the tests and verify it works
end to end before making the final change (commit, push, or saying "done").** Concretely:

1. **Write or update tests in the same change.** A new feature gets new tests, and a bug fix gets a
   test that fails without the fix. Tests live in `tests/`:
   | File | Covers |
   |---|---|
   | `test_catalog.py` | catalog integrity; the 6 categories stay in sync across catalog, dashboard and shop UI |
   | `test_contract.py` | shop and simulator events have exactly the columns in `pipeline.sql` |
   | `test_shop_api.py` | every endpoint and payment method (card/UPI/COD), validation, price tampering, no card-data leaks |
   | `test_generator.py` | simulator funnel logic |
   | `test_dashboard.py` | dashboard via Streamlit `AppTest` with a fake Trino: KPIs, themes, waiting state, SQL guard |
   | `test_storefront_e2e.py` | real browser (Playwright + Edge): search, cart, all checkouts, server-down message, layout |
   | `test_integration.py` | running stack: a real order lands in Iceberg and is queryable in Trino |
2. **Run the whole suite**, not just the file you touched:
   ```bash
   python -m venv .venv && .venv/Scripts/python -m pip install -r requirements-dev.txt   # once
   .venv/Scripts/python -m pytest -rs
   ```
   Everything must pass. A failing test is fixed or explained to the owner, never deleted to go green.
3. **Integration tests must RUN, not skip**, for any change touching the shop, events, pipeline,
   catalog, Trino config or docker-compose: `docker compose up -d --build`, wait for the Flink job
   (`curl -s localhost:8081/jobs/overview` → RUNNING), then run pytest again. A skip in
   `test_integration.py` means the pipeline was **not** verified, so say so explicitly.
4. **Look at UI changes in a real browser** (store :8000, dashboard :8501, light and dark), including
   mobile width. Tests check behavior; your eyes check layout.
5. **Prove a new test can fail.** Break the code on purpose once, see the test go red, then restore it.
6. Report results honestly: pass/fail counts, what was skipped and why.

## Service map (docker-compose.yml)
| Service | Image | Port (host→container) | Notes |
|---|---|---|---|
| kafka | apache/kafka:3.9.0 | 29092 (host listener) | KRaft single node. In-network address: `kafka:9092` |
| kafka-init | apache/kafka:3.9.0 | – | one-shot: creates topics `clicks`, `orders` (3 partitions) |
| kafka-ui | kafbat/kafka-ui:v1.1.0 | 8088→8080 | optional, ~300 MB RAM |
| shop | ./shop | 8000 | Lakeshop storefront + API; produces to `clicks` / `orders`. `KAFKA_BOOTSTRAP` unset = dry run (prints events) |
| generator | ./generator | – | **opt-in**: `--profile simulator`; env `SESSIONS_PER_SEC` (default 20) |
| rustfs | rustfs/rustfs:1.0.0 | 9000 (S3), 9001 (console) | creds `admin` / `password` |
| s3-init | amazon/aws-cli | – | one-shot: creates bucket `warehouse` |
| iceberg-rest | apache/iceberg-rest-fixture:1.8.1 | 8181 | JDBC catalog on SQLite at `/home/iceberg/catalog.db` (volume) |
| jobmanager / taskmanager | ./flink (lakehouse-flink:1.20) | 8081 | config via `FLINK_PROPERTIES` in the `x-flink` anchor |
| flink-job | ./flink | – | one-shot: submits `pipeline.sql` via `sql-client.sh`, **skips if a job is already running**, exits 0 |
| trino | trinodb/trino:470 | **8090**→8080 | catalog from `trino/catalog/lakehouse.properties`; heap pinned in `trino/jvm.config` |
| dashboard | ./dashboard | 8501 | Streamlit, talks to `trino:8080` |

The catalog name `lakehouse` and schema `shop` are the same in Flink and Trino. Keep them in sync.

## Key files
- `flink/sql/pipeline.sql`: **the pipeline.** Kafka source DDL (temporary tables), Iceberg catalog,
  table DDL (`IF NOT EXISTS`), and one `EXECUTE STATEMENT SET` with 4 INSERTs.
- `flink/Dockerfile`: connector jars added via `ADD` from Maven Central. **Versions are coupled**:
  `flink-sql-connector-kafka-<ver>-1.20`, `iceberg-flink-runtime-1.20-<iceberg>`, and
  `iceberg-aws-bundle-<iceberg>` must match the Flink minor (1.20) and each other.
  `flink-shaded-hadoop-2-uber` is required because Iceberg's Flink catalog references
  `org.apache.hadoop.conf.Configuration`, even with a REST catalog.
- `trino/catalog/lakehouse.properties`: Iceberg REST + native S3 (`fs.native-s3.enabled`).
- `catalog/products.json`: the single product catalog, mounted at `/catalog/products.json` into shop and
  generator. Both load it as `Path(__file__).parent.parent / "catalog" / "products.json"`. Keep the six
  categories in sync with `CATEGORY_COLORS` (dashboard) and `CATS` (shop/static/app.js).
- `shop/main.py`: pure functions `authorize`, `click_event`, `order_events` (tested in `tests/`)
  plus the FastAPI routes. `shop/static/`: no-build UI (index.html, styles.css, app.js).
- `generator/generator.py`: `session_events(now, rng)` is pure and tested; `main()` does Kafka I/O.
- `tests/`: the whole test suite (pytest; config in `pytest.ini`, deps in `requirements-dev.txt`).
- `dashboard/app.py`: all SQL is in module-level constants; `query(sql) -> (DataFrame, ms)`.

## Commands
```bash
docker compose up -d --build                  # start everything
docker compose down -v                        # stop and wipe all data (Kafka, S3, catalog)
docker compose up -d --build dashboard        # rebuild one service after editing it
docker compose run --rm flink-job             # submit pipeline.sql (no-op if a job is already running)
docker compose exec trino trino --catalog lakehouse --schema shop   # SQL shell
.venv/Scripts/python -m pytest -rs           # ALL tests (see "Testing is mandatory" above)
.venv/Scripts/python -m pytest -m "not e2e"   # fast loop while iterating (still run everything before done)
cd shop && uvicorn main:app --port 8000       # UI without Docker: events are printed (dry run)
docker compose --profile simulator up -d      # add simulated background traffic
curl -s localhost:8081/jobs/overview          # Flink job state
```
To deploy a changed `pipeline.sql`, cancel the running job first (Flink UI, or
`curl -X PATCH localhost:8081/jobs/<jid>`), then `docker compose run --rm flink-job`. The guard in
`flink-job` refuses to submit while any job is running, because two jobs would write duplicate rows.

## How to verify a change
1. `curl -s localhost:8081/jobs/overview` → state `RUNNING`.
2. `curl -s localhost:8081/jobs/<jid>/checkpoints`: `completed` keeps growing, and `restored` stays 0.
   A growing `restored` count means the job is crash-looping. Read `/jobs/<jid>/exceptions`.
3. In Trino: `SELECT count(*), max(event_time) FROM orders` grows, and `max(event_time)` is within ~15 s of now (UTC).
4. The dashboard at :8501 shows KPIs (and the revenue chart after ~65 s).

## Conventions
- All timestamps are **UTC, timezone-naive** `TIMESTAMP(3)`. The generator emits
  `yyyy-MM-dd HH:mm:ss.SSS` (Flink JSON `SQL` format); the dashboard sets the Trino session to UTC,
  so `localtimestamp` is "now in UTC". Don't introduce `TIMESTAMP_LTZ` or `Z` suffixes without changing all three.
- **Event contract**: shop and generator emit exactly the columns declared in `pipeline.sql`
  (`clicks_src`, `orders_src`). Adding a field means changing all three plus the Iceberg DDL.
  One order event per cart line: `orders` has one product per row.
- **The shop server owns money and identity**: prices, totals, order ids and `event_time` are set in
  `main.py` from the catalog, never taken from the browser. Validate every request with pydantic models.
- **Payments are fake**: only the `TEST_CARDS` numbers may succeed, card inputs keep `autocomplete="off"`
  with non-standard names, and card data must never be logged, stored or put in an event.
- Flink DDL must stay idempotent (`CREATE ... IF NOT EXISTS`).
- Gold tables must stay **append-only** (window TVF aggregations). A regular `GROUP BY` without
  windows produces updates and would need an upsert-enabled Iceberg v2 table with a primary key.
- The dashboard is read-only, apart from the explicit OPTIMIZE button. The SQL playground only allows
  `SELECT/WITH/SHOW/DESCRIBE/EXPLAIN`.
- **UI style is neo-brutalist**: base colors in `dashboard/.streamlit/config.toml`, borders, shadows and
  fonts in the `CSS` constant at the top of `app.py`. Put charts inside `with card(title, tag):` and render
  them with `draw(altair_chart)` so they share styling. `CATEGORY_COLORS` is a fixed, colorblind-validated
  mapping, so don't let colors cycle by position.
- **Dark mode** is a sidebar toggle persisted as `?theme=dark` in the URL. Colors that flip live in `MODES`
  and reach the CSS as variables (`var(--ink)`, `var(--card)`, …), so never hardcode black or white in new
  CSS. Charts read `M["ink"]` / `M["grid"]` and `CATS` (the per-mode category colors). Bright fills (hero,
  KPI tiles) always use `TILE_INK` text. Data grids are canvas-drawn, so dark mode inverts them with a CSS filter.
- Deliberate shortcuts are marked with `ponytail:` comments that name the limit and the upgrade path.
- Keep the stack under ~5 GB RAM (see memory settings in compose). Many users run it on 8–16 GB laptops.

## Known gotchas (learned the hard way)
- **MinIO images are no longer published** (`minio/minio`, `minio/mc` fail to pull as of 2025).
  That's why storage is RustFS and bucket creation uses `amazon/aws-cli`.
- **REST fixture + in-memory SQLite** (the default) gives each pooled connection its own empty DB,
  so you get "no such table: iceberg_tables" or "Failed to load table". Keep the file-backed `CATALOG_URI`.
- **SQLITE_BUSY → `CommitStateUnknownException`**: the 4 Iceberg committers commit concurrently on
  each checkpoint. `?journal_mode=WAL&busy_timeout=30000` on `CATALOG_URI` is required.
- **YAML folded scalars (`>`)** keep newlines on more-indented lines, which breaks multi-line shell
  commands in `command:`. Keep each shell command on one line.
- **Trino OOM-kill (exit 137)**: the image default heap is 80 % of container RAM, which leaves no native
  headroom, so `OPTIMIZE` gets the container killed. `trino/jvm.config` pins `-Xmx1G` inside a `mem_limit: 1536m`.
- **Duplicate Flink jobs**: `docker compose up` re-runs exited one-shot containers. `flink-job` checks
  `/jobs/overview` before submitting. Keep that guard.
- **Streamlit upgrades can break the styling**: the CSS targets `data-testid` attributes
  (`stMetric`, `stColumn`, `stVerticalBlockBorderWrapper`, …), which Streamlit renames between versions.
  After bumping `streamlit` in `requirements.txt`, open the dashboard and check the cards, tabs and KPI tiles.
- **`st.code` / ```` ```sql ```` fences show `[object Object]`** after a fragment re-run (Streamlit 1.41
  syntax-highlighter bug). Use the `code()` helper in `app.py`, which renders a plain fence without a language.
- **Iceberg snapshot `summary` keys are optional.** Flink commits *empty* snapshots on idle checkpoints,
  and those have no `added-records` / `added-data-files`. `summary['key']` then fails the whole query,
  so always use `element_at(summary, 'key')`. The fake Trino in unit tests can't catch this; the
  integration test `test_dashboard_queries_run_on_real_trino` can.
- **"Failed to fetch" in the store** means the browser got no response at all (the shop server is down or
  was restarted under an open page), not an API error. `app.js` turns it into a readable message. Check
  `docker compose ps shop` first.
- **Port 8080** is commonly taken (Airflow), so Trino is published on 8090.
- Flink's first checkpoint can fail with `UnknownHostException` if containers start in the wrong order.
  The fixed-delay restart strategy recovers on its own.
- Docker Desktop with < 6 GB of memory freezes (API returns 500). Check `docker stats` first.

## Ideas that fit the architecture
Schema Registry + Avro; Flink CDC upsert table; late-event side output; scheduled Iceberg maintenance
(`expire_snapshots`, `remove_orphan_files`); Postgres-backed catalog; data-quality checks on gold tables.
