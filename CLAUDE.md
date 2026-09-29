# CLAUDE.md

Context for AI coding assistants working on this repository. Read this before changing anything.

## What this is
A local, Docker Compose–based **streaming lakehouse** project:
`Lakeshop storefront (FastAPI + bento UI) → Kafka (Avro + Schema Registry) → Flink SQL → Apache Iceberg (REST catalog, S3 on RustFS) → Trino → Streamlit`.
An optional simulator (`generator/`) can add background traffic. The shop also owns product **stock**
(SQLite); every stock movement streams to Iceberg, and the dashboard's 📦 Inventory tab forecasts demand
and suggests reorders.
It has two goals: be easy to run (`docker compose up -d --build`) and show senior-level data
engineering (event time, watermarks, exactly-once, table maintenance, time travel).

## 🚀 First run, and busy ports (see [AGENTS.md](AGENTS.md), the same steps for every AI tool)
1. `python scripts/ports.py`: checks every host port; if another program has one, it picks a free
   port, writes `.env` (Compose reads it) and prints the URLs. **Never stop or kill another program to
   free a port: move ours.** Tell the user the printed URLs.
2. `docker compose up -d --build`, then wait for the Flink job (`curl -s localhost:<FLINK_PORT>/jobs/overview`).

All host ports are `${VAR:-default}` in `docker-compose.yml` (`SHOP_PORT`, `DASHBOARD_PORT`,
`FLINK_PORT`, `REGISTRY_PORT`, `KAFKA_UI_PORT`, `TRINO_PORT`, `ICEBERG_REST_PORT`, `S3_PORT`,
`S3_CONSOLE_PORT`, `KAFKA_PORT`; see `.env.example`). Tests and `scripts/demo.py` find them through
`scripts/ports.py` (`host_port()`); the store and dashboard links follow them (`/config.js`, `*_LINK`).
Never hard-code a host port: `tests/test_ports.py` fails on it.

## 📚 Reference docs: read what's relevant before changing things
| Doc | Read it when… |
|---|---|
| [docs/ai/PRD.md](docs/ai/PRD.md) | you need the *why*: users, goals, requirements (S-/P-/D-/X- ids) |
| [docs/ai/Architecture.md](docs/ai/Architecture.md) | you touch interfaces: event contract, API, Flink job, tables, failure modes |
| [docs/ai/Rules.md](docs/ai/Rules.md) | **always**: the full rulebook (R-TEST, R-EVT, R-SHOP, R-CAT, R-INV, R-FLINK, R-SQL, R-UI, R-OPS, R-GIT) |
| [docs/ai/Design.md](docs/ai/Design.md) | you touch UI: tokens, category colours, components, bento grid |
| [docs/ai/Phases.md](docs/ai/Phases.md) | you plan work: what's delivered, roadmap with acceptance criteria |
| [docs/ai/Memory.md](docs/ai/Memory.md) | something looks odd: past incidents, decisions, owner preferences |

When behaviour changes, update the matching doc in the same change (R-GIT-3), and append lessons to Memory.md.

## ⚠️ Testing is mandatory: every create or update, before calling it done
The owner's standing rule: **whenever anything is created or changed, run the tests and verify it works
end to end before making the final change (commit, push, or saying "done").** Concretely:

1. **Write or update tests in the same change.** A new feature gets new tests, and a bug fix gets a
   test that fails without the fix. Tests live in `tests/`:
   | File | Covers |
   |---|---|
   | `test_catalog.py` | catalog integrity; the 6 categories stay in sync across catalog, dashboard and shop UI |
   | `test_contract.py` | producer events == Avro schemas (`schemas/*.avsc`) == Flink source DDL: names, types, Avro round trip |
   | `test_shop_api.py` | every endpoint and payment method (card/UPI/COD), validation, price tampering, no card-data leaks, device from User-Agent |
   | `test_catalog_api.py` | add/list/validate/persist/delete products, reviews rules; a new product can be bought; ids never reused |
   | `test_inventory_api.py` | stock: checkout takes it, 409 when short (no events), restock/settings, ledger, migration, no-oversell race |
   | `test_inventory_plan.py` | forecast + reorder math in `dashboard/inventory.py`, SQL builders and their injection guard |
   | `test_generator.py` | simulator funnel logic |
   | `test_ports.py` | host ports configurable (no hard-coded port in compose), `.env` handling, moving a busy port |
   | `test_maintenance.py` | scheduled maintenance: statements in order, retention/table-name validation, failures isolated per table |
   | `test_dashboard.py` | dashboard via Streamlit `AppTest` with a fake Trino (network stubbed): KPIs, themes, waiting state, SQL guard, Inventory tab (plan, Restock button, product picker), device and registry cards |
   | `test_storefront_e2e.py` | real browser (Playwright + Edge): search, cart, all checkouts, server-down message, layout, catalog admin (add → view → buy, validation, delete), stock limits and sold-out, non-secure context |
   | `test_integration.py` | running stack: real orders (including an admin-added product) land in Iceberg; dashboard SQL and every playground example run on real Trino; lakehouse stock matches the shop; topics carry registered Avro; the registry accepts only FULL-compatible changes; a phone's `device` lands in Iceberg; a maintenance run keeps every row and the pipeline running |
2. **Run the whole suite**, not just the file you touched:
   ```bash
   python -m venv .venv && .venv/Scripts/python -m pip install -r requirements-dev.txt   # once
   .venv/Scripts/python -m pytest -rs
   docker compose build tests && docker compose run --rm tests   # the same suite in the toolbox (Linux + Chromium)
   ```
   Everything must pass. A failing test is fixed or explained to the owner, never deleted to go green.
3. **Integration tests must RUN, not skip**, for any change touching the shop, events, pipeline,
   catalog, inventory, schemas, Trino config or docker-compose: `docker compose up -d --build`, wait for the Flink job
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
| kafka-init | apache/kafka:3.9.0 | – | one-shot: creates topics `clicks`, `orders`, `inventory` (3 partitions) |
| schema-registry | confluentinc/cp-schema-registry:7.9.10 | **8085**→8081 | Avro schemas (`<topic>-value`), compatibility **FULL**, heap 256 MB; stored in Kafka's `_schemas` topic |
| kafka-ui | kafbat/kafka-ui:v1.1.0 | 8088→8080 | optional, ~300 MB RAM; wired to the registry (schemas tab, decoded Avro) |
| shop | ./shop | 8000 | Lakeshop storefront, catalog admin (`/admin.html`) and API; produces Avro to `clicks` / `orders` / `inventory` (registers the schemas at startup, `SCHEMA_REGISTRY_URL`). Catalog **and stock** in SQLite at `SHOP_DB=/data/shop.db` (volume `shop-data`). `KAFKA_BOOTSTRAP` unset = dry run (prints JSON) |
| generator | ./generator | – | **opt-in**: `--profile simulator`; env `SESSIONS_PER_SEC` (default 20); orders go through the shop's checkout (`SHOP_URL`) |
| rustfs | rustfs/rustfs:1.0.0 | 9000 (S3), 9001 (console) | creds `admin` / `password` |
| s3-init | amazon/aws-cli | – | one-shot: creates bucket `warehouse` |
| postgres | postgres:16.4-alpine | – | database behind the Iceberg catalog (volume `catalog-db`) |
| iceberg-rest | ./iceberg-rest (fixture 1.8.1 + Postgres JDBC driver) | 8181 | Iceberg REST catalog, JDBC on Postgres |
| jobmanager / taskmanager | ./flink (lakehouse-flink:1.20) | 8081 | config via `FLINK_PROPERTIES` in the `x-flink` anchor |
| flink-job | ./flink | – | one-shot: submits `pipeline.sql` via `sql-client.sh`, **skips if a job is already running**, exits 0 |
| trino | ./trino (trinodb/trino:470 with **only the Iceberg plugin**) | **8090**→8080 | catalog from `trino/catalog/lakehouse.properties`; heap pinned in `trino/jvm.config` |
| maintenance | ./maintenance | – | every `MAINTENANCE_INTERVAL_MINUTES` (10) through Trino, for every table: `optimize`, `expire_snapshots` (`SNAPSHOT_RETENTION`, 1h), `remove_orphan_files`. `docker compose logs maintenance` |
| dashboard | ./dashboard | 8501 | Streamlit, talks to `trino:8080`; Restock buttons call `SHOP_URL`; `DEMO_DAY_SECONDS` (60); reads the registry (`SCHEMA_REGISTRY_URL`, default `localhost:8085`) |
| tests / demo | `.` (root `Dockerfile`, image `lakehouse-tools`) | – | **profile `tools`**, on demand: `docker compose run --rm tests` (whole suite) / `demo` (traffic + screenshots). Code is baked in: `docker compose build tests` after changes |

The catalog name `lakehouse` and schema `shop` are the same in Flink and Trino. Keep them in sync.

## Key files
- `schemas/*.avsc`: **the event contract** (Avro). Mounted at `/schemas` into shop and generator.
- `flink/sql/pipeline.sql`: **the pipeline.** Kafka source DDL (temporary tables, `avro-confluent`), Iceberg catalog,
  table DDL (`IF NOT EXISTS`), and one `EXECUTE STATEMENT SET` with 5 INSERTs.
- `flink/Dockerfile`: connector jars added via `ADD` from Maven Central. **Versions are coupled**:
  `flink-sql-connector-kafka-<ver>-1.20`, `flink-sql-avro-confluent-registry-1.20.x`,
  `iceberg-flink-runtime-1.20-<iceberg>`, and `iceberg-aws-bundle-<iceberg>` must match the Flink minor
  (1.20) and each other.
  `flink-shaded-hadoop-2-uber` is required because Iceberg's Flink catalog references
  `org.apache.hadoop.conf.Configuration`, even with a REST catalog.
- `trino/catalog/lakehouse.properties`: Iceberg REST + native S3 (`fs.native-s3.enabled`); minimum
  snapshot/orphan retention lowered to 10m for the maintenance service.
- `maintenance/maintain.py`: `statements()` and `run_once()` are pure and tested; `main()` loops.
- `catalog/products.json`: the **seed** catalog (48 products), mounted at `/catalog/products.json`. The shop
  seeds its SQLite database from it (`INSERT OR IGNORE`); the live catalog, including admin-added
  products, is `GET /api/products`, which the simulator also reads. Keep the six categories in sync with
  `CATEGORY_COLORS` (dashboard), `CATS` (`shop/static/common.js`) and `CATEGORIES`/`EMOJI` (`shop/main.py`).
- `shop/main.py`: catalog and stock store (`connect()`, `transaction()` = `BEGIN IMMEDIATE`, `catalog()`,
  `product()`, `create_product()`, `delete_product()`, `reserve_stock()`, `restock()`,
  `set_inventory_settings()`, `snapshot_stock()`), pure functions `authorize`, `device_of`, `click_event`,
  `order_events`, `inventory_event`, `to_avro`, the Kafka publisher (`kafka()`, `publish()`), and the
  FastAPI routes. `shop/static/`:
  no-build UIs: store (`index.html`, `app.js`), catalog admin (`admin.html`, `admin.js`), shared
  helpers (`common.js`), `styles.css`.
- `generator/generator.py`: `session_events(now, rng)` and `to_avro` are pure and tested; `main()` does Kafka I/O.
- `tests/`: the whole test suite (pytest; config in `pytest.ini`, deps in `requirements-dev.txt`).
- `dashboard/app.py`: all SQL is in module-level constants; `query(sql) -> (DataFrame, ms)`.
- `dashboard/inventory.py`: the Inventory tab's logic, kept out of Streamlit so it's unit-testable:
  `plan()` (forecast, safety stock, reorder point, suggestion, status), `inventory_sql()` and
  `daily_sales_sql()` (validates the product id).
- `Dockerfile` (root) + `.dockerignore`: the toolbox image for tests and the demo.
- `scripts/demo.py`: shopper traffic + screenshots (`docker compose run --rm demo`).

## Commands
```bash
docker compose up -d --build                  # start everything
docker compose down -v                        # stop and wipe all data (Kafka, S3, catalog)
docker compose up -d --build --no-deps dashboard   # rebuild ONE service; without --no-deps compose also
                                                   # recreates its dependencies (trino, iceberg-rest) and Flink restarts
docker compose run --rm flink-job             # submit pipeline.sql (no-op if a job is already running)
docker compose exec trino trino --catalog lakehouse --schema shop   # SQL shell
.venv/Scripts/python -m pytest -rs           # ALL tests (see "Testing is mandatory" above)
.venv/Scripts/python -m pytest -m "not e2e"   # fast loop while iterating (still run everything before done)
docker compose run --rm tests                 # the whole suite with only Docker (rebuild: docker compose build tests)
docker compose run --rm demo                  # shopper traffic + screenshots into docs/screenshots/
cd shop && uvicorn main:app --port 8000       # UI without Docker: events are printed (dry run)
docker compose --profile simulator up -d      # add simulated background traffic
curl -s localhost:8081/jobs/overview          # Flink job state
curl -s localhost:8085/subjects               # registered schemas (and /config for the compatibility level)
docker compose logs -f maintenance            # compaction / snapshot expiry per table, every 10 min
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
5. Stock: `SELECT product_id, on_hand_after FROM inventory_movements ORDER BY event_time DESC, seq DESC`
   matches `GET /api/products` (`on_hand`) within ~10 s.
6. Schemas: `curl -s localhost:8085/subjects` lists `clicks-value`, `orders-value`, `inventory-value`, and
   Kafka UI (:8088) shows decoded Avro messages.

## Conventions
- All timestamps are **UTC, timezone-naive** `TIMESTAMP(3)`. The generator emits
  `yyyy-MM-dd HH:mm:ss.SSS` (Flink JSON `SQL` format); the dashboard sets the Trino session to UTC,
  so `localtimestamp` is "now in UTC". Don't introduce `TIMESTAMP_LTZ` or `Z` suffixes without changing all three.
- **Event contract**: `schemas/*.avsc` (Avro, Schema Registry, compatibility **FULL**). Shop and generator
  emit exactly those fields; `pipeline.sql` (`clicks_src`, `orders_src`, `inventory_src`) declares the same
  columns and types. Add fields only as `["null", T]` with `"default": null`; never remove, rename or
  retype (Rules R-EVT-5). Adding a field means changing the schema, both producers, the source DDL, the
  Iceberg DDL and INSERT, and `ALTER TABLE … ADD COLUMN` on existing stacks.
  One order event per cart line: `orders` has one product per row.
- **Event dicts stay JSON-friendly** inside the producers (string timestamps, float prices); `to_avro()`
  converts at the serializer boundary (timestamp-millis, exact decimals). Dry runs print the JSON.
- **The shop server owns money and identity**: prices, totals, order ids and `event_time` are set in
  `main.py` from the catalog, never taken from the browser. Validate every request with pydantic models.
- **Payments are fake**: only the `TEST_CARDS` numbers may succeed, card inputs keep `autocomplete="off"`
  with non-standard names, and card data must never be logged, stored or put in an event.
- Flink DDL must stay idempotent (`CREATE ... IF NOT EXISTS`). Every Iceberg table gets the
  `ALTER TABLE … SET ('write.metadata.delete-after-commit.enabled' = 'true', …)` line in `pipeline.sql`,
  and the maintenance service picks up new tables on its own (`SHOW TABLES`).
- Gold tables must stay **append-only** (window TVF aggregations). A regular `GROUP BY` without
  windows produces updates and would need an upsert-enabled Iceberg v2 table with a primary key.
- **The shop is the system of record for stock** (Rules R-INV): read-modify-write inside
  `transaction()`, never negative, a short checkout is refused whole with 409 and emits nothing, a
  ledger row per change, events published after commit. Product ids are never reused.
- The dashboard is read-only, apart from the explicit OPTIMIZE button and the Restock buttons (which
  call the shop's API). The SQL playground only allows `SELECT/WITH/SHOW/DESCRIBE/EXPLAIN`.
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
- **Never put the Iceberg catalog on SQLite.** In-memory SQLite is per-connection (tables "vanish"), and
  file SQLite fails concurrent commits with `SQLITE_BUSY_SNAPSHOT`, so Flink crash-loops. The catalog
  runs on Postgres; `iceberg-rest/Dockerfile` adds the JDBC driver to the fixture.
- **Windows don't close in a quiet store** (the watermark needs newer events). `REVENUE_SQL` unions gold
  with open minutes from bronze. Keep that pattern for any new windowed chart.
- **Trino sessions in tests must be `timezone="UTC"`**, or `localtimestamp` filters return nothing.
- **YAML folded scalars (`>`)** keep newlines on more-indented lines, which breaks multi-line shell
  commands in `command:`. Keep each shell command on one line.
- **Trino stalls under memory pressure** when its native memory has no room next to the 1 GB heap: the
  stock image loads 56 plugins, and the kernel stalled Trino reclaiming memory up to 59% of the time
  (`/sys/fs/cgroup/memory.pressure` inside the container), so queries timed out. `trino/Dockerfile` keeps
  only the Iceberg plugin. If Trino gets slow, check that file's `full avg60` first.
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
- **Port 8080** is commonly taken (Airflow), so Trino is published on 8090. Any other clash:
  `python scripts/ports.py` moves our port via `.env`.
- Flink's first checkpoint can fail with `UnknownHostException` if containers start in the wrong order.
  The fixed-delay restart strategy recovers on its own.
- Docker Desktop with < 6 GB of memory freezes (API returns 500). Check `docker stats` first.
- **`crypto.randomUUID` exists only in secure contexts** (https, localhost). Over `http://shop:8000` or a
  LAN IP it's undefined, and the store rendered blank. `app.js` uses `crypto.getRandomValues`.
- **The toolbox base must stay `python:3.12-slim-bookworm`**: Playwright 1.49's `install --with-deps`
  fails on Debian 13.
- **Tests must never touch `shop/data/shop.db`** (the developer database). The e2e server uses a temp
  `SHOP_DB`; the session guard `developer_db_untouched` fails the run if it changes.
- **Topics carry Avro only.** The Flink sources have no "ignore parse errors": a JSON or raw message on
  `clicks`/`orders`/`inventory` fails the job (loud beats silently dropping events). Upgrading a stack
  from before Avro: stop producers, wait one checkpoint, `down`, `ALTER TABLE clicks ADD COLUMN device
  varchar` in Trino, then `up -d --build` (Architecture §8).
- **Kafka has no volume**: `docker compose down` empties topics and the registry's `_schemas` topic. The
  shop re-registers all schemas at startup, so that's harmless; Iceberg and products persist.
- **Avro enums don't work with Flink's string columns** (Avro can't resolve enum → string). Use `string`.
- **Unit tests must not touch the network**: on Windows, connecting to a closed localhost port takes ~2 s,
  which made the dashboard tests 4× slower. `test_dashboard.py` stubs `urllib.request.urlopen`.
- **Streaming metadata grows without maintenance**: every commit writes a new `metadata.json` holding
  the whole snapshot list, and Iceberg keeps the old ones by default. It reached 859 MB for 17 MB of
  data here, and Trino slowed and ran out of memory. The `maintenance` service plus
  `delete-after-commit` keep it bounded. `SNAPSHOT_RETENTION` can't go below Trino's 10m minimum.
- **Never `docker compose up` one service without `--no-deps`**: even starting the new `maintenance`
  service recreated Trino (its dependency) twice, and the catalog blink restarted Flink 12 times.
- **Streamlit resets a select when its options change.** Keep option lists in a stable order (the
  Inventory product picker is alphabetical, seeded via `st.session_state`), or auto-refresh throws the
  user's choice away.

## Ideas that fit the architecture
Flink CDC upsert table; late-event side output; stock in transit and a transactional outbox for inventory
events; data-quality checks on gold tables.
