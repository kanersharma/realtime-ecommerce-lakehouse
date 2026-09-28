# CLAUDE.md

Context for AI coding assistants working on this repository. Read this before changing anything.

## What this is
A local, Docker Compose–based **streaming lakehouse** portfolio project:
`generator (Python) → Kafka → Flink SQL → Apache Iceberg (REST catalog, S3 on RustFS) → Trino → Streamlit`.
It has two goals: be easy to run (`docker compose up -d --build`) and show senior-level data
engineering (event time, watermarks, exactly-once, table maintenance, time travel).

## Service map (docker-compose.yml)
| Service | Image | Port (host→container) | Notes |
|---|---|---|---|
| kafka | apache/kafka:3.9.0 | 29092 (host listener) | KRaft single node. In-network address: `kafka:9092` |
| kafka-init | apache/kafka:3.9.0 | – | one-shot: creates topics `clicks`, `orders` (3 partitions) |
| kafka-ui | kafbat/kafka-ui:v1.1.0 | 8088→8080 | optional, ~300 MB RAM |
| generator | ./generator | – | env `SESSIONS_PER_SEC` (default 20) |
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
- `generator/generator.py`: `session_events(now, rng)` is pure and tested; `main()` does Kafka I/O.
- `dashboard/app.py`: all SQL is in module-level constants; `query(sql) -> (DataFrame, ms)`.

## Commands
```bash
docker compose up -d --build                  # start everything
docker compose down -v                        # stop and wipe all data (Kafka, S3, catalog)
docker compose up -d --build dashboard        # rebuild one service after editing it
docker compose run --rm flink-job             # submit pipeline.sql (no-op if a job is already running)
docker compose exec trino trino --catalog lakehouse --schema shop   # SQL shell
python generator/test_generator.py            # generator self-check (no Kafka needed)
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
- Flink DDL must stay idempotent (`CREATE ... IF NOT EXISTS`).
- Gold tables must stay **append-only** (window TVF aggregations). A regular `GROUP BY` without
  windows produces updates and would need an upsert-enabled Iceberg v2 table with a primary key.
- The dashboard is read-only, apart from the explicit OPTIMIZE button. The SQL playground only allows
  `SELECT/WITH/SHOW/DESCRIBE/EXPLAIN`.
- **UI style is neo-brutalist**: base colors in `dashboard/.streamlit/config.toml`, borders, shadows and
  fonts in the `CSS` constant at the top of `app.py`. Put charts inside `with card(title, tag):` and render
  them with `draw(altair_chart)` so they share styling. `CATEGORY_COLORS` is a fixed, colorblind-validated
  mapping, so don't let colors cycle by position.
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
- **Port 8080** is commonly taken (Airflow), so Trino is published on 8090.
- Flink's first checkpoint can fail with `UnknownHostException` if containers start in the wrong order.
  The fixed-delay restart strategy recovers on its own.
- Docker Desktop with < 6 GB of memory freezes (API returns 500). Check `docker stats` first.

## Ideas that fit the architecture
Schema Registry + Avro; Flink CDC upsert table; late-event side output; scheduled Iceberg maintenance
(`expire_snapshots`, `remove_orphan_files`); Postgres-backed catalog; data-quality checks on gold tables.
