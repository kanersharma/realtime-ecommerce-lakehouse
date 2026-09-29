# AGENTS.md

How to run and work on this repo, for AI coding agents (Claude Code, Codex, Cursor, Gemini /
Antigravity, Copilot, …) and for people. Deeper context: [CLAUDE.md](CLAUDE.md) and [docs/ai/](docs/ai).

## First run (in this order)
1. **Docker is running** with at least 6 GB of memory: `docker info` (see "Total Memory").
2. **Ports:** `python scripts/ports.py` (Python 3.8+, standard library only).
   It checks every port the stack publishes. If another program already uses one, it picks the next
   free port, writes it to `.env` (Docker Compose reads that file automatically) and prints the URLs
   to open. Without Python: `cp .env.example .env` and change the busy ports by hand.
   - **Never stop, kill or reconfigure another program to free a port.** Move this stack's port.
   - Give the user the URLs `ports.py` printed, not the defaults, when a port was moved.
3. **Start:** `docker compose up -d --build`. The first build pulls ~1.5 GB and takes 5–10 minutes.
4. **Wait until it's ready** (about 2 minutes after the build):
   - `docker compose ps`: services `running` / `healthy`; the one-shot jobs `kafka-init`, `s3-init`
     and `flink-job` have exited with code 0.
   - The Flink job is `RUNNING`: `curl -s localhost:<FLINK_PORT>/jobs/overview` (default 8081).
   - Trino can show `unhealthy` for a minute or two on a first start (it's still loading): wait,
     don't restart it. The `maintenance` service retries until Trino answers.
5. **Open** the store and the dashboard (defaults http://localhost:8000 and http://localhost:8501),
   place an order, and watch it reach the dashboard within ~15 s.
6. Optional: `docker compose --profile simulator up -d` adds background traffic.

## When something goes wrong
| Symptom | Do this |
|---|---|
| `port is already allocated` / `address already in use` | `python scripts/ports.py`, then `docker compose up -d --build` again |
| Containers restart, or the Docker API returns 500 | Too little memory: give Docker ≥ 6 GB. Stop other stacks only with the user's OK |
| Dashboard says "Waiting for data" for more than 3 minutes | `docker compose logs flink-job`; Flink UI → the job → Exceptions |
| Trino slow or timing out | `docker compose logs maintenance`; `docker compose exec trino cat /sys/fs/cgroup/memory.pressure` |

More in the README's Troubleshooting section.

## Working on the code
- Rebuild one service with `docker compose up -d --build --no-deps <service>`. Without `--no-deps`,
  Compose also recreates its dependencies (Trino, the catalog), and the Flink job restarts.
- Before calling any change done, run the **whole** test suite with the stack up, so the integration
  tests run instead of skipping: `python -m pytest -rs` (dev deps: `requirements-dev.txt`), or
  `docker compose build tests && docker compose run --rm tests` with only Docker. Details and the full
  rulebook: [CLAUDE.md](CLAUDE.md) and [docs/ai/Rules.md](docs/ai/Rules.md).
- Stop with `docker compose down` (keeps the data). `docker compose down -v` deletes all data: ask the
  user first.
- `.env` is per machine and git-ignored: don't commit it. New settings go in `.env.example` too.
