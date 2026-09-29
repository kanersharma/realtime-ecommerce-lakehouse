"""Scheduled Iceberg table maintenance, run through Trino (the `maintenance` service).

Every MAINTENANCE_INTERVAL_MINUTES, for every table in lakehouse.shop:
  1. optimize             compact the small files Flink writes on every checkpoint
  2. expire_snapshots     drop snapshots older than SNAPSHOT_RETENTION (and files only they used)
  3. remove_orphan_files  delete files no snapshot references (failed commits, leftovers), at most
                          hourly: it lists every object under the table (the slowest step, ~15 s a
                          table here), and orphans only come from failed commits

Flink keeps writing meanwhile. Iceberg's optimistic concurrency lets a compaction commit next to
Flink's appends, and the retention (default 1 h) is far longer than a checkpoint (10 s), so files
Flink has written but not committed yet are never "orphans".
"""
import os
import re
import time

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
INTERVAL_MINUTES = float(os.getenv("MAINTENANCE_INTERVAL_MINUTES", "10"))
RETENTION = os.getenv("SNAPSHOT_RETENTION", "1h")
ORPHANS_EVERY_SECONDS = 3600


def statements(table, retention, orphans=True):
    """The maintenance steps for one table, in order."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        raise ValueError(f"unexpected table name {table!r}")
    if not re.fullmatch(r"\d+(ms|s|m|h|d)", retention):
        raise ValueError(f"retention must look like 30m, 1h or 7d, not {retention!r}")
    steps = [f"ALTER TABLE {table} EXECUTE optimize",
             f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')"]
    if orphans:
        steps.append(f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')")
    return steps


def rows(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def stats(cur, table):
    """(live data files, snapshots) of a table."""
    return tuple(rows(cur, f'SELECT (SELECT count(*) FROM "{table}$files"), '
                           f'(SELECT count(*) FROM "{table}$snapshots")')[0])


def run_once(cur, retention=RETENTION, log=print, tables=None, orphans=True):
    """Maintain every table (or just `tables`) once.
    Returns {table: ((files, snapshots) before, after) or the error}."""
    results = {}
    for table in tables or [t for (t,) in rows(cur, "SHOW TABLES")]:
        start = time.time()
        try:
            before = stats(cur, table)
            for sql in statements(table, retention, orphans):
                rows(cur, sql)
            after = stats(cur, table)
            results[table] = (before, after)
            log(f"{table}: data files {before[0]} -> {after[0]}, snapshots {before[1]} -> {after[1]} "
                f"({time.time() - start:.1f} s)")
        except Exception as err:  # e.g. a commit conflict: log it, keep going, retry next round
            results[table] = err
            log(f"{table}: FAILED after {time.time() - start:.1f} s: {err}")
    return results


def main():
    import trino
    print(f"maintenance: every {INTERVAL_MINUTES:g} min, snapshots kept for {RETENTION}", flush=True)
    last_orphans = 0.0
    while True:
        start, wait = time.time(), INTERVAL_MINUTES * 60
        orphans = start - last_orphans >= ORPHANS_EVERY_SECONDS
        try:
            conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="maintenance",
                                       catalog="lakehouse", schema="shop")
            print(f"run: optimize + expire_snapshots{' + remove_orphan_files' if orphans else ''}", flush=True)
            run_once(conn.cursor(), log=lambda line: print(line, flush=True), orphans=orphans)
            last_orphans = start if orphans else last_orphans
        except Exception as err:  # Trino or the catalog not up yet: retry soon, not a whole interval later
            print(f"maintenance: run failed, retrying in 1 min: {err}", flush=True)
            wait = 60
        time.sleep(max(0.0, wait - (time.time() - start)))


if __name__ == "__main__":
    main()
