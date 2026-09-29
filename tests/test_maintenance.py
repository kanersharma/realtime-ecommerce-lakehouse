"""Scheduled maintenance (maintenance/maintain.py) against a fake Trino cursor; no stack needed."""
import pytest

import maintain


class FakeCursor:
    """Answers SHOW TABLES and the stats query; records every statement; can fail one table."""

    def __init__(self, tables=("clicks", "orders"), fail=None):
        self.tables, self.fail, self.sql, self.stats_calls = tables, fail, [], {}

    def execute(self, sql):
        self.sql.append(sql)
        if self.fail and sql.startswith(f"ALTER TABLE {self.fail} EXECUTE optimize"):
            raise RuntimeError("Failed to commit: concurrent update")
        if sql == "SHOW TABLES":
            self.result = [(t,) for t in self.tables]
        elif "$files" in sql:
            table = sql.split('"')[1].split("$")[0]
            n = self.stats_calls[table] = self.stats_calls.get(table, 0) + 1
            self.result = [(40, 300)] if n == 1 else [(2, 180)]  # before, then after
        else:
            self.result = [(0,)]

    def fetchall(self):
        return self.result


def test_three_steps_in_order_with_the_retention():
    assert maintain.statements("clicks", "1h") == [
        "ALTER TABLE clicks EXECUTE optimize",
        "ALTER TABLE clicks EXECUTE expire_snapshots(retention_threshold => '1h')",
        "ALTER TABLE clicks EXECUTE remove_orphan_files(retention_threshold => '1h')",
    ]


def test_orphan_cleanup_can_be_skipped():
    """The service removes orphans at most hourly: it's the slowest step and orphans are rare."""
    assert not any("remove_orphan_files" in s for s in maintain.statements("clicks", "1h", orphans=False))
    cur = FakeCursor()
    maintain.run_once(cur, "1h", lambda _: None, orphans=False)
    assert not any("remove_orphan_files" in s for s in cur.sql)
    assert sum("expire_snapshots" in s for s in cur.sql) == 2


@pytest.mark.parametrize("table,retention", [
    ("clicks", "1 hour"), ("clicks", "1h'; DROP TABLE orders --"), ("clicks", ""),
    ("Clicks; DROP TABLE orders", "1h"), ('clicks"', "1h"),
])
def test_bad_names_and_retentions_never_reach_sql(table, retention):
    with pytest.raises(ValueError):
        maintain.statements(table, retention)


def test_every_table_is_maintained_and_logged():
    cur, lines = FakeCursor(), []
    results = maintain.run_once(cur, "1h", lines.append)
    assert results == {"clicks": ((40, 300), (2, 180)), "orders": ((40, 300), (2, 180))}
    assert sum("EXECUTE optimize" in s for s in cur.sql) == 2
    assert lines[0] == "clicks: data files 40 -> 2, snapshots 300 -> 180 (0.0 s)"


def test_a_run_can_be_limited_to_some_tables():
    cur = FakeCursor()
    assert list(maintain.run_once(cur, "1h", lambda _: None, tables=["orders"])) == ["orders"]
    assert "SHOW TABLES" not in cur.sql and not any("clicks" in s for s in cur.sql)


def test_one_failing_table_does_not_stop_the_others():
    cur, lines = FakeCursor(tables=("clicks", "orders", "funnel_per_minute"), fail="orders"), []
    results = maintain.run_once(cur, "1h", lines.append)
    assert isinstance(results["orders"], RuntimeError)
    assert results["clicks"] == results["funnel_per_minute"] == ((40, 300), (2, 180))
    assert any(line.startswith("orders: FAILED") and "concurrent update" in line for line in lines)
    assert not any("orders EXECUTE expire_snapshots" in s for s in cur.sql)  # stopped at the failure
