"""Dashboard, run headless with streamlit.testing.v1.AppTest against a fake Trino."""
import random
from datetime import datetime, timedelta

import pytest
import streamlit as st
import trino
from streamlit.testing.v1 import AppTest

from conftest import ROOT

APP = str(ROOT / "dashboard" / "app.py")
CATS = ["Beauty", "Books", "Electronics", "Fashion", "Home", "Sports"]


class FakeTrino:
    """Answers each dashboard query with plausible rows; records every SQL string it receives."""

    def __init__(self, fail=False):
        self.fail, self.queries = fail, []

    def cursor(self):
        return self

    def execute(self, sql):
        self.queries.append(sql)
        if self.fail:
            raise RuntimeError("Table 'lakehouse.shop.orders' does not exist")
        cols, self.rows = self.data(sql)
        self.description = [(c,) for c in cols]

    def fetchall(self):
        return self.rows

    @staticmethod
    def data(sql):
        now = datetime(2026, 9, 28, 12, 0)
        rng = random.Random(0)
        if "count_if(event_time >" in sql:
            return ["orders_now", "orders_prev", "revenue_now", "revenue_prev", "freshness_s"], [(1843, 1702, 162384.5, 150120.0, 14)]
        if "WITH c AS" in sql:
            return ["sessions", "page_views", "add_to_carts", "orders"], [(6120, 18240, 4560, 1812)]
        if "revenue_per_minute" in sql and "GROUP BY 1, 2" in sql:
            return ["minute", "category", "revenue", "status"], [
                (now - timedelta(minutes=m), c, rng.uniform(800, 6000), "live" if m <= 2 else "closed")
                for m in range(10, 0, -1) for c in CATS]
        if "product_name" in sql and "GROUP BY" in sql:
            return ["product_name", "category", "units", "revenue"], [("4K Monitor", "Electronics", 61, 20069.0)]
        if "country" in sql and "GROUP BY" in sql:
            return ["country", "revenue"], [("IN", 48000.0), ("US", 40100.0)]
        if "$files" in sql:
            return ["data_files", "total_bytes", "records"], [(92, 92 * 6400, 18520)]
        if "$snapshots" in sql:
            return (["committed_at", "snapshot_id", "operation", "added_records", "added_files", "total_files"],
                    [(now - timedelta(seconds=10 * i), 9_000_000 + i, "append", 50, 2, 92 - 2 * i) for i in range(20)])
        if "count(*) AS n" in sql:
            return ["n"], [(12004 if "VERSION" in sql else 18520,)]
        if sql.startswith("ALTER TABLE"):
            return ["rows"], [(92,)]
        return ["order_id", "product_name"], [("a1b2", "4K Monitor")]


@pytest.fixture
def fake(monkeypatch):
    def make(fail=False):
        conn = FakeTrino(fail)
        monkeypatch.setattr(trino.dbapi, "connect", lambda **kw: conn)
        st.cache_resource.clear()  # conn() is cached across runs
        return conn
    return make


def run(theme=None):
    at = AppTest.from_file(APP, default_timeout=60)
    if theme:
        at.query_params["theme"] = theme
    return at.run()


def test_live_tab_renders_kpis(fake):
    fake()
    at = run()
    assert not at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Revenue · last 5 min"] == "$162,384"
    assert metrics["Orders · last 5 min"] == "1,843"
    assert metrics["Conversion · 15 min"] == "29.6%"
    assert metrics["Data freshness"] == "14 s"
    assert len(at.tabs) == 3


def test_open_minutes_are_marked_provisional(fake):
    fake()
    at = run()
    assert any("hasn't closed yet" in c.value for c in at.caption)


def test_every_query_targets_the_lakehouse(fake):
    conn = fake()
    run()
    assert conn.queries and all("DROP" not in q.upper() and "DELETE" not in q.upper() for q in conn.queries)


def test_waiting_state_when_tables_do_not_exist(fake):
    fake(fail=True)
    at = run()
    assert not at.exception
    assert any("Waiting for data" in i.value for i in at.info)
    assert any("localhost:8000" in i.value for i in at.info)  # points people at the storefront


def test_sql_is_shown_as_plain_fence_not_st_code(fake):
    # st.code renders "[object Object]" after a fragment re-run in Streamlit 1.41 (regression guard)
    fake()
    at = run()
    assert len(at.code) == 0
    assert any(m.value.startswith("```\n") and "FROM orders" in m.value for m in at.markdown)


@pytest.mark.parametrize("theme,paper", [(None, "#FFFBEF"), ("light", "#FFFBEF"), ("dark", "#15130F")])
def test_theme_from_url(fake, theme, paper):
    fake()
    at = run(theme)
    assert at.toggle[0].value == (theme == "dark")
    css = next(m.value for m in at.markdown if "--paper" in m.value)
    assert f"--paper: {paper}" in css


def test_dark_mode_toggle_updates_url(fake):
    fake()
    at = run()
    at.toggle[0].set_value(True).run()
    assert not at.exception
    assert at.query_params["theme"] == ["dark"] or at.query_params["theme"] == "dark"


def test_internals_tab_and_compaction(fake):
    conn = fake()
    at = run()
    labels = {m.label: m.value for m in at.metric}
    assert labels["Live data files"] == "92" and labels["Records"] == "18,520"
    compact = next(b for b in at.button if "Compact" in b.label)
    compact.click().run()
    assert not at.exception
    assert any(q.startswith("ALTER TABLE orders EXECUTE optimize") for q in conn.queries)
    assert any("Done in" in s.value for s in at.success)


@pytest.mark.parametrize("sql,ok", [
    ("SELECT * FROM orders", True),
    ("  with x as (select 1) select * from x;", True),
    ("DROP TABLE orders", False),
    ("DELETE FROM orders", False),
    ("", False),
])
def test_sql_playground_is_read_only(fake, sql, ok):
    conn = fake()
    at = run()
    at.text_area[0].set_value(sql)
    next(b for b in at.button if b.label == "▶ Run").click().run()
    assert not at.exception
    ran = any(q.strip() == sql.strip().rstrip(";") for q in conn.queries) if sql.strip() else False
    assert ran == ok
    assert (len(at.error) == 0) == ok
