"""Dashboard, run headless with streamlit.testing.v1.AppTest against a fake Trino."""
import io
import json
import random
import urllib.error
import urllib.request
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

    # product_id, name, category, on_hand, unit_price, lead, cover, history_days, level, trend, sigma, avg/day, sold_7d
    INVENTORY = [
        ("P002", "4K Monitor", "Electronics", 0, 329.0, 5, 7, 10, 0.5, 0.0, 0.5, 3.0, 21.0),   # out of stock
        ("P001", "Wireless Earbuds", "Electronics", 12, 59.99, 5, 7, 10, 4.0, 0.0, 0.0, 4.0, 28.0),  # reorder now
        ("P015", "Data Engineering Book", "Books", 60, 42.0, 2, 7, 10, 2.0, 0.0, 0.0, 2.0, 14.0),   # OK
        ("P049", "Game Controller", "Electronics", 50, 9.99, 5, 7, 0, 0.0, 0.0, 0.0, 0.0, 0.0),     # no demand
    ]

    def __init__(self, fail=False, inventory=None):
        self.fail, self.queries = fail, []
        self.inventory = self.INVENTORY if inventory is None else inventory

    def cursor(self):
        return self

    def execute(self, sql):
        self.queries.append(sql)
        if self.fail:
            raise RuntimeError("Table 'lakehouse.shop.orders' does not exist")
        cols, self.rows = self.data(sql, self.inventory)
        self.description = [(c,) for c in cols]

    def fetchall(self):
        return self.rows

    @staticmethod
    def data(sql, inventory):
        now = datetime(2026, 9, 28, 12, 0)
        rng = random.Random(0)
        if "inventory_movements" in sql and "regr_slope" in sql:
            return ["product_id", "product_name", "category", "on_hand", "unit_price", "lead_time_days",
                    "target_cover_days", "history_days", "level", "trend", "sigma", "avg_per_day", "sold_7d"], inventory
        if "coalesce(max(device)" in sql:
            return ["device", "sessions", "converted"], [("desktop", 120, 40), ("mobile", 90, 18),
                                                          ("unknown (before v2)", 10, 1)]
        if "sum(quantity) AS units" in sql and "product_id = 'P" in sql:
            return ["age", "units"], [(age, age % 4) for age in range(14, 0, -1)]
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


def unreachable(*args, **kwargs):
    raise urllib.error.URLError("no network in unit tests")


@pytest.fixture
def fake(monkeypatch):
    def make(fail=False, inventory=None):
        monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:9")  # never a live registry...
        monkeypatch.setattr(urllib.request, "urlopen", unreachable)     # ...and no slow connect attempts
        conn = FakeTrino(fail, inventory)
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
    assert len(at.tabs) == 4


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
    ("-- newest first\nSELECT * FROM orders", True),     # a leading comment is fine...
    ("-- looks harmless\nDROP TABLE orders", False),   # ...and can't smuggle a write past the guard
    ("-- only a comment", False),
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


def test_every_playground_example_passes_the_guard_and_runs(fake):
    """Examples go through the same Run button as typed SQL (the real-Trino integration test
    skips the guard, so it couldn't see an example the guard refused)."""
    conn = fake()
    at = run()
    def box():  # widgets must be looked up again after every run
        return next(s for s in at.selectbox if s.label == "Start from an example")

    names = box().options
    for name in names:
        box().set_value(name).run()
        next(b for b in at.button if b.label == "▶ Run").click().run()
        assert not at.exception and len(at.error) == 0, name
        assert any("rows ·" in c.value for c in at.caption), name


# ------------------------------------------------ 📦 Inventory tab
def test_inventory_tab_kpis_and_plan(fake):
    fake()
    at = run()
    assert not at.exception
    m = {x.label: x.value for x in at.metric}
    assert (m["Reorder now"], m["Out of stock"]) == ("2", "1")       # out of stock + at/below reorder point
    assert m["Stock value"] == "$3,739"                                # 12×59.99 + 60×42 + 50×9.99
    assert m["Units sold · 7 days"] == "63" and m["1 demo day"] == "60 s"
    # suggestions, most urgent first: the sold-out monitor, then the earbuds
    assert at.button(key="restock_P002").label == "Restock 38"          # ceil(3×12 + 1.65×0.5×√5) − 0
    assert at.button(key="restock_P001").label == "Restock 36"          # 4×12 − 12
    assert not any(b.key == "restock_P015" for b in at.button)         # OK: nothing to order


def shop_api(monkeypatch):
    """Replace the shop's restock endpoint; returns the list of calls it receives."""
    calls = []

    def urlopen(req, timeout):
        calls.append((req.full_url, req.get_method(), json.loads(req.data)))
        return io.BytesIO(json.dumps({"name": "4K Monitor", "on_hand": 38}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return calls


def test_restock_button_calls_the_shop(fake, monkeypatch):
    monkeypatch.setenv("SHOP_URL", "http://shop.test:8000")  # the toolbox sets its own SHOP_URL
    fake()
    calls = shop_api(monkeypatch)
    at = run()
    at.button(key="restock_P002").click().run()
    assert not at.exception
    assert calls == [("http://shop.test:8000/api/products/P002/restock", "POST", {"quantity": 38})]
    assert any("Restocked 4K Monitor: 38 on hand" in x.value for x in at.success)


def test_restock_all_orders_every_suggestion(fake, monkeypatch):
    fake()
    calls = shop_api(monkeypatch)
    at = run()
    at.button(key="restock_all").click().run()
    assert not at.exception
    assert [(url.rsplit("/", 2)[-2], body) for url, _, body in calls] == [
        ("P002", {"quantity": 38}), ("P001", {"quantity": 36})]
    assert any("Restocked 2 products" in x.value for x in at.success)


@pytest.mark.parametrize("button", ["restock_P002", "restock_all"])
def test_restock_says_so_when_the_shop_is_down(fake, monkeypatch, button):
    def down(req, timeout):
        raise urllib.error.URLError("Connection refused")

    fake()
    monkeypatch.setattr(urllib.request, "urlopen", down)
    at = run()
    at.button(key=button).click().run()
    assert not at.exception
    assert any("Restock failed" in e.value for e in at.error)


def test_inventory_tab_waits_for_stock_data(fake):
    fake(inventory=[])
    at = run()
    assert not at.exception
    assert any("No stock data yet" in i.value for i in at.info)


def test_inventory_demand_chart_query_uses_the_selected_product(fake):
    conn = fake()
    at = run()
    assert any("product_id = 'P002'" in q for q in conn.queries)       # default: the most urgent product
    at.selectbox(key="inv_product").set_value("Data Engineering Book").run()
    assert any("product_id = 'P015'" in q for q in conn.queries)


def test_picked_product_survives_a_refresh_that_reorders_products(fake):
    """Auto-refresh re-sorts products by urgency. Streamlit resets a select whose options change,
    so the options must not follow that order, or the pick jumps back to the first product."""
    conn = fake()
    at = run()
    at.selectbox(key="inv_product").set_value("Wireless Earbuds").run()
    conn.inventory = [(*r[:3], 0, *r[4:]) if r[0] == "P015" else r for r in conn.inventory]  # book sells out
    at.run()
    assert not at.exception
    assert at.selectbox(key="inv_product").value == "Wireless Earbuds"


# ------------------------------------------------ schema v2: device, and the registry card
def test_sessions_by_device_show_conversion_and_pre_v2_rows(fake):
    fake()
    at = run()
    assert not at.exception
    assert any("added to the click schema in v2" in c.value for c in at.caption)


def test_registry_card_lists_subjects_and_the_compatibility_rule(fake, monkeypatch):
    fake()
    answers = {"/config": {"compatibilityLevel": "FULL"},
               "/subjects": ["orders-value", "clicks-value", "inventory-value"],
               "/subjects/clicks-value/versions": [1, 2], "/subjects/orders-value/versions": [1],
               "/subjects/inventory-value/versions": [1]}

    def urlopen(url, timeout):
        return io.BytesIO(json.dumps(answers[url.split(":9", 1)[1]]).encode())

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    at = run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    assert "`clicks-value` · 2 version(s)" in text and "`orders-value` · 1 version(s)" in text
    assert any("Compatibility **FULL**" in c.value for c in at.caption)


def test_registry_card_survives_a_missing_registry(fake):
    fake()
    at = run()
    assert not at.exception
    assert any("Schema Registry not reachable" in c.value for c in at.caption)
