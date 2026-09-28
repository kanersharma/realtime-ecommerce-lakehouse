"""Full pipeline: a real order through the running shop must become queryable in Iceberg via Trino.

Needs the stack: docker compose up -d --build (shop on :8000, Trino on :8090). Skipped when it isn't up,
so a skip here means the end-to-end pipeline was NOT verified. Inside the Docker toolbox
(`docker compose run --rm tests`) the hosts come from SHOP_URL / TRINO_HOST / TRINO_PORT.
"""
import os
import time
import urllib.error
import urllib.request
import uuid

import pytest

pytestmark = pytest.mark.integration
SHOP = os.getenv("SHOP_URL", "http://localhost:8000")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8090"))
TIMEOUT = 120  # checkpoint every 10 s; allow for a cold Flink job


def reachable(url):
    try:
        urllib.request.urlopen(url, timeout=3)
        return True
    except (urllib.error.URLError, OSError):
        return False


@pytest.fixture(scope="module")
def stack():
    if not (reachable(f"{SHOP}/api/products") and reachable(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info")):
        pytest.skip("Docker stack is not running (docker compose up -d --build)")
    import httpx
    import trino
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="integration-test",
                               catalog="lakehouse", schema="shop",
                               timezone="UTC")  # like the dashboard: localtimestamp must be UTC
    return httpx.Client(base_url=SHOP, timeout=10), conn


def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def poll(conn, sql, expected):
    deadline, last = time.time() + TIMEOUT, None
    while time.time() < deadline:
        try:
            last = query(conn, sql)
            if last == expected:
                return
        except Exception as err:  # tables may not exist until the Flink job's first commit
            last = err
        time.sleep(3)
    pytest.fail(f"after {TIMEOUT}s: {sql!r} returned {last!r}, expected {expected!r}")


def test_a_real_order_reaches_the_lakehouse(stack):
    http, conn = stack
    user, session = f"W-it{uuid.uuid4().hex[:12]}", f"S-it{uuid.uuid4().hex[:12]}"
    for kind in ("page_view", "add_to_cart"):
        r = http.post("/api/events", json={"event_type": kind, "product_id": "P021",
                                           "user_id": user, "session_id": session})
        assert r.status_code == 202, r.text
    r = http.post("/api/checkout", json={
        "user_id": user, "session_id": session, "name": "Integration Test", "country": "JP",
        "items": [{"product_id": "P021", "quantity": 3}], "payment": {"method": "cod"}})
    assert r.status_code == 200, r.text

    poll(conn, f"SELECT count(*) FROM clicks WHERE session_id = '{session}'", [[2]])
    poll(conn, f"SELECT product_id, quantity, CAST(total_amount AS double), country, payment_method "
               f"FROM orders WHERE session_id = '{session}'", [["P021", 3, 144.0, "JP", "cod"]])


def test_admin_added_product_flows_to_the_lakehouse(stack):
    """A product created in the catalog admin can be bought, and its order lands in Iceberg."""
    http, conn = stack
    name = f"Integration Probe {uuid.uuid4().hex[:6]}"
    r = http.post("/api/products", json={"name": name, "category": "Home", "price": 12.34, "emoji": "☕",
                                         "description": "Temporary product created by the integration test."})
    assert r.status_code == 201, r.text
    pid = r.json()["id"]
    try:
        session = f"S-it{uuid.uuid4().hex[:12]}"
        r = http.post("/api/checkout", json={
            "user_id": f"W-it{uuid.uuid4().hex[:12]}", "session_id": session, "name": "Integration Test",
            "country": "CA", "items": [{"product_id": pid, "quantity": 2}], "payment": {"method": "cod"}})
        assert r.status_code == 200, r.text
        poll(conn, f"SELECT product_id, product_name, category, CAST(total_amount AS double) "
                   f"FROM orders WHERE session_id = '{session}'", [[pid, name, "Home", 24.68]])
    finally:  # keep the demo catalog clean; the order itself stays in the lakehouse
        assert http.delete(f"/api/products/{pid}").status_code == 204


def dashboard_sql(name):
    """A SQL constant from dashboard/app.py, so tests run exactly what the dashboard runs."""
    import ast
    from conftest import ROOT
    tree = ast.parse((ROOT / "dashboard" / "app.py").read_text(encoding="utf-8"))
    return next(n.value.value for n in tree.body
                if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == name)


def test_a_single_order_shows_in_revenue_per_minute_without_later_traffic(stack):
    """In a quiet store Flink's last window stays open (the watermark needs newer events), so the
    chart must still show that minute, computed live from bronze orders."""
    http, conn = stack
    session = f"S-it{uuid.uuid4().hex[:12]}"
    r = http.post("/api/checkout", json={
        "user_id": f"W-it{uuid.uuid4().hex[:12]}", "session_id": session, "name": "Integration Test",
        "country": "IN", "items": [{"product_id": "P002", "quantity": 1}], "payment": {"method": "cod"}})
    assert r.status_code == 200, r.text
    poll(conn, f"SELECT count(*) FROM orders WHERE session_id = '{session}'", [[1]])
    (minute,) = query(conn, f"SELECT date_trunc('minute', event_time) FROM orders WHERE session_id = '{session}'")[0]
    rows = query(conn, dashboard_sql("REVENUE_SQL"))
    electronics = sum(rev for m, cat, rev, status in rows if m == minute and cat == "Electronics")
    assert electronics >= 329.0, f"minute {minute} missing from the revenue chart: {rows[-6:]}"


def test_dashboard_queries_run_on_real_trino(stack, monkeypatch):
    """Every dashboard query must be valid on real Trino and real Iceberg metadata.

    test_dashboard.py's fake Trino accepts any SQL; this caught `summary['added-records']` failing on
    Flink's empty commits, which only exist in real snapshot history.
    """
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from conftest import ROOT

    monkeypatch.setenv("TRINO_HOST", TRINO_HOST)
    monkeypatch.setenv("TRINO_PORT", str(TRINO_PORT))
    st.cache_resource.clear()
    at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=120).run()

    def problems():
        return [i.value for i in at.info if "Waiting for data" in i.value] + [e.value for e in at.error]

    assert not at.exception
    assert problems() == [], [m.value for m in at.markdown if m.value.startswith("```\nTrino")]
    assert {"Revenue · last 5 min", "Live data files", "Records"} <= {m.label for m in at.metric}
    for table in ("orders", "clicks", "revenue_per_minute", "funnel_per_minute"):
        next(s for s in at.selectbox if s.label == "Table").set_value(table).run()
        assert not at.exception and problems() == [], table


def test_declined_payment_never_reaches_the_lakehouse(stack):
    http, conn = stack
    user, session = f"W-it{uuid.uuid4().hex[:12]}", f"S-it{uuid.uuid4().hex[:12]}"
    r = http.post("/api/checkout", json={
        "user_id": user, "session_id": session, "name": "Integration Test", "country": "IN",
        "items": [{"product_id": "P001", "quantity": 1}],
        "payment": {"method": "card", "card_number": "4000 0000 0000 0002", "expiry": "12/30", "cvc": "123"}})
    assert r.status_code == 402
    time.sleep(25)  # > 2 checkpoints
    assert query(conn, f"SELECT count(*) FROM orders WHERE session_id = '{session}'") == [[0]]


def test_stock_in_the_lakehouse_matches_the_shop(stack):
    """The shop owns stock; its movements must reach Iceberg so Trino's current stock equals the shop's,
    and the dashboard's inventory query must see it (and drop the product once deleted)."""
    import sys
    from conftest import ROOT
    sys.path.insert(0, str(ROOT / "dashboard"))
    import inventory as inv

    http, conn = stack
    name = f"Stock Probe {uuid.uuid4().hex[:6]}"
    r = http.post("/api/products", json={"name": name, "category": "Sports", "price": 7.5, "emoji": "⚽",
                                         "description": "Temporary product for the stock integration test.",
                                         "stock": 10})
    assert r.status_code == 201, r.text
    pid = r.json()["id"]
    try:
        r = http.post("/api/checkout", json={
            "user_id": f"W-it{uuid.uuid4().hex[:12]}", "session_id": f"S-it{uuid.uuid4().hex[:12]}",
            "name": "Integration Test", "country": "IN", "items": [{"product_id": pid, "quantity": 3}],
            "payment": {"method": "cod"}})
        assert r.status_code == 200, r.text
        assert http.post(f"/api/products/{pid}/restock", json={"quantity": 5}).json()["on_hand"] == 12
        shop_stock = {p["id"]: p["on_hand"] for p in http.get("/api/products").json()}[pid]

        poll(conn, f"SELECT reason, delta, on_hand_after FROM inventory_movements "
                   f"WHERE product_id = '{pid}' ORDER BY seq",
             [["initial", 10, 10], ["order", -3, 7], ["restock", 5, 12]])
        rows = {row[0]: row for row in query(conn, inv.inventory_sql())}
        assert rows[pid][3] == shop_stock == 12                     # on_hand column of the dashboard query
    finally:
        assert http.delete(f"/api/products/{pid}").status_code == 204
    poll(conn, f"SELECT count(*) FROM ({inv.inventory_sql()}) WHERE product_id = '{pid}'", [[0]])

