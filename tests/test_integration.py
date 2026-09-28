"""Full pipeline: a real order through the running shop must become queryable in Iceberg via Trino.

Needs the stack: docker compose up -d --build (shop on :8000, Trino on :8090). Skipped when it isn't up,
so a skip here means the end-to-end pipeline was NOT verified.
"""
import time
import urllib.error
import urllib.request
import uuid

import pytest

pytestmark = pytest.mark.integration
SHOP, TRINO_PORT = "http://localhost:8000", 8090
TIMEOUT = 120  # checkpoint every 10 s; allow for a cold Flink job


def reachable(url):
    try:
        urllib.request.urlopen(url, timeout=3)
        return True
    except (urllib.error.URLError, OSError):
        return False


@pytest.fixture(scope="module")
def stack():
    if not (reachable(f"{SHOP}/api/products") and reachable(f"http://localhost:{TRINO_PORT}/v1/info")):
        pytest.skip("Docker stack is not running (docker compose up -d --build)")
    import httpx
    import trino
    conn = trino.dbapi.connect(host="localhost", port=TRINO_PORT, user="integration-test",
                               catalog="lakehouse", schema="shop")
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


def test_dashboard_queries_run_on_real_trino(stack, monkeypatch):
    """Every dashboard query must be valid on real Trino and real Iceberg metadata.

    test_dashboard.py's fake Trino accepts any SQL; this caught `summary['added-records']` failing on
    Flink's empty commits, which only exist in real snapshot history.
    """
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from conftest import ROOT

    monkeypatch.setenv("TRINO_HOST", "localhost")
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
