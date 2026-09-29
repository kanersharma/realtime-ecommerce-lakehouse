"""Full pipeline: a real order through the running shop must become queryable in Iceberg via Trino.

Needs the stack: docker compose up -d --build (shop on :8000, Trino on :8090). Skipped when it isn't up,
so a skip here means the end-to-end pipeline was NOT verified. Inside the Docker toolbox
(`docker compose run --rm tests`) the hosts come from SHOP_URL / TRINO_HOST / TRINO_PORT.
"""
import ast
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import pytest
from ports import host_port  # scripts/ports.py: the environment, then .env, then the default port

pytestmark = pytest.mark.integration
SHOP = os.getenv("SHOP_URL", f"http://localhost:{host_port('SHOP_PORT')}")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = host_port("TRINO_PORT")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", f"localhost:{host_port('KAFKA_PORT')}")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", f"http://localhost:{host_port('REGISTRY_PORT')}")
IPHONE = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1"
TIMEOUT = 120  # checkpoint every 10 s; allow for a cold Flink job


def reachable(url, tries=3):
    """Generous on purpose: Trino busy with a maintenance run can miss a short probe, and a skip here
    silently means "not verified"."""
    for _ in range(tries):
        try:
            urllib.request.urlopen(url, timeout=10)
            return True
        except (urllib.error.URLError, OSError):
            time.sleep(2)
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


def in_stock(http, product_id, quantity):
    """Seed products' stock is shared with demo and simulator traffic, which can sell them out. Top it
    up through the shop (the system of record) so a sold-out product can't fail an unrelated test."""
    r = http.post(f"/api/products/{product_id}/restock", json={"quantity": quantity})
    assert r.status_code == 200, r.text


def test_a_real_order_reaches_the_lakehouse(stack):
    http, conn = stack
    user, session = f"W-it{uuid.uuid4().hex[:12]}", f"S-it{uuid.uuid4().hex[:12]}"
    for kind in ("page_view", "add_to_cart"):
        r = http.post("/api/events", json={"event_type": kind, "product_id": "P021",
                                           "user_id": user, "session_id": session})
        assert r.status_code == 202, r.text
    in_stock(http, "P021", 3)
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
    in_stock(http, "P002", 1)
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


# ------------------------------------------------ Avro + Schema Registry (Phase 10)
def registry(method, path, body=None):
    import httpx
    r = httpx.request(method, REGISTRY + path, json=body, timeout=10,
                      headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    r.raise_for_status()
    return r.json()


def partition_ends(consumer, topic):
    from confluent_kafka import TopicPartition
    return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(3)}


def test_every_topic_carries_registered_avro(stack):
    """Confluent wire format: magic byte 0, then the 4-byte id of a schema registered for the topic."""
    from confluent_kafka import Consumer, TopicPartition
    http, _ = stack
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"it-{uuid.uuid4().hex}",
                         "enable.auto.commit": False})
    try:
        in_stock(http, "P021", 1)
        topics = ("clicks", "orders", "inventory")
        before = {t: partition_ends(consumer, t) for t in topics}
        user, session = f"W-it{uuid.uuid4().hex[:12]}", f"S-it{uuid.uuid4().hex[:12]}"
        assert http.post("/api/events", json={"event_type": "page_view", "product_id": "P021",
                                              "user_id": user, "session_id": session}).status_code == 202
        r = http.post("/api/checkout", json={
            "user_id": user, "session_id": session, "name": "Integration Test", "country": "IN",
            "items": [{"product_id": "P021", "quantity": 1}], "payment": {"method": "cod"}})
        assert r.status_code == 200, r.text
        time.sleep(1)
        assert registry("GET", "/config")["compatibilityLevel"] == "FULL"
        for topic in topics:
            after = partition_ends(consumer, topic)
            consumer.assign([TopicPartition(topic, p, before[topic][p]) for p in after if after[p] > before[topic][p]])
            wanted, values, deadline = sum(after[p] - before[topic][p] for p in after), [], time.time() + 20
            while len(values) < wanted and time.time() < deadline:
                m = consumer.poll(1)
                if m is not None and not m.error():
                    values.append(m.value())
            assert values, f"no new {topic} messages"
            for v in values:
                assert v[0] == 0, f"{topic}: not Confluent Avro: {v[:40]!r}"
                subjects = registry("GET", f"/schemas/ids/{int.from_bytes(v[1:5], 'big')}/subjects")
                assert f"{topic}-value" in subjects
    finally:
        consumer.close()


def test_registry_accepts_only_compatible_schema_changes(stack):
    """FULL compatibility, checked against a scratch subject holding clicks v1 (before `device`)."""
    from conftest import ROOT
    v2 = json.loads((ROOT / "schemas" / "clicks.avsc").read_text(encoding="utf-8"))
    v1 = dict(v2, fields=[f for f in v2["fields"] if f["name"] != "device"])
    subject = f"it-evolution-{uuid.uuid4().hex[:8]}-value"
    registry("POST", f"/subjects/{subject}/versions", {"schema": json.dumps(v1)})
    try:
        def compatible(schema):
            return registry("POST", f"/compatibility/subjects/{subject}/versions/latest",
                            {"schema": json.dumps(schema)})["is_compatible"]

        assert compatible(v2)                                           # optional + default: accepted
        assert not compatible(dict(v1, fields=v1["fields"] + [{"name": "device", "type": "string"}]))
        assert not compatible(dict(v1, fields=[f for f in v1["fields"] if f["name"] != "category"]))
        assert not compatible(dict(v1, fields=[dict(f, type="string") if f["name"] == "event_time" else f
                                               for f in v1["fields"]]))  # changed type
    finally:
        registry("DELETE", f"/subjects/{subject}")
        registry("DELETE", f"/subjects/{subject}?permanent=true")


def test_the_device_of_a_phone_lands_in_iceberg(stack):
    http, conn = stack
    user, session = f"W-it{uuid.uuid4().hex[:12]}", f"S-it{uuid.uuid4().hex[:12]}"
    r = http.post("/api/events", headers={"User-Agent": IPHONE},
                  json={"event_type": "page_view", "product_id": "P001", "user_id": user, "session_id": session})
    assert r.status_code == 202, r.text
    poll(conn, f"SELECT device FROM clicks WHERE session_id = '{session}'", [["mobile"]])


def test_sql_playground_examples_run_on_real_trino(stack):
    """The fake Trino accepts any SQL; every example offered in the playground must really run."""
    from conftest import ROOT
    tree = ast.parse((ROOT / "dashboard" / "app.py").read_text(encoding="utf-8"))
    examples = next(ast.literal_eval(n.value) for n in tree.body
                    if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "EXAMPLES")
    _, conn = stack
    for name, sql in examples.items():
        query(conn, sql)  # raises on invalid SQL or a missing column


# ------------------------------------------------ scheduled maintenance (Phase 11)
def test_scheduled_maintenance_keeps_every_row_and_the_pipeline_running(stack):
    """One maintenance run on the live `orders` table: the compaction rewrites rows without losing any,
    snapshots past the retention are gone, and new orders still land afterwards."""
    import maintain
    http, conn = stack
    lines = []
    for attempt in range(2):  # the scheduled service may be rewriting the same table right now
        result = maintain.run_once(conn.cursor(), maintain.RETENTION, lines.append, tables=["orders"])["orders"]
        if not isinstance(result, Exception):
            break
        time.sleep(10)
    assert not isinstance(result, Exception), lines
    (files_before, _), (files_after, _) = result
    assert files_after <= files_before, lines

    rewrites = query(conn, 'SELECT element_at(summary, \'added-records\'), element_at(summary, \'deleted-records\') '
                           'FROM "orders$snapshots" WHERE operation = \'replace\'')
    assert all(added == deleted for added, deleted in rewrites), rewrites
    assert query(conn, 'SELECT count(*) FROM "orders$snapshots" '
                       "WHERE committed_at < current_timestamp - INTERVAL '65' MINUTE") == [[0]]

    session = f"S-it{uuid.uuid4().hex[:12]}"
    in_stock(http, "P021", 1)
    r = http.post("/api/checkout", json={
        "user_id": f"W-it{uuid.uuid4().hex[:12]}", "session_id": session, "name": "Integration Test",
        "country": "IN", "items": [{"product_id": "P021", "quantity": 1}], "payment": {"method": "cod"}})
    assert r.status_code == 200, r.text
    poll(conn, f"SELECT count(*) FROM orders WHERE session_id = '{session}'", [[1]])
