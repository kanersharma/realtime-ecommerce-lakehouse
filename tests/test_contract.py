"""Event contract: producer events == Avro schemas (schemas/*.avsc) == Flink source DDL (pipeline.sql)."""
import io
import json
import random
import re
from datetime import datetime, timezone
from decimal import Decimal

import fastavro
import pytest

import generator
import main as shop
from conftest import ROOT, source_columns, source_types

AT = datetime(2026, 9, 28, 12, 0, 0, 123000)
TOPICS = {"clicks": "clicks_src", "orders": "orders_src", "inventory": "inventory_src"}
AVRO_OF_SQL = {"STRING": "string", "INT": "int", "BIGINT": "long", "TIMESTAMP(3)": "timestamp-millis"}


def raw_schema(topic):
    return json.loads((ROOT / "schemas" / f"{topic}.avsc").read_text(encoding="utf-8"))


def roundtrip(topic, event, to_avro):
    """Serialize like the producer (to_avro + Avro binary), read it back like a consumer."""
    schema = fastavro.parse_schema(raw_schema(topic))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, to_avro(event))
    buf.seek(0)
    return fastavro.schemaless_reader(buf, schema)


def avro_type(field):
    t = field["type"]
    t = next(x for x in t if x != "null") if isinstance(t, list) else t
    if isinstance(t, dict):
        return f"DECIMAL({t['precision']}, {t['scale']})" if t["logicalType"] == "decimal" else t["logicalType"]
    return t


@pytest.mark.parametrize("topic", TOPICS)
def test_schema_fields_and_types_are_the_flink_source_columns(topic):
    """A type mismatch here would only show up at runtime, as a failing Flink job."""
    ddl = source_types(TOPICS[topic])
    schema = {f["name"]: avro_type(f) for f in raw_schema(topic)["fields"]}
    assert schema == {c: AVRO_OF_SQL.get(t, t) for c, t in ddl.items()}


def test_only_optional_fields_may_be_added_after_v1():
    """Fields added later (clicks.device) must be nullable with a default, or old and new events
    can't be read by the same reader (the registry enforces FULL compatibility)."""
    device = next(f for f in raw_schema("clicks")["fields"] if f["name"] == "device")
    assert device["type"][0] == "null" and device["default"] is None


def test_producer_events_survive_avro_exactly():
    e = shop.click_event(shop.Event(event_type="page_view", product_id="P001",
                                    user_id="W-abcdef", session_id="S-abcdef"), AT, "iPhone Mobile Safari")
    back = roundtrip("clicks", e, shop.to_avro)
    assert back["event_time"] == AT.replace(tzinfo=timezone.utc) and back["device"] == "mobile"
    c = shop.Checkout(user_id="W-abcdef", session_id="S-abcdef", name="A", country="IN",
                      items=[shop.Line(product_id="P002", quantity=3)], payment=shop.Payment(method="cod"))
    (o,) = shop.order_events(c, AT)
    back = roundtrip("orders", o, shop.to_avro)
    assert (back["unit_price"], back["total_amount"]) == (Decimal("329.00"), Decimal("987.00"))
    _, _, m = shop.inventory_event(shop.product("P001"), {"seq": 7, "reason": "order", "delta": -1,
                                                          "on_hand_after": 10, "at": shop.ts(AT)})
    assert roundtrip("inventory", m, shop.to_avro)["seq"] == 7


def test_simulator_events_survive_avro():
    rng = random.Random(3)
    for _ in range(50):
        for topic, _, e in generator.session_events(AT, rng):
            back = roundtrip(topic, e, generator.to_avro)
            assert back["event_time"].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] == e["event_time"]


def test_services_that_read_the_registry_url_get_it_from_compose():
    """The code defaults to localhost, which only works outside Docker: a container without the
    variable can't reach the registry (the dashboard's card once said "not reachable")."""
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    for service in ("shop", "generator", "dashboard"):
        code = "".join(f.read_text(encoding="utf-8") for f in (ROOT / service).glob("*.py"))
        if "SCHEMA_REGISTRY_URL" in code:
            block = re.search(rf"^  {service}:$(.*?)(?=^  \S|^volumes:)", compose, re.S | re.M).group(1)
            assert "SCHEMA_REGISTRY_URL: http://schema-registry:8081" in block, service


def test_pipeline_declares_expected_columns():
    assert "event_time" in source_columns("clicks_src")
    assert {"order_id", "total_amount", "payment_method"} <= source_columns("orders_src")


def test_shop_click_event_matches_clicks_src():
    e = shop.click_event(shop.Event(event_type="page_view", product_id="P001",
                                    user_id="W-abcdef", session_id="S-abcdef"), AT, "")
    assert set(e) == source_columns("clicks_src")


def test_shop_order_event_matches_orders_src():
    c = shop.Checkout(user_id="W-abcdef", session_id="S-abcdef", name="A", country="IN",
                      items=[shop.Line(product_id="P001", quantity=1)], payment=shop.Payment(method="cod"))
    (e,) = shop.order_events(c, AT)
    assert set(e) == source_columns("orders_src")


def test_shop_inventory_event_matches_inventory_src():
    p = shop.product("P001")
    m = {"seq": 1, "reason": "order", "delta": -1, "on_hand_after": 10, "at": shop.ts(AT)}
    topic, key, e = shop.inventory_event(p, m)
    assert (topic, key) == ("inventory", "P001")
    assert set(e) == source_columns("inventory_src")


def test_every_stock_change_emits_contract_shaped_events():
    """restock, settings, add, order, delete and snapshot all produce the same schema."""
    events = []
    p, ev = shop.create_product(shop.NewProduct(name="Contract Probe", category="Home", price=3.5, emoji="☕",
                                                description="Checks the inventory event contract.", stock=5), AT)
    events += ev
    events += shop.restock("P001", 3, AT)[1]
    events += shop.set_inventory_settings("P001", shop.InventorySettings(lead_time_days=4, target_cover_days=9), AT)[1]
    events += shop.reserve_stock(shop.Checkout(user_id="W-contract", session_id="S-contract", name="C", country="IN",
                                               items=[shop.Line(product_id="P001", quantity=1)],
                                               payment=shop.Payment(method="cod")), AT)
    events += shop.snapshot_stock(AT)
    events += shop.delete_product(p["id"], AT)
    assert {r["reason"] for _, _, r in events} == {"initial", "restock", "settings", "order", "snapshot", "removed"}
    assert all(set(e) == source_columns("inventory_src") for _, _, e in events)


def test_simulator_events_match_pipeline():
    seen = {"clicks": set(), "orders": set()}
    rng = random.Random(1)
    for _ in range(300):
        for topic, _, e in generator.session_events(AT, rng):
            seen[topic] |= {frozenset(e)}
    assert seen["clicks"] == {frozenset(source_columns("clicks_src"))}
    assert seen["orders"] == {frozenset(source_columns("orders_src"))}


def test_timestamp_format_is_flink_sql_json():
    assert shop.ts(AT) == generator.ts(AT) == "2026-09-28 12:00:00.123"


def test_categories_and_event_types_are_what_the_dashboard_counts():
    e = shop.click_event(shop.Event(event_type="add_to_cart", product_id="P015",
                                    user_id="W-abcdef", session_id="S-abcdef"), AT, "")
    assert e["event_type"] in ("page_view", "add_to_cart") and e["category"] == "Books"
