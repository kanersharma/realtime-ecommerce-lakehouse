"""Event contract: producers must emit exactly the columns Flink reads (flink/sql/pipeline.sql)."""
import random
from datetime import datetime

import generator
import main as shop
from conftest import source_columns

AT = datetime(2026, 9, 28, 12, 0, 0, 123000)


def test_pipeline_declares_expected_columns():
    assert "event_time" in source_columns("clicks_src")
    assert {"order_id", "total_amount", "payment_method"} <= source_columns("orders_src")


def test_shop_click_event_matches_clicks_src():
    e = shop.click_event(shop.Event(event_type="page_view", product_id="P001",
                                    user_id="W-abcdef", session_id="S-abcdef"), AT)
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
                                    user_id="W-abcdef", session_id="S-abcdef"), AT)
    assert e["event_type"] in ("page_view", "add_to_cart") and e["category"] == "Books"
