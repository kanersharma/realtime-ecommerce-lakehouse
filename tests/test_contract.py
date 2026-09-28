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
