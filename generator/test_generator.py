"""Run: python generator/test_generator.py  (no Kafka needed)"""
import random
from datetime import datetime

from generator import session_events, ts

rng = random.Random(42)
now = datetime(2026, 1, 1, 12, 0, 0)
orders = 0
for _ in range(2000):
    events = session_events(now, rng)
    assert events[0][0] == "clicks" and events[0][2]["event_type"] == "page_view"
    times = [e["event_time"] for _, _, e in events]
    assert times == sorted(times), "events within a session must move forward in time"
    assert all(t <= ts(now) for t in times), "no event may be in the future"
    carted = set()
    for topic, key, e in events:
        assert key == e["user_id"]
        if topic == "clicks" and e["event_type"] == "add_to_cart":
            carted.add(e["product_id"])
        if topic == "orders":
            orders += 1
            assert e["product_id"] in carted, "an order needs a prior add_to_cart"
            assert e["total_amount"] == round(e["quantity"] * e["unit_price"], 2)
assert orders > 0
print(f"ok: 2000 sessions, {orders} orders")
