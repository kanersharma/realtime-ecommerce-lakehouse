"""Simulator (optional background traffic): funnel logic, no Kafka needed."""
import random
from datetime import datetime

import pytest

import generator

NOW = datetime(2026, 1, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def sessions():
    rng = random.Random(42)
    return [generator.session_events(NOW, rng) for _ in range(2000)]


def test_sessions_start_with_a_page_view(sessions):
    assert all(s[0][0] == "clicks" and s[0][2]["event_type"] == "page_view" for s in sessions)


def test_event_times_move_forward_and_never_future(sessions):
    for s in sessions:
        times = [e["event_time"] for _, _, e in s]
        assert times == sorted(times)
        assert all(t <= generator.ts(NOW) for t in times)


def test_messages_are_keyed_by_user(sessions):
    assert all(key == e["user_id"] for s in sessions for _, key, e in s)


def test_orders_follow_add_to_cart_and_prices_add_up(sessions):
    orders = 0
    for s in sessions:
        carted = set()
        for topic, _, e in s:
            if topic == "clicks" and e["event_type"] == "add_to_cart":
                carted.add(e["product_id"])
            if topic == "orders":
                orders += 1
                assert e["product_id"] in carted
                assert e["total_amount"] == round(e["quantity"] * e["unit_price"], 2)
    assert orders > 0


def test_simulator_reads_the_live_catalog_from_the_shop(monkeypatch):
    import io
    import json

    live = [{"id": "P049", "name": "Game Controller", "category": "Electronics", "price": 49.99}]
    monkeypatch.setattr(generator.urllib.request, "urlopen",
                        lambda url, timeout: io.BytesIO(json.dumps(live).encode()))
    assert generator.load_products("http://shop:8000") == [("P049", "Game Controller", "Electronics", 49.99)]


def test_simulator_falls_back_to_the_seed_file():
    seed = generator.load_products(None)
    assert generator.load_products("http://127.0.0.1:9") == seed  # nothing listens on port 9
    assert len(seed) == 48


def test_simulator_uses_the_shared_catalog():
    import json
    from conftest import ROOT
    catalog = json.loads((ROOT / "catalog" / "products.json").read_text(encoding="utf-8"))
    assert [p[0] for p in generator.PRODUCTS] == [p["id"] for p in catalog]


def test_simulated_orders_become_valid_shop_checkouts(sessions):
    """Orders go through the shop (it owns stock), so each session's orders must be a valid checkout."""
    import main as shop
    bodies = 0
    for s in sessions:
        orders = [e for topic, _, e in s if topic == "orders"]
        if not orders:
            continue
        body = generator.checkout_request(orders)
        c = shop.Checkout(**body)                                   # raises if the shop would reject it
        assert shop.authorize(c.payment) is None                     # only test credentials
        assert [(i.product_id, i.quantity) for i in c.items] == [(o["product_id"], o["quantity"]) for o in orders]
        bodies += 1
    assert bodies > 100


def test_a_session_has_one_device_and_a_realistic_mix(sessions):
    """Schema v2: every simulated click carries the session's device."""
    devices = {}
    for events in sessions:
        clicks = [e for topic, _, e in events if topic == "clicks"]
        assert len({e["device"] for e in clicks}) == 1
        devices[clicks[0]["device"]] = devices.get(clicks[0]["device"], 0) + 1
    assert set(devices) == set(generator.DEVICES) and devices["desktop"] > devices["tablet"]


def test_simulator_only_uses_payment_methods_the_shop_accepts(sessions):
    methods = {e["payment_method"] for s in sessions for t, _, e in s if t == "orders"}
    assert methods == {"card", "upi", "cod"}


def test_place_order_survives_a_missing_shop():
    assert generator.place_order("http://127.0.0.1:9", {"items": []}) == 0

