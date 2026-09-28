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
