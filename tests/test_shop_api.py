"""Storefront API through FastAPI's TestClient. Kafka is replaced by the `events` capture fixture."""
import re
from datetime import date, datetime

import pytest

import main as shop

USER, SESSION = "W-test0001", "S-test0001"
TEST_CARD = {"method": "card", "card_number": "4242 4242 4242 4242", "expiry": "12/30", "cvc": "123"}


def checkout_body(pay, **over):
    """A valid checkout (2 × P001 + 1 × P024) paid with `pay`; keyword args override any field."""
    body = {"user_id": USER, "session_id": SESSION, "name": "Test Shopper", "country": "IN",
            "items": [{"product_id": "P001", "quantity": 2}, {"product_id": "P024", "quantity": 1}],
            "payment": pay}
    body.update(over)
    return body


def orders(events):
    return [e for topic, _, e in events if topic == "orders"]


# ------------------------------------------------ catalog + static files
def test_products_endpoint(client):
    r = client.get("/api/products")
    assert r.status_code == 200
    assert len(r.json()) == 48
    assert all(p["seed"] for p in r.json())
    assert {"id", "name", "category", "price", "description", "emoji"} <= set(r.json()[0])


@pytest.mark.parametrize("path,kind", [("/", "text/html"), ("/app.js", "javascript"), ("/styles.css", "text/css")])
def test_ui_files_are_served(client, path, kind):
    r = client.get(path)
    assert r.status_code == 200 and kind in r.headers["content-type"]


# ------------------------------------------------ click events
@pytest.mark.parametrize("event_type", ["page_view", "add_to_cart"])
def test_click_events_go_to_clicks_topic(client, events, event_type):
    r = client.post("/api/events", json={"event_type": event_type, "product_id": "P015",
                                         "user_id": USER, "session_id": SESSION})
    assert r.status_code == 202
    ((topic, key, e),) = events
    assert topic == "clicks" and key == USER
    assert e["event_type"] == event_type and e["category"] == "Books" and e["session_id"] == SESSION
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}", e["event_time"])


IPHONE = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1"
ANDROID_PHONE = "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/126.0 Mobile Safari/537.36"
ANDROID_TABLET = "Mozilla/5.0 (Linux; Android 13; SM-X710) AppleWebKit/537.36 Chrome/126.0 Safari/537.36"
IPAD = "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1"
DESKTOP = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36 Edg/126.0"


@pytest.mark.parametrize("ua,device", [(IPHONE, "mobile"), (ANDROID_PHONE, "mobile"), (IPAD, "tablet"),
                                       (ANDROID_TABLET, "tablet"), (DESKTOP, "desktop"), ("", "desktop")])
def test_the_server_sets_the_device_from_the_user_agent(client, events, ua, device):
    """Schema v2 field. The server derives it, like prices: the browser's JSON can't set it."""
    r = client.post("/api/events", headers={"User-Agent": ua},
                    json={"event_type": "page_view", "product_id": "P015", "user_id": USER,
                          "session_id": SESSION, "device": "fridge"})
    assert r.status_code == 202 and events[0][2]["device"] == device


@pytest.mark.parametrize("patch,status", [
    ({"event_type": "purchase"}, 422),        # only the two funnel events
    ({"product_id": "NOPE"}, 404),
    ({"session_id": "x"}, 422),               # ids must look like browser-generated ids
    ({"user_id": "<script>"}, 422),
])
def test_bad_click_events_are_rejected(client, events, patch, status):
    body = {"event_type": "page_view", "product_id": "P001", "user_id": USER, "session_id": SESSION, **patch}
    assert client.post("/api/events", json=body).status_code == status
    assert events == []


# ------------------------------------------------ checkout: every payment method
@pytest.mark.parametrize("payment,paid_with", [
    ({"method": "cod"}, "COD"),
    # what the browser really sends for COD: empty strings for the hidden card/UPI fields
    ({"method": "cod", "card_number": "", "expiry": "", "cvc": "", "upi_id": ""}, "COD"),
    ({"method": "upi", "upi_id": "kaner@okbank"}, "UPI"),
    (TEST_CARD, "card •••• 4242"),
    ({**TEST_CARD, "card_number": "4242424242424242"}, "card •••• 4242"),
])
def test_successful_checkout(client, events, payment, paid_with):
    r = client.post("/api/checkout", json=checkout_body(payment))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["paid_with"] == paid_with
    assert re.fullmatch(r"LS-[0-9A-F]{8}", body["order_ref"])
    lines = orders(events)
    assert len(lines) == body["lines"] == 2                       # one order event per cart line
    assert body["total"] == round(2 * 59.99 + 9.99, 2) == round(sum(e["total_amount"] for e in lines), 2)
    assert all(e["payment_method"] == payment["method"] and e["session_id"] == SESSION for e in lines)
    assert len({e["order_id"] for e in lines}) == 2


@pytest.mark.parametrize("payment,message", [
    ({**TEST_CARD, "card_number": "4000 0000 0000 0002"}, "declined"),
    ({**TEST_CARD, "card_number": "4000000000009995"}, "Insufficient funds"),
    ({**TEST_CARD, "card_number": "5555 5555 5555 4444"}, "only test cards"),   # real-looking card
    ({**TEST_CARD, "card_number": ""}, "only test cards"),
    ({**TEST_CARD, "expiry": "01/20"}, "Expiry"),
    ({**TEST_CARD, "expiry": "13/30"}, "Expiry"),
    ({**TEST_CARD, "cvc": "12"}, "CVC"),
    ({"method": "upi", "upi_id": "not-a-vpa"}, "UPI"),
    ({"method": "upi"}, "UPI"),
])
def test_failed_payments_emit_no_orders(client, events, payment, message):
    r = client.post("/api/checkout", json=checkout_body(payment))
    assert r.status_code == 402
    assert message.lower() in r.json()["detail"].lower()
    assert events == []


def test_expiry_in_current_month_is_valid():
    today = date(2026, 9, 28)
    assert shop.authorize(shop.Payment(**{**TEST_CARD, "expiry": "09/26"}), today) is None
    assert "Expiry" in shop.authorize(shop.Payment(**{**TEST_CARD, "expiry": "08/26"}), today)


# ------------------------------------------------ checkout: validation and tampering
@pytest.mark.parametrize("over,status", [
    ({"items": []}, 422),
    ({"items": [{"product_id": "P001", "quantity": 0}]}, 422),
    ({"items": [{"product_id": "P001", "quantity": 11}]}, 422),
    ({"items": [{"product_id": "P001", "quantity": 1}] * 21}, 422),
    ({"country": "XX"}, 422),
    ({"name": ""}, 422),
    ({"payment": {"method": "bitcoin"}}, 422),
    ({"items": [{"product_id": "P001", "quantity": 1}, {"product_id": "NOPE", "quantity": 1}]}, 404),
])
def test_invalid_checkouts_are_rejected_without_partial_orders(client, events, over, status):
    assert client.post("/api/checkout", json=checkout_body({"method": "cod"}, **over)).status_code == status
    assert events == []


def test_client_cannot_set_prices(client, events):
    tampered = [{"product_id": "P002", "quantity": 1, "price": 0.01, "unit_price": 0.01, "total_amount": 0.01}]
    r = client.post("/api/checkout", json=checkout_body({"method": "cod"}, items=tampered, total=0.01))
    assert r.status_code == 200
    (e,) = orders(events)
    assert e["unit_price"] == e["total_amount"] == r.json()["total"] == 329.00


def test_card_data_never_leaves_the_request(client, events, capsys):
    client.post("/api/checkout", json=checkout_body(TEST_CARD))
    client.post("/api/checkout", json=checkout_body({**TEST_CARD, "card_number": "4000 0000 0000 0002"}))
    leaked = str(events) + capsys.readouterr().out
    for secret in ("4242424242424242", "4242 4242", "4000", "12/30", "'123'", "cvc", "expiry"):
        assert secret not in leaked


def test_dry_run_prints_events_when_kafka_is_not_configured(monkeypatch, capsys):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    shop.publish("orders", "W-x", {"order_id": "abc"})
    assert '[dry-run] orders: {"order_id": "abc"}' in capsys.readouterr().out


def test_event_time_is_set_by_server_in_utc(client, events):
    client.post("/api/events", json={"event_type": "page_view", "product_id": "P001",
                                     "user_id": USER, "session_id": SESSION})
    t = datetime.strptime(events[0][2]["event_time"], "%Y-%m-%d %H:%M:%S.%f")
    assert abs((shop.now() - t).total_seconds()) < 5


def test_config_js_carries_the_configured_links(client, monkeypatch):
    r = client.get("/config.js")
    assert r.status_code == 200 and "javascript" in r.headers["content-type"]
    assert '"dashboard": "http://localhost:8501"' in r.text and '"kafka_ui": "http://localhost:8088"' in r.text
    monkeypatch.setenv("DASHBOARD_LINK", "http://localhost:18501")
    assert '"dashboard": "http://localhost:18501"' in client.get("/config.js").text
