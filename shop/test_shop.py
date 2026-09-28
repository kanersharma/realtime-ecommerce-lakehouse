"""Run: python shop/test_shop.py  (no Kafka needed)"""
from datetime import date, datetime

from fastapi import HTTPException

from main import CATALOG, Checkout, Event, Line, Payment, authorize, click_event, order_events

TODAY = date(2026, 9, 28)
card = lambda n, exp="12/30", cvc="123": Payment(method="card", card_number=n, expiry=exp, cvc=cvc)

# payments: only test cards work, real-looking numbers never do
assert authorize(card("4242 4242 4242 4242"), TODAY) is None
assert "declined" in authorize(card("4000 0000 0000 0002"), TODAY).lower()
assert "Insufficient" in authorize(card("4000000000009995"), TODAY)
assert "only test cards" in authorize(card("5555555555554444"), TODAY)
assert "Expiry" in authorize(card("4242424242424242", "08/26"), TODAY)   # past month
assert authorize(card("4242424242424242", "09/26"), TODAY) is None      # current month still valid
assert "CVC" in authorize(card("4242424242424242", cvc="12"), TODAY)
assert authorize(Payment(method="upi", upi_id="kaner@okbank"), TODAY) is None
assert "UPI" in authorize(Payment(method="upi", upi_id="not-a-vpa"), TODAY)
assert authorize(Payment(method="cod"), TODAY) is None

# events match the pipeline schema; prices come from the catalog, not the client
at = datetime(2026, 9, 28, 12, 0, 0, 123000)
click = click_event(Event(event_type="add_to_cart", product_id="P002", user_id="W-abc123", session_id="S-abc123"), at)
assert set(click) == {"event_id", "session_id", "user_id", "event_type", "product_id", "category", "event_time"}
assert click["event_time"] == "2026-09-28 12:00:00.123" and click["category"] == "Electronics"

c = Checkout(user_id="W-abc123", session_id="S-abc123", name="Kaner", country="IN",
             items=[Line(product_id="P002", quantity=2), Line(product_id="P024", quantity=1)],
             payment=Payment(method="cod"))
orders = order_events(c, at)
assert len(orders) == 2
assert orders[0]["total_amount"] == round(2 * CATALOG["P002"]["price"], 2)
assert {"order_id", "session_id", "user_id", "product_id", "product_name", "category", "quantity",
        "unit_price", "total_amount", "country", "payment_method", "event_time"} == set(orders[0])

try:
    order_events(c.model_copy(update={"items": [Line(product_id="NOPE", quantity=1)]}), at)
    raise AssertionError("unknown product must be rejected")
except HTTPException as err:
    assert err.status_code == 404

print("ok: payments, click events, order events")
