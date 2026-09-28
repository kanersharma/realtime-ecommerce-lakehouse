"""Demo storefront: serves the shop UI and turns real clicks into the pipeline's Kafka events.

  GET  /api/products   catalog (shared with the simulator: catalog/products.json)
  POST /api/events     page_view | add_to_cart            -> topic `clicks`
  POST /api/checkout   fake payment, one order per line   -> topic `orders`

Event fields and the UTC 'yyyy-MM-dd HH:mm:ss.SSS' timestamp format match flink/sql/pipeline.sql,
so the pipeline and dashboard need no changes. The browser only sends product ids and quantities:
prices, totals, ids and timestamps are set here, so the client can't tamper with them.

Payments are fake. Only published test numbers work; real card numbers are always rejected, and
card data is never stored, logged or sent to Kafka.
"""
import json
import os
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

HERE = Path(__file__).resolve().parent
# Repo: catalog/products.json; container: /catalog/products.json (mounted by docker-compose).
CATALOG = {p["id"]: p for p in json.loads(
    (HERE.parent / "catalog" / "products.json").read_text(encoding="utf-8"))}

COUNTRIES = ["IN", "US", "GB", "DE", "BR", "JP", "AU", "CA"]
ID = r"^[A-Za-z0-9-]{6,64}$"  # browser-generated user/session ids
TEST_CARDS = {  # Stripe-style test numbers
    "4242424242424242": None,  # approved
    "4000000000000002": "Card declined (test card 4000 0000 0000 0002).",
    "4000000000009995": "Insufficient funds (test card 4000 0000 0000 9995).",
}


def ts(dt):
    """Pipeline timestamp format: UTC, 'yyyy-MM-dd HH:mm:ss.SSS'."""
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------- request models
class Event(BaseModel):
    event_type: Literal["page_view", "add_to_cart"]
    product_id: str
    user_id: str = Field(pattern=ID)
    session_id: str = Field(pattern=ID)


class Line(BaseModel):
    product_id: str
    quantity: int = Field(ge=1, le=10)


class Payment(BaseModel):
    method: Literal["card", "upi", "cod"]
    card_number: Optional[str] = None
    expiry: Optional[str] = None  # MM/YY
    cvc: Optional[str] = None
    upi_id: Optional[str] = None


class Checkout(BaseModel):
    user_id: str = Field(pattern=ID)
    session_id: str = Field(pattern=ID)
    name: str = Field(min_length=1, max_length=80)
    country: Literal[tuple(COUNTRIES)]
    items: List[Line] = Field(min_length=1, max_length=20)
    payment: Payment


# ---------------------------------------------------------------- pure logic (tested in test_shop.py)
def product(product_id):
    if product_id not in CATALOG:
        raise HTTPException(404, f"Unknown product {product_id}")
    return CATALOG[product_id]


def authorize(p: Payment, today=None):
    """Fake payment check -> error message, or None when approved."""
    if p.method == "cod":
        return None
    if p.method == "upi":
        ok = re.fullmatch(r"[\w.-]{2,}@[A-Za-z]{2,}", p.upi_id or "")
        return None if ok else "Enter a UPI ID like name@okbank."
    number = re.sub(r"[\s-]", "", p.card_number or "")
    if number not in TEST_CARDS:
        return "Demo store: only test cards work. Try 4242 4242 4242 4242."
    m = re.fullmatch(r"(0[1-9]|1[0-2])/(\d{2})", p.expiry or "")
    today = today or date.today()
    if not m or (2000 + int(m[2]), int(m[1])) < (today.year, today.month):
        return "Expiry must be a future date as MM/YY."
    if not re.fullmatch(r"\d{3,4}", p.cvc or ""):
        return "CVC must be 3 or 4 digits."
    return TEST_CARDS[number]


def click_event(e: Event, at):
    p = product(e.product_id)
    return {"event_id": uuid.uuid4().hex, "session_id": e.session_id, "user_id": e.user_id,
            "event_type": e.event_type, "product_id": p["id"], "category": p["category"],
            "event_time": ts(at)}


def order_events(c: Checkout, at):
    """One order row per cart line: the grain of lakehouse.shop.orders."""
    events = []
    for line in c.items:
        p = product(line.product_id)
        events.append({
            "order_id": uuid.uuid4().hex, "session_id": c.session_id, "user_id": c.user_id,
            "product_id": p["id"], "product_name": p["name"], "category": p["category"],
            "quantity": line.quantity, "unit_price": p["price"],
            "total_amount": round(line.quantity * p["price"], 2),
            "country": c.country, "payment_method": c.payment.method,
            "event_time": ts(at),
        })
    return events


# ---------------------------------------------------------------- Kafka
_producer = None


def publish(topic, key, event):
    """Produce to Kafka, or print when KAFKA_BOOTSTRAP is unset (running the UI without the stack)."""
    global _producer
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    if not bootstrap:
        print(f"[dry-run] {topic}: {json.dumps(event)}", flush=True)
        return
    if _producer is None:
        from confluent_kafka import Producer
        _producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5})
    _producer.produce(topic, key=key, value=json.dumps(event))
    _producer.poll(0)


# ---------------------------------------------------------------- API
app = FastAPI(title="Lakeshop demo store")


@app.get("/api/products")
def products():
    return list(CATALOG.values())


@app.post("/api/events", status_code=202)
def track(e: Event):
    publish("clicks", e.user_id, click_event(e, now()))
    return {"ok": True}


@app.post("/api/checkout")
def checkout(c: Checkout):
    error = authorize(c.payment)
    if error:
        raise HTTPException(402, error)
    events = order_events(c, now())
    for event in events:
        publish("orders", c.user_id, event)
    card = re.sub(r"\D", "", c.payment.card_number or "")
    return {
        "order_ref": "LS-" + uuid.uuid4().hex[:8].upper(),
        "total": round(sum(e["total_amount"] for e in events), 2),
        "lines": len(events),
        "paid_with": f"card •••• {card[-4:]}" if c.payment.method == "card" else c.payment.method.upper(),
    }


app.mount("/", StaticFiles(directory=HERE / "static", html=True), name="static")
