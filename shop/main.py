"""Demo storefront: serves the shop UI and turns real clicks into the pipeline's Kafka events.

  GET    /api/products          catalog (seeded from catalog/products.json, plus products added in /admin)
  POST   /api/products          add a product (admin)
  DELETE /api/products/{id}     remove an admin-added product (seed products are protected)
  GET    /api/catalog/options   categories, allowed emoji per category, badges (for the admin form)
  POST   /api/events            page_view | add_to_cart            -> topic `clicks`
  POST   /api/checkout          fake payment, one order per line   -> topic `orders`

Event fields and the UTC 'yyyy-MM-dd HH:mm:ss.SSS' timestamp format match flink/sql/pipeline.sql,
so the pipeline and dashboard need no changes. The browser only sends product ids and quantities:
prices, totals, ids and timestamps are set here, so the client can't tamper with them.

Payments are fake. Only published test numbers work; real card numbers are always rejected, and
card data is never stored, logged or sent to Kafka.
"""
import json
import os
import re
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

HERE = Path(__file__).resolve().parent
# Seed catalog. Repo: catalog/products.json; container: /catalog/products.json (mounted by compose).
SEED = HERE.parent / "catalog" / "products.json"

CATEGORIES = ["Beauty", "Books", "Electronics", "Fashion", "Home", "Sports"]
BADGES = ["Bestseller", "New", "Deal"]
# Product "photos" the admin can pick from. All render on Windows 10 (Emoji <= 12, below U+1FA70).
EMOJI = {
    "Beauty": ["💄", "💅", "🧴", "💆", "🧼", "🧽", "👄", "💋", "🌸", "🌹", "🎀", "✨", "💇", "☀️", "🧖"],
    "Books": ["📘", "📗", "📕", "📙", "📚", "📖", "📓", "📔", "📒", "🚀", "🍳", "🧠", "🔭", "🧪", "🗺️", "🐉"],
    "Electronics": ["🎧", "🖥️", "⌨️", "🔌", "📱", "💻", "🖱️", "📷", "🎮", "🔋", "⌚", "📺", "🔊", "🕹️", "💾", "🎙️"],
    "Fashion": ["👟", "🧥", "👕", "👛", "👗", "👖", "👜", "🧢", "🧣", "🧤", "👒", "👠", "🕶️", "👔", "🎒", "👞"],
    "Home": ["☕", "🌬️", "💡", "🌵", "🕯️", "🛋️", "🛏️", "🍽️", "🧺", "🧹", "🧸", "🖼️", "⏰", "🍵", "🌿"],
    "Sports": ["🧘", "🏋️", "🚴", "🧊", "⚽", "🏀", "🎾", "🏐", "🏓", "🥊", "⛸️", "🎿", "🏈", "⛳", "🥏", "🏊"],
}

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


# ---------------------------------------------------------------- catalog store (SQLite)
# The shop's operational store: seed products from products.json plus products added in /admin.
# Container: SHOP_DB=/data/shop.db on the `shop-data` volume, so added products survive restarts.
_initialized = set()


def db():
    path = Path(os.getenv("SHOP_DB") or HERE / "data" / "shop.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=10)
    con.row_factory = sqlite3.Row
    if str(path) not in _initialized:
        with con:
            con.execute("""CREATE TABLE IF NOT EXISTS products (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL, price REAL NOT NULL,
                emoji TEXT NOT NULL, description TEXT NOT NULL, rating REAL NOT NULL DEFAULT 0,
                reviews INTEGER NOT NULL DEFAULT 0, badge TEXT,
                created_at TEXT)""")  # created_at IS NULL means a seed product
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS products_name ON products(lower(name))")
            # Idempotent: new seed products reach existing databases, edits never overwrite them.
            for p in json.loads(SEED.read_text(encoding="utf-8")):
                con.execute("INSERT OR IGNORE INTO products (id, name, category, price, emoji, description, "
                            "rating, reviews, badge) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (p["id"], p["name"], p["category"], p["price"], p["emoji"], p["description"],
                             p["rating"], p["reviews"], p.get("badge")))
        _initialized.add(str(path))
    return con


def as_product(row):
    p = dict(row)
    p["seed"] = p.pop("created_at") is None
    return {k: v for k, v in p.items() if v is not None or k == "badge"}


def catalog():
    """All products, seed first then admin-added, as {id: product}."""
    with db() as con:
        rows = con.execute("SELECT * FROM products ORDER BY created_at IS NOT NULL, created_at, id").fetchall()
    return {r["id"]: as_product(r) for r in rows}


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


class NewProduct(BaseModel):
    name: str = Field(min_length=2, max_length=60, pattern=r"^[\w][\w .,'&()+/-]*$")
    category: Literal[tuple(CATEGORIES)]
    price: float = Field(gt=0, le=10000)
    emoji: str
    description: str = Field(min_length=10, max_length=400)
    badge: Optional[Literal[tuple(BADGES)]] = None

    @field_validator("name", "description", mode="before")
    @classmethod
    def tidy(cls, v):  # trim and collapse whitespace before the length/pattern checks
        return " ".join(v.split()) if isinstance(v, str) else v


class Checkout(BaseModel):
    user_id: str = Field(pattern=ID)
    session_id: str = Field(pattern=ID)
    name: str = Field(min_length=1, max_length=80)
    country: Literal[tuple(COUNTRIES)]
    items: List[Line] = Field(min_length=1, max_length=20)
    payment: Payment


# ---------------------------------------------------------------- pure logic (tested in test_shop.py)
def product(product_id):
    with db() as con:
        row = con.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if row is None:
        raise HTTPException(404, f"Unknown product {product_id}")
    return as_product(row)


def create_product(new: NewProduct, at):
    """Validate beyond the model (money precision, emoji list, unique name) and insert -> product."""
    name = new.name
    if round(new.price, 2) != new.price:
        raise HTTPException(422, "Price can have at most 2 decimals.")
    if new.emoji not in EMOJI[new.category]:
        raise HTTPException(422, f"Pick an emoji from the {new.category} set.")
    with db() as con:
        if con.execute("SELECT 1 FROM products WHERE lower(name) = lower(?)", (name,)).fetchone():
            raise HTTPException(409, f"A product called “{name}” already exists.")
        last = con.execute("SELECT max(CAST(substr(id, 2) AS INTEGER)) FROM products").fetchone()[0] or 0
        pid = f"P{last + 1:03d}"
        con.execute("INSERT INTO products (id, name, category, price, emoji, description, badge, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (pid, name, new.category, new.price, new.emoji, new.description, new.badge, ts(at)))
    return product(pid)


def delete_product(product_id):
    p = product(product_id)
    if p["seed"]:
        raise HTTPException(403, "Seed products can't be deleted.")
    with db() as con:
        con.execute("DELETE FROM products WHERE id = ?", (product_id,))


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
    return list(catalog().values())


@app.post("/api/products", status_code=201)
def add_product(new: NewProduct):
    # ponytail: no auth on the admin API; fine for a localhost demo, add a login before exposing it.
    return create_product(new, now())


@app.delete("/api/products/{product_id}", status_code=204)
def remove_product(product_id: str):
    delete_product(product_id)
    return Response(status_code=204)


@app.get("/api/catalog/options")
def catalog_options():
    return {"categories": CATEGORIES, "emoji": EMOJI, "badges": BADGES}


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
