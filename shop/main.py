"""Demo storefront: serves the shop UI and turns real clicks into the pipeline's Kafka events.

  GET    /api/products                  catalog with stock (seed products + products added in /admin)
  POST   /api/products                  add a product (admin), with starting stock
  DELETE /api/products/{id}             remove an admin-added product (seed products are protected)
  PUT    /api/products/{id}/reviews     set rating + review count of an admin-added product
  POST   /api/products/{id}/restock     add stock                            -> topic `inventory`
  PUT    /api/products/{id}/inventory   lead time / target cover days        -> topic `inventory`
  GET    /api/catalog/options           categories, allowed emoji per category, badges (admin form)
  POST   /api/events                    page_view | add_to_cart              -> topic `clicks`
  POST   /api/checkout                  fake payment, stock reserved atomically,
                                        one order per line                   -> `orders` + `inventory`

Events are Avro (schemas/*.avsc), registered in the Schema Registry, whose FULL compatibility rule
rejects breaking changes; the shop registers all three schemas at startup, so an incompatible schema
stops it from starting instead of failing a checkout. The browser only sends product ids and
quantities: prices, totals, ids, stock, timestamps and the device are set here, so the client can't
tamper with them.

Stock: the shop is the system of record. Checkout reserves every line in one SQLite write
transaction (all or nothing), so stock never goes negative and orders beyond it get 409. Every stock
change is a row in the `stock_movements` ledger (its `seq` orders them) and an `inventory` event.

Payments are fake. Only published test numbers work; real card numbers are always rejected, and
card data is never stored, logged or sent to Kafka.
"""
import json
import os
import re
import sqlite3
import threading
import uuid
from collections import Counter
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

HERE = Path(__file__).resolve().parent
# Seed catalog. Repo: catalog/products.json; container: /catalog/products.json (mounted by compose).
SEED = HERE.parent / "catalog" / "products.json"
# Event schemas (the contract with Flink). Repo: schemas/; container: /schemas (mounted by compose).
SCHEMAS = HERE.parent / "schemas"
TOPICS = ["clicks", "orders", "inventory"]

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

# Inventory defaults. "Days" are demo days: the dashboard maps one to DEMO_DAY_SECONDS (default 60 s).
LEAD_TIME_DAYS = {"Beauty": 2, "Books": 2, "Electronics": 5, "Fashion": 4, "Home": 3, "Sports": 3}
TARGET_COVER_DAYS = 7   # days of demand a refill should cover
ADDED_STARTING_STOCK = 50  # default for products added in the admin (and for pre-inventory databases)

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


def starting_stock(product_id):
    """Deterministic 15-74 units per seed product, so some sell out much sooner than others."""
    return 15 + (int(product_id[1:]) * 37) % 60


# ---------------------------------------------------------------- store (SQLite)
# The shop's operational store: products (catalog + stock) and the stock_movements ledger.
# Container: SHOP_DB=/data/shop.db on the `shop-data` volume, so it survives restarts.
# Writes use BEGIN IMMEDIATE (take the write lock first), so concurrent checkouts serialize instead
# of reading the same stock. That is safe on SQLite because the shop is its only writer; the Iceberg
# catalog, with many concurrent writers, is on Postgres (docs/ai/Memory.md).
_initialized = set()
_init_lock = threading.Lock()


@contextmanager
def connect():
    """Autocommit connection (each statement commits); use transaction() for multi-statement writes."""
    path = Path(os.getenv("SHOP_DB") or HERE / "data" / "shop.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=10, isolation_level=None)
    con.row_factory = sqlite3.Row
    try:
        if str(path) not in _initialized:
            with _init_lock:
                if str(path) not in _initialized:
                    _init(con)
                    _initialized.add(str(path))
        yield con
    finally:
        con.close()


@contextmanager
def transaction():
    with connect() as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            yield con
        except BaseException:
            con.execute("ROLLBACK")
            raise
        con.execute("COMMIT")


def _init(con):
    """Create or migrate the schema and seed the catalog. Idempotent: new seed products reach
    existing databases, and nothing already there is overwritten."""
    con.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
    con.execute("BEGIN IMMEDIATE")
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL, price REAL NOT NULL,
            emoji TEXT NOT NULL, description TEXT NOT NULL, rating REAL NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0, badge TEXT,
            created_at TEXT)""")  # created_at IS NULL means a seed product
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS products_name ON products(lower(name))")
        # Inventory columns arrived in Phase 9; ALTER TABLE upgrades databases created before it.
        migrating = "on_hand" not in {r["name"] for r in con.execute("PRAGMA table_info(products)")}
        if migrating:
            con.execute("ALTER TABLE products ADD COLUMN on_hand INTEGER NOT NULL DEFAULT 0")
            con.execute("ALTER TABLE products ADD COLUMN lead_time_days INTEGER NOT NULL DEFAULT 3")
            con.execute(f"ALTER TABLE products ADD COLUMN target_cover_days INTEGER NOT NULL "
                        f"DEFAULT {TARGET_COVER_DAYS}")
        con.execute("""CREATE TABLE IF NOT EXISTS stock_movements (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, product_id TEXT NOT NULL, reason TEXT NOT NULL,
            delta INTEGER NOT NULL, on_hand_after INTEGER NOT NULL, at TEXT NOT NULL)""")
        at = ts(now())
        for p in json.loads(SEED.read_text(encoding="utf-8")):
            stock = starting_stock(p["id"])
            cur = con.execute(
                "INSERT OR IGNORE INTO products (id, name, category, price, emoji, description, rating, "
                "reviews, badge, on_hand, lead_time_days, target_cover_days) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (p["id"], p["name"], p["category"], p["price"], p["emoji"], p["description"], p["rating"],
                 p["reviews"], p.get("badge"), stock, LEAD_TIME_DAYS[p["category"]], TARGET_COVER_DAYS))
            if cur.rowcount:
                _move(con, p["id"], "initial", stock, stock, at)
        if migrating:  # products that existed before inventory get starting stock too
            for r in con.execute("SELECT id, category, created_at FROM products WHERE on_hand = 0").fetchall():
                stock = starting_stock(r["id"]) if r["created_at"] is None else ADDED_STARTING_STOCK
                con.execute("UPDATE products SET on_hand = ?, lead_time_days = ? WHERE id = ?",
                            (stock, LEAD_TIME_DAYS[r["category"]], r["id"]))
                _move(con, r["id"], "initial", stock, stock, at)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _move(con, product_id, reason, delta, on_hand_after, at):
    """Append to the stock ledger; `seq` gives every movement a global order."""
    cur = con.execute("INSERT INTO stock_movements (product_id, reason, delta, on_hand_after, at) "
                      "VALUES (?, ?, ?, ?, ?)", (product_id, reason, delta, on_hand_after, at))
    return {"seq": cur.lastrowid, "reason": reason, "delta": delta, "on_hand_after": on_hand_after, "at": at}


def _row(con, product_id):
    row = con.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if row is None:
        raise HTTPException(404, f"Unknown product {product_id}")
    return row


def as_product(row):
    p = dict(row)
    p["seed"] = p.pop("created_at") is None
    return {k: v for k, v in p.items() if v is not None or k == "badge"}


def catalog():
    """All products, seed first then admin-added, as {id: product}."""
    with connect() as con:
        rows = con.execute("SELECT * FROM products ORDER BY created_at IS NOT NULL, created_at, id").fetchall()
    return {r["id"]: as_product(r) for r in rows}


def product(product_id):
    with connect() as con:
        return as_product(_row(con, product_id))


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


class Reviews(BaseModel):
    rating: float = Field(default=0, ge=0, le=5)
    reviews: int = Field(default=0, ge=0, le=100_000)


class Restock(BaseModel):
    quantity: int = Field(ge=1, le=10_000)


class InventorySettings(BaseModel):
    lead_time_days: int = Field(ge=1, le=60)       # days a refill takes to arrive
    target_cover_days: int = Field(ge=1, le=90)    # days of demand a refill should cover


class NewProduct(BaseModel):
    name: str = Field(min_length=2, max_length=60, pattern=r"^[\w][\w .,'&()+/-]*$")
    category: Literal[tuple(CATEGORIES)]
    price: float = Field(gt=0, le=10000)
    emoji: str
    description: str = Field(min_length=10, max_length=400)
    badge: Optional[Literal[tuple(BADGES)]] = None
    rating: float = Field(default=0, ge=0, le=5)          # optional starting reviews (manual or random
    reviews: int = Field(default=0, ge=0, le=100_000)     # in the admin); 0/0 shows "No reviews yet"
    stock: int = Field(default=ADDED_STARTING_STOCK, ge=0, le=10_000)
    lead_time_days: Optional[int] = Field(default=None, ge=1, le=60)   # None: the category's default
    target_cover_days: int = Field(default=TARGET_COVER_DAYS, ge=1, le=90)

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


# ---------------------------------------------------------------- catalog logic
def check_reviews(rating, reviews):
    """A rating needs reviews and vice versa; ratings have one decimal, like the seed data."""
    if round(rating, 1) != rating:
        raise HTTPException(422, "Rating can have at most 1 decimal.")
    if reviews == 0 and rating != 0:
        raise HTTPException(422, "A product without reviews can't have a rating.")
    if reviews > 0 and rating < 1:
        raise HTTPException(422, "Rating must be between 1.0 and 5.0 when there are reviews.")


def set_reviews(product_id, r: Reviews):
    if product(product_id)["seed"]:
        raise HTTPException(403, "Seed products keep their curated reviews.")
    check_reviews(r.rating, r.reviews)
    with connect() as con:
        con.execute("UPDATE products SET rating = ?, reviews = ? WHERE id = ?", (r.rating, r.reviews, product_id))
    return product(product_id)


def create_product(new: NewProduct, at):
    """Validate beyond the model (money precision, emoji list, unique name) and insert.
    -> (product, events): its starting stock is the first inventory movement."""
    if round(new.price, 2) != new.price:
        raise HTTPException(422, "Price can have at most 2 decimals.")
    if new.emoji not in EMOJI[new.category]:
        raise HTTPException(422, f"Pick an emoji from the {new.category} set.")
    check_reviews(new.rating, new.reviews)
    lead_time = new.lead_time_days or LEAD_TIME_DAYS[new.category]
    with transaction() as con:
        if con.execute("SELECT 1 FROM products WHERE lower(name) = lower(?)", (new.name,)).fetchone():
            raise HTTPException(409, f"A product called “{new.name}” already exists.")
        # Never reuse an id, even of a deleted product: the lakehouse keys history (stock movements,
        # orders, forecasts) by product_id. The ledger remembers deleted products' ids.
        last = con.execute("SELECT max(n) FROM (SELECT CAST(substr(id, 2) AS INTEGER) AS n FROM products "
                           "UNION ALL SELECT CAST(substr(product_id, 2) AS INTEGER) FROM stock_movements)"
                           ).fetchone()[0] or 0
        pid = f"P{last + 1:03d}"
        con.execute("INSERT INTO products (id, name, category, price, emoji, description, rating, reviews, "
                    "badge, created_at, on_hand, lead_time_days, target_cover_days) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (pid, new.name, new.category, new.price, new.emoji, new.description, new.rating,
                     new.reviews, new.badge, ts(at), new.stock, lead_time, new.target_cover_days))
        m = _move(con, pid, "initial", new.stock, new.stock, ts(at))
        p = as_product(_row(con, pid))
    return p, [inventory_event(p, m)]


def delete_product(product_id, at):
    """-> events: a final 'removed' movement, so the lakehouse drops it from stock views."""
    with transaction() as con:
        p = as_product(_row(con, product_id))
        if p["seed"]:
            raise HTTPException(403, "Seed products can't be deleted.")
        m = _move(con, product_id, "removed", -p["on_hand"], 0, ts(at))
        con.execute("DELETE FROM products WHERE id = ?", (product_id,))
    return [inventory_event(p, m)]


# ---------------------------------------------------------------- inventory logic
def inventory_event(p, m):
    """One stock movement, in the `inventory` topic's schema (inventory_src in pipeline.sql)."""
    return ("inventory", p["id"], {
        "seq": m["seq"], "product_id": p["id"], "product_name": p["name"], "category": p["category"],
        "reason": m["reason"], "delta": m["delta"], "on_hand_after": m["on_hand_after"],
        "unit_price": p["price"], "lead_time_days": p["lead_time_days"],
        "target_cover_days": p["target_cover_days"], "event_time": m["at"],
    })


def restock(product_id, quantity, at):
    with transaction() as con:
        row = _row(con, product_id)
        on_hand = row["on_hand"] + quantity
        con.execute("UPDATE products SET on_hand = ? WHERE id = ?", (on_hand, product_id))
        m = _move(con, product_id, "restock", quantity, on_hand, ts(at))
        p = as_product(_row(con, product_id))
    return p, [inventory_event(p, m)]


def set_inventory_settings(product_id, s: InventorySettings, at):
    with transaction() as con:
        row = _row(con, product_id)
        con.execute("UPDATE products SET lead_time_days = ?, target_cover_days = ? WHERE id = ?",
                    (s.lead_time_days, s.target_cover_days, product_id))
        m = _move(con, product_id, "settings", 0, row["on_hand"], ts(at))
        p = as_product(_row(con, product_id))
    return p, [inventory_event(p, m)]


def reserve_stock(c: "Checkout", at):
    """Take stock for every line in one transaction: all lines or none.
    -> inventory events. Raises 404 for unknown products, 409 when any line exceeds stock."""
    need = Counter()
    for line in c.items:
        need[line.product_id] += line.quantity
    moves = []
    with transaction() as con:
        rows = {pid: _row(con, pid) for pid in need}
        for pid, qty in need.items():
            left = rows[pid]["on_hand"]
            if left < qty:
                name = rows[pid]["name"]
                raise HTTPException(409, f"{name} is sold out." if left == 0 else
                                    f"Only {left} left of {name}. Lower the quantity and try again.")
        for pid, qty in need.items():
            on_hand = rows[pid]["on_hand"] - qty
            con.execute("UPDATE products SET on_hand = ? WHERE id = ?", (on_hand, pid))
            moves.append((as_product(rows[pid]), _move(con, pid, "order", -qty, on_hand, ts(at))))
    return [inventory_event(p, m) for p, m in moves]


def snapshot_stock(at):
    """Publish every product's current stock (a 'snapshot' movement) at startup, so the lakehouse
    catches up even if it was reset or missed an event."""
    with transaction() as con:
        rows = con.execute("SELECT * FROM products ORDER BY id").fetchall()
        return [inventory_event(as_product(r), _move(con, r["id"], "snapshot", 0, r["on_hand"], ts(at)))
                for r in rows]


# ---------------------------------------------------------------- orders and clicks
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


def device_of(user_agent):
    """mobile | tablet | desktop from the User-Agent, the usual analytics heuristic."""
    # ponytail: iPads in desktop mode send a Mac User-Agent and count as desktop; a real UA parser
    # (or client hints) if device accuracy ever matters.
    if "iPad" in user_agent or "Tablet" in user_agent or ("Android" in user_agent and "Mobile" not in user_agent):
        return "tablet"
    if "Mobi" in user_agent or "iPhone" in user_agent:
        return "mobile"
    return "desktop"


def click_event(e: Event, at, user_agent):
    p = product(e.product_id)
    return {"event_id": uuid.uuid4().hex, "session_id": e.session_id, "user_id": e.user_id,
            "event_type": e.event_type, "product_id": p["id"], "category": p["category"],
            "event_time": ts(at), "device": device_of(user_agent)}


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


# ---------------------------------------------------------------- Kafka (Avro + Schema Registry)
def to_avro(event, ctx=None):
    """Event dict -> Avro record: the timestamp string becomes a datetime (timestamp-millis, UTC) and
    money a Decimal (Avro decimal, exact). Event dicts stay JSON-friendly for the API and dry runs."""
    record = dict(event, event_time=datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S.%f"))
    for money in ("unit_price", "total_amount"):
        if money in record:
            record[money] = Decimal(str(record[money]))
    return record


_kafka = None


def kafka():
    """(producer, {topic: serializer}), created once. Registers every schema first, so the registry's
    compatibility check fails the shop's startup, not a customer's checkout."""
    global _kafka
    if _kafka is None:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")})
        serializers = {}
        for topic in TOPICS:
            schema = (SCHEMAS / f"{topic}.avsc").read_text(encoding="utf-8")
            registry.register_schema(f"{topic}-value", Schema(schema, "AVRO"))
            serializers[topic] = AvroSerializer(registry, schema, to_avro)
        producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
                             "enable.idempotence": True, "linger.ms": 5})
        _kafka = producer, serializers
    return _kafka


def publish(topic, key, event):
    """Produce Avro to Kafka, or print JSON when KAFKA_BOOTSTRAP is unset (the UI without the stack)."""
    if not os.getenv("KAFKA_BOOTSTRAP"):
        print(f"[dry-run] {topic}: {json.dumps(event)}", flush=True)
        return
    from confluent_kafka.serialization import MessageField, SerializationContext
    producer, serializers = kafka()
    producer.produce(topic, key=key, value=serializers[topic](event, SerializationContext(topic, MessageField.VALUE)))
    producer.poll(0)


def publish_all(events):
    # ponytail: events go out after the DB commit, so a crash in between loses them (at-most-once);
    # the startup snapshot re-syncs stock. A transactional outbox would make this exactly-once.
    for topic, key, event in events:
        publish(topic, key, event)


# ---------------------------------------------------------------- API
@asynccontextmanager
async def lifespan(app):
    publish_all(snapshot_stock(now()))
    yield


app = FastAPI(title="Lakeshop demo store", lifespan=lifespan)


@app.get("/api/products")
def products():
    return list(catalog().values())


@app.post("/api/products", status_code=201)
def add_product(new: NewProduct):
    # ponytail: no auth on the admin API; fine for a localhost demo, add a login before exposing it.
    p, events = create_product(new, now())
    publish_all(events)
    return p


@app.delete("/api/products/{product_id}", status_code=204)
def remove_product(product_id: str):
    publish_all(delete_product(product_id, now()))
    return Response(status_code=204)


@app.put("/api/products/{product_id}/reviews")
def update_reviews(product_id: str, r: Reviews):
    return set_reviews(product_id, r)


@app.post("/api/products/{product_id}/restock")
def restock_product(product_id: str, r: Restock):
    p, events = restock(product_id, r.quantity, now())
    publish_all(events)
    return p


@app.put("/api/products/{product_id}/inventory")
def update_inventory(product_id: str, s: InventorySettings):
    p, events = set_inventory_settings(product_id, s, now())
    publish_all(events)
    return p


@app.get("/api/catalog/options")
def catalog_options():
    return {"categories": CATEGORIES, "emoji": EMOJI, "badges": BADGES,
            "lead_time_days": LEAD_TIME_DAYS, "target_cover_days": TARGET_COVER_DAYS,
            "starting_stock": ADDED_STARTING_STOCK}


@app.post("/api/events", status_code=202)
def track(e: Event, request: Request):
    publish("clicks", e.user_id, click_event(e, now(), request.headers.get("user-agent", "")))
    return {"ok": True}


@app.post("/api/checkout")
def checkout(c: Checkout):
    error = authorize(c.payment)
    if error:
        raise HTTPException(402, error)
    at = now()
    stock_events = reserve_stock(c, at)  # all lines or none; 404 / 409 before anything is emitted
    events = order_events(c, at)
    for event in events:
        publish("orders", c.user_id, event)
    publish_all(stock_events)
    card = re.sub(r"\D", "", c.payment.card_number or "")
    return {
        "order_ref": "LS-" + uuid.uuid4().hex[:8].upper(),
        "total": round(sum(e["total_amount"] for e in events), 2),
        "lines": len(events),
        "paid_with": f"card •••• {card[-4:]}" if c.payment.method == "card" else c.payment.method.upper(),
    }


app.mount("/", StaticFiles(directory=HERE / "static", html=True), name="static")
