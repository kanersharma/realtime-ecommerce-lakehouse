"""Simulated e-commerce traffic (optional background load, `--profile simulator`).

Each shopping session is a small funnel: 1-5 page views, each may become an
add-to-cart, each cart may become an order. Traffic follows a 10-minute sine
wave so the dashboard has something to show. Messages are keyed by user_id,
so one user's events stay ordered within a single partition.

Clicks go straight to Kafka, backdated up to 3 s (out of order on purpose, to exercise the
watermark). Orders go through the shop's checkout (SHOP_URL), because the shop owns stock: that
keeps sales and inventory consistent, and sold-out products are refused like for real shoppers.
Without a shop the orders go straight to Kafka (no stock is tracked then).
"""
import json
import math
import os
import random
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Shared with the storefront (shop/), so simulated and real events use the same products.
# Repo: catalog/products.json; container: /catalog/products.json (mounted by docker-compose).
CATALOG = Path(__file__).resolve().parent.parent / "catalog" / "products.json"


def load_products(shop_url=None):
    """(product_id, name, category, unit_price) tuples: the shop's live catalog (seed + admin-added
    products) when reachable, otherwise the seed file."""
    try:
        if not shop_url:
            raise OSError("no shop configured")
        with urllib.request.urlopen(f"{shop_url}/api/products", timeout=3) as r:
            items = json.load(r)
    except (OSError, ValueError):
        items = json.loads(CATALOG.read_text(encoding="utf-8"))
    return [(p["id"], p["name"], p["category"], p["price"]) for p in items]


PRODUCTS = load_products()
COUNTRIES = {"IN": 30, "US": 25, "GB": 10, "DE": 10, "BR": 8, "JP": 7, "AU": 5, "CA": 5}
PAYMENTS = {"card": 55, "upi": 25, "cod": 20}  # the methods the shop accepts
PAY_WITH = {  # test credentials only: the shop refuses anything else
    "card": {"method": "card", "card_number": "4242 4242 4242 4242", "expiry": "12/30", "cvc": "123"},
    "upi": {"method": "upi", "upi_id": "simulator@okbank"},
    "cod": {"method": "cod"},
}

CART_RATE = 0.25      # page_view -> add_to_cart
PURCHASE_RATE = 0.40  # add_to_cart -> order


def ts(dt):
    """Flink SQL JSON default timestamp format: 'yyyy-MM-dd HH:mm:ss.SSS' (UTC)."""
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def session_events(now, rng):
    """One shopping session -> list of (topic, key, event).

    Event times start ~3s in the past and step forward by <=150ms, so they are
    slightly out of order across sessions (exercises the 5s watermark) but
    never in the future (which would push the watermark ahead of reality).
    """
    user_id = f"U{rng.randint(1, 5000):05d}"
    session_id = uuid.uuid4().hex[:16]
    country = rng.choices(list(COUNTRIES), weights=list(COUNTRIES.values()))[0]
    t = now - timedelta(seconds=3)
    events = []

    def step():
        nonlocal t
        t += timedelta(milliseconds=rng.randint(20, 150))
        return ts(t)

    def click(event_type, product):
        return ("clicks", user_id, {
            "event_id": uuid.uuid4().hex, "session_id": session_id, "user_id": user_id,
            "event_type": event_type, "product_id": product[0], "category": product[2],
            "event_time": step(),
        })

    for _ in range(rng.randint(1, 5)):
        product = rng.choice(PRODUCTS)
        events.append(click("page_view", product))
        if rng.random() < CART_RATE:
            events.append(click("add_to_cart", product))
            if rng.random() < PURCHASE_RATE:
                qty = rng.choices([1, 2, 3], weights=[80, 15, 5])[0]
                events.append(("orders", user_id, {
                    "order_id": uuid.uuid4().hex, "session_id": session_id, "user_id": user_id,
                    "product_id": product[0], "product_name": product[1], "category": product[2],
                    "quantity": qty, "unit_price": product[3],
                    "total_amount": round(qty * product[3], 2),
                    "country": country,
                    "payment_method": rng.choices(list(PAYMENTS), weights=list(PAYMENTS.values()))[0],
                    "event_time": step(),
                }))
    return events


def checkout_request(orders):
    """The shop checkout body for one session's simulated orders (prices are the shop's, not ours)."""
    first = orders[0]
    return {
        "user_id": first["user_id"], "session_id": first["session_id"], "name": "Simulated Shopper",
        "country": first["country"], "payment": PAY_WITH[first["payment_method"]],
        "items": [{"product_id": o["product_id"], "quantity": o["quantity"]} for o in orders],
    }


def place_order(shop_url, body):
    """POST a checkout -> HTTP status (200 placed, 409 sold out, ...)."""
    req = urllib.request.Request(f"{shop_url}/api/checkout", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except urllib.error.URLError:  # shop restarting: skip this order, keep simulating
        return 0


def main():
    from confluent_kafka import Producer  # imported here so tests run without Kafka

    base_rate = float(os.getenv("SESSIONS_PER_SEC", "20"))
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        "enable.idempotence": True,  # no duplicates on producer retries
        "linger.ms": 50,
        "compression.type": "zstd",
    })
    global PRODUCTS
    shop_url = os.getenv("SHOP_URL")
    PRODUCTS = load_products(shop_url)
    rng = random.Random()
    sent = {"clicks": 0, "orders": 0, "sold_out": 0}
    last_log = time.time()
    print(f"generator: ~{base_rate} sessions/sec; clicks -> Kafka, orders -> "
          f"{shop_url + '/api/checkout' if shop_url else 'Kafka (no stock tracking)'}", flush=True)

    try:
        while True:
            tick = time.time()
            rate = base_rate * (1 + 0.5 * math.sin(2 * math.pi * tick / 600))
            n = int(rate) + (rng.random() < rate % 1)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for _ in range(n):
                orders = []
                for topic, key, event in session_events(now, rng):
                    if topic == "orders" and shop_url:
                        orders.append(event)
                        continue
                    producer.produce(topic, key=key, value=json.dumps(event))
                    sent[topic] += 1
                if orders:
                    status = place_order(shop_url, checkout_request(orders))
                    if status == 200:
                        sent["orders"] += len(orders)
                    elif status == 409:
                        sent["sold_out"] += 1
            producer.poll(0)
            if tick - last_log >= 10:
                PRODUCTS = load_products(shop_url)  # pick up products added in the admin
                print(f"clicks={sent['clicks']} orders={sent['orders']} sold_out={sent['sold_out']} "
                      f"(rate {rate:.1f} sessions/s)", flush=True)
                last_log = tick
            time.sleep(max(0.0, 1 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
