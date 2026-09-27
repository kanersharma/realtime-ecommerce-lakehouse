"""Simulated e-commerce traffic -> Kafka topics `clicks` and `orders`.

Each shopping session is a small funnel: 1-5 page views, each may become an
add-to-cart, each cart may become an order. Traffic follows a 10-minute sine
wave so the dashboard has something to show. Messages are keyed by user_id,
so one user's events stay ordered within a single partition.
"""
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

PRODUCTS = [  # (product_id, name, category, unit_price)
    ("P001", "Wireless Earbuds", "Electronics", 59.99),
    ("P002", "4K Monitor", "Electronics", 329.00),
    ("P003", "Mechanical Keyboard", "Electronics", 89.50),
    ("P004", "USB-C Hub", "Electronics", 34.99),
    ("P005", "Running Shoes", "Fashion", 120.00),
    ("P006", "Denim Jacket", "Fashion", 75.00),
    ("P007", "Cotton T-Shirt", "Fashion", 15.99),
    ("P008", "Leather Wallet", "Fashion", 45.00),
    ("P009", "Coffee Grinder", "Home", 49.00),
    ("P010", "Air Purifier", "Home", 179.99),
    ("P011", "Desk Lamp", "Home", 29.99),
    ("P012", "Yoga Mat", "Sports", 25.00),
    ("P013", "Dumbbell Set", "Sports", 99.00),
    ("P014", "Cycling Helmet", "Sports", 65.00),
    ("P015", "Data Engineering Book", "Books", 42.00),
    ("P016", "Sci-Fi Novel", "Books", 12.99),
    ("P017", "Face Serum", "Beauty", 28.50),
    ("P018", "Sunscreen SPF50", "Beauty", 14.00),
]
COUNTRIES = {"IN": 30, "US": 25, "GB": 10, "DE": 10, "BR": 8, "JP": 7, "AU": 5, "CA": 5}
PAYMENTS = {"card": 55, "upi": 20, "wallet": 15, "cod": 10}

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


def main():
    from confluent_kafka import Producer  # imported here so tests run without Kafka

    base_rate = float(os.getenv("SESSIONS_PER_SEC", "20"))
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        "enable.idempotence": True,  # no duplicates on producer retries
        "linger.ms": 50,
        "compression.type": "zstd",
    })
    rng = random.Random()
    sent = {"clicks": 0, "orders": 0}
    last_log = time.time()
    print(f"generator: ~{base_rate} sessions/sec -> Kafka", flush=True)

    try:
        while True:
            tick = time.time()
            rate = base_rate * (1 + 0.5 * math.sin(2 * math.pi * tick / 600))
            n = int(rate) + (rng.random() < rate % 1)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for _ in range(n):
                for topic, key, event in session_events(now, rng):
                    producer.produce(topic, key=key, value=json.dumps(event))
                    sent[topic] += 1
            producer.poll(0)
            if tick - last_log >= 10:
                print(f"sent clicks={sent['clicks']} orders={sent['orders']} (rate {rate:.1f} sessions/s)", flush=True)
                last_log = tick
            time.sleep(max(0.0, 1 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
