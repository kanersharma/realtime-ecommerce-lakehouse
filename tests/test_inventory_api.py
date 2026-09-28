"""Inventory in the shop (system of record): stock reservation at checkout, restock, settings,
the ledger, migration of pre-inventory databases and the events sent to the `inventory` topic."""
import sqlite3
import threading
from datetime import datetime

import pytest
from fastapi import HTTPException

import main as shop

IDS = {"user_id": "W-stock001", "session_id": "S-stock001"}
AT = datetime(2026, 9, 28, 12, 0, 0)


def buy(client, *lines, method="cod"):
    return client.post("/api/checkout", json={
        **IDS, "name": "Buyer", "country": "IN", "payment": {"method": method},
        "items": [{"product_id": pid, "quantity": qty} for pid, qty in lines]})


def stock(pid):
    return shop.catalog()[pid]["on_hand"]


def inventory(events):
    return [e for topic, _, e in events if topic == "inventory"]


def add_product(client, **over):
    body = {"name": "Stock Probe", "category": "Home", "price": 10.0, "emoji": "☕",
            "description": "A product created to test inventory behaviour.", **over}
    r = client.post("/api/products", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ------------------------------------------------ seed stock and settings
def test_seed_products_start_with_stock_and_category_lead_times(client):
    products = client.get("/api/products").json()
    for p in products:
        assert p["on_hand"] == shop.starting_stock(p["id"]) and 15 <= p["on_hand"] <= 74
        assert p["lead_time_days"] == shop.LEAD_TIME_DAYS[p["category"]]
        assert p["target_cover_days"] == shop.TARGET_COVER_DAYS
    assert len({p["on_hand"] for p in products}) > 10          # varied, so some sell out sooner


def test_options_include_inventory_defaults(client):
    o = client.get("/api/catalog/options").json()
    assert o["lead_time_days"] == shop.LEAD_TIME_DAYS
    assert (o["target_cover_days"], o["starting_stock"]) == (7, 50)


# ------------------------------------------------ checkout reserves stock
def test_checkout_decrements_stock_and_emits_movements(client, events):
    before = stock("P002"), stock("P024")
    r = buy(client, ("P002", 2), ("P024", 1))
    assert r.status_code == 200, r.text
    assert (stock("P002"), stock("P024")) == (before[0] - 2, before[1] - 1)
    moves = inventory(events)
    assert [(m["product_id"], m["reason"], m["delta"], m["on_hand_after"]) for m in moves] == [
        ("P002", "order", -2, before[0] - 2), ("P024", "order", -1, before[1] - 1)]
    assert moves[0]["seq"] < moves[1]["seq"]
    assert (moves[0]["product_name"], moves[0]["unit_price"], moves[0]["lead_time_days"]) == ("4K Monitor", 329.0, 5)


def test_order_beyond_stock_is_rejected_without_side_effects(client, events):
    p = add_product(client, stock=3)
    events.clear()
    r = buy(client, (p["id"], 2), (p["id"], 2))                 # duplicate lines add up: 4 > 3
    assert r.status_code == 409
    assert r.json()["detail"] == "Only 3 left of Stock Probe. Lower the quantity and try again."
    assert stock(p["id"]) == 3 and events == []


def test_multi_line_order_is_all_or_nothing(client, events):
    p = add_product(client, stock=1)
    before = stock("P001")
    r = buy(client, ("P001", 1), (p["id"], 2))                  # first line fits, second doesn't
    assert r.status_code == 409
    assert stock("P001") == before and stock(p["id"]) == 1       # nothing taken
    assert [t for t, _, _ in events if t in ("orders",)] == []


def test_sold_out_product(client, events):
    p = add_product(client, stock=2)
    assert buy(client, (p["id"], 2)).status_code == 200
    assert stock(p["id"]) == 0
    r = buy(client, (p["id"], 1))
    assert r.status_code == 409 and r.json()["detail"] == "Stock Probe is sold out."


def test_declined_payment_does_not_touch_stock(client, events):
    before = stock("P001")
    r = buy(client, ("P001", 1), method="card")                  # no card number: declined
    assert r.status_code == 402 and stock("P001") == before and events == []


def test_concurrent_checkouts_never_oversell(shop_db):
    """25 shoppers race for the last 7 units: exactly 7 succeed, stock ends at 0, never below."""
    p, _ = shop.create_product(shop.NewProduct(
        name="Race Probe", category="Home", price=5.0, emoji="☕",
        description="A product for the concurrency test.", stock=7), AT)
    wins, rejects, errors = [], [], []

    def shopper(i):
        c = shop.Checkout(user_id=f"W-race{i:04d}", session_id=f"S-race{i:04d}", name="Racer",
                          country="IN", items=[shop.Line(product_id=p["id"], quantity=1)],
                          payment=shop.Payment(method="cod"))
        try:
            shop.reserve_stock(c, AT)
            wins.append(i)
        except HTTPException as e:
            rejects.append(e.status_code)
        except Exception as e:  # pragma: no cover - would be a real bug (e.g. "database is locked")
            errors.append(repr(e))

    threads = [threading.Thread(target=shopper, args=(i,)) for i in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(wins) == 7 and rejects == [409] * 18
    assert stock(p["id"]) == 0
    with sqlite3.connect(shop_db) as con:
        orders = con.execute("SELECT count(*), min(on_hand_after) FROM stock_movements "
                             "WHERE product_id = ? AND reason = 'order'", (p["id"],)).fetchone()
    assert orders == (7, 0)


# ------------------------------------------------ restock and settings
def test_restock_adds_stock_and_emits_a_movement(client, events):
    before = stock("P010")
    r = client.post("/api/products/P010/restock", json={"quantity": 40})
    assert r.status_code == 200 and r.json()["on_hand"] == before + 40
    (m,) = inventory(events)
    assert (m["reason"], m["delta"], m["on_hand_after"]) == ("restock", 40, before + 40)


@pytest.mark.parametrize("quantity", [0, -5, 10_001])
def test_restock_validation(client, quantity):
    assert client.post("/api/products/P010/restock", json={"quantity": quantity}).status_code == 422


def test_restock_unknown_product(client):
    assert client.post("/api/products/P999/restock", json={"quantity": 5}).status_code == 404


def test_inventory_settings(client, events):
    r = client.put("/api/products/P002/inventory", json={"lead_time_days": 9, "target_cover_days": 21})
    assert r.status_code == 200 and (r.json()["lead_time_days"], r.json()["target_cover_days"]) == (9, 21)
    (m,) = inventory(events)
    assert (m["reason"], m["delta"], m["lead_time_days"], m["target_cover_days"]) == ("settings", 0, 9, 21)


@pytest.mark.parametrize("body", [{"lead_time_days": 0, "target_cover_days": 7},
                                  {"lead_time_days": 61, "target_cover_days": 7},
                                  {"lead_time_days": 3, "target_cover_days": 91},
                                  {"lead_time_days": 3}])
def test_inventory_settings_validation(client, body):
    assert client.put("/api/products/P002/inventory", json=body).status_code == 422


# ------------------------------------------------ admin-added products
def test_added_product_defaults_and_initial_movement(client, events):
    p = add_product(client)
    assert (p["on_hand"], p["lead_time_days"], p["target_cover_days"]) == (50, 3, 7)  # Home lead time
    (m,) = inventory(events)
    assert (m["product_id"], m["reason"], m["delta"], m["on_hand_after"]) == (p["id"], "initial", 50, 50)


def test_added_product_custom_inventory(client):
    p = add_product(client, stock=0, lead_time_days=12, target_cover_days=30)
    assert (p["on_hand"], p["lead_time_days"], p["target_cover_days"]) == (0, 12, 30)


@pytest.mark.parametrize("over", [{"stock": -1}, {"stock": 10_001}, {"lead_time_days": 0}, {"target_cover_days": 0}])
def test_added_product_inventory_validation(client, over):
    body = {"name": "Stock Probe", "category": "Home", "price": 10.0, "emoji": "☕",
            "description": "A product created to test inventory behaviour.", **over}
    assert client.post("/api/products", json=body).status_code == 422


def test_deleting_a_product_emits_removed(client, events):
    p = add_product(client, stock=8)
    events.clear()
    assert client.delete(f"/api/products/{p['id']}").status_code == 204
    (m,) = inventory(events)
    assert (m["reason"], m["delta"], m["on_hand_after"]) == ("removed", -8, 0)


# ------------------------------------------------ ledger, snapshot, migration
def test_snapshot_publishes_current_stock_of_every_product(client):
    buy(client, ("P001", 1))
    snap = [e for _, _, e in shop.snapshot_stock(AT)]
    assert len(snap) == 48 and {e["reason"] for e in snap} == {"snapshot"}
    assert {e["product_id"]: e["on_hand_after"] for e in snap}["P001"] == stock("P001")
    seqs = [e["seq"] for e in snap]
    assert seqs == sorted(seqs) and len(set(seqs)) == 48


def test_every_seed_product_has_an_initial_ledger_row(shop_db):
    shop.catalog()
    with sqlite3.connect(shop_db) as con:
        rows = con.execute("SELECT product_id, delta, on_hand_after FROM stock_movements "
                           "WHERE reason = 'initial'").fetchall()
    assert len(rows) == 48 and all(d == a == shop.starting_stock(pid) for pid, d, a in rows)


def test_pre_inventory_database_is_migrated(shop_db):
    """A catalog database from before inventory (Phase 8) keeps its products and gains stock."""
    with sqlite3.connect(shop_db) as con:
        con.execute("""CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL,
            price REAL NOT NULL, emoji TEXT NOT NULL, description TEXT NOT NULL, rating REAL NOT NULL DEFAULT 0,
            reviews INTEGER NOT NULL DEFAULT 0, badge TEXT, created_at TEXT)""")
        con.execute("INSERT INTO products VALUES ('P001','Wireless Earbuds','Electronics',59.99,'🎧','Old seed row.',4.5,1284,NULL,NULL)")
        con.execute("INSERT INTO products VALUES ('P049','Old Gadget','Electronics',9.99,'🎮','Added before inventory existed.',0,0,NULL,'2026-09-28 10:00:00.000')")
    products = shop.catalog()
    assert products["P049"]["name"] == "Old Gadget" and products["P049"]["on_hand"] == 50
    assert products["P001"]["on_hand"] == shop.starting_stock("P001")
    assert products["P049"]["lead_time_days"] == 5 and len(products) == 49
    with sqlite3.connect(shop_db) as con:
        assert con.execute("SELECT count(*) FROM stock_movements WHERE reason = 'initial'").fetchone()[0] == 49
