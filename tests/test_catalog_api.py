"""Catalog admin API: add, list, validate, persist and delete products (fresh SQLite per test)."""
import pytest

import main as shop

GOOD = {"name": "Game Controller", "category": "Electronics", "price": 49.99, "emoji": "🎮",
        "description": "Wireless controller with hall-effect sticks and 40 hours of battery.", "badge": "New"}


def add(client, **over):
    return client.post("/api/products", json={**GOOD, **over})


def test_options_drive_the_admin_form(client):
    r = client.get("/api/catalog/options")
    assert r.status_code == 200
    body = r.json()
    assert body["categories"] == shop.CATEGORIES
    assert body["badges"] == ["Bestseller", "New", "Deal"]
    assert "🎮" in body["emoji"]["Electronics"]


def test_add_product_gets_next_id_and_appears_in_the_catalog(client):
    r = add(client)
    assert r.status_code == 201, r.text
    p = r.json()
    assert p["id"] == "P049" and p["seed"] is False
    assert (p["rating"], p["reviews"]) == (0, 0)                     # new products start unreviewed
    listed = client.get("/api/products").json()
    assert listed[-1]["id"] == "P049" and len(listed) == 49
    assert add(client, name="Retro Handheld", emoji="🕹️").json()["id"] == "P050"


def test_added_products_persist_across_restarts(client):
    add(client)
    shop._initialized.clear()  # simulate a fresh process: re-open and re-seed the same database file
    names = [p["name"] for p in client.get("/api/products").json()]
    assert names.count("Game Controller") == 1 and len(names) == 49


def test_names_are_trimmed_and_unique_case_insensitively(client):
    assert add(client, name="  Game   Controller ").json()["name"] == "Game Controller"
    r = add(client, name="game controller")
    assert r.status_code == 409 and "already exists" in r.json()["detail"]
    assert add(client, name="4k monitor").status_code == 409          # clashes with a seed product


@pytest.mark.parametrize("over,status,detail", [
    ({"name": "X"}, 422, None),                                         # too short
    ({"name": "<script>alert(1)</script>"}, 422, None),                 # no markup in names
    ({"name": "a" * 61}, 422, None),
    ({"category": "Toys"}, 422, None),
    ({"price": 0}, 422, None),
    ({"price": -5}, 422, None),
    ({"price": 10000.01}, 422, None),
    ({"price": 1.234}, 422, "2 decimals"),
    ({"emoji": "🦖"}, 422, "Electronics set"),                          # not in the picker
    ({"emoji": "📘"}, 422, "Electronics set"),                          # valid emoji, wrong category
    ({"emoji": "<img src=x>"}, 422, "Electronics set"),
    ({"description": "too short"}, 422, None),
    ({"badge": "Hot"}, 422, None),
])
def test_invalid_products_are_rejected(client, over, status, detail):
    r = add(client, **over)
    assert r.status_code == status, r.text
    if detail:
        assert detail in r.json()["detail"]
    assert len(client.get("/api/products").json()) == 48


def test_new_product_can_be_bought_and_emits_its_own_data(client, events):
    pid = add(client).json()["id"]
    ids = {"user_id": "W-admin01", "session_id": "S-admin01"}
    assert client.post("/api/events", json={"event_type": "page_view", "product_id": pid, **ids}).status_code == 202
    r = client.post("/api/checkout", json={**ids, "name": "Buyer", "country": "US",
                                           "items": [{"product_id": pid, "quantity": 3}],
                                           "payment": {"method": "cod"}})
    assert r.status_code == 200 and r.json()["total"] == 149.97
    (_, _, click), (_, _, order) = events
    assert click["product_id"] == pid and click["category"] == "Electronics"
    assert (order["product_name"], order["unit_price"], order["total_amount"]) == ("Game Controller", 49.99, 149.97)


def test_delete_added_product(client):
    pid = add(client).json()["id"]
    assert client.delete(f"/api/products/{pid}").status_code == 204
    assert pid not in {p["id"] for p in client.get("/api/products").json()}
    assert client.post("/api/events", json={"event_type": "page_view", "product_id": pid,
                                            "user_id": "W-admin01", "session_id": "S-admin01"}).status_code == 404


def test_seed_products_cannot_be_deleted(client):
    r = client.delete("/api/products/P001")
    assert r.status_code == 403
    assert "P001" in {p["id"] for p in client.get("/api/products").json()}
    assert client.delete("/api/products/P999").status_code == 404


def test_deleted_id_is_not_reused_while_later_ids_exist(client):
    first, second = add(client).json()["id"], add(client, name="Retro Handheld", emoji="🕹️").json()["id"]
    client.delete(f"/api/products/{first}")
    assert add(client, name="Studio Mic", emoji="🎙️").json()["id"] == "P051"
    assert second == "P050"


# ------------------------------------------------ reviews
def test_new_product_can_start_with_reviews(client):
    p = add(client, rating=4.6, reviews=128).json()
    assert (p["rating"], p["reviews"]) == (4.6, 128)
    listed = {x["id"]: x for x in client.get("/api/products").json()}
    assert (listed[p["id"]]["rating"], listed[p["id"]]["reviews"]) == (4.6, 128)


@pytest.mark.parametrize("rating,reviews,detail", [
    (4.55, 10, "1 decimal"),
    (4.5, 0, "without reviews"),
    (0, 10, "between 1.0 and 5.0"),
    (0.5, 10, "between 1.0 and 5.0"),
    (5.1, 10, None),                  # pydantic: le=5
    (4.5, -1, None),                  # pydantic: ge=0
    (4.5, 100_001, None),             # pydantic: le=100000
])
def test_invalid_reviews_are_rejected(client, rating, reviews, detail):
    r = add(client, rating=rating, reviews=reviews)
    assert r.status_code == 422, r.text
    if detail:
        assert detail in r.json()["detail"]
    pid = add(client).json()["id"]
    r = client.put(f"/api/products/{pid}/reviews", json={"rating": rating, "reviews": reviews})
    assert r.status_code == 422
    assert client.get("/api/products").json()[-1]["reviews"] == 0   # unchanged


def test_set_and_clear_reviews_of_an_added_product(client):
    pid = add(client).json()["id"]
    r = client.put(f"/api/products/{pid}/reviews", json={"rating": 3.9, "reviews": 2048})
    assert r.status_code == 200 and (r.json()["rating"], r.json()["reviews"]) == (3.9, 2048)
    r = client.put(f"/api/products/{pid}/reviews", json={"rating": 0, "reviews": 0})
    assert (r.json()["rating"], r.json()["reviews"]) == (0, 0)       # back to "No reviews yet"


def test_seed_reviews_are_protected(client):
    r = client.put("/api/products/P001/reviews", json={"rating": 1.0, "reviews": 1})
    assert r.status_code == 403
    assert client.get("/api/products").json()[0]["rating"] == 4.5
    assert client.put("/api/products/P999/reviews", json={"rating": 4, "reviews": 1}).status_code == 404
