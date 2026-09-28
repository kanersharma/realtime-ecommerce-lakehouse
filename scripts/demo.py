"""Demo helper: send real shopper traffic through Lakeshop, then screenshot the store and the dashboard.

Needs the running stack (docker compose up -d --build) and the dev deps (requirements-dev.txt).

    .venv/Scripts/python scripts/demo.py                      # traffic + all screenshots (~6 min)
    .venv/Scripts/python scripts/demo.py --traffic-minutes 0  # screenshots only
    .venv/Scripts/python scripts/demo.py --no-screenshots     # traffic only

Traffic goes through the shop's public API (the same endpoints the browser uses), so it flows
through Kafka -> Flink -> Iceberg exactly like clicks in the UI. Screenshots land in docs/screenshots/.
"""
import argparse
import json
import random
import threading
import time
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "screenshots"
SHOP, DASHBOARD = "http://localhost:8000", "http://localhost:8501"
PRODUCTS = json.loads((ROOT / "catalog" / "products.json").read_text(encoding="utf-8"))
COUNTRIES = {"IN": 30, "US": 25, "GB": 10, "DE": 10, "BR": 8, "JP": 7, "AU": 5, "CA": 5}
PAYMENTS = [
    ({"method": "card", "card_number": "4242 4242 4242 4242", "expiry": "12/30", "cvc": "123"}, 50),
    ({"method": "upi", "upi_id": "shopper@okbank"}, 25),
    ({"method": "cod"}, 15),
    ({"method": "card", "card_number": "4000 0000 0000 0002", "expiry": "12/30", "cvc": "123"}, 10),  # declined
]


# ------------------------------------------------ traffic
def shopper(http, rng):
    """One session: browse 1-4 products, maybe add to cart, maybe check out. Returns (orders, declined)."""
    user, session = f"W-demo{uuid.uuid4().hex[:10]}", f"S-demo{uuid.uuid4().hex[:10]}"
    ids = {"user_id": user, "session_id": session}
    cart = {}
    # bestsellers get looked at more, like a real store
    weights = [3 if p.get("badge") == "Bestseller" else 1 for p in PRODUCTS]
    for p in rng.choices(PRODUCTS, weights=weights, k=rng.randint(1, 4)):
        http.post("/api/events", json={"event_type": "page_view", "product_id": p["id"], **ids})
        if rng.random() < 0.4:
            qty = rng.choices([1, 2, 3], weights=[75, 20, 5])[0]
            http.post("/api/events", json={"event_type": "add_to_cart", "product_id": p["id"], **ids})
            cart[p["id"]] = cart.get(p["id"], 0) + qty
    if not cart or rng.random() > 0.55:  # cart abandonment
        return 0, 0
    payment = rng.choices([p for p, _ in PAYMENTS], weights=[w for _, w in PAYMENTS])[0]
    r = http.post("/api/checkout", json={
        **ids, "name": "Demo Shopper",
        "country": rng.choices(list(COUNTRIES), weights=list(COUNTRIES.values()))[0],
        "items": [{"product_id": k, "quantity": min(v, 10)} for k, v in cart.items()],
        "payment": payment})
    return (1, 0) if r.status_code == 200 else (0, 1)


def traffic(minutes, sessions_per_sec=2.0, stop=None, quiet=False):
    rng = random.Random()
    orders = declined = sessions = 0
    end = time.time() + minutes * 60
    with httpx.Client(base_url=SHOP, timeout=10) as http:
        while time.time() < end and not (stop and stop.is_set()):
            tick = time.time()
            for _ in range(rng.randint(int(sessions_per_sec) - 1, int(sessions_per_sec) + 1)):
                try:
                    o, d = shopper(http, rng)
                except httpx.TransportError:  # shop restarting: skip this session, keep going
                    time.sleep(1)
                    continue
                orders, declined, sessions = orders + o, declined + d, sessions + 1
            if not quiet and sessions % 60 < 3:
                print(f"  {sessions} sessions, {orders} orders, {declined} declined", flush=True)
            time.sleep(max(0.0, 1 - (time.time() - tick)))
    if not quiet:
        print(f"traffic done: {sessions} sessions, {orders} orders, {declined} declined payments")


# ------------------------------------------------ screenshots
def shoot(page, name):
    OUT.mkdir(parents=True, exist_ok=True)
    page.wait_for_timeout(600)  # let transitions settle
    page.screenshot(path=str(OUT / f"{name}.png"))
    print(f"  saved docs/screenshots/{name}.png")


def store_screenshots(browser):
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    page.goto(SHOP)
    page.locator(".product").first.wait_for()
    shoot(page, "store-home")

    page.locator(".section-head").scroll_into_view_if_needed()
    page.mouse.wheel(0, -60)
    shoot(page, "store-products")

    page.locator("#search").fill("stream")
    page.locator(".section-head").scroll_into_view_if_needed()
    shoot(page, "store-search")
    page.locator("#search").fill("")

    page.get_by_role("button", name="4K Monitor, $329.00").click()
    page.locator("#pd-plus").click()
    shoot(page, "store-product")
    page.locator("#pd-add").click()
    page.get_by_role("button", name="Add Streaming Systems Handbook to cart").click()
    page.get_by_role("button", name="Add Insulated Water Bottle to cart").click()
    page.locator("#cart-btn").click()
    shoot(page, "store-cart")

    page.locator("#to-checkout").click()
    page.locator('input[name="name"]').fill("Ada Lovelace")
    page.locator('select[name="country"]').select_option("GB")
    page.locator('input[name="demo_card"]').fill("4000 0000 0000 0002")
    page.locator('input[name="demo_exp"]').fill("12/30")
    page.locator('input[name="demo_cvc"]').fill("123")
    page.locator("#pay-btn").click()
    page.locator("#co-error").wait_for()
    shoot(page, "store-declined")

    page.locator("#fill-test").click()
    shoot(page, "store-checkout")
    page.locator("#pay-btn").click()
    page.locator("#co-success").wait_for()
    shoot(page, "store-success")
    ctx.close()

    # catalog admin: the list, then the add dialog filled in but NOT saved (keeps the catalog clean)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    page.goto(f"{SHOP}/admin.html")
    page.locator("#rows tr").first.wait_for()
    shoot(page, "admin-catalog")
    page.locator("#add-btn").click()
    page.locator('#add-form [name="name"]').fill("Retro Game Controller")
    page.locator("#add-category").select_option("Electronics")
    page.locator('#add-form [name="price"]').fill("49.99")
    page.locator("#add-badge").select_option("New")
    page.locator('#add-form [name="description"]').fill(
        "Wireless controller with hall-effect sticks, 40-hour battery and a satisfyingly clicky D-pad.")
    page.locator('.emoji-choice input[value="🎮"]').check(force=True)
    page.locator('#add-form [name="rating"]').fill("4.7")
    page.locator('#add-form [name="reviews"]').fill("1286")
    shoot(page, "admin-add")
    ctx.close()

    mobile = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True)
    page = mobile.new_page()
    page.goto(SHOP)
    page.locator(".product").first.wait_for()
    shoot(page, "store-mobile")
    mobile.close()


def dashboard_screenshots(browser):
    for theme in ("light", "dark"):
        ctx = browser.new_context(viewport={"width": 1440, "height": 1500})
        page = ctx.new_page()
        page.goto(f"{DASHBOARD}/?theme={theme}")
        page.get_by_text("Revenue · last 5 min").wait_for(timeout=90_000)
        page.get_by_text("Top products").wait_for(timeout=90_000)
        page.locator(".vega-embed").nth(2).wait_for(timeout=90_000)  # revenue, funnel and country charts
        page.wait_for_timeout(2500)  # let the chart animations settle
        shoot(page, f"dashboard-{theme}")
        if theme == "light":
            page.get_by_role("tab", name="🔬 Lakehouse Internals").click()
            page.get_by_text("Commits over time").wait_for(timeout=60_000)
            page.wait_for_timeout(3000)
            shoot(page, "dashboard-internals")
            page.get_by_role("tab", name="🧪 SQL Playground").click()
            page.get_by_text("Start from an example").wait_for()
            page.get_by_label("Start from an example").click()
            page.get_by_role("option", name="Cart abandonment by category").click()
            page.get_by_role("button", name="▶ Run").click()
            page.get_by_text("rows ·").wait_for(timeout=60_000)
            page.wait_for_timeout(1500)
            shoot(page, "dashboard-sql")
        ctx.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--traffic-minutes", type=float, default=4)
    ap.add_argument("--no-screenshots", action="store_true")
    args = ap.parse_args()

    httpx.get(f"{SHOP}/api/products", timeout=5).raise_for_status()  # fail fast if the stack is down
    if args.traffic_minutes:
        print(f"sending shopper traffic for {args.traffic_minutes} min …")
        traffic(args.traffic_minutes)
    if args.no_screenshots:
        return
    from playwright.sync_api import sync_playwright
    # keep a trickle of shoppers going while capturing, so "Data freshness" shows real latency
    stop = threading.Event()
    threading.Thread(target=traffic, args=(60, 1.0, stop, True), daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge")
        print("store screenshots …")
        store_screenshots(browser)
        print("waiting 75 s so the last 1-minute windows close …")
        time.sleep(75)
        print("dashboard screenshots …")
        dashboard_screenshots(browser)
        browser.close()
    stop.set()


if __name__ == "__main__":
    main()
