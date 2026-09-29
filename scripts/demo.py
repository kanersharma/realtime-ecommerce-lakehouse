"""Demo helper: send real shopper traffic through Lakeshop, then screenshot the store and the dashboard.

Needs the running stack (docker compose up -d --build). Run it either way:

    docker compose run --rm demo                              # in Docker: nothing to install
    docker compose run --rm demo --no-screenshots             # traffic only
    .venv/Scripts/python scripts/demo.py                      # locally (dev deps + Edge)
    .venv/Scripts/python scripts/demo.py --traffic-minutes 0  # screenshots only

Env: SHOP_URL / DASHBOARD_URL (default localhost) and DEMO_BROWSER (msedge locally, chromium in Docker).

Traffic goes through the shop's public API (the same endpoints the browser uses), so it flows
through Kafka -> Flink -> Iceberg exactly like clicks in the UI. Screenshots land in docs/screenshots/.
"""
import argparse
import json
import os
import random
import threading
import time
import uuid
from pathlib import Path

import httpx

from ports import host_port  # scripts/ports.py: the environment, then .env, then the default port

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "screenshots"
SHOP = os.getenv("SHOP_URL", f"http://localhost:{host_port('SHOP_PORT')}")
DASHBOARD = os.getenv("DASHBOARD_URL", f"http://localhost:{host_port('DASHBOARD_PORT')}")
BROWSER = os.getenv("DEMO_BROWSER", "msedge")  # "chromium" = Playwright's bundled browser
PRODUCTS = json.loads((ROOT / "catalog" / "products.json").read_text(encoding="utf-8"))
SCREENSHOT_PRODUCTS = ["P002", "P021", "P020"]  # the store screenshots buy these
CHARTED = "Wireless Earbuds"  # P001, a bestseller: the inventory screenshot charts its daily sales
COUNTRIES = {"IN": 30, "US": 25, "GB": 10, "DE": 10, "BR": 8, "JP": 7, "AU": 5, "CA": 5}
USER_AGENTS = {  # the shop derives the click's `device` from these (schema v2)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36": 45,
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1": 25,
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/126.0 Mobile Safari/537.36": 20,
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1": 10,
}
PAYMENTS = [
    ({"method": "card", "card_number": "4242 4242 4242 4242", "expiry": "12/30", "cvc": "123"}, 50),
    ({"method": "upi", "upi_id": "shopper@okbank"}, 25),
    ({"method": "cod"}, 15),
    ({"method": "card", "card_number": "4000 0000 0000 0002", "expiry": "12/30", "cvc": "123"}, 10),  # declined
]


# ------------------------------------------------ traffic
def shopper(http, rng):
    """One session: browse 1-4 products, maybe add to cart, maybe check out.
    Returns (orders, declined, sold_out)."""
    user, session = f"W-demo{uuid.uuid4().hex[:10]}", f"S-demo{uuid.uuid4().hex[:10]}"
    ids = {"user_id": user, "session_id": session}
    ua = {"User-Agent": rng.choices(list(USER_AGENTS), weights=list(USER_AGENTS.values()))[0]}
    cart = {}
    # bestsellers get looked at more, like a real store
    weights = [3 if p.get("badge") == "Bestseller" else 1 for p in PRODUCTS]
    for p in rng.choices(PRODUCTS, weights=weights, k=rng.randint(1, 4)):
        http.post("/api/events", headers=ua, json={"event_type": "page_view", "product_id": p["id"], **ids})
        if rng.random() < 0.4:
            qty = rng.choices([1, 2, 3], weights=[75, 20, 5])[0]
            http.post("/api/events", headers=ua, json={"event_type": "add_to_cart", "product_id": p["id"], **ids})
            cart[p["id"]] = cart.get(p["id"], 0) + qty
    if not cart or rng.random() > 0.55:  # cart abandonment
        return 0, 0, 0
    payment = rng.choices([p for p, _ in PAYMENTS], weights=[w for _, w in PAYMENTS])[0]
    r = http.post("/api/checkout", json={
        **ids, "name": "Demo Shopper",
        "country": rng.choices(list(COUNTRIES), weights=list(COUNTRIES.values()))[0],
        "items": [{"product_id": k, "quantity": min(v, 10)} for k, v in cart.items()],
        "payment": payment})
    return (r.status_code == 200, r.status_code == 402, r.status_code == 409)


def traffic(minutes, sessions_per_sec=2.0, stop=None, quiet=False):
    rng = random.Random()
    orders = declined = sold_out = sessions = 0
    end = time.time() + minutes * 60
    with httpx.Client(base_url=SHOP, timeout=10) as http:
        while time.time() < end and not (stop and stop.is_set()):
            tick = time.time()
            for _ in range(rng.randint(int(sessions_per_sec) - 1, int(sessions_per_sec) + 1)):
                try:
                    o, d, x = shopper(http, rng)
                except httpx.TransportError:  # shop restarting: skip this session, keep going
                    time.sleep(1)
                    continue
                orders, declined, sold_out, sessions = orders + o, declined + d, sold_out + x, sessions + 1
            if not quiet and sessions % 60 < 3:
                print(f"  {sessions} sessions, {orders} orders, {declined} declined, {sold_out} sold out", flush=True)
            time.sleep(max(0.0, 1 - (time.time() - tick)))
    if not quiet:
        print(f"traffic done: {sessions} sessions, {orders} orders, {declined} declined payments, "
              f"{sold_out} refused as sold out")


def ensure_stock(pids, minimum=10):
    """Top up only these products (others may stay sold out, which is realistic)."""
    with httpx.Client(base_url=SHOP, timeout=10) as http:
        stock = {p["id"]: p["on_hand"] for p in http.get("/api/products").json()}
        for pid in pids:
            if stock.get(pid, 0) < minimum:
                http.post(f"/api/products/{pid}/restock", json={"quantity": minimum * 3})


# ------------------------------------------------ screenshots
def shoot(page, name):
    OUT.mkdir(parents=True, exist_ok=True)
    page.wait_for_timeout(600)  # let transitions settle
    page.screenshot(path=str(OUT / f"{name}.png"))
    print(f"  saved docs/screenshots/{name}.png")


def to_products(page):
    """Scroll the product grid's heading to just below the sticky top bar."""
    page.locator(".section-head").evaluate(
        "h => scrollTo(0, h.getBoundingClientRect().top + scrollY"
        " - document.querySelector('.topbar').offsetHeight - 24)")


def store_screenshots(browser):
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()
    page.goto(SHOP)
    page.locator(".product").first.wait_for()
    shoot(page, "store-home")

    to_products(page)
    shoot(page, "store-products")

    page.locator("#search").fill("stream")
    to_products(page)
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
    page.locator("#add-dlg").evaluate("d => d.scrollTo(0, 0)")  # show the form from the top
    shoot(page, "admin-add")
    page.keyboard.press("Escape")
    page.locator('[data-stock="P002"]').click()        # the stock dialog, filled in but not saved
    page.locator('#stock-form [data-plus="50"]').click()
    shoot(page, "admin-stock")
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
            page.get_by_text("Sessions by device").first.scroll_into_view_if_needed()
            page.get_by_text("Event schemas").first.wait_for(timeout=60_000)
            page.get_by_text("Sessions by device").first.evaluate("e => e.scrollIntoView({block: 'start'})")
            page.mouse.wheel(0, -40)
            shoot(page, "dashboard-devices")
            page.get_by_role("tab", name="📦 Inventory").click()
            page.get_by_text("Reorder suggestions").wait_for(timeout=60_000)
            page.get_by_text("Days of cover").first.wait_for(timeout=60_000)
            # a bestseller always has sales history; the default (most urgent) product may have none
            page.locator(".st-key-inv_product input").click()
            page.keyboard.type(CHARTED)  # the option list is virtualized: filter, don't scroll
            page.keyboard.press("Enter")
            page.get_by_text(f"{CHARTED}: ").first.wait_for(timeout=60_000)  # the chart's caption: rerun done
            page.wait_for_timeout(4000)
            shoot(page, "dashboard-inventory")
            page.get_by_role("tab", name="🔬 Lakehouse Internals").click()
            page.get_by_text("Commits over time").wait_for(timeout=60_000)
            page.wait_for_timeout(3000)
            shoot(page, "dashboard-internals")
            page.get_by_role("tab", name="🧪 SQL Playground").click()
            page.get_by_text("Start from an example").wait_for()
            page.get_by_label("Start from an example").click()
            page.get_by_role("option", name="Schema evolution: clicks by device").click()
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
        if not args.no_screenshots:
            ensure_stock(["P001"], minimum=40)  # so CHARTED sells all through the traffic
        traffic(args.traffic_minutes)
    if args.no_screenshots:
        return
    from playwright.sync_api import sync_playwright
    # keep a trickle of shoppers going while capturing, so "Data freshness" shows real latency
    stop = threading.Event()
    threading.Thread(target=traffic, args=(60, 1.0, stop, True), daemon=True).start()
    with sync_playwright() as p:
        browser = p.chromium.launch(**({} if BROWSER == "chromium" else {"channel": BROWSER}))
        ensure_stock(SCREENSHOT_PRODUCTS)
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
