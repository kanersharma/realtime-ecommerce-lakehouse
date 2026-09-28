"""Real-browser tests of Lakeshop: a user clicks through the store and we check the UI AND the events.

The shop runs in-process (uvicorn in a thread) with publish() captured, so each test can assert
exactly which Kafka events its clicks produced. Uses the installed Edge by default; set
E2E_BROWSER=chromium to use Playwright's bundled Chromium instead.
"""
import os
import socket
import threading
import time

import pytest
import uvicorn

import main as shop

pytestmark = pytest.mark.e2e
playwright_api = pytest.importorskip("playwright.sync_api")
expect = playwright_api.expect
SENT = []  # (topic, key, event) captured from the running shop


def catalog():
    return list(shop.catalog().values())  # the test's fresh, seeded database (conftest.shop_db)


def matching(q):
    """Products the store's search should show for q (same fields as app.js render())."""
    return [p for p in catalog() if q in f"{p['name']} {p['category']} {p['description']}".lower()]


@pytest.fixture(scope="module")
def base_url():
    original = shop.publish
    shop.publish = lambda topic, key, event: SENT.append((topic, key, event))
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(shop.app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while not server.started:
        assert time.time() < deadline, "shop server did not start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(5)
    shop.publish = original


@pytest.fixture(scope="module")
def browser():
    with playwright_api.sync_playwright() as p:
        channel = os.getenv("E2E_BROWSER", "msedge")
        b = p.chromium.launch(**({} if channel == "chromium" else {"channel": channel}))
        yield b
        b.close()


@pytest.fixture
def page(browser, base_url):
    SENT.clear()
    context = browser.new_context(viewport={"width": 1440, "height": 1000})  # fresh storage per test
    pg = context.new_page()
    errors = []
    pg.on("pageerror", lambda e: errors.append(str(e)))
    # JS errors fail the test. "Failed to load resource" is Chrome logging an expected 402 / aborted request.
    pg.on("console", lambda m: m.type == "error" and "Failed to load resource" not in m.text and errors.append(m.text))
    pg.goto(base_url)
    expect(pg.locator(".product")).to_have_count(len(catalog()))
    yield pg
    context.close()
    assert errors == [], f"browser errors: {errors}"


def sent(topic, kind=None):
    return [e for t, _, e in SENT if t == topic and (kind is None or e.get("event_type") == kind)]


def wait_for(predicate, timeout=5):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for events"
        time.sleep(0.05)


def add_to_cart(page, name, qty=1):
    page.get_by_role("button", name=name).first.click()
    for _ in range(qty - 1):
        page.locator("#pd-plus").click()
    page.locator("#pd-add").click()


def open_checkout(page):
    page.locator("#cart-btn").click()
    page.locator("#to-checkout").click()
    expect(page.locator("#checkout-dlg")).to_be_visible()


# ------------------------------------------------ browsing
def test_home_page_bento(page):
    bestsellers = sum(p.get("badge") == "Bestseller" for p in catalog())
    expect(page.locator(".product.wide")).to_have_count(bestsellers)  # bestsellers span two columns
    expect(page.locator(".cat")).to_have_count(7)               # All + six categories
    expect(page.locator("#hero-product")).to_contain_text("Streaming Systems Handbook")
    expect(page.locator(".demo-banner")).to_contain_text("no real payments")


def test_search_filters_products(page):
    page.locator("#search").fill("book")
    expect(page.locator(".product")).to_have_count(len(matching("book")))
    expect(page.locator("#results-title")).to_contain_text("book")
    page.locator("#search").fill("zzzz")
    expect(page.locator(".product")).to_have_count(0)
    expect(page.locator("#empty")).to_be_visible()
    page.locator("#search").fill("")
    expect(page.locator(".product")).to_have_count(len(catalog()))


def test_category_filter(page):
    page.locator('.cat[data-cat="Beauty"]').click()
    expect(page.locator(".product")).to_have_count(sum(p["category"] == "Beauty" for p in catalog()))
    expect(page.locator('.cat[data-cat="Beauty"]')).to_have_attribute("aria-pressed", "true")
    page.locator('.cat[data-cat="All"]').click()
    expect(page.locator(".product")).to_have_count(len(catalog()))


def test_opening_a_product_sends_page_view(page):
    page.get_by_role("button", name="4K Monitor, $329.00").click()
    expect(page.locator("#product-dlg")).to_be_visible()
    expect(page.locator("#pd-name")).to_have_text("4K Monitor")
    wait_for(lambda: sent("clicks", "page_view"))
    (e,) = sent("clicks", "page_view")
    assert e["product_id"] == "P002" and e["category"] == "Electronics"
    expect(page.locator("#evt-count")).to_have_text("1")
    page.keyboard.press("Escape")
    expect(page.locator("#product-dlg")).to_be_hidden()


def test_keyboard_can_open_a_product(page):
    page.get_by_role("button", name="Yoga Mat, $25.00").focus()
    page.keyboard.press("Enter")
    expect(page.locator("#pd-name")).to_have_text("Yoga Mat")


# ------------------------------------------------ cart
def test_add_to_cart_from_dialog_and_quick_add(page):
    add_to_cart(page, "Data Engineering Book, $42.00", qty=2)
    expect(page.locator("#cart-count")).to_have_text("2")
    page.get_by_role("button", name="Add Lip Balm Trio to cart").click()
    expect(page.locator("#cart-count")).to_have_text("3")
    wait_for(lambda: len(sent("clicks", "add_to_cart")) == 2)
    assert [e["product_id"] for e in sent("clicks", "add_to_cart")] == ["P015", "P024"]
    expect(page.locator("#toast")).to_contain_text("Lip Balm Trio")


def test_cart_quantities_subtotal_and_remove(page):
    add_to_cart(page, "Data Engineering Book, $42.00", qty=2)
    page.locator("#cart-btn").click()
    expect(page.locator("#cart-subtotal")).to_have_text("$84.00")
    page.locator('[data-inc="P015"]').click()
    expect(page.locator("#cart-subtotal")).to_have_text("$126.00")
    page.locator('[data-dec="P015"]').click()
    expect(page.locator("#cart-subtotal")).to_have_text("$84.00")
    page.locator('[data-rm="P015"]').click()
    expect(page.locator("#cart-lines")).to_contain_text("Your cart is empty")
    expect(page.locator("#to-checkout")).to_be_disabled()
    expect(page.locator("#cart-count")).to_have_text("0")


def test_cart_survives_reload(page):
    add_to_cart(page, "Yoga Mat, $25.00")
    page.reload()
    expect(page.locator("#cart-count")).to_have_text("1")


# ------------------------------------------------ checkout
@pytest.mark.parametrize("method,fill,paid_with", [
    ("cod", {}, "COD"),
    ("upi", {"upi": "kaner@okbank"}, "UPI"),
    ("card", "test-card", "card •••• 4242"),
])
def test_checkout_succeeds_and_emits_orders(page, method, fill, paid_with):
    add_to_cart(page, "Data Engineering Book, $42.00", qty=2)
    page.get_by_role("button", name="Add Lip Balm Trio to cart").click()
    open_checkout(page)
    expect(page.locator("#pay-btn")).to_have_text("Pay $93.99")
    page.locator('input[name="name"]').fill("Test Shopper")
    page.locator('select[name="country"]').select_option("DE")
    page.locator(f'input[name="method"][value="{method}"]').check()
    if fill == "test-card":
        page.locator("#fill-test").click()
    for name, value in dict(fill if isinstance(fill, dict) else {}).items():
        page.locator(f'input[name="{name}"]').fill(value)
    page.locator("#pay-btn").click()

    expect(page.locator("#co-success")).to_be_visible()
    expect(page.locator("#co-summary")).to_contain_text("$93.99")
    expect(page.locator("#co-summary")).to_contain_text(paid_with)
    expect(page.locator("#cart-count")).to_have_text("0")
    orders = sent("orders")
    assert len(orders) == 2
    assert {e["product_id"] for e in orders} == {"P015", "P024"}
    assert all(e["payment_method"] == method and e["country"] == "DE" for e in orders)
    # the whole funnel belongs to one session, so the dashboard's conversion counts it
    assert len({e["session_id"] for _, _, e in SENT}) == 1


def test_only_the_selected_payment_fields_show(page):
    add_to_cart(page, "Yoga Mat, $25.00")
    open_checkout(page)
    expect(page.locator(".card-fields")).to_be_visible()
    page.locator('input[name="method"][value="cod"]').check()
    expect(page.locator(".card-fields")).to_be_hidden()
    expect(page.locator(".cod-fields")).to_be_visible()
    page.locator('input[name="method"][value="upi"]').check()
    expect(page.locator(".upi-fields")).to_be_visible()
    # card inputs never offer the browser's saved (real) cards
    for name in ("demo_card", "demo_exp", "demo_cvc"):
        expect(page.locator(f'input[name="{name}"]')).to_have_attribute("autocomplete", "off")


@pytest.mark.parametrize("card,message", [
    ("4000 0000 0000 0002", "declined"),
    ("5555 5555 5555 4444", "only test cards"),
])
def test_failed_card_keeps_cart_and_emits_no_orders(page, card, message):
    add_to_cart(page, "Yoga Mat, $25.00")
    open_checkout(page)
    page.locator('input[name="name"]').fill("Test Shopper")
    page.locator('input[name="demo_card"]').fill(card)
    page.locator('input[name="demo_exp"]').fill("12/30")
    page.locator('input[name="demo_cvc"]').fill("123")
    page.locator("#pay-btn").click()
    expect(page.locator("#co-error")).to_be_visible()
    expect(page.locator("#co-error")).to_contain_text(message, ignore_case=True)
    expect(page.locator("#cart-count")).to_have_text("1")
    assert sent("orders") == []
    page.locator("#fill-test").click()                 # fixing the card clears the stale error
    expect(page.locator("#co-error")).to_be_hidden()


def test_name_is_required(page):
    add_to_cart(page, "Yoga Mat, $25.00")
    open_checkout(page)
    page.locator('input[name="method"][value="cod"]').check()
    page.locator("#pay-btn").click()
    expect(page.locator("#co-error")).to_have_text("Please enter your name.")
    assert sent("orders") == []


def test_server_down_shows_a_helpful_message(page):
    """The 'Failed to fetch' report: a checkout while the server is unreachable."""
    add_to_cart(page, "Yoga Mat, $25.00")
    open_checkout(page)
    page.route("**/api/checkout", lambda route: route.abort())
    page.locator('input[name="name"]').fill("Test Shopper")
    page.locator('input[name="method"][value="cod"]').check()
    page.locator("#pay-btn").click()
    expect(page.locator("#co-error")).to_contain_text("Can't reach the Lakeshop server")
    expect(page.locator("#co-error")).not_to_contain_text("Failed to fetch")
    expect(page.locator("#pay-btn")).to_be_enabled()
    expect(page.locator("#cart-count")).to_have_text("1")


def test_server_down_on_load(browser, base_url):
    context = browser.new_context()
    pg = context.new_page()
    pg.route("**/api/products", lambda route: route.abort())
    pg.goto(base_url)
    expect(pg.locator("#empty")).to_contain_text("Can't reach the Lakeshop server")
    context.close()


# ------------------------------------------------ layout
@pytest.mark.parametrize("width", [375, 768, 1440])
def test_no_horizontal_scroll(page, width):
    page.set_viewport_size({"width": width, "height": 900})
    assert page.evaluate("document.documentElement.scrollWidth") <= width


# ------------------------------------------------ catalog admin (admin.html)
NEW = {"name": "Game Controller", "category": "Electronics", "price": "49.99", "emoji": "🎮",
       "description": "Wireless controller with hall-effect sticks and 40 hours of battery."}


def open_admin(page, base_url):
    page.goto(f"{base_url}/admin.html")
    expect(page.locator("#rows tr")).to_have_count(len(catalog()))


def fill_new_product(page, **over):
    d = {**NEW, **over}
    page.locator("#add-btn").click()
    expect(page.locator("#add-dlg")).to_be_visible()
    page.locator('#add-form [name="name"]').fill(d["name"])
    page.locator("#add-category").select_option(d["category"])
    page.locator('#add-form [name="price"]').fill(d["price"])
    page.locator('#add-form [name="description"]').fill(d["description"])
    page.locator(f'.emoji-choice input[value="{d["emoji"]}"]').check(force=True)


def test_admin_lists_the_catalog(page, base_url):
    open_admin(page, base_url)
    expect(page.locator("#st-total")).to_have_text(str(len(catalog())))
    expect(page.locator("#st-split")).to_contain_text("0 added by you")
    expect(page.locator("[data-del]")).to_have_count(0)            # seed products can't be deleted
    page.locator("#search").fill("P015")
    expect(page.locator("#rows tr")).to_have_count(1)
    expect(page.locator("#rows")).to_contain_text("Data Engineering Book")
    page.locator("#search").fill("")
    page.locator('.cat[data-cat="Books"]').click()
    expect(page.locator("#rows tr")).to_have_count(sum(p["category"] == "Books" for p in catalog()))


def test_store_links_to_the_catalog(page, base_url):
    page.locator("#catalog-link").click()
    expect(page).to_have_url(f"{base_url}/admin.html")
    expect(page.locator("#add-btn")).to_be_visible()


def test_emoji_picker_follows_the_category(page, base_url):
    open_admin(page, base_url)
    page.locator("#add-btn").click()
    page.locator("#add-category").select_option("Books")
    expect(page.locator(".emoji-choice")).to_have_count(len(shop.EMOJI["Books"]))
    expect(page.locator('.emoji-choice input[value="📘"]')).to_have_count(1)
    expect(page.locator('.emoji-choice input[value="🎮"]')).to_have_count(0)


def test_add_product_then_buy_it_end_to_end(page, base_url):
    """Admin adds a product -> it's in the store -> a shopper buys it -> events carry its data."""
    open_admin(page, base_url)
    fill_new_product(page)
    page.locator("#add-badge").select_option("New")
    preview = page.locator("#preview")
    expect(preview).to_contain_text("Game Controller")
    expect(preview).to_contain_text("$49.99")
    expect(preview).to_contain_text("NEW")
    page.locator("#save-btn").click()

    expect(page.locator("#add-dlg")).to_be_hidden()
    expect(page.locator("#toast")).to_contain_text("Added 🎮 Game Controller (P049)")
    expect(page.locator('tr[data-id="P049"]')).to_contain_text("Added")
    expect(page.locator("#st-split")).to_contain_text("1 added by you")

    page.locator('tr[data-id="P049"] a:has-text("View")').click()     # deep link into the store
    expect(page.locator(".product")).to_have_count(1)
    expect(page.locator(".product")).to_contain_text("No reviews yet")
    page.get_by_role("button", name="Game Controller, $49.99").click()
    wait_for(lambda: sent("clicks", "page_view"))
    assert sent("clicks", "page_view")[0]["product_id"] == "P049"
    page.locator("#pd-add").click()
    open_checkout(page)
    page.locator('input[name="name"]').fill("Test Shopper")
    page.locator('input[name="method"][value="cod"]').check()
    page.locator("#pay-btn").click()
    expect(page.locator("#co-success")).to_be_visible()
    (order,) = sent("orders")
    assert (order["product_id"], order["product_name"], order["unit_price"]) == ("P049", "Game Controller", 49.99)


@pytest.mark.parametrize("over,message", [
    ({"name": ""}, "Give the product a name"),
    ({"name": "<b>Bold</b>"}, "Names can use"),
    ({"name": "4k monitor"}, "already exists"),
    ({"price": "1.234"}, "at most 2 decimals"),
    ({"price": "0"}, "between $0.01"),
    ({"description": "short"}, "at least 10 characters"),
])
def test_add_product_validation(page, base_url, over, message):
    open_admin(page, base_url)
    fill_new_product(page, **over)
    page.locator("#save-btn").click()
    expect(page.locator("#add-error")).to_contain_text(message)
    expect(page.locator("#add-dlg")).to_be_visible()
    page.locator('#add-form [name="description"]').press("End")    # editing clears the stale error
    page.locator('#add-form [name="description"]').type("!")
    expect(page.locator("#add-error")).to_be_hidden()
    assert len(catalog()) == 48


def test_delete_an_added_product(page, base_url):
    import httpx
    body = {**NEW, "price": 49.99}
    assert httpx.post(f"{base_url}/api/products", json=body).status_code == 201
    open_admin(page, base_url)
    expect(page.locator("[data-del]")).to_have_count(1)            # only the added product
    page.once("dialog", lambda d: d.accept())
    page.locator('[data-del="P049"]').click()
    expect(page.locator('tr[data-id="P049"]')).to_have_count(0)
    expect(page.locator("#toast")).to_contain_text("Deleted Game Controller")
    assert "P049" not in shop.catalog()


@pytest.mark.parametrize("width", [375, 1440])
def test_admin_has_no_horizontal_page_scroll(page, base_url, width):
    page.set_viewport_size({"width": width, "height": 900})
    open_admin(page, base_url)
    assert page.evaluate("document.documentElement.scrollWidth") <= width
