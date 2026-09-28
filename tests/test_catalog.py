"""The catalog is shared by the shop, the simulator, the dashboard colors and the UI tints."""
import json
import re

import main as shop
from conftest import ROOT

PRODUCTS = json.loads((ROOT / "catalog" / "products.json").read_text(encoding="utf-8"))
CATEGORIES = {"Beauty", "Books", "Electronics", "Fashion", "Home", "Sports"}


def test_products_are_complete_and_unique():
    assert len(PRODUCTS) == 48
    assert len({p["id"] for p in PRODUCTS}) == len(PRODUCTS)
    for p in PRODUCTS:
        assert re.fullmatch(r"P\d{3}", p["id"])
        assert p["name"] and p["description"] and p["emoji"]
        assert p["category"] in CATEGORIES
        assert p["price"] > 0 and round(p["price"], 2) == p["price"]
        assert 0 <= p["rating"] <= 5 and p["reviews"] >= 0
        assert p.get("badge") in (None, "Bestseller", "New", "Deal")


def test_emoji_render_on_windows_10():
    # Windows 10's Segoe UI Emoji stops at Emoji 12; newer ones (e.g. 🪴 U+1FAB4) render as empty boxes.
    for p in PRODUCTS:
        assert all(ord(ch) < 0x1FA70 for ch in p["emoji"]), f"{p['id']} {p['emoji']} is too new"


def test_every_category_has_eight_products():
    counts = {c: sum(p["category"] == c for p in PRODUCTS) for c in CATEGORIES}
    assert counts == {c: 8 for c in CATEGORIES}


def test_shop_categories_match():
    assert set(shop.CATEGORIES) == set(shop.EMOJI) == CATEGORIES


def test_admin_emoji_picker_renders_on_windows_10():
    for category, choices in shop.EMOJI.items():
        assert len(choices) == len(set(choices)) >= 12, category
        for e in choices:
            assert all(ord(ch) < 0x1FA70 for ch in e), f"{category} {e} is too new"


def test_seed_emoji_are_in_the_admin_picker():
    for p in PRODUCTS:
        assert p["emoji"] in shop.EMOJI[p["category"]], p["id"]


def test_categories_match_dashboard_colors():
    app = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
    for mode in ("light", "dark"):
        block = re.search(rf'"{mode}":\s*\{{("Beauty".*?)\}}', app, re.S).group(1)
        assert set(re.findall(r'"(\w+)":', block)) == CATEGORIES, mode


def test_categories_match_storefront_tints():
    js = (ROOT / "shop" / "static" / "common.js").read_text(encoding="utf-8")
    block = re.search(r"const CATS = \{(.*?)\n\};", js, re.S).group(1)
    assert set(re.findall(r"^\s*(\w+):", block, re.M)) == CATEGORIES


def test_featured_product_exists():
    js = (ROOT / "shop" / "static" / "app.js").read_text(encoding="utf-8")
    featured = re.search(r'const FEATURED = "(P\d{3})"', js).group(1)
    assert featured in {p["id"] for p in PRODUCTS}
