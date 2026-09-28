"""The catalog is shared by the shop, the simulator, the dashboard colors and the UI tints."""
import json
import re

from conftest import ROOT

PRODUCTS = json.loads((ROOT / "catalog" / "products.json").read_text(encoding="utf-8"))
CATEGORIES = {"Beauty", "Books", "Electronics", "Fashion", "Home", "Sports"}


def test_products_are_complete_and_unique():
    assert len(PRODUCTS) == 24
    assert len({p["id"] for p in PRODUCTS}) == len(PRODUCTS)
    for p in PRODUCTS:
        assert re.fullmatch(r"P\d{3}", p["id"])
        assert p["name"] and p["description"] and p["emoji"]
        assert p["category"] in CATEGORIES
        assert p["price"] > 0 and round(p["price"], 2) == p["price"]
        assert 0 <= p["rating"] <= 5 and p["reviews"] >= 0
        assert p.get("badge") in (None, "Bestseller", "New", "Deal")


def test_every_category_has_products():
    assert {p["category"] for p in PRODUCTS} == CATEGORIES


def test_categories_match_dashboard_colors():
    app = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
    for mode in ("light", "dark"):
        block = re.search(rf'"{mode}":\s*\{{("Beauty".*?)\}}', app, re.S).group(1)
        assert set(re.findall(r'"(\w+)":', block)) == CATEGORIES, mode


def test_categories_match_storefront_tints():
    js = (ROOT / "shop" / "static" / "app.js").read_text(encoding="utf-8")
    block = re.search(r"const CATS = \{(.*?)\n\};", js, re.S).group(1)
    assert set(re.findall(r"^\s*(\w+):", block, re.M)) == CATEGORIES


def test_featured_product_exists():
    js = (ROOT / "shop" / "static" / "app.js").read_text(encoding="utf-8")
    featured = re.search(r'const FEATURED = "(P\d{3})"', js).group(1)
    assert featured in {p["id"] for p in PRODUCTS}
