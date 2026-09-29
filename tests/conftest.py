"""Shared fixtures. Run everything with:  .venv/Scripts/python -m pytest   (see README "Testing")."""
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / d) for d in ("shop", "generator", "dashboard", "maintenance", "scripts")]

import main as shop  # noqa: E402  (shop/main.py)


def source_types(table):
    """{column: SQL type} of a Kafka source table in flink/sql/pipeline.sql (the event contract)."""
    sql = (ROOT / "flink" / "sql" / "pipeline.sql").read_text(encoding="utf-8")
    body = re.search(rf"CREATE TEMPORARY TABLE {table} \((.*?)\) WITH", sql, re.S).group(1)
    lines = [line.split("--")[0].strip().rstrip(",") for line in body.splitlines()]
    return {line.split(None, 1)[0]: line.split(None, 1)[1] for line in lines
            if line and not line.startswith("WATERMARK")}


def source_columns(table):
    return set(source_types(table))


@pytest.fixture(autouse=True, scope="session")
def developer_db_untouched():
    """Guard for Rules R-CAT-6: the test run must not create or modify shop/data/shop.db."""
    dev = ROOT / "shop" / "data" / "shop.db"
    before = dev.stat().st_mtime if dev.exists() else None
    yield
    after = dev.stat().st_mtime if dev.exists() else None
    assert after == before, "tests wrote to the developer database shop/data/shop.db"


@pytest.fixture(autouse=True)
def shop_db(tmp_path, monkeypatch):
    """Every test gets a fresh catalog database, seeded from catalog/products.json.
    The shop reads SHOP_DB on each request, so this also covers the in-process e2e server."""
    monkeypatch.setenv("SHOP_DB", str(tmp_path / "shop.db"))
    return tmp_path / "shop.db"


@pytest.fixture
def events(monkeypatch):
    """Capture what the shop would send to Kafka: list of (topic, key, event)."""
    sent = []
    monkeypatch.setattr(shop, "publish", lambda topic, key, event: sent.append((topic, key, event)))
    return sent


@pytest.fixture
def client(events):
    from fastapi.testclient import TestClient
    return TestClient(shop.app)
