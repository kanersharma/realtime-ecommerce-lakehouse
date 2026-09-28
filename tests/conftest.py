"""Shared fixtures. Run everything with:  .venv/Scripts/python -m pytest   (see README "Testing")."""
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "shop"), str(ROOT / "generator")]

import main as shop  # noqa: E402  (shop/main.py)


def source_columns(table):
    """Column names of a Kafka source table in flink/sql/pipeline.sql (the event contract)."""
    sql = (ROOT / "flink" / "sql" / "pipeline.sql").read_text(encoding="utf-8")
    body = re.search(rf"CREATE TEMPORARY TABLE {table} \((.*?)\) WITH", sql, re.S).group(1)
    return {line.split()[0] for line in body.strip().splitlines()
            if line.strip() and not line.strip().startswith(("WATERMARK", "--"))}


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
