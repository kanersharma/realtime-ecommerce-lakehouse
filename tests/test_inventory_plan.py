"""Reorder math in dashboard/inventory.py: forecast, safety stock, reorder point, suggestion, status."""
import math

import pytest

import inventory as inv


def plan(on_hand, level=4.0, trend=0.0, sigma=0.0, avg=4.0, history=14, lead=3, cover=7):
    return inv.plan(on_hand, level, trend, sigma, avg, history, lead, cover)


def test_steady_demand_well_stocked():
    p = plan(100)
    assert p["forecast_per_day"] == 4 and p["days_of_cover"] == 25
    assert (p["reorder_point"], p["order_up_to"]) == (12, 40)             # 4×3, 4×(3+7)
    assert (p["status"], p["suggested_qty"]) == ("OK", 0)


@pytest.mark.parametrize("on_hand,status,qty", [
    (12, "Reorder now", 28),      # at the reorder point: order up to 40
    (5, "Reorder now", 35),
    (20, "Reorder soon", 20),     # within 2 days (8 units) of the reorder point
    (21, "OK", 0),
])
def test_reorder_thresholds(on_hand, status, qty):
    p = plan(on_hand)
    assert (p["status"], p["suggested_qty"]) == (status, qty)


def test_trend_raises_the_forecast_over_the_lead_time():
    # average demand over a 3-day lead time with +1/day trend: 4 + 1 × (3 + 1) / 2
    assert plan(100, trend=1.0)["forecast_per_day"] == 6


def test_falling_demand_is_clamped_at_zero():
    p = plan(100, level=1.0, trend=-2.0, lead=5)
    assert p["forecast_per_day"] == 0 and p["days_of_cover"] is None
    assert (p["status"], p["suggested_qty"]) == ("No demand", 0)


def test_safety_stock_grows_with_volatility_and_lead_time():
    p = plan(100, sigma=2.0, lead=4)
    assert math.isclose(p["safety_stock"], inv.Z * 2 * 2)                  # z × σ × √4
    assert math.isclose(p["reorder_point"], 4 * 4 + inv.Z * 4)


def test_sold_out_uses_the_uncensored_average():
    """While sold out nobody can buy, so recent zeros pull the EWMA down; use the plain average."""
    p = plan(0, level=0.5, avg=3.0)
    assert p["forecast_per_day"] == 3 and p["status"] == "Out of stock"
    assert p["suggested_qty"] == 30                                        # 3 × (3 + 7) − 0


def test_new_product_without_history():
    p = plan(50, level=0, avg=0, history=0)
    assert (p["forecast_per_day"], p["status"], p["suggested_qty"]) == (0, "No demand", 0)


def test_sold_out_without_measurable_demand_still_gets_a_suggestion():
    """Sold out for the whole window (or never stocked): demand is unknown, not zero."""
    for history in (0, 14):
        p = plan(0, level=0, avg=0, history=history, lead=3, cover=7)
        assert (p["status"], p["forecast_per_day"]) == ("Out of stock", inv.MIN_SOLD_OUT_DEMAND)
        assert p["suggested_qty"] == 10                                  # 1/day × (3 + 7)


def test_suggested_quantity_is_rounded_up():
    assert plan(12, level=4.3)["suggested_qty"] == math.ceil(4.3 * 10 - 12)


@pytest.mark.parametrize("trend,level,arrow", [(0.5, 4, "↗"), (-0.5, 4, "↘"), (0.1, 4, "→"), (1, 0, "→")])
def test_trend_arrow(trend, level, arrow):
    assert inv.trend_arrow(trend, level) == arrow


def test_statuses_are_ordered_by_urgency():
    assert inv.STATUSES[0] == "Out of stock" and inv.STATUSES[-1] == "No demand"


def test_sql_is_built_from_config_only():
    sql = inv.inventory_sql(day_seconds=30, history_days=10, decay=0.5)
    assert "/ 30)" in sql and "sequence(1, 10)" in sql and "power(0.5" in sql and "INTERVAL '330' SECOND" in sql


@pytest.mark.parametrize("bad", ["P001' OR '1'='1", "X001", "P", "", "P01; DROP TABLE orders"])
def test_daily_sales_sql_rejects_anything_but_a_product_id(bad):
    with pytest.raises(ValueError):
        inv.daily_sales_sql(bad)
