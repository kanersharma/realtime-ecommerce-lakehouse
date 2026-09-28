"""Inventory planning for the dashboard's 📦 Inventory tab: pure functions, unit-tested.

Time runs in *demo days*: one day = DEMO_DAY_SECONDS (default 60 s), so a live demo shows stock,
demand and reorder points move within minutes instead of weeks.

Trino computes, per product, from zero-filled daily sales over the last HISTORY_DAYS complete days:
  level  = exponentially weighted average of daily units (weight DECAY^(age-1): yesterday counts most)
  trend  = least-squares slope of daily units per day (regr_slope)
  sigma  = standard deviation of daily units
This module turns those into a reorder plan (a classic order-up-to policy):
  forecast/day   = max(0, level + trend * (lead_time + 1) / 2)   average demand while a refill is on its way
  safety stock   = Z * sigma * sqrt(lead_time)                    Z = 1.65, about a 95 % service level
  reorder point  = forecast/day * lead_time + safety stock         order when stock falls to this
  order-up-to    = forecast/day * (lead_time + target_cover) + safety stock
  suggested qty  = ceil(order-up-to - on_hand)
Demand is *censored* while a product is sold out (nobody can buy it), so its recent zeros understate
demand. For sold-out products the forecast is therefore at least the plain average over its history,
and at least MIN_SOLD_OUT_DEMAND: a product sold out for the whole window (or never sold yet) still
gets a restock suggestion of lead time + target cover units, instead of "order nothing".
"""
import math
import os

DAY_SECONDS = int(os.getenv("DEMO_DAY_SECONDS", "60"))
HISTORY_DAYS = 14
DECAY = 0.7
Z = 1.65
SOON_DAYS = 2  # "Reorder soon": within this many days of the reorder point
MIN_SOLD_OUT_DEMAND = 1.0  # units/day assumed for a sold-out product with no measurable demand

# Most urgent first; the dashboard sorts and colors by this.
STATUSES = ["Out of stock", "Reorder now", "Reorder soon", "OK", "No demand"]


def plan(on_hand, level, trend, sigma, avg_per_day, history_days, lead_time_days, target_cover_days):
    """-> forecast_per_day, days_of_cover (None without demand), safety_stock, reorder_point,
    order_up_to, suggested_qty, status."""
    forecast = max(0.0, level + trend * (lead_time_days + 1) / 2) if history_days else 0.0
    if on_hand <= 0:
        forecast = max(forecast, avg_per_day, MIN_SOLD_OUT_DEMAND)  # censored demand while sold out
    safety = Z * sigma * math.sqrt(lead_time_days)
    reorder_point = forecast * lead_time_days + safety
    order_up_to = forecast * (lead_time_days + target_cover_days) + safety

    if on_hand <= 0:
        status = "Out of stock"
    elif forecast == 0:
        status = "No demand"
    elif on_hand <= reorder_point:
        status = "Reorder now"
    elif on_hand <= reorder_point + forecast * SOON_DAYS:
        status = "Reorder soon"
    else:
        status = "OK"

    needs_order = status in ("Out of stock", "Reorder now", "Reorder soon")
    return {
        "forecast_per_day": forecast,
        "days_of_cover": on_hand / forecast if forecast > 0 else None,
        "safety_stock": safety,
        "reorder_point": reorder_point,
        "order_up_to": order_up_to,
        "suggested_qty": max(0, math.ceil(order_up_to - on_hand)) if needs_order else 0,
        "status": status,
    }


def trend_arrow(trend, level):
    """↗ / ↘ when the daily trend moves demand by more than 5 % of its level per day, else →."""
    if level <= 0 or abs(trend) <= 0.05 * level:
        return "→"
    return "↗" if trend > 0 else "↘"


def inventory_sql(day_seconds=DAY_SECONDS, history_days=HISTORY_DAYS, decay=DECAY):
    """Per-product stock (latest movement from the shop) and demand statistics (zero-filled daily
    sales from bronze orders). Numbers are ints/floats from config, never user input."""
    window = (history_days + 1) * day_seconds
    return f"""
-- one row per product: current stock + demand statistics over the last {history_days} complete demo days
WITH cfg AS (
  SELECT CAST(floor(to_unixtime(current_timestamp) / {day_seconds}) AS bigint) AS today
), latest AS (   -- the shop is the system of record; its newest movement per product is current stock
  SELECT *,
         row_number() OVER (PARTITION BY product_id ORDER BY event_time DESC, seq DESC) AS rn,
         min(event_time) OVER (PARTITION BY product_id) AS first_seen
  FROM inventory_movements
), stock AS (
  SELECT product_id, product_name, category, on_hand_after AS on_hand,
         CAST(unit_price AS double) AS unit_price, lead_time_days, target_cover_days,
         CAST(floor(to_unixtime(with_timezone(first_seen, 'UTC')) / {day_seconds}) AS bigint) AS first_day
  FROM latest
  WHERE rn = 1 AND reason <> 'removed'
), days AS (     -- calendar of complete days, so days without sales count as 0
  SELECT cfg.today - k AS day, k AS age FROM cfg CROSS JOIN UNNEST(sequence(1, {history_days})) AS t(k)
), sales AS (
  SELECT product_id,
         CAST(floor(to_unixtime(with_timezone(event_time, 'UTC')) / {day_seconds}) AS bigint) AS day,
         sum(quantity) AS units
  FROM orders
  WHERE event_time > localtimestamp - INTERVAL '{window}' SECOND
  GROUP BY 1, 2
), series AS (
  SELECT s.product_id, d.age, CAST(coalesce(x.units, 0) AS double) AS units
  FROM stock s
  CROSS JOIN days d
  LEFT JOIN sales x ON x.product_id = s.product_id AND x.day = d.day
  WHERE d.day >= s.first_day
), stats AS (
  SELECT product_id,
         count(*) AS history_days,
         sum(units * power({decay}, age - 1)) / sum(power({decay}, age - 1)) AS level,
         coalesce(regr_slope(units, CAST(-age AS double)), 0) AS trend,
         coalesce(stddev_samp(units), 0) AS sigma,
         avg(units) AS avg_per_day,
         sum(CASE WHEN age <= 7 THEN units ELSE 0 END) AS sold_7d
  FROM series
  GROUP BY 1
)
SELECT s.product_id, s.product_name, s.category, s.on_hand, s.unit_price,
       s.lead_time_days, s.target_cover_days,
       coalesce(st.history_days, 0) AS history_days, coalesce(st.level, 0) AS level,
       coalesce(st.trend, 0) AS trend, coalesce(st.sigma, 0) AS sigma,
       coalesce(st.avg_per_day, 0) AS avg_per_day, coalesce(st.sold_7d, 0) AS sold_7d
FROM stock s
LEFT JOIN stats st ON st.product_id = s.product_id
ORDER BY s.product_id
"""


def daily_sales_sql(product_id, day_seconds=DAY_SECONDS, history_days=HISTORY_DAYS):
    """Zero-filled units sold per complete demo day for one product (for the demand chart)."""
    if not (product_id[:1] == "P" and product_id[1:].isdigit()):
        raise ValueError(f"not a product id: {product_id!r}")
    window = (history_days + 1) * day_seconds
    return f"""
WITH cfg AS (
  SELECT CAST(floor(to_unixtime(current_timestamp) / {day_seconds}) AS bigint) AS today
), days AS (
  SELECT cfg.today - k AS day, k AS age FROM cfg CROSS JOIN UNNEST(sequence(1, {history_days})) AS t(k)
), sales AS (
  SELECT CAST(floor(to_unixtime(with_timezone(event_time, 'UTC')) / {day_seconds}) AS bigint) AS day,
         sum(quantity) AS units
  FROM orders
  WHERE product_id = '{product_id}' AND event_time > localtimestamp - INTERVAL '{window}' SECOND
  GROUP BY 1
)
SELECT d.age, coalesce(s.units, 0) AS units
FROM days d LEFT JOIN sales s ON s.day = d.day
ORDER BY d.age DESC
"""
