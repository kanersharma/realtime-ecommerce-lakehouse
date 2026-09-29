"""Streamlit dashboard: every number is a live Trino query against Iceberg tables.

Tabs:
  Live Business       - KPIs, revenue per minute, funnel, top products (auto-refresh)
  Inventory           - stock, demand forecast (EWMA + trend), days of cover, reorder suggestions
  Lakehouse Internals - Iceberg snapshots, small files, compaction, time travel
  SQL Playground      - read-only ad-hoc Trino SQL
"""
import json
import os
import time
import urllib.request

import altair as alt
import pandas as pd
import streamlit as st
import trino

import inventory as inv  # pure forecasting / reorder math (dashboard/inventory.py, unit-tested)

TABLES = ["orders", "clicks", "inventory_movements", "revenue_per_minute", "funnel_per_minute"]
SHOP_URL = os.getenv("SHOP_URL", "http://localhost:8000")  # restock goes to the shop, the system of record
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")  # read-only: subjects and versions
# Stock status colors: reserved for status (never a category), always shown with the status text.
STATUS_COLORS = {"Out of stock": "#C62828", "Reorder now": "#F2A900", "Reorder soon": "#FFE08A",
                 "OK": "#7CE0C3", "No demand": "#C9C9C9"}
READ_ONLY = ("select", "with", "show", "describe", "explain")
LINKS = {
    "🛒 Lakeshop (make real events)": "http://localhost:8000",
    "Flink UI (jobs, checkpoints)": "http://localhost:8081",
    "Kafka UI (topics, messages)": "http://localhost:8088",
    "Trino UI (queries)": "http://localhost:8090",
    "RustFS console (Parquet files)": "http://localhost:9001",
}

st.set_page_config(page_title="Real-time Lakehouse", page_icon="⚡", layout="wide")

# ---------------------------------------------------------------- neo-brutalist look (light + dark)
# Base theme lives in .streamlit/config.toml; this adds borders, hard shadows, type and dark mode.
YELLOW, PINK, BLUE = "#FFD23F", "#FF6FB5", "#2F6BFF"
TILE_INK = "#111111"  # text on bright fills (hero, KPI tiles, pink buttons) stays dark in both modes
KPI_COLORS = [YELLOW, PINK, "#7CE0C3", "#A9C4FF", "#FFB27A"]
# Colors that flip with the theme.
MODES = {
    "light": {"paper": "#FFFBEF", "card": "#FFFFFF", "field": "#FFF1C1", "side": "#FFF1C1",
              "ink": "#111111", "grid": "#1111111A"},
    "dark":  {"paper": "#15130F", "card": "#22201B", "field": "#2E2A22", "side": "#1C1A16",
              "ink": "#FFFBEF", "grid": "#FFFBEF1F"},
}
# Fixed category -> color, so a category keeps its color whatever the data holds. Same hues in both
# modes, stepped for each card surface; both validated with the dataviz palette validator
# (adjacent pairs in stack order: lightness band, chroma, CVD separation, contrast).
CATEGORY_COLORS = {
    "light": {"Beauty": "#E83E8C", "Books": "#8A5A00", "Electronics": "#2F6BFF",
              "Fashion": "#E8700A", "Home": "#128A5E", "Sports": "#5FB7FF"},
    "dark":  {"Beauty": "#E83E8C", "Books": "#A87520", "Electronics": "#3D78FF",
              "Fashion": "#D9660A", "Home": "#179A68", "Sports": "#3E9BE6"},
}

with st.sidebar:
    st.header("⚡ Real-time Lakehouse")
    # Kept in the URL (?theme=dark) so the choice survives reloads and can be shared.
    dark = st.toggle("🌙 Dark mode", value=st.query_params.get("theme") == "dark")
    st.query_params["theme"] = "dark" if dark else "light"
MODE = "dark" if dark else "light"
M = MODES[MODE]
CATS = CATEGORY_COLORS[MODE]

CSS = f"""
@import url('https://fonts.googleapis.com/css2?family=Archivo+Black&family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@500&display=swap');
:root {{ --paper: {M["paper"]}; --card: {M["card"]}; --field: {M["field"]}; --side: {M["side"]}; --ink: {M["ink"]}; }}

html, body, p, li, label, input, textarea, button, [data-testid="stMarkdownContainer"] {{
  font-family: 'Space Grotesk', sans-serif !important;
}}
h1, h2, h3 {{ font-family: 'Archivo Black', sans-serif !important; letter-spacing: -0.02em; }}
code, pre {{ font-family: 'JetBrains Mono', monospace !important; }}
.block-container {{ padding-top: 3.5rem; }}
/* Streamlit fades elements while a fragment re-runs; with 10 s auto-refresh and multi-second queries the
   page would be faded half the time. Keep it readable: the header's RUNNING indicator shows activity. */
[data-stale="true"] {{ opacity: 1 !important; transition: none !important; }}

/* page colors (flip with the theme) */
.stApp, [data-testid="stHeader"] {{ background: var(--paper); }}
.stApp, .stApp p, .stApp li, .stApp label, .stApp h1, .stApp h2, .stApp h3, .stApp small,
[data-testid="stCaptionContainer"], [data-testid="stWidgetLabel"], [data-testid="stHeader"] button,
[data-testid="stSliderTickBar"] div {{ color: var(--ink); }}

/* hero */
.hero {{ background: {YELLOW}; border: 3px solid var(--ink); box-shadow: 8px 8px 0 var(--ink); padding: 28px 32px; margin-bottom: 28px; }}
.stApp .hero h1, .stApp .hero p, .hero .chips {{ color: {TILE_INK}; }}
.hero h1 {{ font-size: 3rem; line-height: 1.05; margin: 8px 0 12px; padding: 0; }}
.hero h1 .mark {{ background: {PINK}; border: 3px solid {TILE_INK}; padding: 0 10px; display: inline-block; transform: rotate(-1deg); }}
.hero p {{ font-size: 1.05rem; font-weight: 500; margin: 0 0 16px; max-width: 760px; }}
.kicker {{ display: inline-flex; align-items: center; gap: 8px; background: {TILE_INK}; color: #fff; font-weight: 700;
          font-size: .8rem; letter-spacing: .12em; padding: 4px 10px; }}
.dot {{ width: 10px; height: 10px; border-radius: 50%; background: #3DFF8B; animation: pulse 1.4s infinite; }}
@keyframes pulse {{ 50% {{ opacity: .25; }} }}
.chips {{ display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-weight: 700; }}
.chips span {{ background: #fff; border: 2px solid {TILE_INK}; box-shadow: 3px 3px 0 {TILE_INK}; padding: 3px 10px; font-size: .85rem; }}

/* KPI tiles: bright fills, dark text in both modes */
[data-testid="stMetric"] {{ border: 3px solid var(--ink); box-shadow: 6px 6px 0 var(--ink); padding: 14px 18px; background: var(--card); }}
[data-testid="stMetricValue"] {{ font-family: 'Archivo Black', sans-serif !important; font-size: 2rem; }}
[data-testid="stMetricLabel"] p {{ font-weight: 700 !important; text-transform: uppercase; letter-spacing: .04em; font-size: .72rem !important; }}
[data-testid="stMetricLabel"] div, [data-testid="stMetricLabel"] p, [data-testid="stMetricDelta"] div {{ white-space: normal !important; overflow: visible !important; }}
[data-testid="stMetricDelta"] {{ font-weight: 700; }}
{"".join(f'[data-testid="stHorizontalBlock"] > [data-testid="stColumn"]:nth-child({i + 1}) [data-testid="stMetric"] {{ background: {c}; }}' for i, c in enumerate(KPI_COLORS))}
[data-testid="stColumn"] [data-testid="stMetric"] *, .stApp [data-testid="stColumn"] [data-testid="stMetric"] p {{ color: {TILE_INK} !important; }}
[data-testid="stMetricDelta"] svg {{ fill: {TILE_INK}; }}

/* tabs */
.stTabs [data-baseweb="tab-list"] {{ gap: 12px; }}
.stTabs [data-baseweb="tab"] {{ border: 3px solid var(--ink); background: var(--card); padding: 6px 16px; box-shadow: 4px 4px 0 var(--ink); }}
.stTabs [data-baseweb="tab"] p {{ font-weight: 700; }}
.stTabs [aria-selected="true"] {{ background: var(--ink); }}
.stApp .stTabs [aria-selected="true"] p {{ color: var(--paper); }}
.stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] {{ display: none; }}

/* buttons */
.stButton > button {{ border: 3px solid var(--ink); border-radius: 0; box-shadow: 4px 4px 0 var(--ink); font-weight: 700;
                     background: var(--card); color: var(--ink); transition: transform .08s, box-shadow .08s; }}
.stButton > button[kind="primary"], .stApp .stButton > button[kind="primary"] p {{ background: {PINK}; color: {TILE_INK}; }}
.stButton > button:hover {{ transform: translate(2px, 2px); box-shadow: 2px 2px 0 var(--ink); border-color: var(--ink); color: var(--ink); }}
.stButton > button:active {{ transform: translate(4px, 4px); box-shadow: none; }}

/* inputs & menus */
[data-baseweb="select"] > div, [data-baseweb="textarea"], .stTextArea textarea {{
  border: 3px solid var(--ink) !important; border-radius: 0 !important; background: var(--field) !important; color: var(--ink) !important; }}
[data-baseweb="select"] div, [data-baseweb="select"] svg {{ color: var(--ink); }}
[data-baseweb="popover"] ul, [data-baseweb="menu"] {{ background: var(--card) !important; }}
[data-baseweb="popover"] li {{ color: var(--ink); }}

/* expanders, tables, alerts, code */
[data-testid="stExpander"] details {{ border: 2px solid var(--ink); border-radius: 0; background: var(--card); }}
[data-testid="stExpander"] summary, [data-testid="stExpander"] summary svg {{ color: var(--ink); }}
[data-testid="stExpander"] summary p {{ font-family: 'JetBrains Mono', monospace !important; font-size: .8rem; }}
[data-testid="stDataFrame"] {{ border: 2px solid var(--ink); }}
[data-testid="stAlert"] {{ border: 3px solid var(--ink); border-radius: 0; box-shadow: 5px 5px 0 var(--ink); background: var(--card); }}
[data-testid="stAlert"] > div {{ background: transparent; color: var(--ink); }}
[data-testid="stCode"] pre, .stCode pre, .stMarkdown pre {{ border: 2px solid var(--ink); border-radius: 0; background: var(--field) !important; }}
.stMarkdown pre code, [data-testid="stCode"] code {{ color: var(--ink); }}

/* chart cards (st.container(border=True) holding a .card-title) */
[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] .card-title) {{
  border: 3px solid var(--ink) !important; border-radius: 0 !important; box-shadow: 8px 8px 0 var(--ink); background: var(--card);
}}
.card-title {{ font-family: 'Archivo Black', sans-serif; font-size: 1.25rem; margin: 0 0 4px; color: var(--ink); }}
.card-title .tag {{ font-family: 'Space Grotesk', sans-serif; font-size: .7rem; font-weight: 700; letter-spacing: .1em;
                   background: var(--ink); color: var(--paper); padding: 2px 8px; margin-left: 8px; vertical-align: middle; }}

/* sidebar */
[data-testid="stSidebar"] {{ border-right: 3px solid var(--ink); background: var(--side); }}
[data-testid="stSidebar"] a {{ color: var(--ink); font-weight: 700; text-decoration: underline 3px {PINK}; }}
"""
if dark:
    # Data grids are drawn on a canvas that CSS can't recolor, so flip the light grid instead.
    # The border is set to black because it gets inverted too.
    CSS += """[data-testid="stDataFrame"] { filter: invert(1) hue-rotate(180deg); border-color: #000; }\n"""
st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)


def card(title, tag=None):
    """A bordered 'card' container with a heavy title; use as `with card(...):`."""
    box = st.container(border=True)
    tag_html = f'<span class="tag">{tag}</span>' if tag else ""
    box.markdown(f'<div class="card-title">{title}{tag_html}</div>', unsafe_allow_html=True)
    return box


def brutal(chart, height=300):
    """Shared Altair styling: ink-colored axes, no chart border, Space Grotesk type."""
    return (chart.properties(height=height)
            .configure(font="Space Grotesk", background="transparent")
            .configure_view(stroke=None)
            .configure_axis(domainColor=M["ink"], domainWidth=2, tickColor=M["ink"], labelColor=M["ink"],
                            titleColor=M["ink"], gridColor=M["grid"], labelFontSize=12, titleFontWeight=700)
            .configure_legend(labelFontSize=12, labelColor=M["ink"], symbolStrokeColor=M["ink"],
                              symbolStrokeWidth=1.5, orient="bottom", title=None))


def draw(chart, height=300):
    st.altair_chart(brutal(chart, height), use_container_width=True, theme=None)


@st.cache_resource
def conn():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="dashboard",
        catalog="lakehouse",
        schema="shop",
        timezone="UTC",  # event_time is stored as UTC, so localtimestamp == UTC now
    )


def query(sql):
    """Run SQL on Trino -> (DataFrame, elapsed ms)."""
    start = time.perf_counter()
    cur = conn().cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[c[0] for c in cur.description])
    return df, (time.perf_counter() - start) * 1000


def code(text):
    # Syntax-highlighted blocks (st.code, or ```sql fences) render "[object Object]" after a
    # fragment re-run in Streamlit 1.41, so SQL is shown as a plain fence without a language.
    st.markdown(f"```\n{text.strip()}\n```")


def show_sql(sql, ms, rows):
    with st.expander(f"🔍 SQL · {ms:.0f} ms · {rows} rows"):
        code(sql)


# ---------------------------------------------------------------- queries
KPI_SQL = """
SELECT
  count_if(event_time >  localtimestamp - INTERVAL '5' MINUTE) AS orders_now,
  count_if(event_time <= localtimestamp - INTERVAL '5' MINUTE) AS orders_prev,
  CAST(coalesce(sum(total_amount) FILTER (WHERE event_time >  localtimestamp - INTERVAL '5' MINUTE), 0) AS double) AS revenue_now,
  CAST(coalesce(sum(total_amount) FILTER (WHERE event_time <= localtimestamp - INTERVAL '5' MINUTE), 0) AS double) AS revenue_prev,
  date_diff('second', max(event_time), localtimestamp) AS freshness_s
FROM orders
WHERE event_time > localtimestamp - INTERVAL '10' MINUTE
"""

FUNNEL_SQL = """
WITH c AS (
  SELECT count(DISTINCT session_id)          AS sessions,
         count_if(event_type = 'page_view')   AS page_views,
         count_if(event_type = 'add_to_cart') AS add_to_carts
  FROM clicks
  WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
), o AS (
  SELECT count(*) AS orders
  FROM orders
  WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
)
SELECT c.*, o.orders FROM c CROSS JOIN o
"""

REVENUE_SQL = """
-- Closed minutes: the gold table written by Flink's 1-minute tumbling windows.
-- Open minutes: a window only closes when the watermark (newest event - 5 s) passes its end, and
-- the watermark only moves when new events arrive. In a quiet store the latest minutes would never
-- show, so they are computed live from the bronze orders table (committed every 10 s).
WITH cutoff AS (
  SELECT coalesce(max(window_end), TIMESTAMP '1970-01-01 00:00:00') AS t FROM revenue_per_minute
), closed AS (
  SELECT window_start AS minute, category, CAST(sum(revenue) AS double) AS revenue, 'closed' AS status
  FROM revenue_per_minute
  WHERE window_start > localtimestamp - INTERVAL '30' MINUTE
  GROUP BY 1, 2
), open_minutes AS (
  SELECT date_trunc('minute', event_time) AS minute, category,
         CAST(sum(total_amount) AS double) AS revenue, 'live' AS status
  FROM orders CROSS JOIN cutoff
  WHERE event_time >= cutoff.t AND event_time > localtimestamp - INTERVAL '30' MINUTE
  GROUP BY 1, 2
)
SELECT * FROM closed
UNION ALL
SELECT * FROM open_minutes
ORDER BY 1
"""

TOP_PRODUCTS_SQL = """
SELECT product_name, category,
       sum(quantity)                      AS units,
       CAST(sum(total_amount) AS double)  AS revenue
FROM orders
WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 10
"""

COUNTRY_SQL = """
SELECT country, CAST(sum(total_amount) AS double) AS revenue
FROM orders
WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
GROUP BY 1
ORDER BY 2 DESC
"""

DEVICE_SQL = """
-- `device` arrived in clicks schema v2; events from before it read NULL (Iceberg added the column
-- in place, without rewriting old files).
WITH s AS (
  SELECT session_id, coalesce(max(device), 'unknown (before v2)') AS device
  FROM clicks
  WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
  GROUP BY 1
), o AS (
  SELECT DISTINCT session_id FROM orders WHERE event_time > localtimestamp - INTERVAL '15' MINUTE
)
SELECT s.device, count(*) AS sessions, count(o.session_id) AS converted
FROM s LEFT JOIN o ON o.session_id = s.session_id
GROUP BY 1
ORDER BY 2 DESC
"""


def registry(path):
    with urllib.request.urlopen(f"{REGISTRY_URL}{path}", timeout=3) as r:
        return json.load(r)


def waiting(err):
    st.info(
        "⏳ **Waiting for data.** Go shop at [Lakeshop](http://localhost:8000) (or start the "
        "simulator with `docker compose --profile simulator up -d`). Flink commits to Iceberg every "
        "10 s, and the first windowed aggregates appear ~1 minute later. This page refreshes on its own."
    )
    with st.expander("Details"):
        code(str(err))


def pct_delta(now, prev):
    return f"{(now - prev) / prev:+.1%} vs prev 5m" if prev else None


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    refresh = st.selectbox("Auto-refresh", [5, 10, 30, 0], index=1,
                           format_func=lambda s: f"every {s}s" if s else "off")
    st.markdown("**Open the other UIs**")
    for label, url in LINKS.items():
        st.markdown(f"- [{label}]({url})")
    st.markdown("**Data flow**")
    st.markdown("""```
Lakeshop  (+ optional simulator)
   │  Avro (Schema Registry)
   ▼
Kafka  clicks · orders
       · inventory
   │
   ▼
Flink SQL  watermarks, windows
   │  commit per checkpoint
   ▼
Iceberg on RustFS (S3)
   │
   ▼
Trino  →  this dashboard
```""")

st.markdown("""
<div class="hero">
  <span class="kicker"><span class="dot"></span>LIVE · STREAMING LAKEHOUSE</span>
  <h1>Real-time E-commerce <span class="mark">Lakehouse</span></h1>
  <p>Every number on this page is a live SQL query against Apache Iceberg tables that a
     Flink job writes every 10 seconds. Open any 🔍 to see the exact query.</p>
  <div class="chips"><span>LAKESHOP</span>→<span>KAFKA</span>→<span>FLINK SQL</span>→<span>APACHE ICEBERG</span>→<span>TRINO</span>→<span>YOU</span></div>
</div>
""", unsafe_allow_html=True)

live_tab, inventory_tab, internals_tab, sql_tab = st.tabs(
    ["📈 Live Business", "📦 Inventory", "🔬 Lakehouse Internals", "🧪 SQL Playground"])


# ---------------------------------------------------------------- live tab
@st.fragment(run_every=refresh or None)
def live():
    try:
        kpi, kpi_ms = query(KPI_SQL)
        funnel, funnel_ms = query(FUNNEL_SQL)
    except Exception as err:  # tables not created yet / Trino still starting
        waiting(err)
        return

    k, f = kpi.iloc[0], funnel.iloc[0]
    aov = k.revenue_now / k.orders_now if k.orders_now else 0
    conversion = f.orders / f.sessions if f.sessions else 0
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue · last 5 min", f"${k.revenue_now:,.0f}", pct_delta(k.revenue_now, k.revenue_prev))
    c2.metric("Orders · last 5 min", f"{int(k.orders_now):,}", pct_delta(k.orders_now, k.orders_prev))
    c3.metric("Avg order value", f"${aov:,.2f}")
    c4.metric("Conversion · 15 min", f"{conversion:.1%}", help="orders ÷ sessions")
    c5.metric("Data freshness", "–" if pd.isna(k.freshness_s) else f"{k.freshness_s:.0f} s",
              help="Now minus the newest event visible in Iceberg. "
                   "≈ checkpoint interval (10 s) + event-time jitter.")
    show_sql(KPI_SQL, kpi_ms, len(kpi))

    st.write("")
    left, right = st.columns([2, 1], gap="large")
    with left, card("Revenue per minute", "GOLD WINDOWS + LIVE"):
        rev, ms = query(REVENUE_SQL)
        if rev.empty:
            st.caption("No orders in the last 30 minutes. Place one in the store and it appears here "
                       "within about 10 s.")
        else:
            rev["time"] = pd.to_datetime(rev.minute).dt.strftime("%H:%M")
            rev["window"] = rev.status.map({"closed": "closed (gold)", "live": "open (live, provisional)"})
            draw(alt.Chart(rev).mark_bar(stroke=M["ink"], strokeWidth=1.5).encode(
                x=alt.X("time:O", title=None, axis=alt.Axis(labelAngle=0, labelOverlap="greedy")),
                y=alt.Y("revenue:Q", title="Revenue ($)", stack=True),
                color=alt.Color("category:N", scale=alt.Scale(
                    domain=list(CATS), range=list(CATS.values()))),
                order=alt.Order("category:N"),
                opacity=alt.condition(alt.datum.status == "live", alt.value(0.4), alt.value(1.0)),
                strokeDash=alt.condition(alt.datum.status == "live", alt.value([4, 3]), alt.value([1, 0])),
                tooltip=["time:O", "category:N", alt.Tooltip("revenue:Q", format="$,.0f"), "window:N"],
            ))
            if (rev.status == "live").any():
                st.caption("Faded, dashed bars are minutes Flink hasn't closed yet. The window closes "
                           "once newer events move the watermark past it, and then the bar turns solid.")
        show_sql(REVENUE_SQL, ms, len(rev))
    with right, card("Funnel", "LAST 15 MIN"):
        steps = pd.DataFrame({
            "step": ["Sessions", "Page views", "Add to cart", "Orders"],
            "count": [f.sessions, f.page_views, f.add_to_carts, f.orders],
        })
        base = alt.Chart(steps).encode(
            y=alt.Y("step:N", sort=None, title=None),
            x=alt.X("count:Q", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False),
                    scale=alt.Scale(domain=[0, max(steps["count"].max(), 1) * 1.45])),  # room for labels
        )
        draw(base.mark_bar(fill=YELLOW, stroke=M["ink"], strokeWidth=2.5, height=34)
             .encode(tooltip=["step", alt.Tooltip("count:Q", format=",")])
             + base.mark_text(align="left", dx=8, fontWeight=700, fontSize=14, color=M["ink"])
             .encode(text=alt.Text("count:Q", format=",")), height=300)
        show_sql(FUNNEL_SQL, funnel_ms, len(funnel))

    st.write("")
    left, right = st.columns([2, 1], gap="large")
    with left, card("Top products", "LAST 15 MIN"):
        top, ms = query(TOP_PRODUCTS_SQL)
        st.dataframe(top, hide_index=True, use_container_width=True, column_config={
            "product_name": "Product", "category": "Category", "units": "Units",
            "revenue": st.column_config.ProgressColumn(
                "Revenue", format="$%.0f", min_value=0,
                max_value=float(top.revenue.max()) if len(top) else 1.0),
        })
        show_sql(TOP_PRODUCTS_SQL, ms, len(top))
    with right, card("Revenue by country", "LAST 15 MIN"):
        countries, ms = query(COUNTRY_SQL)
        draw(alt.Chart(countries).mark_bar(fill=BLUE, stroke=M["ink"], strokeWidth=2).encode(
            x=alt.X("country:N", sort="-y", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("revenue:Q", title="Revenue ($)"),
            tooltip=["country", alt.Tooltip("revenue:Q", format="$,.0f")],
        ), height=340)
        show_sql(COUNTRY_SQL, ms, len(countries))

    st.write("")
    left, right = st.columns([2, 1], gap="large")
    with left, card("Sessions by device", "LAST 15 MIN · SCHEMA V2"):
        devices, ms = query(DEVICE_SQL)
        devices["label"] = [f"{n:,} · {c / n:.0%}" for n, c in zip(devices.sessions, devices.converted)]
        base = alt.Chart(devices).encode(
            y=alt.Y("device:N", sort=None, title=None),
            x=alt.X("sessions:Q", title=None, axis=alt.Axis(labels=False, ticks=False, grid=False),
                    scale=alt.Scale(domain=[0, max(devices.sessions.max() if len(devices) else 1, 1) * 1.8])))
        draw(base.mark_bar(fill=PINK, stroke=M["ink"], strokeWidth=2, height=28)
             .encode(tooltip=["device", "sessions", "converted"])
             + base.mark_text(align="left", dx=8, fontWeight=700, color=M["ink"]).encode(text="label:N"),
             height=max(120, 48 * len(devices)))
        st.caption("Sessions · share that ordered. `device` was added to the click schema in v2 (mobile / "
                   "tablet / desktop, from the User-Agent); older events have none and show as unknown.")
        show_sql(DEVICE_SQL, ms, len(devices))
    with right, card("Event schemas", "REGISTRY"):
        try:
            level = registry("/config")["compatibilityLevel"]
            st.markdown("\n".join(f"- `{s}` · {len(registry(f'/subjects/{s}/versions'))} version(s)"
                                   for s in sorted(registry("/subjects"))))
            st.caption(f"Compatibility **{level}**: every new version must read the old events and be "
                       "readable by the old consumers, so a producer can't break the Flink job. Only "
                       "optional fields with a default can be added.")
        except Exception as err:
            st.caption(f"Schema Registry not reachable at {REGISTRY_URL}: {err}")


with live_tab:
    live()


# ---------------------------------------------------------------- inventory tab
INVENTORY_SQL = inv.inventory_sql()


def restock_via_shop(product_id, quantity):
    """POST to the shop (it owns stock); the new stock reaches this page via Kafka → Flink → Iceberg."""
    req = urllib.request.Request(
        f"{SHOP_URL}/api/products/{product_id}/restock", data=json.dumps({"quantity": int(quantity)}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def cover_text(r):
    if r.on_hand <= 0:
        return "sold out"
    return "no demand yet" if pd.isna(r.days_of_cover) else f"{r.days_of_cover:.1f} days of cover"


@st.fragment(run_every=refresh or None)
def inventory_view():
    try:
        stock, ms = query(INVENTORY_SQL)
    except Exception as err:  # table not created yet / Trino still starting
        waiting(err)
        return
    if stock.empty:
        st.info("⏳ **No stock data yet.** The shop publishes a stock snapshot when it starts, and every "
                "order and restock after that. It reaches this page within about 10 s.")
        return

    plans = pd.DataFrame([inv.plan(r.on_hand, r.level, r.trend, r.sigma, r.avg_per_day, r.history_days,
                                   r.lead_time_days, r.target_cover_days) for r in stock.itertuples()])
    df = pd.concat([stock, plans], axis=1)
    df["trend_arrow"] = [inv.trend_arrow(t, l) for t, l in zip(df.trend, df.level)]
    df["priority"] = df.status.map(inv.STATUSES.index)
    df = df.sort_values(["priority", "days_of_cover"], na_position="last").reset_index(drop=True)

    needs = int(df.status.isin(["Out of stock", "Reorder now"]).sum())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Reorder now", needs, help="Out of stock, or at/below the reorder point")
    c2.metric("Out of stock", int((df.on_hand <= 0).sum()))
    c3.metric("Stock value", f"${(df.on_hand * df.unit_price).sum():,.0f}")
    c4.metric("Units sold · 7 days", f"{int(df.sold_7d.sum()):,}")
    c5.metric("1 demo day", f"{inv.DAY_SECONDS} s", help="Days run fast so a live demo shows demand, "
              "lead times and reorders within minutes (DEMO_DAY_SECONDS).")

    st.write("")
    with card("Reorder suggestions", "FORECAST · EWMA + TREND"):
        todo = df[df.suggested_qty > 0].head(6)
        if todo.empty:
            st.caption("Nothing to reorder right now: every product with demand is above its reorder point.")
        else:
            cols = st.columns(3)
            for i, r in enumerate(todo.itertuples()):
                with cols[i % 3]:
                    st.markdown(f"**{r.product_name}**  \n{r.status} · {r.on_hand} on hand · {cover_text(r)}  \n"
                                f"forecast {r.forecast_per_day:.1f}/day · lead time {r.lead_time_days} d")
                    if st.button(f"Restock {r.suggested_qty}", key=f"restock_{r.product_id}", type="primary"):
                        try:
                            p = restock_via_shop(r.product_id, r.suggested_qty)
                            st.success(f"Restocked {p['name']}: {p['on_hand']} on hand. This page catches up "
                                       "in about 10 s (shop → Kafka → Flink → Iceberg).")
                        except Exception as err:
                            st.error(f"Restock failed: {err}")
            if len(df[df.suggested_qty > 0]) > 1 and st.button(
                    f"Restock all {int((df.suggested_qty > 0).sum())} suggestions", key="restock_all"):
                try:
                    for r in df[df.suggested_qty > 0].itertuples():
                        restock_via_shop(r.product_id, r.suggested_qty)
                    st.success(f"Restocked {int((df.suggested_qty > 0).sum())} products. This page catches up "
                               "in about 10 s.")
                except Exception as err:
                    st.error(f"Restock failed: {err}")
        st.caption(f"Suggested quantity = forecast × (lead time + target cover) + safety stock − on hand. "
                   f"Safety stock = {inv.Z} × σ(daily demand) × √lead time (about a 95 % service level).")

    st.write("")
    left, right = st.columns([3, 2], gap="large")
    with left, card("Days of cover", "MOST URGENT 15"):
        urgent = df[(df.on_hand <= 0) | df.days_of_cover.notna()].head(15).copy()
        if urgent.empty:
            st.caption("No sales yet, so there's no demand to measure cover against. Shop a little, or run "
                       "the demo traffic, and check back in a few demo days.")
        else:
            urgent["cover"] = urgent.days_of_cover.fillna(0).clip(upper=60)
            urgent["label"] = [("sold out" if o <= 0 else f"{c:.1f} d") for o, c in zip(urgent.on_hand, urgent.cover)]
            base = alt.Chart(urgent).encode(y=alt.Y("product_name:N", sort=None, title=None))
            draw(base.mark_bar(stroke=M["ink"], strokeWidth=1.5, height=16).encode(
                     x=alt.X("cover:Q", title="Days of cover (demo days)"),
                     color=alt.Color("status:N", scale=alt.Scale(domain=list(STATUS_COLORS),
                                                                 range=list(STATUS_COLORS.values()))),
                     tooltip=["product_name", "status", "on_hand", alt.Tooltip("forecast_per_day:Q", format=".1f"),
                              alt.Tooltip("cover:Q", format=".1f", title="days of cover"), "lead_time_days"])
                 + base.mark_tick(color=M["ink"], thickness=3, size=22).encode(x="lead_time_days:Q")
                 + base.mark_text(align="left", dx=6, fontWeight=700, color=M["ink"]).encode(
                     x="cover:Q", text="label:N"),
                 height=max(220, 26 * len(urgent)))
            st.caption("The tick is the product's lead time: a bar shorter than its tick runs out "
                       "before a refill ordered now could arrive. A longer bar can still say Reorder now "
                       "when demand is lumpy: safety stock covers the swings, not just the average.")

    with right, card("Demand & forecast", "DAILY"):
        names = dict(zip(df.product_name, df.product_id))
        # Streamlit resets a select whose options change, and df's urgency order changes with every
        # refresh, so list names alphabetically and start on the most urgent product.
        if st.session_state.get("inv_product") not in names:
            st.session_state["inv_product"] = df.product_name.iloc[0]
        pick = st.selectbox("Product", sorted(names), key="inv_product")
        row = df[df.product_id == names[pick]].iloc[0]
        sql = inv.daily_sales_sql(row.product_id)
        hist, ms2 = query(sql)
        horizon = int(row.lead_time_days + row.target_cover_days)
        series = pd.concat([
            pd.DataFrame({"day": [f"-{a}" for a in hist.age], "units": hist.units.astype(float), "kind": "sold"}),
            pd.DataFrame({"day": [f"+{h}" for h in range(1, horizon + 1)], "units": row.forecast_per_day,
                          "kind": "forecast"}),
        ], ignore_index=True)
        order = list(series.day)
        x = alt.X("day:O", sort=order, title="demo days (today = 0)",
                  axis=alt.Axis(labelOverlap="greedy", labelSeparation=6, labelAngle=0))
        draw(alt.Chart(series[series.kind == "sold"]).mark_bar(fill=BLUE, stroke=M["ink"], strokeWidth=1.5).encode(
                 x=x, y=alt.Y("units:Q", title="Units per day"), tooltip=["day", "units"])
             + alt.Chart(series[series.kind == "forecast"]).mark_line(color=PINK, strokeWidth=3, strokeDash=[6, 4]).encode(
                 x=x, y="units:Q", tooltip=["day", alt.Tooltip("units:Q", format=".2f", title="forecast/day")]),
             height=260)
        st.caption(f"{pick}: {row.on_hand} on hand · forecast {row.forecast_per_day:.2f}/day {row.trend_arrow} · "
                   f"reorder point {row.reorder_point:.0f} · {row.status}")
        show_sql(sql, ms2, len(hist))

    st.write("")
    with card("All products", "STOCK · FORECAST · REORDER"):
        table = df[["status", "product_name", "category", "on_hand", "sold_7d", "forecast_per_day", "trend_arrow",
                    "days_of_cover", "lead_time_days", "reorder_point", "suggested_qty"]]
        st.dataframe(table, hide_index=True, use_container_width=True, column_config={
            "status": "Status", "product_name": "Product", "category": "Category", "on_hand": "On hand",
            "sold_7d": st.column_config.NumberColumn("Sold · 7 d", format="%d"),
            "forecast_per_day": st.column_config.NumberColumn("Forecast / day", format="%.2f"),
            "trend_arrow": "Trend",
            "days_of_cover": st.column_config.NumberColumn("Days of cover", format="%.1f"),
            "lead_time_days": st.column_config.NumberColumn("Lead time (d)", format="%d"),
            "reorder_point": st.column_config.NumberColumn("Reorder point", format="%.0f"),
            "suggested_qty": st.column_config.NumberColumn("Suggested qty", format="%d"),
        })
        show_sql(INVENTORY_SQL, ms, len(stock))


with inventory_tab:
    inventory_view()


# ---------------------------------------------------------------- internals tab
@st.fragment
def internals():
    st.markdown(
        "Flink commits to Iceberg on **every checkpoint (10 s)**. Each commit is an atomic "
        "**snapshot**, and each snapshot adds a few small Parquet files. That gives readers "
        "exactly-once, consistent data, but it also causes the classic *small files problem*. "
        "**Compaction** fixes that."
    )
    table = st.selectbox("Table", TABLES)
    try:
        files_sql = f"""
SELECT count(*)                           AS data_files,
       coalesce(sum(file_size_in_bytes), 0) AS total_bytes,
       coalesce(sum(record_count), 0)       AS records
FROM "{table}$files"
"""
        files, files_ms = query(files_sql)
        snaps_sql = f"""
-- element_at, not summary['key']: Flink's empty commits (idle checkpoints) lack these keys
SELECT committed_at, snapshot_id, operation,
       CAST(coalesce(element_at(summary, 'added-records'), '0')    AS bigint) AS added_records,
       CAST(coalesce(element_at(summary, 'added-data-files'), '0') AS bigint) AS added_files,
       CAST(element_at(summary, 'total-data-files')                AS bigint) AS total_files
FROM "{table}$snapshots"
ORDER BY committed_at DESC
LIMIT 200
"""
        snaps, snaps_ms = query(snaps_sql)
    except Exception as err:
        waiting(err)
        return

    fr = files.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Snapshots (shown)", len(snaps))
    c2.metric("Live data files", f"{int(fr.data_files):,}")
    c3.metric("Avg file size", f"{fr.total_bytes / max(fr.data_files, 1) / 1024:,.1f} KB",
              help="Parquet likes ~128-512 MB files. Tiny files = slow scans.")
    c4.metric("Records", f"{int(fr.records):,}")
    show_sql(files_sql, files_ms, len(files))

    if st.button(f"🧹 Compact `{table}` now (Trino OPTIMIZE)", type="primary"):
        sql = f"ALTER TABLE {table} EXECUTE optimize"
        with st.spinner("Rewriting small files into larger ones…"):
            _, ms = query(sql)
        after, _ = query(files_sql)
        st.success(f"Done in {ms / 1000:.1f} s: {fr.data_files:,} → {after.iloc[0].data_files:,} data files. "
                   "A new `replace` snapshot was committed. Flink kept writing throughout "
                   "(optimistic concurrency).")
        code(sql)

    st.write("")
    with card("Commits over time", "1 SNAPSHOT PER CHECKPOINT"):
        if not snaps.empty:
            draw(alt.Chart(snaps).mark_line(color=PINK, strokeWidth=3, point=alt.OverlayMarkDef(
                    fill=PINK, stroke=M["ink"], strokeWidth=1.5, size=60)).encode(
                x=alt.X("committed_at:T", title=None, scale=alt.Scale(type="utc")),  # UTC like the rest of the page
                y=alt.Y("added_records:Q", title="Records added"),
                tooltip=[alt.Tooltip("committed_at:T", format="%H:%M:%S", formatType="utc"), "operation", "added_records", "added_files"],
            ), height=240)
        st.dataframe(snaps.astype({"snapshot_id": str}), hide_index=True, use_container_width=True)  # ids, not numbers
        show_sql(snaps_sql, snaps_ms, len(snaps))

    st.write("")
    with card("⏪ Time travel", "FOR VERSION AS OF"):
        if len(snaps) <= 1:
            st.caption("Needs at least two snapshots.")
            return
        snap = st.select_slider(
            "Query the table as it was at snapshot…",
            options=list(snaps.snapshot_id[::-1]),
            format_func=lambda s: str(snaps.set_index("snapshot_id").committed_at[s])[:19],
        )
        tt_sql = f"SELECT count(*) AS n FROM {table} FOR VERSION AS OF {snap}"
        then, ms = query(tt_sql)
        current = query(f"SELECT count(*) AS n FROM {table}")[0].n[0]
        a, b = st.columns(2)
        a.metric("Rows at that snapshot", f"{then.n[0]:,}")
        b.metric("Rows now", f"{current:,}", f"+{current - then.n[0]:,} since")
        show_sql(tt_sql, ms, 1)


with internals_tab:
    internals()


# ---------------------------------------------------------------- SQL tab
EXAMPLES = {
    "Latest orders": "SELECT * FROM orders ORDER BY event_time DESC LIMIT 20",
    "Revenue per minute (gold)": "SELECT * FROM revenue_per_minute ORDER BY window_start DESC LIMIT 20",
    "Cart abandonment by category": """SELECT c.category,
       count(*) AS carts,
       count(o.order_id) AS purchased,
       round(1 - count(o.order_id) * 1.0 / count(*), 3) AS abandonment_rate
FROM clicks c
LEFT JOIN orders o ON o.session_id = c.session_id AND o.product_id = c.product_id
WHERE c.event_type = 'add_to_cart'
GROUP BY 1
ORDER BY abandonment_rate DESC""",
    "Current stock (latest movement per product)": """SELECT product_id, product_name, on_hand_after AS on_hand, reason, event_time
FROM (SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY event_time DESC, seq DESC) AS rn
      FROM inventory_movements)
WHERE rn = 1 AND reason <> 'removed'
ORDER BY on_hand""",
    "Stock history of the 4K Monitor": """SELECT seq, reason, delta, on_hand_after, event_time
FROM inventory_movements
WHERE product_id = 'P002'
ORDER BY seq DESC""",
    "Schema evolution: clicks by device": """-- device arrived in clicks schema v2: older rows read NULL (no files were rewritten)
SELECT coalesce(device, 'NULL (before schema v2)') AS device,
       count(*) AS clicks, min(event_time) AS first_seen, max(event_time) AS last_seen
FROM clicks
GROUP BY 1
ORDER BY first_seen""",
    "Iceberg partitions": 'SELECT * FROM "orders$partitions"',
    "Table DDL": "SHOW CREATE TABLE orders",
}

@st.fragment  # Run re-executes only this tab, not the whole page
def playground():
    st.markdown("Read-only Trino SQL against `lakehouse.shop`. Try joins across tables, "
                "Iceberg metadata tables (`\"orders$snapshots\"`, `\"orders$files\"`) or "
                "`FOR TIMESTAMP AS OF`.")
    example = st.selectbox("Start from an example", list(EXAMPLES))
    sql = st.text_area("SQL", EXAMPLES[example], height=180, key=f"sql_{example}")
    if st.button("▶ Run", type="primary"):
        sql = sql.strip().rstrip(";")
        words = " ".join(line for line in sql.splitlines() if not line.strip().startswith("--")).split()
        if not words or words[0].lower() not in READ_ONLY:  # the first keyword after any -- comments
            st.error(f"Playground is read-only. Start with one of: {', '.join(READ_ONLY).upper()}.")
        else:
            try:
                df, ms = query(sql)
                st.caption(f"{len(df):,} rows · {ms:.0f} ms")
                st.dataframe(df, hide_index=True, use_container_width=True)
            except Exception as err:
                st.error(str(err))


with sql_tab:
    playground()
