"""Streamlit dashboard: every number is a live Trino query against Iceberg tables.

Tabs:
  Live Business       - KPIs, revenue per minute, funnel, top products (auto-refresh)
  Lakehouse Internals - Iceberg snapshots, small files, compaction, time travel
  SQL Playground      - read-only ad-hoc Trino SQL
"""
import os
import time

import altair as alt
import pandas as pd
import streamlit as st
import trino

TABLES = ["orders", "clicks", "revenue_per_minute", "funnel_per_minute"]
READ_ONLY = ("select", "with", "show", "describe", "explain")
LINKS = {
    "Flink UI (jobs, checkpoints)": "http://localhost:8081",
    "Kafka UI (topics, messages)": "http://localhost:8088",
    "Trino UI (queries)": "http://localhost:8090",
    "RustFS console (Parquet files)": "http://localhost:9001",
}

st.set_page_config(page_title="Real-time Lakehouse", page_icon="⚡", layout="wide")

# ---------------------------------------------------------------- neo-brutalist look
# Base colors live in .streamlit/config.toml; this adds the borders, hard shadows and type.
INK, PAPER, YELLOW, PINK, BLUE = "#111111", "#FFFBEF", "#FFD23F", "#FF6FB5", "#2F6BFF"
# Fixed category -> color, so a category keeps its color whatever the data holds.
# Validated for the PAPER surface with the dataviz palette validator (CVD + contrast checks).
CATEGORY_COLORS = {
    "Beauty": "#E83E8C", "Books": "#8A5A00", "Electronics": "#2F6BFF",
    "Fashion": "#E8700A", "Home": "#128A5E", "Sports": "#5FB7FF",
}
KPI_COLORS = [YELLOW, PINK, "#7CE0C3", "#A9C4FF", "#FFB27A"]

CSS = f"""
@import url('https://fonts.googleapis.com/css2?family=Archivo+Black&family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@500&display=swap');

html, body, p, li, label, input, textarea, button, [data-testid="stMarkdownContainer"] {{
  font-family: 'Space Grotesk', sans-serif !important;
}}
h1, h2, h3 {{ font-family: 'Archivo Black', sans-serif !important; letter-spacing: -0.02em; color: {INK}; }}
code, pre {{ font-family: 'JetBrains Mono', monospace !important; }}
.block-container {{ padding-top: 3.5rem; }}

/* hero */
.hero {{ background: {YELLOW}; border: 3px solid {INK}; box-shadow: 8px 8px 0 {INK}; padding: 28px 32px; margin-bottom: 28px; }}
.hero h1 {{ font-size: 3rem; line-height: 1.05; margin: 8px 0 12px; padding: 0; }}
.hero h1 .mark {{ background: {PINK}; border: 3px solid {INK}; padding: 0 10px; display: inline-block; transform: rotate(-1deg); }}
.hero p {{ font-size: 1.05rem; font-weight: 500; margin: 0 0 16px; max-width: 760px; }}
.kicker {{ display: inline-flex; align-items: center; gap: 8px; background: {INK}; color: #fff; font-weight: 700;
          font-size: .8rem; letter-spacing: .12em; padding: 4px 10px; }}
.dot {{ width: 10px; height: 10px; border-radius: 50%; background: #3DFF8B; animation: pulse 1.4s infinite; }}
@keyframes pulse {{ 50% {{ opacity: .25; }} }}
.chips {{ display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-weight: 700; }}
.chips span {{ background: #fff; border: 2px solid {INK}; box-shadow: 3px 3px 0 {INK}; padding: 3px 10px; font-size: .85rem; }}

/* KPI tiles */
[data-testid="stMetric"] {{ border: 3px solid {INK}; box-shadow: 6px 6px 0 {INK}; padding: 14px 18px; background: #fff; }}
[data-testid="stMetricValue"] {{ font-family: 'Archivo Black', sans-serif !important; font-size: 2rem; }}
[data-testid="stMetricLabel"] p {{ font-weight: 700 !important; text-transform: uppercase; letter-spacing: .04em; font-size: .72rem !important; }}
[data-testid="stMetricLabel"] div, [data-testid="stMetricLabel"] p, [data-testid="stMetricDelta"] div {{ white-space: normal !important; overflow: visible !important; }}
[data-testid="stMetricDelta"], [data-testid="stMetricDelta"] div {{ font-weight: 700; color: {INK} !important; }}
[data-testid="stMetricDelta"] svg {{ fill: {INK} !important; }}
{"".join(f'[data-testid="stHorizontalBlock"] > [data-testid="stColumn"]:nth-child({i + 1}) [data-testid="stMetric"] {{ background: {c}; }}' for i, c in enumerate(KPI_COLORS))}

/* tabs */
.stTabs [data-baseweb="tab-list"] {{ gap: 12px; }}
.stTabs [data-baseweb="tab"] {{ border: 3px solid {INK}; background: #fff; padding: 6px 16px; box-shadow: 4px 4px 0 {INK}; }}
.stTabs [data-baseweb="tab"] p {{ font-weight: 700; }}
.stTabs [aria-selected="true"] {{ background: {INK}; }}
.stTabs [aria-selected="true"] p {{ color: #fff; }}
.stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] {{ display: none; }}

/* buttons */
.stButton > button {{ border: 3px solid {INK}; border-radius: 0; box-shadow: 4px 4px 0 {INK}; font-weight: 700;
                     background: #fff; color: {INK}; transition: transform .08s, box-shadow .08s; }}
.stButton > button[kind="primary"] {{ background: {PINK}; color: {INK}; }}
.stButton > button:hover {{ transform: translate(2px, 2px); box-shadow: 2px 2px 0 {INK}; border-color: {INK}; color: {INK}; }}
.stButton > button:active {{ transform: translate(4px, 4px); box-shadow: none; }}

/* inputs, expanders, tables, alerts, code */
[data-baseweb="select"] > div, [data-baseweb="textarea"], .stTextArea textarea {{ border: 3px solid {INK} !important; border-radius: 0 !important; }}
[data-testid="stExpander"] details {{ border: 2px solid {INK}; border-radius: 0; background: #fff; }}
[data-testid="stExpander"] summary p {{ font-family: 'JetBrains Mono', monospace !important; font-size: .8rem; }}
[data-testid="stDataFrame"] {{ border: 2px solid {INK}; }}
[data-testid="stAlert"] {{ border: 3px solid {INK}; border-radius: 0; box-shadow: 5px 5px 0 {INK}; }}
[data-testid="stCode"] pre, .stCode pre {{ border: 2px solid {INK}; border-radius: 0; }}

/* chart cards (st.container(border=True)) */
[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] .card-title) {{
  border: 3px solid {INK} !important; border-radius: 0 !important; box-shadow: 8px 8px 0 {INK}; background: #fff;
}}
.card-title {{ font-family: 'Archivo Black', sans-serif; font-size: 1.25rem; margin: 0 0 4px; }}
.card-title .tag {{ font-family: 'Space Grotesk', sans-serif; font-size: .7rem; font-weight: 700; letter-spacing: .1em;
                   background: {INK}; color: #fff; padding: 2px 8px; margin-left: 8px; vertical-align: middle; }}

/* sidebar */
[data-testid="stSidebar"] {{ border-right: 3px solid {INK}; }}
[data-testid="stSidebar"] a {{ color: {INK}; font-weight: 700; text-decoration: underline 3px {PINK}; }}
"""
st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)


def card(title, tag=None):
    """A bordered 'card' container with a heavy title; use as `with card(...):`."""
    box = st.container(border=True)
    tag_html = f'<span class="tag">{tag}</span>' if tag else ""
    box.markdown(f'<div class="card-title">{title}{tag_html}</div>', unsafe_allow_html=True)
    return box


def brutal(chart, height=300):
    """Shared Altair styling: black axes, no chart border, Space Grotesk type."""
    return (chart.properties(height=height)
            .configure(font="Space Grotesk", background="transparent")
            .configure_view(stroke=None)
            .configure_axis(domainColor=INK, domainWidth=2, tickColor=INK, labelColor=INK, titleColor=INK,
                            gridColor="#1111111A", labelFontSize=12, titleFontWeight=700)
            .configure_legend(labelFontSize=12, symbolStrokeColor=INK, symbolStrokeWidth=1.5, orient="bottom", title=None))


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


def show_sql(sql, ms, rows):
    with st.expander(f"🔍 SQL · {ms:.0f} ms · {rows} rows"):
        st.code(sql.strip(), language="sql")


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
-- Gold table written by a Flink 1-minute tumbling window
SELECT window_start AS minute, category, CAST(sum(revenue) AS double) AS revenue
FROM revenue_per_minute
WHERE window_start > localtimestamp - INTERVAL '30' MINUTE
GROUP BY 1, 2
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


def waiting(err):
    st.info(
        "⏳ **Waiting for data.** The Flink job creates the Iceberg tables on startup and "
        "commits data on every checkpoint (10 s). The first windowed aggregates appear "
        "~1 minute later. This page refreshes on its own."
    )
    with st.expander("Details"):
        st.code(str(err))


def pct_delta(now, prev):
    return f"{(now - prev) / prev:+.1%} vs prev 5m" if prev else None


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.header("⚡ Real-time Lakehouse")
    refresh = st.selectbox("Auto-refresh", [5, 10, 30, 0], index=1,
                           format_func=lambda s: f"every {s}s" if s else "off")
    st.markdown("**Open the other UIs**")
    for label, url in LINKS.items():
        st.markdown(f"- [{label}]({url})")
    st.markdown("**Data flow**")
    st.markdown("""```
generator (Python)
   │  JSON events
   ▼
Kafka  clicks · orders
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
  <div class="chips"><span>KAFKA</span>→<span>FLINK SQL</span>→<span>APACHE ICEBERG</span>→<span>TRINO</span>→<span>YOU</span></div>
</div>
""", unsafe_allow_html=True)

live_tab, internals_tab, sql_tab = st.tabs(["📈 Live Business", "🔬 Lakehouse Internals", "🧪 SQL Playground"])


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
    with left, card("Revenue per minute", "GOLD · 1-MIN WINDOWS"):
        rev, ms = query(REVENUE_SQL)
        if rev.empty:
            st.caption("First 1-minute window closes ~65 s after data starts flowing "
                       "(window end + 5 s watermark delay).")
        else:
            rev["time"] = pd.to_datetime(rev.minute).dt.strftime("%H:%M")
            draw(alt.Chart(rev).mark_bar(stroke=INK, strokeWidth=1.5).encode(
                x=alt.X("time:O", title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("revenue:Q", title="Revenue ($)", stack=True),
                color=alt.Color("category:N", scale=alt.Scale(
                    domain=list(CATEGORY_COLORS), range=list(CATEGORY_COLORS.values()))),
                order=alt.Order("category:N"),
                tooltip=["time:O", "category:N", alt.Tooltip("revenue:Q", format="$,.0f")],
            ))
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
        draw(base.mark_bar(fill=YELLOW, stroke=INK, strokeWidth=2.5, height=34)
             .encode(tooltip=["step", alt.Tooltip("count:Q", format=",")])
             + base.mark_text(align="left", dx=8, fontWeight=700, fontSize=14, color=INK)
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
        draw(alt.Chart(countries).mark_bar(fill=BLUE, stroke=INK, strokeWidth=2).encode(
            x=alt.X("country:N", sort="-y", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("revenue:Q", title="Revenue ($)"),
            tooltip=["country", alt.Tooltip("revenue:Q", format="$,.0f")],
        ), height=340)
        show_sql(COUNTRY_SQL, ms, len(countries))


with live_tab:
    live()


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
SELECT committed_at, snapshot_id, operation,
       CAST(summary['added-records']    AS bigint) AS added_records,
       CAST(summary['added-data-files'] AS bigint) AS added_files,
       CAST(summary['total-data-files'] AS bigint) AS total_files
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
        st.code(sql, language="sql")

    st.write("")
    with card("Commits over time", "1 SNAPSHOT PER CHECKPOINT"):
        if not snaps.empty:
            draw(alt.Chart(snaps).mark_line(color=PINK, strokeWidth=3, point=alt.OverlayMarkDef(
                    fill=PINK, stroke=INK, strokeWidth=1.5, size=60)).encode(
                x=alt.X("committed_at:T", title=None),
                y=alt.Y("added_records:Q", title="Records added"),
                tooltip=[alt.Tooltip("committed_at:T", format="%H:%M:%S"), "operation", "added_records", "added_files"],
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
        if not sql or sql.split()[0].lower() not in READ_ONLY:
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
