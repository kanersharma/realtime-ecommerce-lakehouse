"""Streamlit dashboard: every number is a live Trino query against Iceberg tables.

Tabs:
  Live Business       - KPIs, revenue per minute, funnel, top products (auto-refresh)
  Lakehouse Internals - Iceberg snapshots, small files, compaction, time travel
  SQL Playground      - read-only ad-hoc Trino SQL
"""
import os
import time

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
    return f"{(now - prev) / prev:+.1%} vs prev 5 min" if prev else None


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

st.title("Real-time E-commerce Lakehouse")
st.caption("Kafka → Flink SQL → Apache Iceberg on S3 (RustFS) → Trino. "
           "Every number below is a live SQL query against Iceberg tables; open 🔍 to see it.")

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

    left, right = st.columns([2, 1])
    with left:
        st.subheader("Revenue per minute by category")
        rev, ms = query(REVENUE_SQL)
        if rev.empty:
            st.caption("First 1-minute window closes ~65 s after data starts flowing "
                       "(window end + 5 s watermark delay).")
        else:
            st.bar_chart(rev.pivot(index="minute", columns="category", values="revenue"), stack=True)
        show_sql(REVENUE_SQL, ms, len(rev))
    with right:
        st.subheader("Funnel · last 15 min")
        steps = pd.Series({
            "1 · Sessions": f.sessions, "2 · Page views": f.page_views,
            "3 · Add to cart": f.add_to_carts, "4 · Orders": f.orders,
        }, name="count")
        st.bar_chart(steps, horizontal=True)
        show_sql(FUNNEL_SQL, funnel_ms, len(funnel))

    left, right = st.columns([2, 1])
    with left:
        st.subheader("Top products · last 15 min")
        top, ms = query(TOP_PRODUCTS_SQL)
        st.dataframe(top, hide_index=True, use_container_width=True, column_config={
            "revenue": st.column_config.ProgressColumn(
                "revenue", format="$%.0f", min_value=0,
                max_value=float(top.revenue.max()) if len(top) else 1.0),
        })
        show_sql(TOP_PRODUCTS_SQL, ms, len(top))
    with right:
        st.subheader("Revenue by country")
        countries, ms = query(COUNTRY_SQL)
        st.bar_chart(countries.set_index("country")["revenue"])
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

    st.subheader("Commits over time")
    if not snaps.empty:
        st.line_chart(snaps.set_index("committed_at")[["added_records"]])
    st.dataframe(snaps, hide_index=True, use_container_width=True)
    show_sql(snaps_sql, snaps_ms, len(snaps))

    st.subheader("⏪ Time travel")
    if len(snaps) > 1:
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
