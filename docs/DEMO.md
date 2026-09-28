# 🎬 Demo guide

A 10-minute walkthrough for showing the project to someone: a team, a meetup, or yourself.
You shop in a real store, and seconds later the purchase shows up in a lakehouse dashboard.

## Before the demo (5 minutes)

```bash
docker compose up -d --build                           # start everything
.venv/Scripts/python scripts/demo.py --no-screenshots  # optional: 4 min of shopper traffic so charts are full
```

Open two browser windows side by side:

| Left | Right |
|---|---|
| 🛒 Lakeshop: http://localhost:8000 | 📊 Dashboard: http://localhost:8501 |

---

## 1 · The store is real

![Lakeshop home](screenshots/store-home.png)

Lakeshop is a working storefront: search, categories, product pages, a cart and checkout. Every
action is an **event**. The green **📡 Live events** tile counts what this browser has sent to Kafka.

![Bento product grid](screenshots/store-products.png)

## 2 · Browse and search: `page_view` events

![Search results](screenshots/store-search.png)

Type into the search bar, then open a product. Opening it sends a `page_view` event to the Kafka
topic `clicks`, keyed by the anonymous shopper id.

![Product dialog](screenshots/store-product.png)

## 3 · Add to cart: `add_to_cart` events

![Cart drawer](screenshots/store-cart.png)

Each add sends an `add_to_cart` event. The cart survives a page reload, since it lives in local storage.

## 4 · Checkout with test payments

![Declined card](screenshots/store-declined.png)

Try the declined test card `4000 0000 0000 0002` first: the payment fails and **no order event is
produced**. Then press **Fill test card** (`4242 4242 4242 4242`), or choose UPI or cash on delivery.

![Checkout](screenshots/store-checkout.png)

![Order placed](screenshots/store-success.png)

The server prices the order from the catalog (the browser can't change prices) and emits **one
`orders` event per cart line**.

> Payments are fake. Only published test numbers work, and card details are never stored or sent anywhere.

## 5 · Watch it land (about 15 seconds)

![Dashboard](screenshots/dashboard-light.png)

Within about one Flink checkpoint (10 s), your order is part of the KPIs, the funnel and top products.
Points worth mentioning:

- **Data freshness** shows the end-to-end latency: now minus the newest event in Iceberg.
- **Revenue per minute** comes from a *gold* table that Flink builds with 1-minute tumbling windows.
  A window appears once the watermark passes its end, so it arrives about 65 s after the minute starts.
- Open any **🔍 SQL** to show the exact Trino query and its latency.

Dark mode is a toggle in the sidebar, or the link http://localhost:8501/?theme=dark:

![Dashboard dark mode](screenshots/dashboard-dark.png)

## 6 · Under the hood: the lakehouse

![Lakehouse internals](screenshots/dashboard-internals.png)

- Every checkpoint is an **Iceberg snapshot**, which gives readers exactly-once data but lots of small files.
- Press **🧹 Compact now** to run Trino's `OPTIMIZE` while Flink keeps writing.
- Drag the **time travel** slider to query the table as it was at any earlier snapshot.

![SQL playground](screenshots/dashboard-sql.png)

The SQL playground runs read-only SQL across tables that a streaming job wrote and a batch engine
reads. *Cart abandonment by category* joins `clicks` and `orders`.

## 7 · The mobile view

<img src="screenshots/store-mobile.png" width="320" alt="Lakeshop on mobile">

---

**Other UIs worth showing:** Flink job graph and checkpoints (http://localhost:8081), messages
arriving in Kafka (http://localhost:8088), and the Parquet files in the RustFS console
(http://localhost:9001, `admin` / `password`).

To refresh these screenshots after a UI change: `.venv/Scripts/python scripts/demo.py`.
