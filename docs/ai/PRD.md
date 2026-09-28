# PRD: Real-time E-commerce Lakehouse

> Product requirements for AI agents and contributors. It covers **what** the product is and **why**.
> For *how*, see [Architecture.md](Architecture.md). For *what not to break*, see [Rules.md](Rules.md).

## 1. Summary
A laptop-runnable **streaming lakehouse** with a real storefront on top. A shopper uses
**Lakeshop** (search, cart, fake checkout); every action becomes a Kafka event. Flink SQL writes those
events exactly-once into Apache Iceberg; Trino queries them; a **live dashboard** shows the business
metrics about 10–15 s later, along with the lakehouse internals (snapshots, small files, compaction,
time travel). One command starts everything: `docker compose up -d --build`.

## 2. Problem
- Most streaming demos stop at "events go into Kafka" and use a synthetic generator. This one starts
  from **real user actions** and ends in an **open table format**, with the hard parts visible: event
  time, watermarks, exactly-once, table maintenance and time travel.

## 3. Users and personas
| Persona | Wants | Where they spend time |
|---|---|---|
| **Visitor** | To grasp the project in 60 s and see it working | README screenshots, `docs/DEMO.md` |
| **Engineer** | To see depth: design decisions, trade-offs, tests | README "How it works", Flink UI, code, tests |
| **Demo presenter** | A reliable 10-minute live demo | Lakeshop + dashboard side by side |
| **Learner** | To understand a streaming lakehouse by poking at one | SQL playground, Internals tab, other UIs |
| **AI coding agent** | Enough context to change things safely | `CLAUDE.md`, `docs/ai/*` |

## 4. Goals
1. **Real events.** Every store interaction produces pipeline events; no mock is required.
2. **Visible streaming concepts.** Freshness, windows, snapshots, compaction and time travel are all
   observable in the UI.
3. **One-command local run** on an 8–16 GB laptop (the stack stays under ~5 GB of RAM).
4. **Professional presentation.** A cohesive neo-brutalist design, dark mode, mobile-friendly store.
5. **Verified correctness.** An automated test suite, including a live end-to-end pipeline test.

## 5. Non-goals
- Production deployment, auth, multi-tenancy, real payments or real PII.
- High throughput or HA. The single-broker, single-JobManager setup is deliberate (see Architecture §9).
- A general-purpose e-commerce platform: the store exists to generate meaningful events.

## 6. Functional requirements

### 6.1 Lakeshop storefront (`shop/`, http://localhost:8000)
| ID | Requirement |
|---|---|
| S-1 | Seed catalog of 48 fake products (8 in each of 6 categories), each with a description, rating, reviews, an emoji "photo" and an optional badge (Bestseller/New/Deal) |
| S-2 | Bento-grid home: hero with featured product, live event counter, test-payment hints, category tiles, product grid (bestsellers are wide tiles) |
| S-3 | Live search over name, category and description, plus a category filter |
| S-4 | Product dialog with a quantity stepper; opening it emits `page_view` |
| S-5 | Add to cart from the dialog or quick-add; emits `add_to_cart`; the cart persists in localStorage |
| S-6 | Cart drawer: quantity +/−, remove, subtotal, free shipping |
| S-7 | Checkout: name, country (IN/US/GB/DE/BR/JP/AU/CA), payment by card, UPI or cash on delivery |
| S-8 | Fake payments: `4242…` approves; `4000 0000 0000 0002` and `…9995` decline; any other card is refused |
| S-9 | A successful checkout emits **one `orders` event per cart line**; a failed one emits nothing |
| S-10 | Clear errors: validation, declines, and "can't reach the server" (never a raw "Failed to fetch") |
| S-11 | Anonymous `user_id` (per browser) and `session_id` (per tab) tie the funnel together |
| S-12 | Catalog admin (`/admin.html`): list, search and filter all products, with stats |
| S-13 | "+ Add product": name, category, price, badge, description, and an emoji picker per category, with a live preview; the product is instantly buyable and its orders reach the dashboard |
| S-14 | Admin-added products persist across restarts and can be deleted; seed products are protected |
| S-15 | *(Phase 9)* Inventory: stock per product, decremented by orders and increased by restocks, with a forecasting and reorder dashboard (see Phases.md §2.0) |

### 6.2 Pipeline
| ID | Requirement |
|---|---|
| P-1 | Kafka topics `clicks` and `orders` (3 partitions each, keyed by `user_id`) |
| P-2 | Flink SQL job with 4 sinks: bronze `clicks` and `orders`; gold `revenue_per_minute` and `funnel_per_minute` |
| P-3 | Event-time processing: 5 s watermark and 1-minute tumbling windows |
| P-4 | Exactly-once into Iceberg through 10 s checkpoints |
| P-5 | The job is submitted automatically and never duplicated |
| P-6 | An optional simulator (`--profile simulator`) generates background traffic in the same schema |

### 6.3 Dashboard (`dashboard/`, http://localhost:8501)
| ID | Requirement |
|---|---|
| D-1 | Live tab: KPIs (5-minute revenue and orders with deltas, AOV, 15-minute conversion, data freshness), revenue per minute by category, funnel, top products, revenue by country; auto-refresh |
| D-2 | Every widget exposes its SQL and latency |
| D-3 | Internals tab per table: snapshots, file counts and sizes, one-click `OPTIMIZE`, time-travel slider |
| D-4 | SQL playground, read-only (`SELECT/WITH/SHOW/DESCRIBE/EXPLAIN` only), with examples |
| D-5 | Light and dark themes, persisted in the URL (`?theme=dark`) |
| D-6 | A friendly "waiting for data" state that points users to the store |

### 6.4 Developer experience
| ID | Requirement |
|---|---|
| X-1 | `docker compose up -d --build` starts everything; `down -v` wipes it |
| X-2 | `pytest` suite: unit, contract, API, dashboard, browser e2e and live integration tests (91 today) |
| X-3 | `scripts/demo.py` generates traffic and regenerates the README screenshots |
| X-4 | Docs: README, `docs/DEMO.md`, `CLAUDE.md`, `docs/ai/*` |

## 7. Non-functional requirements
| Area | Target |
|---|---|
| Freshness | Event to visible in Trino in ≤ ~15 s (checkpoint 10 s + commit) |
| Footprint | Whole stack ≤ ~5 GB RAM; Docker Desktop needs ≥ 6 GB allocated |
| Startup | Pipeline running within ~2 min of `up` (after images are cached) |
| Correctness | No duplicate or partial rows (exactly-once); the server sets prices and totals |
| Security | Demo credentials only; card data never stored, logged or emitted; read-only SQL playground |
| Accessibility | Keyboard-operable store (native `<dialog>`, focus states), labelled inputs, colour never the only signal |
| Portability | Works on Windows, macOS and Linux (LF line endings enforced for mounted files) |

## 8. Success metrics
- A new visitor goes from `git clone` to seeing their own order on the dashboard in **< 15 minutes**.
- The test suite is green with **zero skips** when the stack is up.
- README screenshots match the current UI (regenerate with `scripts/demo.py`).

## 9. Open questions and future scope
See [Phases.md](Phases.md) §2: Schema Registry/Avro, CDC upserts, late-event handling, scheduled
table maintenance, a Postgres-backed catalog, data-quality checks, CI, Kubernetes, and a hosted demo.
