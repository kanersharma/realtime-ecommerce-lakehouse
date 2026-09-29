# Design

> The visual and interaction system for both UIs. Rules that must never break are in
> [Rules.md](Rules.md) §R-UI; this file explains the system so new UI fits in.

## 1. Principles
1. **Neo-brutalism**: flat saturated colour, thick ink borders, hard offset shadows, chunky type, no
   gradients and no blur. It should feel confident and a little playful, never glossy.
2. **One brand across both apps.** The store and the dashboard share tokens, fonts and category
   colours, so a category looks the same wherever it appears.
3. **Show the machinery.** Every number can reveal its SQL (🔍). Tags such as `GOLD · 1-MIN WINDOWS`
   explain where data comes from.
4. **Honest demo.** Test mode, fake payments and dry runs are labelled clearly in the UI.

## 2. Tokens

### Colour
| Token | Light | Dark | Use |
|---|---|---|---|
| paper | `#FFFBEF` | `#15130F` | page background |
| card | `#FFFFFF` | `#22201B` | cards, tabs, inputs (store) |
| field | `#FFF1C1` | `#2E2A22` | dashboard inputs, code blocks |
| side | `#FFF1C1` | `#1C1A16` | dashboard sidebar |
| ink | `#111111` | `#FFFBEF` | text, borders, shadows (flips with the theme) |
| grid | `#1111111A` | `#FFFBEF1F` | chart gridlines |
| TILE_INK | `#111111` | `#111111` | text on bright fills, in both modes |

Accent fills (same in both modes): yellow `#FFD23F`, pink `#FF6FB5`, mint `#7CE0C3`, periwinkle
`#A9C4FF`, peach `#FFB27A`, blue `#2F6BFF`. KPI tiles cycle through yellow → pink → mint → periwinkle → peach.

### Category colours (charts, stripes, store category borders)
Fixed per category, never by position. Both sets were validated with the dataviz palette validator
(adjacent pairs in stack order Beauty → Books → Electronics → Fashion → Home → Sports).

| Category | Light (surface #FFF) | Dark (surface #22201B) | Store tint (tile background) |
|---|---|---|---|
| Beauty | `#E83E8C` | `#E83E8C` | `#FFD6E8` |
| Books | `#8A5A00` | `#A87520` | `#F3E3C7` |
| Electronics | `#2F6BFF` | `#3D78FF` | `#D6E2FF` |
| Fashion | `#E8700A` | `#D9660A` | `#FFE0C7` |
| Home | `#128A5E` | `#179A68` | `#CDF2E0` |
| Sports | `#5FB7FF` | `#3E9BE6` | `#D6EEFF` |

Light mode carries a CVD warning (Home/Fashion ΔE 7.4) and one sub-3:1 contrast (Sports). Both are
legal because every stacked bar has ink separator strokes, a legend and tooltips. Keep those.

### Type
| Role | Font | Notes |
|---|---|---|
| Display / headings / KPI values / prices | **Archivo Black** | tight tracking (−0.02em) |
| Body / UI | **Space Grotesk** 400/500/700 | labels are uppercase and letter-spaced in KPI tiles |
| Code / SQL | **JetBrains Mono** 500 | dashboard only |

Loaded from Google Fonts. Never override `span` globally in Streamlit, because its icons are
Material-Symbol ligatures in spans.

### Shape
| Element | Border | Shadow | Radius |
|---|---|---|---|
| Dashboard card / KPI | 3 px ink | 6–8 px ink, offset down-right | 0 (sharp) |
| Store tile (bento) | 3 px ink | 6 px ink | 14 px |
| Buttons | 3 px ink | 4 px; hover shifts 2 px and shrinks the shadow; active removes it | 0 dashboard / 10 px store |
| Pills / tags | 2 px ink | none | full (store) / 0 (dashboard tag) |

The difference is deliberate: the dashboard is sharp and technical; the store is bento, with rounded
tiles for friendliness.

## 3. Dashboard components
- **Hero**: yellow block, ink kicker with a pulsing green "LIVE" dot, a pink highlighted word, and
  chips showing the data flow.
- **Tabs**: bordered buttons; the selected one inverts (ink background, paper text).
- **KPI tiles**: `st.metric` in coloured tiles; deltas in ink with arrows (never green/red alone).
- **Card**: `with card("Title", "TAG"):` gives a bordered container with a heavy title and an ink tag.
- **Charts**: `draw(alt.Chart(...))` gives transparent background, ink axes, recessive grid and a
  bottom legend. Bars have ink strokes (1.5–2.5 px); the funnel has direct value labels; tooltips everywhere.
- **Revenue per minute** (tag `GOLD WINDOWS + LIVE`): closed minutes are solid; still-open minutes
  (from bronze) are 40 % opacity with dashed outlines, explained by a caption. Axis labels use
  `labelOverlap="greedy"` so they never collide.
- **SQL disclosure**: `show_sql()` renders an expander "🔍 SQL · N ms · N rows" using `code()`.
- **Live tab, bottom row (schema v2):** "Sessions by device" (tag `LAST 15 MIN · SCHEMA V2`): pink
  horizontal bars per device with a direct label "sessions · share that ordered", and a caption explaining that
  `device` arrived in v2 ("unknown (before v2)" for older events). Next to it, "Event schemas" (tag
  `SCHEMA REGISTRY`): one line per subject with its version count, and the compatibility level in a
  caption. If the registry is down, the card says so instead of failing.
- **📦 Inventory tab**: five KPI tiles (Reorder now = out of stock + at/below the reorder point, Out of
  stock, Stock value, Units sold · 7 days, 1 demo day), then:
  - **Reorder suggestions** (tag `FORECAST · EWMA + TREND`): up to 6 products in a 3-column grid, each
    with status, stock, cover, forecast and lead time in words, and a pink **Restock N** button; a
    **Restock all N suggestions** button below; a caption with the formula.
  - **Days of cover** (tag `MOST URGENT 15`): horizontal bars coloured by status, a direct label
    ("sold out" or "N.N d"), and a thick ink tick at the product's lead time.
  - **Demand & forecast** (tag `DAILY`): a product select (alphabetical, starting on the most urgent
    product); blue bars for units sold per demo day, and a dashed pink line for the forecast over
    lead time + target cover.
  - **All products**: the full table (status, stock, 7-day sales, forecast, trend arrow ↗ → ↘, cover,
    lead time, reorder point, suggested quantity).

  Status colours are fixed, like category colours, and always come with the status word:

  | Status | Colour |
  |---|---|
  | Out of stock | `#C62828` |
  | Reorder now | `#F2A900` |
  | Reorder soon | `#FFE08A` |
  | OK | `#7CE0C3` |
  | No demand | `#C9C9C9` |
- **Dark mode**: sidebar toggle; the canvas data grid is inverted with `filter: invert(1) hue-rotate(180deg)`.
- **Auto-refresh never fades content**: Streamlit's stale-element fade is switched off (`[data-stale]`
  stays opaque), so a live page stays readable; the header's RUNNING indicator shows activity.

## 4. Store (bento grid)
- **Grid**: 12 columns at desktop, 6 below 1000 px, 2 below 640 px; `grid-auto-flow: dense` fills gaps.
- **Intro bento**: hero (7 cols × 2 rows, yellow), live events (5 cols, mint), test payments
  (5 cols, periwinkle), and a categories row (12 cols, 7 buttons with category-colour bottom borders).
- **Product tiles**: 3 cols; **Bestsellers span 6** in a horizontal layout that shows the description.
  The tile's art panel uses the category tint with a big emoji; a black badge shows Bestseller/New/Deal;
  a small stripe carries the category colour.
- **Dialogs** use native `<dialog>` (focus trap, Esc to close, backdrop click closes): product detail,
  a right-side cart drawer, and checkout.
- **Checkout**: fieldsets "1 · Shipping" and "2 · Payment [TEST MODE]"; payment methods as
  radio-cards (the selected one turns yellow with a shadow); errors in a pink bordered box that clears
  on edit; the success screen links to the dashboard.
- **Feedback**: an ink toast with a pink shadow for "Added …"; the live-events tile updates with the
  last event name.
- **Stock states** (never colour alone): at or below 5 units a tile shows red **"Only N left"**; a
  sold-out tile fades its art to grey, gets a rotated ink **SOLD OUT** stamp, and its button becomes a
  disabled "Sold out". The product dialog shows "In stock" / "Only N left" / "Sold out" above the
  stepper, and quantities clamp to the stock ("Only 3 left: added 3"). The cart warns per line, and a
  refused checkout shows the server's 409 message in the pink error box.
- **Copy tone**: short, friendly and a little cheeky ("Pay when it (doesn't) arrive. It's a demo.").

## 5. Catalog admin (`/admin.html`)
- The same topbar as the store, plus an ink **CATALOG** pill, a search box, "← Store" and a pink
  **"+ Add product"** button. The store's topbar links here with a **🗂️ Catalog** button.
- **Stats bento:** products (yellow; "N seed · N added by you"), average price with range (mint), stock
  (peach; units on hand, "N sold out · N low") and newest product (periwinkle), above the same 7
  category buttons as the store (filters).
- **Product table** inside one tile (horizontal scroll inside the tile on narrow screens, never on the
  page): emoji thumb on the category tint, mono id, name with a one-line description, category chip
  with the category colour as a thick bottom border, price, stock (a red **SOLD OUT** chip, an amber
  **"N · low"** chip, or the number), badge, rating ("No reviews yet" for new), Source ("Seed" or a
  yellow "Added" chip), and actions (**View** deep-links to `/?q=name`; **📦** on every row; 🗑️ only
  on added products). A newly added row flashes yellow. Added products also get a **⭐** action that
  opens a small reviews dialog (manual values, 🎲 Random, No reviews, with a live star preview).
- **Stock dialog (📦):** the product, a big **units on hand** number, a "Restock" fieldset (add units,
  with +10 / +50 / +100 shortcuts) and "Reorder settings" (lead time and target cover in days, with a
  one-line explanation). One Save applies both.
- **Add dialog:** two columns (they stack below 1000 px). The form has fieldsets "1 · Details" (name,
  category, price, badge, description with a live counter) and "2 · Photo" (an emoji grid as a
  radiogroup; the selected emoji turns yellow with a shadow; the choices follow the category),
  "3 · Inventory" (starting stock 50; the lead-time placeholder shows the category default; target
  cover 7) and "4 · Reviews" (optional rating and count, **🎲 Random reviews** for realistic values, **No reviews**). The
  **live preview** is a real store tile, so what you see is what the store shows. Errors appear in the
  pink box and clear as soon as you edit.

## 6. Accessibility checklist
- Every input has a `<label>`. Icon buttons have `aria-label`s. Product tiles are `role="button"`
  with `tabindex="0"` and Enter/Space handling.
- Visible focus: a 3 px pink outline.
- Toasts and results use `aria-live="polite"`.
- Colour is never the only carrier of meaning: legends, labels, arrows, text.
- The layout has no horizontal scroll at 375, 768 or 1440 px (tested).

## 7. Screenshots
`docker compose run --rm demo` (or `scripts/demo.py` locally) captures 18 images into `docs/screenshots/`:
store and admin at 1440×900 plus mobile at 390 px @2x; dashboard at 1440×1500 in light, dark,
inventory, internals, SQL (the schema-evolution example) and the device/registry row
(`dashboard-devices`). Review every image before committing,
because screenshots on real data have exposed real bugs (see Memory.md).
