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
- **SQL disclosure**: `show_sql()` renders an expander "🔍 SQL · N ms · N rows" using `code()`.
- **Dark mode**: sidebar toggle; the canvas data grid is inverted with `filter: invert(1) hue-rotate(180deg)`.

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
- **Copy tone**: short, friendly and a little cheeky ("Pay when it (doesn't) arrive. It's a demo.").

## 5. Accessibility checklist
- Every input has a `<label>`. Icon buttons have `aria-label`s. Product tiles are `role="button"`
  with `tabindex="0"` and Enter/Space handling.
- Visible focus: a 3 px pink outline.
- Toasts and results use `aria-live="polite"`.
- Colour is never the only carrier of meaning: legends, labels, arrows, text.
- The layout has no horizontal scroll at 375, 768 or 1440 px (tested).

## 6. Screenshots
`scripts/demo.py` captures 13 images into `docs/screenshots/` (store at 1440×900 plus mobile at 390 px
@2x; dashboard at 1440×1500 in light, dark, internals and SQL). Review every image before committing,
because screenshots on real data have exposed real bugs (see Memory.md).
